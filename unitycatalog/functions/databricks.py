import datetime
import decimal
import inspect
import json
import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from typing_extensions import override

from unitycatalog.functions.client import BaseFunctionClient, FunctionExecutionResult

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        ColumnTypeName,
        CreateFunction,
        FunctionInfo,
        FunctionParameterInfo,
    )
    from databricks.sdk.service.sql import StatementParameterListItem

EXECUTE_FUNCTION_ARG_NAME = "__execution_args__"
DEFAULT_EXECUTE_FUNCTION_ARGS = {
    "wait_timeout": "30s",
    "row_limit": 100,
    "byte_limit": 4096,
}

_logger = logging.getLogger(__name__)


def get_default_databricks_workspace_client() -> "WorkspaceClient":
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise ImportError(
            "Could not import databricks-sdk python package. "
            "If you want to use databricks backend then "
            "please install it with `pip install databricks-sdk`."
        ) from e
    return WorkspaceClient()


def column_type_to_python_type(column_type: Enum) -> Any:
    from databricks.sdk.service.catalog import ColumnTypeName

    mapping = {
        # numpy array is not accepted, it's not json serializable
        ColumnTypeName.ARRAY: (list, tuple),
        # a string expression in base64 format
        ColumnTypeName.BINARY: str,
        ColumnTypeName.BOOLEAN: bool,
        # tinyint type
        ColumnTypeName.BYTE: int,
        ColumnTypeName.CHAR: str,
        ColumnTypeName.DATE: (datetime.date, str),
        # no precision and scale check, rely on SQL function to validate
        ColumnTypeName.DECIMAL: (decimal.Decimal, float),
        ColumnTypeName.DOUBLE: float,
        ColumnTypeName.FLOAT: float,
        ColumnTypeName.INT: int,
        ColumnTypeName.INTERVAL: (datetime.timedelta, str),
        ColumnTypeName.LONG: int,
        ColumnTypeName.MAP: dict,
        # ref: https://docs.databricks.com/en/error-messages/datatype-mismatch-error-class.html#null_type
        # it's not supported in return data type as well `[UNSUPPORTED_DATATYPE] Unsupported data type "NULL". SQLSTATE: 0A000`
        ColumnTypeName.NULL: type(None),
        ColumnTypeName.SHORT: int,
        ColumnTypeName.STRING: str,
        ColumnTypeName.STRUCT: dict,
        # not allowed for python udf, users should only pass string
        ColumnTypeName.TABLE_TYPE: str,
        ColumnTypeName.TIMESTAMP: (datetime.datetime, str),
        ColumnTypeName.TIMESTAMP_NTZ: (datetime.datetime, str),
        # it's a type that can be defined in scala, python shouldn't force check the type here
        # ref: https://www.waitingforcode.com/apache-spark-sql/used-defined-type/read
        ColumnTypeName.USER_DEFINED_TYPE: object,
    }
    if column_type not in mapping:
        raise ValueError(f"Unsupported column type: {column_type}")
    return mapping[column_type]


def validate_param(param: Any, param_info: "FunctionParameterInfo"):
    from databricks.sdk.service.catalog import ColumnTypeName

    column_type = param_info.type_name
    if is_time_type(column_type) and isinstance(param, str):
        try:
            datetime.datetime.fromisoformat(param)
        except ValueError as e:
            raise ValueError(f"Invalid datetime string: {param}, expecting ISO format.") from e
    elif column_type == ColumnTypeName.INTERVAL:
        # only day-time interval is supported, no year-month interval
        if (
            isinstance(param, datetime.timedelta)
            and param_info.type_text != "interval day to second"
        ):
            raise ValueError(
                f"Invalid interval type text: {param_info.type_text}, expecting 'interval day to second', "
                "python timedelta can only be used for day-time interval."
            )
        # Only DAY TO SECOND is supported in python udf
        # rely on the SQL function for checking the interval format
        elif isinstance(param, str) and not (
            param.startswith("INTERVAL") and param.endswith("DAY TO SECOND")
        ):
            raise ValueError(
                f"Invalid interval string: {param}, expecting format `INTERVAL '[+|-] d[...] [h]h:[m]m:[s]s.ms[ms][ms][us][us][us]' DAY TO SECOND`."
            )


def extract_function_name(sql_body: str) -> str:
    """
    Extract function name from the sql body.
    CREATE FUNCTION syntax reference: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax
    """
    pattern = re.compile(
        r"""
        CREATE\s+(?:OR\s+REPLACE\s+)?      # Match 'CREATE OR REPLACE' or just 'CREATE'
        (?:TEMPORARY\s+)?                  # Match optional 'TEMPORARY'
        FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?  # Match 'FUNCTION' and optional 'IF NOT EXISTS'
        ([\w.]+)                           # Capture the function name (including schema if present)
        \s*\(                              # Match opening parenthesis after function name
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    match = pattern.search(sql_body)
    if match:
        return match.group(1)
    raise ValueError(
        "Could not extract function name from the sql body. Please "
        "make sure the sql body follows the syntax of CREATE FUNCTION "
        "statement in Databricks: "
        "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax."
    )


class DatabricksFunctionClient(BaseFunctionClient):
    """
    Databricks UC function calling client
    """

    def __init__(self, client: Optional["WorkspaceClient"] = None, **kwargs: Any) -> None:
        if client is None:
            client = get_default_databricks_workspace_client()
        self.client = client
        self.warehouse_id = kwargs.get("warehouse_id")
        self.cluster_id = kwargs.get("cluster_id")
        self.profile = kwargs.get("profile")
        self.spark = None
        super().__init__()

    def set_default_spark_session(self):
        if self.spark is None or self.spark.is_stopped:
            try:
                from databricks.connect.session import DatabricksSession as SparkSession

                if self.profile:
                    builder = SparkSession.builder.profile(self.profile)
                else:
                    builder = SparkSession.builder

                if hasattr(SparkSession.Builder, "serverless"):
                    self.spark = builder.serverless(True).getOrCreate()
                else:
                    _logger.warning(
                        "Serverless is not supported by the current databricks-connect "
                        "version, please upgrade to a newer version for connecting "
                        "to serverless compute in Databricks with `pip install databricks-connect -U`."
                    )
                    # serverless is not supported in older versions of databricks-connect
                    # a cluster id must be provided instead
                    if self.cluster_id:
                        # TODO: validate this
                        self.spark = builder.remote(cluster_id=self.cluster_id).getOrCreate()
                    else:
                        raise ValueError(
                            "cluster_id is required for connecting to a spark session in Databricks. "
                            "Please provide it when constructing the DatabricksFunctionClient."
                        )
            except ImportError as e:
                raise ImportError(
                    "Could not import databricks-connect python package. "
                    "If you want to use databricks with dbconnect to create UC functions "
                    "then please install it with `pip install databricks-connect`."
                ) from e

    def stop_spark_session(self):
        if self.spark is not None and not self.spark.is_stopped:
            self.spark.stop()

    @override
    def create_function(
        self,
        sql_function_body: Optional[str] = None,
        function_info: Optional["CreateFunction"] = None,
    ) -> "FunctionInfo":
        """
        Create a UC function with the given sql body or function info.

        Args:
            sql_function_body (str, optional): The sql body of the function. Defaults to None.
                It should follow the syntax of CREATE FUNCTION statement in Databricks.
                Ref: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax

                .. Note:: This is supported using databricks connect; if databricks-connect version is 15.1.0 or above,
                    by default it uses serverless compute; otherwise please provide a cluster_id when constructing the client
                    and the client will use remote cluster to create the function.
            function_info (CreateFunction, optional): The function info. Defaults to None.

        Returns:
            FunctionInfo: The created function info.
        """
        if sql_function_body and function_info:
            raise ValueError("Only one of sql_function_body and function_info should be provided.")
        if sql_function_body:
            _logger.info("Using databricks connect to create function.")
            self.set_default_spark_session()
            try:
                self.spark.sql(sql_function_body)  # type: ignore
            except Exception as e:
                raise ValueError(
                    f"Failed to create function with sql body: {sql_function_body}"
                ) from e
            return self.retrieve_function(extract_function_name(sql_function_body))  # type: ignore
        if function_info:
            return self.client.functions.create(function_info)
        raise ValueError("Either function_info or sql_function_body should be provided.")

    @override
    def retrieve_function(
        self, function_name: str, **kwargs: Any
    ) -> Union["FunctionInfo", List["FunctionInfo"]]:
        splits = function_name.split(".")
        if len(splits) != 3:
            raise ValueError(
                f"Invalid function name: {function_name}, expecting format <catalog_name>.<schema_name>.<function_name>."
            )
        if splits[-1] == "*":
            functions = self.client.functions.list(catalog_name=splits[0], schema_name=splits[1])
            return list(functions)
        return self.client.functions.get(function_name, include_browse=kwargs.get("include_browse"))

    @override
    def _validate_param_type(self, value: Any, param_info: "FunctionParameterInfo") -> None:
        value_python_type = column_type_to_python_type(param_info.type_name)
        if not isinstance(value, value_python_type):
            raise ValueError(
                f"Parameter {param_info.name} should be of type {param_info.type_name.value} "
                f"(corresponding python type {value_python_type}), but got {type(value)}"
            )
        validate_param(value, param_info)

    @override
    def execute_function(
        self, function_name: str, parameters: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> FunctionExecutionResult:
        """
        Execute a UC function by name with the given parameters.

        Args:
            function_name (str): The name of the function to execute.
            parameters (Dict[str, Any], optional): The parameters to pass to the function. Defaults to None.
            kwargs: additional key-value pairs to include when executing the function.
                Allowed keys for retreiiving functions are:
                - include_browse: bool (default to False)
                    Whether to include functions in the response for which the principal can only access selective
                    metadata for.
                Allowed keys for executing functions are:
                - wait_timeout: str (default to `30s`)
                    The time in seconds the call will wait for the statement's result set as `Ns`, where `N` can be set
                    to 0 or to a value between 5 and 50.

                    When set to `0s`, the statement will execute in asynchronous mode and the call will not wait for the
                    execution to finish. In this case, the call returns directly with `PENDING` state and a statement ID
                    which can be used for polling with :method:statementexecution/getStatement.

                    When set between 5 and 50 seconds, the call will behave synchronously up to this timeout and wait
                    for the statement execution to finish. If the execution finishes within this time, the call returns
                    immediately with a manifest and result data (or a `FAILED` state in case of an execution error). If
                    the statement takes longer to execute, `on_wait_timeout` determines what should happen after the
                    timeout is reached.
                - row_limit: int (default to 100)
                    Applies the given row limit to the statement's result set, but unlike the `LIMIT` clause in SQL, it
                    also sets the `truncated` field in the response to indicate whether the result was trimmed due to
                    the limit or not.
                - byte_limit: int (default to 4096)
                    Applies the given byte limit to the statement's result size. Byte counts are based on internal data
                    representations and might not match the final size in the requested `format`. If the result was
                    truncated due to the byte limit, then `truncated` in the response is set to `true`. When using
                    `EXTERNAL_LINKS` disposition, a default `byte_limit` of 100 GiB is applied if `byte_limit` is not
                    explcitly set.

        Returns:
            FunctionExecutionResult: The result of executing the function.
        """
        return super().execute_function(function_name, parameters, **kwargs)

    @override
    def _execute_uc_function(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any], **kwargs: Any
    ) -> Any:
        from databricks.sdk.service.sql import StatementState

        if (
            function_info.input_params
            and function_info.input_params.parameters
            and any(
                p.name == EXECUTE_FUNCTION_ARG_NAME for p in function_info.input_params.parameters
            )
        ):
            raise ValueError(
                "Parameter name conflicts with the reserved argument name for executing "
                f"functions: {EXECUTE_FUNCTION_ARG_NAME}. "
                f"Please rename the parameter {EXECUTE_FUNCTION_ARG_NAME}."
            )

        # avoid modifying the original dict
        execute_statement_args = {**DEFAULT_EXECUTE_FUNCTION_ARGS}
        allowed_execute_statement_args = inspect.signature(
            self.client.statement_execution.execute_statement
        ).parameters
        if not any(
            p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
            for p in allowed_execute_statement_args.values()
        ):
            invalid_params: Set[str] = set()
            passed_execute_statement_args = parameters.pop(EXECUTE_FUNCTION_ARG_NAME, {})
            for k, v in passed_execute_statement_args.items():
                if k in allowed_execute_statement_args:
                    execute_statement_args[k] = v
                else:
                    invalid_params.add(k)
            if invalid_params:
                raise ValueError(
                    f"Invalid parameters for executing functions: {invalid_params}. "
                    f"Allowed parameters are: {allowed_execute_statement_args.keys()}."
                )

        if self.warehouse_id:
            parametrized_statement = get_execute_function_sql_stmt(function_info, parameters)
            response = self.client.statement_execution.execute_statement(
                statement=parametrized_statement.statement,
                warehouse_id=self.warehouse_id,
                parameters=parametrized_statement.parameters,
                **execute_statement_args,  # type: ignore
            )
            # TODO: the first time the warehouse is invoked, it might take longer than
            # expected, so it's still pending even after 6 times of retry;
            # we should see if we can check the warehouse status before invocation, and
            # increase the wait time if needed
            if (
                response.status
                and response.status.state == StatementState.PENDING
                and response.statement_id
            ):
                statement_id = response.statement_id
                _logger.info("Retrying to get statement execution status...")
                retry_cnt = 1
                while retry_cnt < 7:
                    time.sleep(2**retry_cnt)
                    _logger.info(f"Retry times: {retry_cnt}")
                    response = self.client.statement_execution.get_statement(statement_id)
                    if response.status is None or response.status.state != StatementState.PENDING:
                        break
                    retry_cnt += 1
                if response.status and response.status.state == StatementState.PENDING:
                    return FunctionExecutionResult(
                        error=f"Statement execution is still pending after {retry_cnt-1} times. "
                        "Please try increase the wait_timeout argument for executing the function."
                    )
            if response.status is None:
                return FunctionExecutionResult(error=f"Statement execution failed: {response}")
            if response.status.state != StatementState.SUCCEEDED:
                error = response.status.error
                if error is None:
                    return FunctionExecutionResult(
                        error=f"Statement execution failed but no error message was provided: {response}"
                    )
                return FunctionExecutionResult(error=f"{error.error_code}: {error.message}")

            manifest = response.manifest
            if manifest is None:
                return FunctionExecutionResult(
                    error="Statement execution succeeded but no manifest was returned."
                )
            truncated = manifest.truncated
            if response.result is None:
                return FunctionExecutionResult(
                    error="Statement execution succeeded but no result was provided."
                )
            data_array = response.result.data_array
            if is_scalar(function_info):
                value = None
                if data_array and len(data_array) > 0 and len(data_array[0]) > 0:
                    # value is always string type
                    value = data_array[0][0]
                return FunctionExecutionResult(format="SCALAR", value=value, truncated=truncated)
            else:
                try:
                    import pandas as pd
                except ImportError as e:
                    raise ImportError(
                        "Could not import pandas python package. Please install it with `pip install pandas`."
                    ) from e

                schema = manifest.schema
                if schema is None or schema.columns is None:
                    return FunctionExecutionResult(
                        error="Statement execution succeeded but no schema was provided for table function."
                    )
                columns = [c.name for c in schema.columns]
                if data_array is None:
                    data_array = []
                pdf = pd.DataFrame(data_array, columns=columns)
                csv_buffer = StringIO()
                pdf.to_csv(csv_buffer, index=False)
                return FunctionExecutionResult(
                    format="CSV", value=csv_buffer.getvalue(), truncated=truncated
                )

        # TODO: support serverless
        raise ValueError("warehouse_id is required for executing UC functions in Databricks")


def is_scalar(function: "FunctionInfo") -> bool:
    from databricks.sdk.service.catalog import ColumnTypeName

    return function.data_type != ColumnTypeName.TABLE_TYPE


@dataclass
class ParameterizedStatement:
    statement: str
    parameters: List["StatementParameterListItem"]


def get_execute_function_sql_stmt(
    function: "FunctionInfo", parameters: Dict[str, Any]
) -> ParameterizedStatement:
    from databricks.sdk.service.catalog import ColumnTypeName
    from databricks.sdk.service.sql import StatementParameterListItem

    parts: List[str] = []
    output_params: List[StatementParameterListItem] = []
    if is_scalar(function):
        parts.append("SELECT IDENTIFIER(:function_name)(")
        output_params.append(
            StatementParameterListItem(name="function_name", value=function.full_name)
        )
    else:
        # TODO: IDENTIFIER doesn't work
        parts.append(f"SELECT * FROM {function.full_name}(")

    if parameters and function.input_params and function.input_params.parameters:
        args: List[str] = []
        use_named_args = False
        for param_info in function.input_params.parameters:
            if param_info.name not in parameters:
                # validate_input_params has validated param_info.parameter_default exists
                use_named_args = True
            else:
                arg_clause = ""
                if use_named_args:
                    arg_clause += f"{param_info.name} => "
                param_value = parameters[param_info.name]
                if param_info.type_name in (
                    ColumnTypeName.ARRAY,
                    ColumnTypeName.MAP,
                    ColumnTypeName.STRUCT,
                ):
                    # Use from_json to restore values of complex types.
                    json_value_str = json.dumps(param_value)
                    arg_clause += f"from_json(:{param_info.name}, :{param_info.name}_type)"
                    output_params.append(
                        StatementParameterListItem(name=param_info.name, value=json_value_str)
                    )
                    output_params.append(
                        StatementParameterListItem(
                            name=f"{param_info.name}_type", value=param_info.type_text
                        )
                    )
                elif param_info.type_name == ColumnTypeName.BINARY:
                    # Use ubbase64 to restore binary values.
                    arg_clause += f"unbase64(:{param_info.name})"
                    output_params.append(
                        StatementParameterListItem(name=param_info.name, value=param_value)
                    )
                elif is_time_type(param_info.type_name):
                    date_str = (
                        param_value if isinstance(param_value, str) else param_value.isoformat()
                    )
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name, value=date_str, type=param_info.type_text
                        )
                    )
                elif param_info.type_name == ColumnTypeName.INTERVAL:
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name,
                            value=convert_timedelta_to_interval_str(param_value)
                            if not isinstance(param_value, str)
                            else param_value,
                            type=param_info.type_text,
                        )
                    )
                else:
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name, value=param_value, type=param_info.type_text
                        )
                    )
                args.append(arg_clause)
        parts.append(",".join(args))
    parts.append(")")
    statement = "".join(parts)
    return ParameterizedStatement(statement=statement, parameters=output_params)


def is_time_type(column_type: "ColumnTypeName") -> bool:
    from databricks.sdk.service.catalog import ColumnTypeName

    return column_type in (
        ColumnTypeName.DATE,
        ColumnTypeName.TIMESTAMP,
        ColumnTypeName.TIMESTAMP_NTZ,
    )


def convert_timedelta_to_interval_str(time_val: datetime.timedelta) -> str:
    """
    Convert a timedelta object to a string representing an interval in the format of 'INTERVAL "d hh:mm:ss.ssssss"'.
    """
    days = time_val.days
    hours, remainder = divmod(time_val.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = time_val.microseconds
    return f"INTERVAL '{days} {hours}:{minutes}:{seconds}.{microseconds}' DAY TO SECOND"
