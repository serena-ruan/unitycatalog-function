import datetime
import base64
import decimal
import json
from dataclasses import dataclass
from hashlib import md5
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Type, Union

# TODO: validate this against pydantic v1 and v2
from pydantic import Field, create_model, BaseModel

SQL_TYPE_TO_PYTHON_TYPE_MAPPING = {
    # numpy array is not accepted, it's not json serializable
    "ARRAY": (list, tuple),
    "BINARY": (bytes, str),
    "BOOLEAN": bool,
    # tinyint type
    "BYTE": int,
    "CHAR": str,
    "DATE": (datetime.date, str),
    # no precision and scale check, rely on SQL function to validate
    "DECIMAL": (decimal.Decimal, float),
    "DOUBLE": float,
    "FLOAT": float,
    "INT": int,
    "INTERVAL": (datetime.timedelta, str),
    "LONG": int,
    "MAP": dict,
    # ref: https://docs.databricks.com/en/error-messages/datatype-mismatch-error-class.html#null_type
    # it's not supported in return data type as well `[UNSUPPORTED_DATATYPE] Unsupported data type "NULL". SQLSTATE: 0A000`
    "NULL": type(None),
    "SHORT": int,
    "STRING": str,
    "STRUCT": dict,
    # not allowed for python udf, users should only pass string
    "TABLE_TYPE": str,
    "TIMESTAMP": (datetime.datetime, str),
    "TIMESTAMP_NTZ": (datetime.datetime, str),
    # it's a type that can be defined in scala, python shouldn't force check the type here
    # ref: https://www.waitingforcode.com/apache-spark-sql/used-defined-type/read
    "USER_DEFINED_TYPE": object,
}

UC_TYPE_JSON_MAPPING = {
    **SQL_TYPE_TO_PYTHON_TYPE_MAPPING,
    "INTEGER": int,
    # The binary field should be a string expression in base64 format
    "BINARY": bytes,
    "INTERVAL DAY TO SECOND": (datetime.timedelta, str),
}


def column_type_to_python_type(column_type: str) -> Any:
    if column_type not in SQL_TYPE_TO_PYTHON_TYPE_MAPPING:
        raise ValueError(f"Unsupported column type: {column_type}")
    return SQL_TYPE_TO_PYTHON_TYPE_MAPPING[column_type]


def is_time_type(column_type: str) -> bool:
    return column_type in (
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
    )


def validate_param(param: Any, column_type: str, param_type_text: str) -> None:
    """
    Validate the parameter against the parameter info.

    Args:
        param (Any): The parameter to validate.
        column_type (str): The column type name.
        param_type_text (str): The parameter type text.
    """
    if is_time_type(column_type) and isinstance(param, str):
        try:
            datetime.datetime.fromisoformat(param)
        except ValueError as e:
            raise ValueError(f"Invalid datetime string: {param}, expecting ISO format.") from e
    elif column_type == "INTERVAL":
        # only day-time interval is supported, no year-month interval
        if isinstance(param, datetime.timedelta) and param_type_text != "interval day to second":
            raise ValueError(
                f"Invalid interval type text: {param_type_text}, expecting 'interval day to second', "
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
    elif column_type == "BINARY" and isinstance(param, str) and not is_base64_encoded(param):
        # the string value for BINARY column must be base64 encoded
        raise ValueError(
            f"The string input for column type BINARY must be base64 encoded, invalid input: {param}."
        )


def is_base64_encoded(s: str) -> bool:
    try:
        base64.b64decode(s, validate=True)
        return True
    except (base64.binascii.Error, ValueError):
        return False


def convert_timedelta_to_interval_str(time_val: datetime.timedelta) -> str:
    """
    Convert a timedelta object to a string representing an interval in the format of 'INTERVAL "d hh:mm:ss.ssssss"'.
    """
    days = time_val.days
    hours, remainder = divmod(time_val.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = time_val.microseconds
    return f"INTERVAL '{days} {hours}:{minutes}:{seconds}.{microseconds}' DAY TO SECOND"


class FullFunctionName(NamedTuple):
    catalog_name: str
    schema_name: str
    function_name: str


def validate_full_function_name(function_name: str) -> FullFunctionName:
    """
    Validate the full function name follows the format <catalog_name>.<schema_name>.<function_name>.

    Args:
        function_name (str): The full function name.

    Returns:
        FullFunctionName: The parsed full function name.
    """
    splits = function_name.split(".")
    if len(splits) != 3:
        raise ValueError(
            f"Invalid function name: {function_name}, expecting format <catalog_name>.<schema_name>.<function_name>."
        )
    return FullFunctionName(catalog_name=splits[0], schema_name=splits[1], function_name=splits[2])


def uc_type_json_to_pydantic_type(uc_type_json: Union[str, Dict]) -> Type:
    """
    Convert Unity Catalog type json to Pydantic type.
    """
    if isinstance(uc_type_json, str):
        type_name = uc_type_json.upper()
        if type_name in UC_TYPE_JSON_MAPPING:
            return Union[UC_TYPE_JSON_MAPPING[type_name]]
        # the type text contains the precision and scale
        elif type_name.startswith("DECIMAL"):
            return Union[decimal.Decimal, float]
        else:
            raise TypeError(
                f"Type {uc_type_json} is not supported. Supported "
                f"types are: {UC_TYPE_JSON_MAPPING.keys()}"
            )
    if isinstance(uc_type_json, dict):
        tpe = uc_type_json["type"]
        if tpe == "array":
            element_type = uc_type_json_to_pydantic_type(uc_type_json["elementType"])
            if uc_type_json["containsNull"]:
                element_type = Optional[element_type]
            return Union[List[element_type], Tuple[element_type]]
        elif tpe == "map":
            key_type = uc_type_json["keyType"]
            if key_type != "string":
                raise TypeError(f"Only support STRING key type for MAP but got {key_type}.")
            value_type = uc_type_json_to_pydantic_type(uc_type_json["valueType"])
            if uc_type_json["valueContainsNull"]:
                value_type = Optional[value_type]
            return Dict[str, value_type]
        elif tpe == "struct":
            fields = {}
            for field in uc_type_json["fields"]:
                field_type = uc_type_json_to_pydantic_type(field["type"])
                comment = field.get("metadata", {}).get("comment")
                if field.get("nullable"):
                    field_type = Optional[field_type]
                    fields[field["name"]] = (field_type, Field(default=None, description=comment))
                else:
                    fields[field["name"]] = (field_type, Field(..., description=comment))
            uc_type_json_str = json.dumps(uc_type_json, sort_keys=True)
            type_hash = md5(uc_type_json_str.encode()).hexdigest()[:8]
            return create_model(f"Struct_{type_hash}", **fields)
    raise TypeError(f"Unknown type {uc_type_json}.")


# TODO: add UC OSS support
def supported_param_info_types():
    types = ()
    try:
        from databricks.sdk.service.catalog import FunctionParameterInfo

        types += (FunctionParameterInfo,)
    except ImportError:
        pass

    return types


# TODO: add UC OSS support
def supported_function_info_types():
    types = ()
    try:
        from databricks.sdk.service.catalog import FunctionInfo

        types += (FunctionInfo,)
    except ImportError:
        pass

    return types


@dataclass
class PydanticField:
    pydantic_type: Type
    description: Optional[str] = None
    default: Optional[Any] = None


def param_info_to_pydantic_type(param_info: Any) -> PydanticField:
    """
    Convert Unity Catalog function parameter information to Pydantic type.

    Args:
        param_info: The Unity Catalog function parameter information.
            It must be either databricks.sdk.service.catalog.FunctionParameterInfo or
            unitycatalog.types.function_info.InputParamsParameter object.
    """
    if not isinstance(param_info, supported_param_info_types()):
        raise TypeError(f"Unsupported parameter info type: {type(param_info)}")
    if param_info.type_json is None:
        raise ValueError(f"Parameter type json is None for parameter {param_info.name}.")
    type_json = json.loads(param_info.type_json)
    nullable = type_json.get("nullable")
    pydantic_type = uc_type_json_to_pydantic_type(type_json["type"])
    if nullable:
        pydantic_type = Optional[pydantic_type]
    default = None
    description = param_info.comment or ""
    if param_info.parameter_default:
        pydantic_type = Optional[pydantic_type]
        # Note: DEFAULT is supported for LANGUAGE SQL only.
        # TODO: verify this for all types
        default = json.loads(param_info.parameter_default)
        description = f"{description} (Default: {param_info.parameter_default})"
    return PydanticField(
        pydantic_type=pydantic_type,
        description=description,
        default=default,
    )


def generate_function_input_params_schema(function_info: Any) -> Type[BaseModel]:
    """
    Generate a Pydantic model based on a Unity Catalog function information.

    Args:
        function_info: The Unity Catalog function information.
            It must either be databricks.sdk.service.catalog.FunctionInfo or
            unitycatalog.types.function_info.FunctionInfo object.

    Returns:
        The Pydantic model representing the input parameters of the function.
    """
    if not isinstance(function_info, supported_function_info_types()):
        raise TypeError(f"Unsupported function info type: {type(function_info)}")
    if function_info.input_params is None:
        return BaseModel
    param_infos = function_info.input_params.parameters
    if param_infos is None:
        raise ValueError("Function input parameters are None.")
    fields = {}
    for param_info in param_infos:
        pydantic_field = param_info_to_pydantic_type(param_info)
        fields[param_info.name] = (
            pydantic_field.pydantic_type,
            Field(default=pydantic_field.default, description=pydantic_field.description),
        )
    return create_model(
        f"{function_info.catalog_name}__{function_info.schema_name}__{function_info.name}__params",
        **fields,
    )
