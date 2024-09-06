from typing import Any, List
import datetime
import decimal


def column_type_to_python_type(column_type: str) -> Any:
    mapping = {
        # numpy array is not accepted, it's not json serializable
        "ARRAY": (list, tuple),
        # a string expression in base64 format
        "BINARY": str,
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
    if column_type not in mapping:
        raise ValueError(f"Unsupported column type: {column_type}")
    return mapping[column_type]


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


def convert_timedelta_to_interval_str(time_val: datetime.timedelta) -> str:
    """
    Convert a timedelta object to a string representing an interval in the format of 'INTERVAL "d hh:mm:ss.ssssss"'.
    """
    days = time_val.days
    hours, remainder = divmod(time_val.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = time_val.microseconds
    return f"INTERVAL '{days} {hours}:{minutes}:{seconds}.{microseconds}' DAY TO SECOND"


def validate_full_function_name(function_name: str) -> List[str]:
    """
    Validate the full function name follows the format <catalog_name>.<schema_name>.<function_name>.

    Args:
        function_name (str): The full function name.

    Returns:
        List[str]: The splits of the full function name.
    """
    splits = function_name.split(".")
    if len(splits) != 3:
        raise ValueError(
            f"Invalid function name: {function_name}, expecting format <catalog_name>.<schema_name>.<function_name>."
        )
    return splits
