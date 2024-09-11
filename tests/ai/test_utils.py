import pytest
import datetime
from unitycatalog.ai.utils import (
    column_type_to_python_type,
    convert_timedelta_to_interval_str,
    validate_full_function_name,
)


def test_full_function_name():
    result = validate_full_function_name("catalog.schema.function")
    assert result.catalog_name == "catalog"
    assert result.schema_name == "schema"
    assert result.function_name == "function"

    with pytest.raises(ValueError, match=r"Invalid function name"):
        validate_full_function_name("catalog.schema.function.extra")


def test_column_type_to_python_type_errors():
    with pytest.raises(ValueError, match=r"Unsupported column type"):
        column_type_to_python_type("INVALID_TYPE")


@pytest.mark.parametrize(
    ("time_val", "expected"),
    [
        (
            datetime.timedelta(days=1, seconds=1, microseconds=123456),
            "INTERVAL '1 0:0:1.123456' DAY TO SECOND",
        ),
        (datetime.timedelta(hours=100), "INTERVAL '4 4:0:0.0' DAY TO SECOND"),
        (datetime.timedelta(days=1, hours=10, minutes=3), "INTERVAL '1 10:3:0.0' DAY TO SECOND"),
    ],
)
def test_convert_timedelta_to_interval_str(time_val, expected):
    assert convert_timedelta_to_interval_str(time_val) == expected
