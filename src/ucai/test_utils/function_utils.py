import logging
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Generator, NamedTuple, Optional

from ucai.core.databricks import DatabricksFunctionClient
from ucai.core.utils.function_processing_utils import get_tool_name

CATALOG = "ml"
SCHEMA = "serena_uc_test"

_logger = logging.getLogger(__name__)


def random_func_name():
    """
    Generate a random function name in the format of `<catalog>.<schema>.<function_name>`.
    """
    return f"{CATALOG}.{SCHEMA}.test_{uuid.uuid4().hex[:4]}"

def named_func_name(func: Callable[..., Any]) -> str:
    """
    Generate a named function name in the format of `<catalog>.<schema>.<function_name>`.
    This utility is used for the python function callable API wherein the name of the
    function that is created within Unity Catalog is based on the input callable's name.
    """
    return f"{CATALOG}.{SCHEMA}.test_{func.__name__}"

@contextmanager
def generate_func_name_and_cleanup(client: DatabricksFunctionClient):
    func_name = random_func_name()
    try:
        yield func_name
    finally:
        try:
            client.client.functions.delete(func_name)
        except Exception as e:
            _logger.warning(f"Fail to delete function: {e}")


class FunctionObj(NamedTuple):
    full_function_name: str
    comment: str
    tool_name: str


@contextmanager
def create_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    func_name: Optional[str] = None,
    sql_body: Optional[str] = None,
) -> Generator[FunctionObj, None, None]:
    func_name = func_name or random_func_name()
    comment = "Executes Python code and returns its stdout."
    sql_body = (
        sql_body
        or f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT '{comment}'
AS $$
    import sys
    from io import StringIO
    stdout = StringIO()
    sys.stdout = stdout
    exec(code)
    return stdout.getvalue()
$$
"""
    )
    try:
        client.create_function(sql_function_body=sql_body)
        yield FunctionObj(
            full_function_name=func_name, comment=comment, tool_name=get_tool_name(func_name)
        )
    finally:
        try:
            client.client.functions.delete(func_name)
        except Exception as e:
            _logger.warning(f"Fail to delete function: {e}")

@contextmanager
def create_python_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    func: Callable[..., Any] = None,
) -> Generator[FunctionObj, None, None]:
    func_name = named_func_name(func)
    try:
        func_info = client.create_python_function(func=func, catalog=CATALOG, schema=SCHEMA)
        yield FunctionObj(
            full_function_name=func_name,
            comment=func_info.comment,
            tool_name=get_tool_name(func_name),
        )
    finally:
        try:
            client.client.functions.delete(func_name)
        except Exception as e:
            _logger.warning(f"Fail to delete function: {e}"
)