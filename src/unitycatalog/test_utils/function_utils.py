import logging
import uuid
from contextlib import contextmanager
from typing import NamedTuple, Optional

from unitycatalog.ai.databricks import DatabricksFunctionClient

CATALOG = "ml"
SCHEMA = "serena_uc_test"

_logger = logging.getLogger(__name__)


def random_func_name():
    """
    Generate a random function name in the format of `<catalog>.<schema>.<function_name>`.
    """
    return f"{CATALOG}.{SCHEMA}.test_{uuid.uuid4().hex[:4]}"


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


@contextmanager
def create_function_and_cleanup(
    client: DatabricksFunctionClient,
    *,
    func_name: Optional[str] = None,
    sql_body: Optional[str] = None,
    return_func_name: bool = False,
):
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
        if return_func_name:
            yield func_name
        else:
            yield FunctionObj(full_function_name=func_name, comment=comment)
    finally:
        try:
            client.client.functions.delete(func_name)
        except Exception as e:
            _logger.warning(f"Fail to delete function: {e}")
