import pytest
from unitycatalog.ai.client import (
    set_uc_function_client,
    get_uc_function_client,
    FunctionExecutionResult,
)
from unitycatalog.ai.databricks import DatabricksFunctionClient
from unitycatalog_ai_langchain.toolkit import LangchainToolkit
import json
from contextlib import contextmanager
from typing import NamedTuple
import logging
import uuid
from unittest import mock
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from tests.helper_functions import requires_databricks


@pytest.fixture
def client() -> DatabricksFunctionClient:
    with mock.patch(
        "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        return DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")


CATALOG = "ml"
SCHEMA = "serena_uc_test"
_logger = logging.getLogger(__name__)


class FunctionObj(NamedTuple):
    full_function_name: str
    comment: str


@contextmanager
def create_function_and_cleanup(client: DatabricksFunctionClient):
    func_name = f"{CATALOG}.{SCHEMA}.test_{uuid.uuid4().hex[:4]}"
    comment = "Executes Python code and returns its stdout."
    sql_body = f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
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
    try:
        client.create_function(sql_function_body=sql_body)
        yield FunctionObj(full_function_name=func_name, comment=comment)
    finally:
        try:
            client.client.functions.delete(func_name)
        except Exception as e:
            _logger.warning(f"Fail to delete function: {e}")


@pytest.fixture
def set_default_client(client: DatabricksFunctionClient):
    set_uc_function_client(client)
    yield
    set_uc_function_client(None)


@requires_databricks
def test_toolkit_e2e(set_default_client):
    client = get_uc_function_client()
    with create_function_and_cleanup(client) as func_obj:
        toolkit = LangchainToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.full_function_name
        assert tool.description == func_obj.comment
        tool.args_schema(**{"code": "print(1)"})
        result = json.loads(tool.func(code="print(1)"))["value"]
        assert result == "1\n"

        toolkit = LangchainToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.full_function_name in [t.name for t in toolkit.tools]


@requires_databricks
def test_multiple_toolkits(set_default_client):
    client = get_uc_function_client()
    with create_function_and_cleanup(client) as func_obj:
        toolkit1 = LangchainToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = LangchainToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.name == func_obj.full_function_name][0]
        input_args = {"code": "print(1)"}
        assert tool1.func(**input_args) == tool2.func(**input_args)


def test_toolkit_creation_errors():
    with pytest.raises(ValueError, match=r"No client provided"):
        LangchainToolkit(function_names=[])

    with pytest.raises(ValueError, match=r"instance of BaseFunctionClient expected"):
        LangchainToolkit(function_names=[], client="client")


def generate_function_info():
    parameters = [
        {
            "name": "x",
            "type_text": "string",
            "type_json": '{"name":"x","type":"string","nullable":true,"metadata":{"EXISTS_DEFAULT":"\\"123\\"","default":"\\"123\\"","CURRENT_DEFAULT":"\\"123\\""}}',
            "type_name": "STRING",
            "type_precision": 0,
            "type_scale": 0,
            "position": 17,
            "parameter_type": "PARAM",
            "parameter_default": '"123"',
        }
    ]
    return FunctionInfo(
        catalog_name="catalog",
        schema_name="schema",
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
    )


def test_uc_function_to_langchain_tool(client: DatabricksFunctionClient):
    mock_function_info = generate_function_info()
    with (
        mock.patch(
            "unitycatalog.ai.databricks.DatabricksFunctionClient.get_function",
            return_value=mock_function_info,
        ),
        mock.patch(
            "unitycatalog.ai.databricks.DatabricksFunctionClient.execute_function",
            return_value=FunctionExecutionResult(format="SCALAR", value="some_string"),
        ),
    ):
        tool = LangchainToolkit.uc_function_to_langchain_tool(
            client=client, function_name=f"{CATALOG}.{SCHEMA}.test"
        )
        assert json.loads(tool.func(x="some_string"))["value"] == "some_string"
