import pytest
import json
import logging
import uuid
from typing import NamedTuple
from contextlib import contextmanager
from unittest import mock
from pydantic import ValidationError


from unitycatalog.ai.client import (
    set_uc_function_client,
    get_uc_function_client,
    FunctionExecutionResult,
)
from unitycatalog.ai.databricks import DatabricksFunctionClient
from unitycatalog_ai_llamaindex.toolkit import LlamaIndexToolkit
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from tests.helper_functions import requires_databricks

CATALOG = "ml"
SCHEMA = "ben_uc_test"
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
            _logger.warning(f"Failed to delete function: {e}")


@pytest.fixture
def client():
    with mock.patch(
        "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        yield DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")


@pytest.fixture
def set_default_client(client):
    set_uc_function_client(client)
    yield
    set_uc_function_client(None)


@requires_databricks
def test_toolkit_e2e(set_default_client):
    client = get_uc_function_client()
    with create_function_and_cleanup(client) as func_obj:
        toolkit = LlamaIndexToolkit(function_names=[func_obj.full_function_name])
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.full_function_name
        assert tool.metadata.description == func_obj.comment
        assert tool.client_config == client.to_dict()

        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        # Test wildcard function names
        toolkit = LlamaIndexToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.full_function_name in [t.metadata.name for t in toolkit.tools]


@requires_databricks
def test_multiple_toolkits(set_default_client):
    client = get_uc_function_client()
    with create_function_and_cleanup(client) as func_obj:
        toolkit1 = LlamaIndexToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = LlamaIndexToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.metadata.name == func_obj.full_function_name][0]
        input_args = {"code": "print(1)"}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"No client provided"):
        LlamaIndexToolkit(function_names=[])

    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        LlamaIndexToolkit(function_names=[], client="client")



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


def test_uc_function_to_llama_tool(client):
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
        tool = LlamaIndexToolkit.uc_function_to_llama_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test",
            client=client
        )
        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"
