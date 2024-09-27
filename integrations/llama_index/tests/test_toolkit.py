import json
import logging
import os
import uuid
from contextlib import contextmanager
from typing import NamedTuple
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic import ValidationError
from unitycatalog.ai.client import (
    FunctionExecutionResult,
    set_uc_function_client,
)
from unitycatalog.ai.databricks import DatabricksFunctionClient
from unitycatalog.ai.utils.function_processing_utils import get_tool_name

from tests.helper_functions import requires_databricks
from ucai_llamaindex.toolkit import UCFunctionToolkit

CATALOG = "ml"
SCHEMA = "ben_uc_test"
USE_SERVERLESS = "USE_SERVERLESS"

_logger = logging.getLogger(__name__)


def get_client() -> DatabricksFunctionClient:
    with mock.patch(
        "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        if os.environ.get(USE_SERVERLESS, "false").lower() == "true":
            return DatabricksFunctionClient()
        else:
            return DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")


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


@contextmanager
def set_default_client(client: DatabricksFunctionClient):
    try:
        set_uc_function_client(client)
        yield
    finally:
        set_uc_function_client(None)


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.metadata.name == func_obj.full_function_name
        assert tool.metadata.return_direct
        assert tool.metadata.description == func_obj.comment
        assert tool.client_config == client.to_dict()

        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        assert len(toolkit.tools) >= 1
        assert func_obj.full_function_name in [t.metadata.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_toolkit_e2e_manually_passing_client(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client) as func_obj:
        toolkit = UCFunctionToolkit(
            function_names=[func_obj.full_function_name], client=client, return_direct=True
        )
        tools = toolkit.tools
        assert len(tools) == 1
        tool = tools[0]
        assert tool.name == func_obj.full_function_name.replace(".", "__")
        assert tool.metadata.return_direct
        assert tool.description == func_obj.comment
        assert tool.client_config == client.to_dict()
        input_args = {"code": "print(1)"}
        result = json.loads(tool.fn(**input_args))["value"]
        assert result == "1\n"

        toolkit = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"], client=client)
        assert len(toolkit.tools) >= 1
        assert get_tool_name(func_obj.full_function_name) in [t.name for t in toolkit.tools]


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_multiple_toolkits(use_serverless, monkeypatch):
    monkeypatch.setenv(USE_SERVERLESS, str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client) as func_obj:
        toolkit1 = UCFunctionToolkit(function_names=[func_obj.full_function_name])
        toolkit2 = UCFunctionToolkit(function_names=[f"{CATALOG}.{SCHEMA}.*"])
        tool1 = toolkit1.tools[0]
        tool2 = [t for t in toolkit2.tools if t.metadata.name == func_obj.full_function_name][0]
        input_args = {"code": "print(1)"}
        result1 = json.loads(tool1.fn(**input_args))["value"]
        result2 = json.loads(tool2.fn(**input_args))["value"]
        assert result1 == result2


def test_toolkit_creation_errors():
    with pytest.raises(ValidationError, match=r"No client provided"):
        UCFunctionToolkit(function_names=[])

    with pytest.raises(ValidationError, match=r"Input should be an instance of BaseFunctionClient"):
        UCFunctionToolkit(function_names=[], client="client")


def test_toolkit_function_argument_errors(client):
    with pytest.raises(
        ValidationError,
        match=r".*Cannot create tool instances without function_names being provided.*",
    ):
        UCFunctionToolkit(client=client)


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
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name=f"{CATALOG}.{SCHEMA}.test", client=client, return_direct=True
        )
        # Validate passthrough of LlamaIndex argument
        assert tool.metadata.return_direct

        result = json.loads(tool.fn(x="some_string"))["value"]
        assert result == "some_string"


def test_toolkit_with_invalid_function_input(client):
    """Test toolkit with invalid input parameters for function conversion."""
    mock_function_info = generate_function_info()

    with (
        mock.patch(
            "unitycatalog.ai.utils.client_utils.validate_or_set_default_client", return_value=client
        ),
        mock.patch.object(client, "get_function", return_value=mock_function_info),
    ):
        # Test with invalid input params that are not matching expected schema
        invalid_inputs = {"unexpected_key": "value"}
        tool = UCFunctionToolkit.uc_function_to_llama_tool(
            function_name="catalog.schema.test", client=client, return_direct=True
        )

        with pytest.raises(ValueError, match="Extra parameters provided that are not defined"):
            tool.fn(**invalid_inputs)
