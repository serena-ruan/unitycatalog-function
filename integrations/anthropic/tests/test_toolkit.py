from unittest import mock

import pytest
from anthropic import Anthropic
from anthropic.types import Message, TextBlock, ToolUseBlock
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from ucai.core.client import set_uc_function_client
from ucai.core.utils.function_processing_utils import get_tool_name
from ucai.test_utils.client_utils import get_client, requires_databricks, set_default_client
from ucai.test_utils.function_utils import create_function_and_cleanup

from ucai_anthropic.toolkit import UCFunctionToolkit


def mock_anthropic_tool_response(function_name, input_data):
    return Message(
        content=[
            TextBlock(text="Certainly! I'll use the tool to fetch the data.", type="text"),
            ToolUseBlock(
                id="toolu_01A09q90qw90lq917835lq9",
                name=function_name,
                input=input_data,
                type="tool_use",
            ),
        ],
        role="assistant",
        model="claude-3-5-sonnet-20240620"
    )


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_tool_calling_with_anthropic(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, return_func_name=True) as func_obj:
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {"role": "user", "content": "What is the weather in Paris?"},
        ]

        converted_func_name = get_tool_name(func_name)
        with mock.patch(
            "anthropic.Anthropic.messages.create",
            return_value=mock_anthropic_tool_response(
                function_name=converted_func_name, input_data={"location": "Paris"}
            ),
        ):
            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620",
                messages=messages,
                tools=tools,
                max_tokens=512
            )

            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("location"), str)

            result = client.execute_function(func_name, arguments)
            assert result.value == "27.6 celsius"

            function_call_result_message = {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "content": result.value,
                        "tool_use_id": tool_calls[1].id,
                    }
                ],
            }

            final_response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620",
                messages=[*messages, {"role": "assistant", "content": tool_calls}, function_call_result_message],
                tools=tools,
                max_tokens=200,
            )


@requires_databricks
@pytest.mark.parametrize("use_serverless", [True, False])
def test_tool_calling_with_multiple_tools_anthropic(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
    client = get_client()
    with set_default_client(client), create_function_and_cleanup(client, return_func_name=True) as func_obj:
        func_name = func_obj.full_function_name
        toolkit = UCFunctionToolkit(function_names=[func_name])
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {"role": "user", "content": "What is the weather in Paris and New York?"},
        ]

        converted_func_name = get_tool_name(func_name)
        with mock.patch(
            "anthropic.Anthropic.messages.create",
            return_value=mock_anthropic_tool_response(
                function_name=converted_func_name, input_data={"location": "Paris"}
            ),
        ):
            response = Anthropic().messages.create(
                model="claude-3-5-sonnet-20240620",
                messages=messages,
                tools=tools,
                max_tokens=512
            )
            tool_calls = response.content
            assert len(tool_calls) == 2
            assert tool_calls[1].name == converted_func_name
            arguments = tool_calls[1].input
            assert isinstance(arguments.get("location"), str)

            result = client.execute_function(func_name, arguments)
            assert result.value == "27.6 celsius"

            function_call_result_message = {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "content": result.value,
                        "tool_use_id": tool_calls[1].id,
                    }
                ],
            }

            with mock.patch(
                "anthropic.Anthropic.messages.create",
                return_value=mock_anthropic_tool_response(
                    function_name=converted_func_name, input_data={"location": "New York"}
                ),
            ):
                Anthropic().messages.create(
                    model="claude-3-5-sonnet-20240620",
                    messages=[*messages, {"role": "assistant", "content": tool_calls}, function_call_result_message],
                    tools=tools,
                    max_tokens=200,
                )


@pytest.mark.parametrize("use_serverless", [True, False])
def test_anthropic_toolkit_initialization(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
    client = get_client()

    with pytest.raises(
        ValueError,
        match=r"No client provided, either set the client when creating a toolkit or set the default client",
    ):
        toolkit = UCFunctionToolkit(function_names=[])

    set_uc_function_client(client)
    toolkit = UCFunctionToolkit(function_names=[])
    assert len(toolkit.tools) == 0
    set_uc_function_client(None)

    toolkit = UCFunctionToolkit(function_names=[], client=client)
    assert len(toolkit.tools) == 0


def generate_function_info(parameters, catalog="catalog", schema="schema"):
    return FunctionInfo(
        catalog_name=catalog,
        schema_name=schema,
        name="test",
        input_params=FunctionParameterInfos(
            parameters=[FunctionParameterInfo(**param) for param in parameters]
        ),
        full_name=f"{catalog}.{schema}.test",
        comment="Executes Python code and returns its stdout.",
    )


@pytest.mark.parametrize("use_serverless", [True, False])
def test_anthropic_tool_definition_generation(use_serverless, monkeypatch):
    monkeypatch.setenv("USE_SERVERLESS", str(use_serverless))
    client = get_client()
    with set_default_client(client):
        function_info = generate_function_info(
            [
                {
                    "name": "code",
                    "type_text": "string",
                    "type_json": '{"name":"code","type":"string","nullable":true,"metadata":{"comment":"Python code to execute. Remember to print the final result to stdout."}}',
                    "type_name": "STRING",
                    "type_precision": 0,
                    "type_scale": 0,
                    "position": 0,
                    "parameter_type": "PARAM",
                    "comment": "Python code to execute. Remember to print the final result to stdout.",
                }
            ]
        )

        function_definition = UCFunctionToolkit.uc_function_to_anthropic_tool(
            function_info=function_info,
            client=client
        )

        assert function_definition.to_dict() == {
            "name": get_tool_name(function_info.full_name),
            "description": function_info.comment,
            "input_schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "default": None,
                        "description": "Python code to execute. Remember to print the final result to stdout.",
                        "title": "Code",
                    }
                },
                "required": [],
            },
        }
