import json
import logging
import uuid
from contextlib import contextmanager
from typing import Dict, List, Optional
from unittest import mock

import openai
import pytest
from databricks.sdk.service.catalog import (
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from openai.types.chat.chat_completion import (
    ChatCompletion,
    ChatCompletionMessage,
    Choice,
)
from openai.types.chat.chat_completion_message import ChatCompletionMessageToolCall
from openai.types.chat.chat_completion_message_tool_call import Function
from openai.types.completion_usage import CompletionTokensDetails, CompletionUsage
from unitycatalog.ai.client import get_uc_function_client, set_uc_function_client
from unitycatalog.ai.databricks import DatabricksFunctionClient
from unitycatalog.ai.utils import get_tool_name

from tests.helper_functions import requires_databricks
from unitycatalog_ai_openai.toolkit import OpenAIToolkit

CATALOG = "ml"
SCHEMA = "serena_uc_test"

_logger = logging.getLogger(__name__)


@pytest.fixture
def client() -> DatabricksFunctionClient:
    with mock.patch(
        "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        return DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")


def mock_chat_completion_response(func_name: str, function: Function):
    return ChatCompletion(
        id="chatcmpl-mock",
        choices=[
            Choice(
                finish_reason="tool_calls",
                index=0,
                logprobs=None,
                message=ChatCompletionMessage(
                    content=None,
                    refusal=None,
                    role="assistant",
                    function_call=None,
                    tool_calls=[
                        ChatCompletionMessageToolCall(
                            id="call_mock",
                            function=function,
                            type="function",
                        )
                    ],
                ),
            )
        ],
        created=1727076144,
        model="gpt-4o-mini-2024-07-18",
        object="chat.completion",
        service_tier=None,
        system_fingerprint="fp_mock",
        usage=CompletionUsage(
            completion_tokens=32,
            prompt_tokens=116,
            total_tokens=148,
            completion_tokens_details=CompletionTokensDetails(reasoning_tokens=0),
        ),
    )


def random_func_name():
    return f"{CATALOG}.{SCHEMA}.test_{uuid.uuid4().hex[:4]}"


@contextmanager
def create_function_and_cleanup(
    client: DatabricksFunctionClient,
    func_name: Optional[str] = None,
    sql_body: Optional[str] = None,
):
    func_name = func_name or random_func_name()
    sql_body = (
        sql_body
        or f"""CREATE OR REPLACE FUNCTION {func_name}(code STRING COMMENT 'Python code to execute. Remember to print the final result to stdout.')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Executes Python code and returns its stdout.'
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
        yield func_name
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
def test_tool_calling(set_default_client):
    client = get_uc_function_client()
    with create_function_and_cleanup(client) as func_name:
        toolkit = OpenAIToolkit(function_names=[func_name])
        tools = toolkit.tools
        assert len(tools) == 1

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What is the result of 2**10?"},
        ]

        converted_func_name = get_tool_name(func_name)
        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                converted_func_name,
                function=Function(
                    arguments='{"code":"result = 2**10\\nprint(result)"}',
                    name=converted_func_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
            tool_calls = response.choices[0].message.tool_calls
            assert len(tool_calls) == 1
            tool_call = tool_calls[0]
            assert tool_call.function.name == converted_func_name
            arguments = json.loads(tool_call.function.arguments)
            assert isinstance(arguments.get("code"), str)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments)
            assert result.value == "1024\n"

            # Create a message containing the result of the function call
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            assistant_message = response.choices[0].message.to_dict()
            completion_payload = {
                "model": "gpt-4o-mini",
                "messages": [*messages, assistant_message, function_call_result_message],
            }
            # Generate final response
            openai.chat.completions.create(
                model=completion_payload["model"], messages=completion_payload["messages"]
            )


@requires_databricks
def test_tool_calling_work_with_non_json_schema(client):
    func_name = random_func_name()
    function_name = func_name.split(".")[-1]
    sql_body = f"""CREATE FUNCTION {func_name}(start DATE, end DATE)
RETURNS TABLE(day_of_week STRING, day DATE)
COMMENT 'Calculate the weekdays between start and end, return as a table'
RETURN SELECT extract(DAYOFWEEK_ISO FROM day), day
            FROM (SELECT sequence({function_name}.start, {function_name}.end)) AS T(days)
                LATERAL VIEW explode(days) AS day
            WHERE extract(DAYOFWEEK_ISO FROM day) BETWEEN 1 AND 5;
"""

    with create_function_and_cleanup(client, func_name=func_name, sql_body=sql_body):
        toolkit = OpenAIToolkit(function_names=[func_name], client=client)
        tools = toolkit.tools
        assert len(tools) == 1
        assert tools[0]["function"]["strict"] is False

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What are the weekdays between 2024-01-01 and 2024-01-07?"},
        ]

        converted_func_name = get_tool_name(func_name)
        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                converted_func_name,
                function=Function(
                    arguments='{"start":"2024-01-01","end":"2024-01-07"}',
                    name=converted_func_name,
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
            )
            tool_calls = response.choices[0].message.tool_calls
            assert len(tool_calls) == 1
            tool_call = tool_calls[0]
            assert tool_call.function.name == converted_func_name
            arguments = json.loads(tool_call.function.arguments)
            assert isinstance(arguments.get("start"), str)
            assert isinstance(arguments.get("end"), str)

            # execute the function based on the arguments
            result = client.execute_function(func_name, arguments)
            assert result.value is not None

            # Create a message containing the result of the function call
            function_call_result_message = {
                "role": "tool",
                "content": json.dumps({"content": result.value}),
                "tool_call_id": tool_call.id,
            }
            assistant_message = response.choices[0].message.to_dict()
            completion_payload = {
                "model": "gpt-4o-mini",
                "messages": [*messages, assistant_message, function_call_result_message],
            }
            # Generate final response
            openai.chat.completions.create(
                model=completion_payload["model"], messages=completion_payload["messages"]
            )


@requires_databricks
def test_tool_choice_param(client):
    cap_func = random_func_name()
    sql_body1 = f"""CREATE FUNCTION {cap_func}(s STRING)
RETURNS STRING
COMMENT 'Capitalizes the input string'
LANGUAGE PYTHON
AS $$
  return s.capitalize()
$$
"""
    upper_func = random_func_name()
    sql_body2 = f"""CREATE FUNCTION {upper_func}(s STRING)
RETURNS STRING
COMMENT 'Uppercases the input string'
LANGUAGE PYTHON
AS $$
  return s.upper()
$$
"""
    with (
        create_function_and_cleanup(client, func_name=cap_func, sql_body=sql_body1),
        create_function_and_cleanup(client, func_name=upper_func, sql_body=sql_body2),
    ):
        toolkit = OpenAIToolkit(function_names=[cap_func, upper_func], client=client)
        tools = toolkit.tools
        assert len(tools) == 2

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What's the result after capitalize 'abc'?"},
        ]

        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                get_tool_name(cap_func),
                function=Function(
                    arguments='{"s":"abc"}',
                    name=get_tool_name(cap_func),
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                tool_choice="required",
            )
        tool_calls = response.choices[0].message.tool_calls
        assert len(tool_calls) == 1
        tool_call = tool_calls[0]
        assert tool_call.function.name == get_tool_name(cap_func)
        arguments = json.loads(tool_call.function.arguments)
        result = client.execute_function(cap_func, arguments)
        assert result.value == "Abc"

        messages = [
            {
                "role": "system",
                "content": "You are a helpful customer support assistant. Use the supplied tools to assist the user.",
            },
            {"role": "user", "content": "What's the result after uppercase 'abc'?"},
        ]
        with mock.patch(
            "openai.chat.completions.create",
            return_value=mock_chat_completion_response(
                get_tool_name(upper_func),
                function=Function(
                    arguments='{"s":"abc"}',
                    name=get_tool_name(upper_func),
                ),
            ),
        ):
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=tools,
                tool_choice={"type": "function", "function": {"name": get_tool_name(upper_func)}},
            )
        tool_calls = response.choices[0].message.tool_calls
        assert len(tool_calls) == 1
        tool_call = tool_calls[0]
        assert tool_call.function.name == get_tool_name(upper_func)
        arguments = json.loads(tool_call.function.arguments)
        result = client.execute_function(upper_func, arguments)
        assert result.value == "ABC"


def test_openai_toolkit_initialization(client):
    with pytest.raises(
        ValueError,
        match=r"No client provided, either set the client when creating the tool, or set the default client",
    ):
        toolkit = OpenAIToolkit(function_names=[])

    set_uc_function_client(client)
    toolkit = OpenAIToolkit(function_names=[])
    assert len(toolkit.tools) == 0
    set_uc_function_client(None)

    toolkit = OpenAIToolkit(function_names=[], client=client)
    assert len(toolkit.tools) == 0


def generate_function_info(parameters: List[Dict], catalog="catalog", schema="schema"):
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


def test_function_definition_generation(set_default_client):
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

    function_definition = OpenAIToolkit.uc_function_to_openai_function_definition(
        function_info=function_info
    )
    assert function_definition == {
        "type": "function",
        "function": {
            "name": get_tool_name(function_info.full_name),
            "description": function_info.comment,
            "strict": True,
            "parameters": {
                "properties": {
                    "code": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "description": "Python code to execute. Remember to print the final result to stdout.",
                        "title": "Code",
                    }
                },
                "title": get_tool_name(function_info.full_name) + "__params",
                "type": "object",
                "additionalProperties": False,
                "required": ["code"],
            },
        },
    }
