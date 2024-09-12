import json
import logging
import os
from typing import Any, Dict, List, Optional

from langchain_core.pydantic_v1 import BaseModel, Field, root_validator
from langchain_core.tools import StructuredTool, Tool
from unitycatalog.ai.client import BaseFunctionClient, get_uc_function_client
from unitycatalog.ai.utils import (
    generate_function_input_params_schema,
    validate_full_function_name,
)

_logger = logging.getLogger(__name__)

UC_LIST_FUNCTIONS_MAX_RESULTS = 100


def get_tool_name(func_name: str) -> str:
    if len(func_name) > 64:
        tool_name = func_name[-64:]
        _logger.warning(
            f"Function name {func_name} is too long, truncating to 64 characters {tool_name}."
        )
        return tool_name
    return func_name


def validate_or_set_default_client(client: Optional[BaseFunctionClient] = None):
    client = client or get_uc_function_client()
    if client is None:
        raise ValueError(
            "No client provided, either set the client when creating the "
            "LangchainToolkit, or set the default client using "
            "unitycatalog.ai.client.set_uc_function_client(client)."
        )
    return client


class LangchainToolkit(BaseModel):
    function_names: List[str] = Field(
        default_factory=list,
        description="The list of function names in the form of 'catalog.schema.function'",
    )

    tools_dict: Dict[str, Tool] = Field(default_factory=dict)

    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="The client for managing functions, must be an instance of BaseFunctionClient",
    )

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def validate_toolkit(cls, values) -> Dict[str, Any]:
        client = validate_or_set_default_client(values.get("client"))
        values["client"] = client

        function_names = values["function_names"]
        tools_dict = values["tools_dict"]
        for name in function_names:
            if name not in tools_dict:
                full_func_name = validate_full_function_name(name)
                if full_func_name.function_name == "*":
                    token = "None"
                    while token is not None:
                        functions = client.list_functions(
                            catalog=full_func_name.catalog_name,
                            schema=full_func_name.schema_name,
                            max_results=os.environ.get(
                                "UC_LIST_FUNCTIONS_MAX_RESULTS", UC_LIST_FUNCTIONS_MAX_RESULTS
                            ),
                            page_token=token if token != "None" else None,
                        )
                        for f in functions:
                            if f.full_name not in tools_dict:
                                tools_dict[f.full_name] = cls.uc_function_to_langchain_tool(
                                    function_info=f
                                )
                        token = functions.token
                else:
                    tools_dict[name] = cls.uc_function_to_langchain_tool(function_name=name)
        values["tools_dict"] = tools_dict
        return values

    @classmethod
    def uc_function_to_langchain_tool(
        cls,
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> StructuredTool:
        """
        Convert a UC function to Langchain StructuredTool

        Args:
            client: The client for managing functions, must be an instance of BaseFunctionClient
            function_name (optional): The full name of the function in the form of 'catalog.schema.function'
            function_info (optional): The function info object returned by the client.get_function() method

            .. note::
                Only one of function_name or function_info should be provided.
        """
        if function_name and function_info:
            raise ValueError("Only one of function_name or function_info should be provided.")
        client = validate_or_set_default_client(client)

        if function_name:
            function_info = client.get_function(function_name)
        elif function_info:
            function_name = function_info.full_name
        else:
            raise ValueError("Either function_name or function_info should be provided.")

        def func(*args: Any, **kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )
            return result.to_json()

        return StructuredTool(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            func=func,
            args_schema=generate_function_input_params_schema(function_info),
        )

    @property
    def tools(self) -> List[Tool]:
        return list(self.tools_dict.values())
