import os
from typing import Any, Dict, List, Optional

from openai.lib._tools import pydantic_function_tool
from openai.types.chat import ChatCompletionToolParam
from pydantic import BaseModel, ConfigDict, Field, model_validator
from unitycatalog.ai.client import BaseFunctionClient, validate_or_set_default_client
from unitycatalog.ai.utils import (
    generate_function_input_params_schema,
    get_tool_name,
    validate_full_function_name,
)

UC_LIST_FUNCTIONS_MAX_RESULTS = "100"


class OpenAIToolkit(BaseModel):
    function_names: List[str] = Field(
        default_factory=list,
        description="The list of function names in the form of 'catalog.schema.function'",
    )

    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="The client for managing functions, must be an instance of BaseFunctionClient",
    )

    tools_dict: Dict[str, ChatCompletionToolParam] = Field(
        default_factory=dict,
        description="The tools dictionary storing the function name and tool definition mapping, no need to provide this field",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "OpenAIToolkit":
        self.client = validate_or_set_default_client(self.client)

        tools_dict = {}
        for name in self.function_names:
            if name not in tools_dict:
                full_func_name = validate_full_function_name(name)
                if full_func_name.function_name == "*":
                    token = None
                    while True:
                        functions = self.client.list_functions(
                            catalog=full_func_name.catalog_name,
                            schema=full_func_name.schema_name,
                            max_results=int(
                                os.environ.get(
                                    "UC_LIST_FUNCTIONS_MAX_RESULTS", UC_LIST_FUNCTIONS_MAX_RESULTS
                                )
                            ),
                            page_token=token,
                        )
                        for f in functions:
                            if f.full_name not in tools_dict:
                                tools_dict[f.full_name] = (
                                    self.uc_function_to_openai_function_definition(
                                        client=self.client, function_info=f
                                    )
                                )
                        token = functions.token
                        if token is None:
                            break
                else:
                    tools_dict[name] = self.uc_function_to_openai_function_definition(
                        client=self.client, function_name=name
                    )
        self.tools_dict = tools_dict
        return self

    @classmethod
    def uc_function_to_openai_function_definition(
        cls,
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> ChatCompletionToolParam:
        """
        Convert a UC function to OpenAI function definition.

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

        function_input_params_schema = generate_function_input_params_schema(
            function_info, strict=True
        )
        tool = pydantic_function_tool(
            function_input_params_schema.pydantic_model,
            name=get_tool_name(function_name),
            description=function_info.comment or "",
        )
        # strict is set to true only if all params are supported JSON schema types
        tool["function"]["strict"] = function_input_params_schema.strict
        return tool

    @property
    def tools(self) -> List[ChatCompletionToolParam]:
        return list(self.tools_dict.values())
