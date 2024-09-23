import json

from pydantic import BaseModel, Field, model_validator, ConfigDict
from typing import Any, Dict, List, Optional, Callable
from llama_index.core.tools import FunctionTool
from llama_index.core.tools.types import ToolMetadata

from unitycatalog.ai.client import BaseFunctionClient
from unitycatalog.ai.utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
    validate_or_set_default_client,
)


class UnityCatalogTool(FunctionTool):
    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool",
    )

    def __init__(self, fn: Callable, metadata: ToolMetadata, client_config: Dict[str, Any], *args, **kwargs):
        super().__init__(fn=fn, metadata=metadata, *args, **kwargs)

        self.client_config = client_config


class LlamaIndexToolkit(BaseModel):
    function_names: List[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format",
    )
    tools_dict: Dict[str, FunctionTool] = Field(default_factory=dict)
    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="Client for managing functions",
    )
    return_direct: bool = Field(
        default=False,
        description="Whether the tool should return the output directly",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode='after')
    def validate_toolkit(self) -> 'LlamaIndexToolkit':
        client = validate_or_set_default_client(self.client)
        self.client = client

        function_names = self.function_names
        tools_dict = self.tools_dict

        self.tools_dict = process_function_names(
            function_names=function_names,
            tools_dict=tools_dict,
            client=client,
            uc_function_to_tool_func=self.uc_function_to_llama_tool,
        )
        return self

    @classmethod
    def uc_function_to_llama_tool(
        cls,
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        return_direct: Optional[bool] = False,
    ) -> FunctionTool:
        if function_name and function_info:
            raise ValueError("Only one of function_name or function_info should be provided.")
        client = validate_or_set_default_client(client)

        if function_name:
            function_info = client.get_function(function_name)
        elif function_info:
            function_name = function_info.full_name
        else:
            raise ValueError("Either function_name or function_info should be provided.")

        fn_schema = generate_function_input_params_schema(function_info)

        def func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )
            return result.to_json()

        metadata = ToolMetadata(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            fn_schema=fn_schema,
            return_direct=return_direct,
        )

        return UnityCatalogTool(
            fn=func,
            metadata=metadata,
            client_config=client.to_dict(),
        )

    @property
    def tools(self) -> List[FunctionTool]:
        return list(self.tools_dict.values())
