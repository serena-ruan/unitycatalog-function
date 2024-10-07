import json
from typing import Any, Callable, Dict, List, Optional

from crewai_tools import BaseTool as CrewAIBaseTool
from pydantic import BaseModel, ConfigDict, Field, model_validator, PrivateAttr
from ucai.core.client import BaseFunctionClient
from ucai.core.utils.client_utils import validate_or_set_default_client
from ucai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

class UnityCatalogTool(CrewAIBaseTool):
    fn: Callable = Field(
        description="Callable that will override the CrewAI _run() method."
    )
    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool"
    )

    def __init__(self, fn: Callable, client_config: Dict[str, Any], *args, **kwargs):
        super().__init__(fn=fn, client_config=client_config, *args, **kwargs)

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into tools.
    TODO
    """

    function_names: List[str] = Field(
        default_factory=list,
        description="List of function names in 'catalog.schema.function' format",
    )
    tools_dict: Dict[str, CrewAIBaseTool] = Field(default_factory=dict)
    client: Optional[BaseFunctionClient] = Field(
        default=None,
        description="Client for managing functions",
    )
    return_direct: bool = Field(
        default=False,
        description="Whether the tool should return the output directly",
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        """
        Validates the toolkit configuration and processes function names.

        TODO
        """
        client = validate_or_set_default_client(self.client)
        self.client = client

        if not self.function_names:
            raise ValueError("Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=client,
            uc_function_to_tool_func=self.uc_function_to_crewai_tool,
            return_direct=self.return_direct,
        )
        return self

    @staticmethod
    def uc_function_to_crewai_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        **kwargs,
    ) -> CrewAIBaseTool:
        """
        Converts a Unity Catalog function into a CrewAI tool.

        Logic (UC to crewAI)
        -  function_name OR function_info.full_name : name (required)
        -  client.get_function().comment converted to docstring: description (required, looks for docstring)
        - generate_function_input_params_schema(function_info) : args_schema
        -  func_logic_below : func (required)
        -  : args_schema (required, inferred from type hints)


        name: str
        '''The unique name of the tool that clearly communicates its purpose.'''
        description: str
        '''Used to tell the model how/when/why to use the tool.'''
        args_schema: Type[PydanticBaseModel] = Field(default_factory=_ArgsSchemaPlaceholder)
        '''The schema for the arguments that the tool accepts.'''
        description_updated: bool = False
        '''Flag to check if the description has been updated.'''
        cache_function: Optional[Callable] = lambda _args, _result: True
        '''Function that will be used to determine if the tool should be cached, should return a boolean. If None, the tool will be cached.'''
        result_as_answer: bool = False
        '''Flag to check if the tool should be the final agent answer.'''

        TODO
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

        def func(**kwargs: Any) -> str:
            args_json = json.loads(json.dumps(kwargs, default=str))
            result = client.execute_function(
                function_name=function_name,
                parameters=args_json,
            )
            return result.to_json()

        print('------------------')
        print('------------------')
        print('------------------')
        print('------------------')
        print(func)
        return UnityCatalogTool(
            # UnityCatalogTool params
            fn=func,
            client_config=client.to_dict(), # TODO probbaly need this to be passed
            # CrewAI params
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            args_schema=generate_function_input_params_schema(function_info).pydantic_model,
        )

    @property
    def tools(self) -> List[CrewAIBaseTool]:
        """
        Retrieves the list of tools managed by the toolkit.

        Returns:
            List[BaseTool]: A list of tools available in the toolkit.
        """
        return list(self.tools_dict.values())
