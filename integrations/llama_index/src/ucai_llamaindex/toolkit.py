from typing import Any, Callable, Dict, List, Optional

from llama_index.core.tools import FunctionTool
from llama_index.core.tools.types import ToolMetadata
from pydantic import BaseModel, ConfigDict, Field, ValidationError, create_model, model_validator
from ucai.core.client import BaseFunctionClient
from ucai.core.utils.client_utils import validate_or_set_default_client
from ucai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)


class UnityCatalogTool(FunctionTool):
    """
    A tool class that integrates Unity Catalog functions into a tool structure.

    Attributes:
        client_config (Dict[str, Any]): Configuration of the client for managing the tool.
    """

    client_config: Dict[str, Any] = Field(
        description="Configuration of the client for managing the tool",
    )

    def __init__(
        self, fn: Callable, metadata: ToolMetadata, client_config: Dict[str, Any], *args, **kwargs
    ):
        """
        Initializes the UnityCatalogTool.

        Args:
            fn (Callable): The function that represents the tool's functionality.
            metadata (ToolMetadata): Metadata about the tool, including name, description, and schema.
            client_config (Dict[str, Any]): Configuration dictionary for the client used to manage the tool.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(*args, fn=fn, metadata=metadata, **kwargs)
        self.client_config = client_config


class UCFunctionToolkit(BaseModel):
    """
    A toolkit for managing Unity Catalog functions and converting them into tools.

    Attributes:
        function_names (List[str]): List of function names in 'catalog.schema.function' format.
        tools_dict (Dict[str, FunctionTool]): A dictionary mapping function names to their corresponding tools.
        client (Optional[BaseFunctionClient]): The client used to manage functions.
        return_direct (bool): Whether the tool should return the output directly.
    """

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

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        """
        Validates the toolkit configuration and processes function names.

        Returns:
            UCFunctionToolkit: The validated and updated toolkit instance for LlamaIndex integration.
        """
        client = validate_or_set_default_client(self.client)
        self.client = client

        if not self.function_names:
            raise ValueError("Cannot create tool instances without function_names being provided.")

        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=client,
            uc_function_to_tool_func=self.uc_function_to_llama_tool,
            return_direct=self.return_direct,
        )
        return self

    @staticmethod
    def uc_function_to_llama_tool(
        *,
        client: Optional[BaseFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
        return_direct: Optional[bool] = False,
    ) -> FunctionTool:
        """
        Converts a Unity Catalog function into a Llama tool.
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

        fn_schema = generate_function_input_params_schema(function_info)
        pydantic_model = fn_schema.pydantic_model

        # Enforce strict validation by setting 'extra' to 'forbid' for both outer and inner models
        WrappedModel = create_model(
            f"{pydantic_model.__name__}_Wrapper",
            properties=(pydantic_model, Field(...)),
            model_config=ConfigDict(extra="forbid"),
        )

        def func(**kwargs: Any) -> str:
            """
            Executes the Unity Catalog function with the provided parameters.
            """
            try:
                # Validate input parameters using WrappedModel
                validated_input = WrappedModel(**kwargs)
            except ValidationError as e:
                raise ValueError("Extra parameters provided that are not defined") from e

            # Extract the 'properties' field containing the actual parameters
            function_params = validated_input.properties.model_dump()

            # Execute the function with the parameters
            result = client.execute_function(
                function_name=function_name,
                parameters=function_params,
            )
            return result.to_json()

        metadata = ToolMetadata(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            fn_schema=WrappedModel,
            return_direct=return_direct,
        )

        return UnityCatalogTool(
            fn=func,
            metadata=metadata,
            client_config=client.to_dict(),
        )

    @property
    def tools(self) -> List[FunctionTool]:
        """
        Retrieves the list of tools managed by the toolkit.

        Returns:
            List[FunctionTool]: A list of tools available in the toolkit.
        """
        return list(self.tools_dict.values())
