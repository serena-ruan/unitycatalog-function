import ast
import inspect
from textwrap import dedent, indent
from typing import Any, Callable, Union, get_type_hints

from ucai.core.utils.type_utils import python_type_to_sql_type

FORBIDDEN_PARAMS = ['self', 'cls']

def parse_docstring(docstring: str) -> dict[str, str]:
    """
    Parses the docstring to extract comments for parameters, return value, and exceptions raised.
    Uses a state machine approach to handle different sections like Args, Returns, and Raises.
    """
    parsed_comments = {}
    current_param = None
    description_lines = []

    class State:
        ARGS = "ARGS"
        RETURNS = "RETURNS"
        RAISES = "RAISES"
        END = "END"

    def add_param_comment():
        """Helper to add current parameter and its description to the parsed comments."""
        if current_param and description_lines:
            parsed_comments[current_param] = " ".join(description_lines).strip()

    state = None
    for line in docstring.splitlines():
        stripped_line = line.strip()

        if stripped_line.startswith("Args:"):
            state = State.ARGS
            current_param = None
            description_lines = []
            continue
        elif stripped_line.startswith("Returns:"):
            add_param_comment()
            state = State.RETURNS
            continue
        elif stripped_line.startswith("Raises:"):
            add_param_comment()
            state = State.RAISES
            continue

        if state == State.ARGS:
            if ":" in stripped_line:
                add_param_comment()
                param_part, comment_part = stripped_line.split(":", 1)
                current_param = param_part.split("(")[0].strip()
                description_lines = [comment_part.strip()]
            elif current_param:
                description_lines.append(stripped_line)
        elif state == State.RETURNS:
            parsed_comments["return"] = stripped_line
            state = State.END
        elif state == State.RAISES:
            parsed_comments["raises"] = stripped_line
            state = State.END

    add_param_comment()

    return parsed_comments

def extract_function_body(func: Callable[..., Any]) -> tuple[str, int]:
    """
    Extracts the body of a function as a string without the signature or docstring,
    dedents the code, and returns the indentation unit used in the function (e.g., 2 or 4 spaces).
    """
    source_lines, _ = inspect.getsourcelines(func)
    dedented_source = dedent(''.join(source_lines))

    parsed_source = ast.parse(dedented_source)
    func_name = func.__name__

    class FunctionBodyExtractor(ast.NodeVisitor):
        def __init__(self):
            self.function_body = ''
            self.indent_unit = 4 
            self.found = False

        def visit_FunctionDef(self, node: ast.FunctionDef):
            if not self.found and node.name == func_name:
                self.found = True
                self.extract_body(node)

        def extract_body(self, node: ast.FunctionDef):
            body = node.body
            # Skip the docstring
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                body = body[1:]
            
            if not body:
                return 

            start_lineno = body[0].lineno
            end_lineno = body[-1].end_lineno

            source_lines = dedented_source.splitlines(keepends=True)
            function_body_lines = source_lines[start_lineno - 1:end_lineno]

            self.function_body = dedent(''.join(function_body_lines)).rstrip('\n')

            indents = [stmt.col_offset for stmt in body if stmt.col_offset is not None]
            if indents:
                self.indent_unit = min(indents)

    extractor = FunctionBodyExtractor()
    extractor.visit(parsed_source)

    return extractor.function_body, extractor.indent_unit

def validate_type_hint(hint: Any) -> str:
    """Validates and returns the SQL type for a given type hint."""
    # Handle typing.Optional, which is Union[type, None]
    if hasattr(hint, '__origin__') and hint.__origin__ is Union:
        non_none_types = [t for t in hint.__args__ if t is not type(None)]
        if len(non_none_types) == 1:
            return python_type_to_sql_type(non_none_types[0])
        else:
            raise ValueError(f"Unsupported type union: {hint}")
    if hint is Any:
        raise ValueError("Unsupported Python type: typing.Any is not allowed. Please specify a concrete type.")
    return python_type_to_sql_type(hint)

def generate_type_hint_error_message(param_name: str, param_hint: Any, exception: Exception) -> str:
    """
    Generate an informative error message for unsupported parameter types, especially for Lists, Tuples, and Dicts.
    Args:
        param_name: The name of the parameter with the type hint issue.
        param_hint: The unsupported type hint.
        exception: The original exception raised.
    Returns:
        str: A detailed error message guiding the user on how to resolve the issue.
    """
    if hasattr(param_hint, '__origin__'):
        origin = param_hint.__origin__
        if origin == list:
            return (
                f"Error in parameter '{param_name}': List type requires a specific element type. "
                f"Please define the internal type for the list, e.g., List[int]. Original error: {exception}"
            )
        elif origin == tuple:
            return (
                f"Error in parameter '{param_name}': Tuple type requires specific element types. "
                f"Please define the types for the tuple elements, e.g., Tuple[int, str]. Original error: {exception}"
            )
        elif origin == dict:
            return (
                f"Error in parameter '{param_name}': Dict type requires both key and value types. "
                f"Please define the internal types for the dict, e.g., Dict[str, int]. Original error: {exception}"
            )
    return (
        f"Error in parameter '{param_name}': type {param_hint} is not supported. "
        f"Original error: {exception}"
    )

def format_default_value(default: Any) -> str:
    """Formats the default value for SQL."""
    if default is None:
        return 'NULL'
    elif isinstance(default, str):
        return f"'{default}'"
    else:
        return str(default)

def unwrap_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """Unwraps staticmethod or classmethod to get the actual function."""
    if isinstance(func, (staticmethod, classmethod)):
        func = func.__func__
    return func

def process_parameter(
    param_name: str,
    param: inspect.Parameter,
    type_hints: dict[str, Any],
    docstring_comments: dict[str, str]
) -> str:
    """Processes a single parameter and returns its SQL definition."""
    if param_name in FORBIDDEN_PARAMS:
        raise ValueError(f"Parameter '{param_name}' is not allowed in the function signature.")

    if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
        kind = 'var-positional (*args)' if param.kind == inspect.Parameter.VAR_POSITIONAL else 'var-keyword (**kwargs)'
        raise ValueError(f"Parameter '{param_name}' is a {kind} parameter, which is not supported in SQL functions.")

    if param_name not in type_hints:
        raise ValueError(f"Missing type hint for parameter: {param_name}.")

    param_hint = type_hints[param_name]

    try:
        sql_type = validate_type_hint(param_hint)
    except ValueError as e:
        error_message = generate_type_hint_error_message(param_name, param_hint, e)
        raise ValueError(error_message) from e

    param_comment = docstring_comments.get(param_name, f"Parameter {param_name}").replace("'", '"')

    if param.default is inspect.Parameter.empty:
        return f"{param_name} {sql_type} COMMENT '{param_comment}'"
    else:
        default_value = format_default_value(param.default)
        return f"{param_name} {sql_type} DEFAULT {default_value} COMMENT '{param_comment}'"

def assemble_sql_body(
    catalog: str,
    schema: str,
    func_name: str,
    sql_params: list[str],
    sql_return_type: str,
    func_comment: str,
    indented_body: str
) -> str:
    """Assembles the final SQL function body."""
    sql_params_str = ', '.join(sql_params)
    sql_body = f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.{func_name}({sql_params_str})
    RETURNS {sql_return_type}
    LANGUAGE PYTHON
    COMMENT '{func_comment}'
    AS $$
{indented_body}
    $$;
    """
    return sql_body

def generate_sql_function_body(
    func: Callable[..., Any],
    func_comment: str,
    catalog: str,
    schema: str
) -> str:
    """
    Generate SQL body for creating the function in Unity Catalog.

    Args:
        func: The Python callable function to convert into a UDF.
        func_comment: A short description for the function.
        catalog: The catalog name.
        schema: The schema name.

    Returns:
        str: SQL statement for creating the UDF.
    """
    func = unwrap_function(func)
    func_name = func.__name__
    signature = inspect.signature(func)
    type_hints = get_type_hints(func)

    sql_return_type = validate_return_type(func_name, type_hints)

    docstring = inspect.getdoc(func) or ""
    docstring_comments = parse_docstring(docstring)

    sql_params = []
    for param_name, param in signature.parameters.items():
        sql_param = process_parameter(param_name, param, type_hints, docstring_comments)
        sql_params.append(sql_param)

    function_body, indent_unit = extract_function_body(func)
    indented_body = indent(function_body, ' ' * indent_unit)

    sql_body = assemble_sql_body(
        catalog, schema, func_name, sql_params, sql_return_type, func_comment, indented_body
    )

    return sql_body

def validate_return_type(func_name: str, type_hints: dict[str, Any]) -> str:
    """Validates and returns the SQL return type for the function."""
    if 'return' not in type_hints:
        raise ValueError(
            f"Return type for function '{func_name}' is not defined. Please provide a return type."
        )
    return_type_hint = type_hints['return']
    try:
        sql_return_type = validate_type_hint(return_type_hint)
    except ValueError as e:
        raise ValueError(
            f"Error in return type: {return_type_hint} is not supported."
        ) from e
    return sql_return_type
