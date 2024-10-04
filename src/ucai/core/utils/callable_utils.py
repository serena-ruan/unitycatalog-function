import ast
import inspect
from textwrap import dedent
from typing import Any, Callable, get_type_hints

from ucai.core.utils.type_utils import python_type_to_sql_type, validate_container_type

FORBIDDEN_PARAMS = ['self', 'cls']

def extract_function_body(func: Callable[..., Any]) -> str:
    """Extracts the body of a function as a string without the signature or docstring, preserving indentation."""
    parsed_source, source_lines = parse_dedented_source(func)
    
    function_def = parsed_source.body[0]

    body_start_line = function_def.body[0].lineno - 1
    
    if isinstance(function_def.body[0], ast.Expr) and isinstance(function_def.body[0].value, ast.Constant):
        body_start_line += len(function_def.body[0].value.s.splitlines())

    function_body_lines = source_lines[body_start_line:]

    min_indent = min((len(line) - len(line.lstrip()) for line in function_body_lines if line.strip()), default=0)
    
    function_body = "\n".join([line[min_indent:] if len(line) >= min_indent else line for line in function_body_lines])

    return function_body

def parse_docstring(docstring: str) -> dict:
    """Parses the docstring to extract comments for parameters, return value, and exceptions raised."""
    parsed_comments = {}
    current_param = None
    description_lines = []

    if "Args:" in docstring:
        args_part = docstring.split("Args:")[1].strip().split("Returns:")[0].split("Raises:")[0].strip()
        for line in args_part.splitlines():
            if line.strip() and ":" in line:
                if current_param and description_lines:
                    parsed_comments[current_param] = " ".join(description_lines).strip()
                
                param_part, comment_part = line.split(":", 1)
                param = param_part.split("(")[0].strip()
                current_param = param
                description_lines = [comment_part.strip()]
            elif current_param:
                description_lines.append(line.strip())
        
        if current_param and description_lines:
            parsed_comments[current_param] = " ".join(description_lines).strip()

    if "Returns:" in docstring:
        return_part = docstring.split("Returns:")[1].strip().split("Raises:")[0].strip()
        parsed_comments['return'] = return_part

    if "Raises:" in docstring:
        raises_part = docstring.split("Raises:")[1].strip()
        parsed_comments['raises'] = raises_part

    return parsed_comments



def parse_dedented_source(func: Callable[..., Any]) -> tuple:
    """Extracts and dedents the source code of a function."""
    full_source = inspect.getsource(func)
    dedented_source = dedent(full_source)
    return ast.parse(dedented_source), full_source.splitlines()


def validate_type_hint(hint: Any) -> str:
    """Validates and returns the SQL type for a given type hint."""
    if isinstance(hint, type):
        return python_type_to_sql_type(hint)
    else:
        return validate_container_type(hint)


def generate_sql_function_body(func: Callable[..., Any], func_comment: str, catalog: str, schema: str) -> str:
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
    # unwrap staticmethod or classmethod for compatilibity with Python<=3.9
    if isinstance(func, staticmethod) or isinstance(func, classmethod):
            func = func.__func__ 

    func_name = func.__name__

    signature = inspect.signature(func)
    type_hints = get_type_hints(func)

    if 'return' not in type_hints:
        raise ValueError(f"Return type for function '{func_name}' is not defined. Please provide a return type.")
    
    return_type_hint = type_hints.get('return')

    try:
        sql_return_type = validate_type_hint(return_type_hint)
    except ValueError as e:
        raise ValueError(f"Error in return type: {return_type_hint} is not supported.") from e

    docstring = inspect.getdoc(func) or ""
    docstring_comments = parse_docstring(docstring)

    sql_params = []
    for param_name, _ in signature.parameters.items():
        if param_name in FORBIDDEN_PARAMS:
            raise ValueError(f"Parameter '{param_name}' is not allowed in the function signature.")

        if param_name in type_hints:
            param_hint = type_hints[param_name]

            try:
                sql_type = validate_type_hint(param_hint)
            except ValueError as e:
                raise ValueError(f"Error in parameter '{param_name}': type {param_hint} is not supported.") from e

            param_comment = docstring_comments.get(param_name, f"Parameter {param_name}")
            sql_params.append(f"{param_name} {sql_type} COMMENT '{param_comment}'")
        else:
            raise ValueError(f"Missing type hint for parameter: {param_name}.")

    function_body = extract_function_body(func)

    sql_body = f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.{func_name}({', '.join(sql_params)})
    RETURNS {sql_return_type}
    LANGUAGE PYTHON
    COMMENT '{func_comment}'
    AS $$
{function_body}
    $$;
    """
    
    return sql_body
