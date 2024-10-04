import re
from typing import Any, Dict, List, Union

import pytest

from ucai.core.utils.callable_utils import generate_sql_function_body


def test_simple_function_no_docstring():
    def simple_func(a: int, b: int) -> int:
        return a + b

    sql_body = generate_sql_function_body(simple_func, "Simple addition", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.simple_func(a INTEGER COMMENT 'Parameter a', b INTEGER COMMENT 'Parameter b')
    RETURNS INTEGER
    LANGUAGE PYTHON
    COMMENT 'Simple addition'
    AS $$
return a + b
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_nested():
    def outer_func(x: int, y: int) -> str:
        """
        A function that demonstrates nested functions.

        Args:
            x: The x parameter
            y: The y parameter
        
        Returns:
            str: A string representation of the sum of x and y
        """
        def inner_func(a: int) -> int:
            return a + y
        
        return str(inner_func(x))

    sql_body = generate_sql_function_body(outer_func, "Nested function example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.outer_func(x INTEGER COMMENT 'The x parameter', y INTEGER COMMENT 'The y parameter')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Nested function example'
    AS $$
def inner_func(a: int) -> int:
    return a + y

return str(inner_func(x))
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_class():
    def func_with_class(a: int) -> str:
        """
        A function that defines a class inside.

        Args:
            a: The parameter a
        
        Returns:
            str: A string representation of the object created
        """
        class Example:
            def __init__(self, val: int):
                self.val = val

            def double(self) -> int:
                return self.val * 2

        obj = Example(a)
        return str(obj.double())

    sql_body = generate_sql_function_body(func_with_class, "Class definition inside function", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_class(a INTEGER COMMENT 'The parameter a')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Class definition inside function'
    AS $$
class Example:
    def __init__(self, val: int):
        self.val = val

    def double(self) -> int:
        return self.val * 2

obj = Example(a)
return str(obj.double())
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_imports():
    def func_with_import(a: int) -> str:
        """
        A function that imports a module and returns a result.

        Args:
            a: The input parameter
        
        Returns:
            str: A string representation of a result
        """
        import math
        return str(math.sqrt(a))

    sql_body = generate_sql_function_body(func_with_import, "Function with import", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_import(a INTEGER COMMENT 'The input parameter')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with import'
    AS $$
import math
return str(math.sqrt(a))
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_detailed_docstring():
    def detailed_func(a: int, b: int) -> int:
        """
        A detailed function example.

        Args:
            a: The first number
            b: The second number
        
        Returns:
            int: The sum of a and b
        
        Raises:
            ValueError: If a or b are negative
        """
        if a < 0 or b < 0:
            raise ValueError("Both numbers must be non-negative")
        return a + b

    sql_body = generate_sql_function_body(detailed_func, "Detailed docstring example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.detailed_func(a INTEGER COMMENT 'The first number', b INTEGER COMMENT 'The second number')
    RETURNS INTEGER
    LANGUAGE PYTHON
    COMMENT 'Detailed docstring example'
    AS $$
if a < 0 or b < 0:
    raise ValueError("Both numbers must be non-negative")
return a + b
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_multiline_docstring():
    def multiline_docstring_func(a: int, b: int) -> str:
        """
        A function with a multiline docstring.

        This docstring spans multiple lines and
        describes the function in detail.

        Args:
            a: The first number
            b: The second number
        
        Returns:
            str: The string representation of the sum of a and b
        """
        return str(a + b)

    sql_body = generate_sql_function_body(multiline_docstring_func, "Multiline docstring example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.multiline_docstring_func(a INTEGER COMMENT 'The first number', b INTEGER COMMENT 'The second number')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Multiline docstring example'
    AS $$
return str(a + b)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_lambda():
    def lambda_func(x: int) -> str:
        """
        A function with a lambda expression.

        Args:
            x: The input value
        
        Returns:
            str: A string representation of the lambda result
        """
        square = lambda a: a * a
        return str(square(x))

    sql_body = generate_sql_function_body(lambda_func, "Lambda function example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.lambda_func(x INTEGER COMMENT 'The input value')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Lambda function example'
    AS $$
square = lambda a: a * a
return str(square(x))
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_decorator():
    @staticmethod
    def decorated_func(a: int, b: int) -> int:
        """
        A static method decorated function.

        Args:
            a: First integer
            b: Second integer
        
        Returns:
            int: Sum of a and b
        """
        return a + b

    sql_body = generate_sql_function_body(decorated_func, "Decorated function example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.decorated_func(a INTEGER COMMENT 'First integer', b INTEGER COMMENT 'Second integer')
    RETURNS INTEGER
    LANGUAGE PYTHON
    COMMENT 'Decorated function example'
    AS $$
return a + b
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_complex_return_type():
    def complex_return_func() -> dict:
        """
        A function with a complex return type.

        Returns:
            dict: A dictionary with a string key and a list of integers as value
        """
        return {"numbers": [1, 2, 3]}

    sql_body = generate_sql_function_body(complex_return_func, "Complex return type example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.complex_return_func()
    RETURNS MAP
    LANGUAGE PYTHON
    COMMENT 'Complex return type example'
    AS $$
return {"numbers": [1, 2, 3]}
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_try_except():
    def try_except_func(a: int, b: int) -> int:
        """
        A function with try-except block.

        Args:
            a: First number
            b: Second number
        
        Returns:
            int: Sum of a and b
        """
        try:
            return a + b
        except Exception as e:
            raise ValueError(f"Invalid operation") from e

    sql_body = generate_sql_function_body(try_except_func, "Try-except block example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.try_except_func(a INTEGER COMMENT 'First number', b INTEGER COMMENT 'Second number')
    RETURNS INTEGER
    LANGUAGE PYTHON
    COMMENT 'Try-except block example'
    AS $$
try:
    return a + b
except Exception as e:
    raise ValueError(f"Invalid operation") from e
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_without_return_type_hints():
    def no_return_type_hint_func(a: int, b: int):
        return a + b

    with pytest.raises(ValueError, match="Return type for function 'no_return_type_hint_func' is not defined"):
        generate_sql_function_body(no_return_type_hint_func, "No return type", "test_catalog", "test_schema")

def test_function_without_arg_type_hints():
    def no_arg_type_hint_func(a, b) -> int:
        return a + b

    with pytest.raises(ValueError, match="Missing type hint for parameter: a"):
        generate_sql_function_body(no_arg_type_hint_func, "No arg type hints", "test_catalog", "test_schema")

def test_function_with_unsupported_return_type():
    class CustomType:
        pass

    def unsupported_return_type_func() -> CustomType:
        return CustomType()

    with pytest.raises(ValueError, match="Error in return type"):
        generate_sql_function_body(unsupported_return_type_func, "Unsupported return type", "test_catalog", "test_schema")

def test_function_with_unsupported_param_type():
    def unsupported_param_type_func(a: object) -> str:
        return str(a)

    with pytest.raises(ValueError, match="Error in parameter 'a'"):
        generate_sql_function_body(unsupported_param_type_func, "Unsupported param type", "test_catalog", "test_schema")

def test_function_with_var_args():
    def var_args_func(a: int, *args, **kwargs) -> str:
        return str(a)

    with pytest.raises(ValueError, match="Missing type hint for parameter: args"):
        generate_sql_function_body(var_args_func, "Variable arguments function", "test_catalog", "test_schema")

def test_function_with_multiple_return_paths():
    def multiple_return_func(a: int) -> str:
        if a > 0:
            return "Positive"
        else:
            return "Negative"

    sql_body = generate_sql_function_body(multiple_return_func, "Multiple return paths", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.multiple_return_func(a INTEGER COMMENT 'Parameter a')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Multiple return paths'
    AS $$
if a > 0:
    return "Positive"
else:
    return "Negative"
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_nested_function():
    def outer_func(x: int, y: int) -> str:
        """
        A function that demonstrates nested functions.

        Args:
            x: The x parameter
            y: The y parameter
        
        Returns:
            str: A string representation of the sum of x and y
        """
        def inner_func(a: int) -> int:
            return a + y
        
        return str(inner_func(x))

    sql_body = generate_sql_function_body(outer_func, "Nested function example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.outer_func(x INTEGER COMMENT 'The x parameter', y INTEGER COMMENT 'The y parameter')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Nested function example'
    AS $$
def inner_func(a: int) -> int:
    return a + y

return str(inner_func(x))
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_returning_none():
    def func_returning_none(a: int) -> None:
        """
        A function that returns None.

        Args:
            a: An integer
        """
        return None

    with pytest.raises(ValueError, match="Error in return type: <class 'NoneType'> is not supported"):
        generate_sql_function_body(func_returning_none, "Function returning None", "test_catalog", "test_schema")

def test_function_returning_any():
    def func_returning_any(a: int) -> Any:
        """
        A function that returns Any type.

        Args:
            a: An integer
        """
        return a

    with pytest.raises(ValueError, match="Error in return type: typing.Any is not supported"):
        generate_sql_function_body(func_returning_any, "Function returning Any", "test_catalog", "test_schema")

def test_function_returning_union():
    def func_returning_union(a: int) -> Union[int, str]:
        """
        A function that returns a Union of int and str.

        Args:
            a: An integer
        
        Returns:
            Union[int, str]: Either an integer or a string
        """
        if a > 0:
            return a
        return str(a)

    with pytest.raises(ValueError, match=re.escape("Error in return type: typing.Union[int, str] is not supported.")):
        generate_sql_function_body(func_returning_union, "Function returning Union", "test_catalog", "test_schema")

def test_function_with_self():
    """Test that functions using 'self' raise an exception."""
    def func_with_self(self, a: int) -> int:
        """Example function with 'self'."""
        return a * 2
    
    with pytest.raises(ValueError, match="Parameter 'self' is not allowed in the function signature"):
        generate_sql_function_body(func_with_self, "Function with self", "test_catalog", "test_schema")


def test_function_with_cls():
    """Test that functions using 'cls' raise an exception."""
    def func_with_cls(cls, a: int) -> int:
        """Example function with 'cls'."""
        return a + 5
    
    with pytest.raises(ValueError, match="Parameter 'cls' is not allowed in the function signature"):
        generate_sql_function_body(func_with_cls, "Function with cls", "test_catalog", "test_schema")

def test_function_with_google_docstring():
    def my_function(a: int, b: str) -> float:
        """
        This function adds the length of a string to an integer.

        Args:
            a (int): The integer to add to.
            b (str): The string to get the length of.

        Returns:
            float: The sum of the integer and the length of the string.
        """
        return a + len(b)

    sql_body = generate_sql_function_body(my_function, "Google-style docstring example", "test_catalog", "test_schema")

    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.my_function(a INTEGER COMMENT 'The integer to add to.', b STRING COMMENT 'The string to get the length of.')
    RETURNS DOUBLE
    LANGUAGE PYTHON
    COMMENT 'Google-style docstring example'
    AS $$
return a + len(b)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_multiline_argument_description():
    def my_multiline_arg_function(
        a: int, 
        b: str
    ) -> str:
        """
        This function has a multi-line argument list.

        Args:
            a: The first argument, which is an integer.
               The integer is guaranteed to be positive.
            b: The second argument, which is a string.
               The string should be more than 100 characters long.

        Returns:
            str: A string that concatenates the integer and string.
        """
        return f"{a}-{b}"

    sql_body = generate_sql_function_body(my_multiline_arg_function, "Multiline arg description example", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.my_multiline_arg_function(a INTEGER COMMENT 'The first argument, which is an integer. The integer is guaranteed to be positive.', b STRING COMMENT 'The second argument, which is a string. The string should be more than 100 characters long.')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Multiline arg description example'
    AS $$
return f"{a}-{b}"
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_list_input():
    def func_with_list(a: List[int]) -> str:
        """
        A function that accepts a list of integers.

        Args:
            a: A list of integers
        
        Returns:
            str: A string representation of the list
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_list, "Function with list input", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_list(a ARRAY<INTEGER> COMMENT 'A list of integers')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with list input'
    AS $$
return str(a)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_map_input():
    def func_with_map(a: Dict[str, int]) -> str:
        """
        A function that accepts a map with string keys and integer values.

        Args:
            a: A map with string keys and integer values
        
        Returns:
            str: A string representation of the map
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_map, "Function with map input", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_map(a MAP<STRING, INTEGER> COMMENT 'A map with string keys and integer values')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with map input'
    AS $$
return str(a)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_list_return():
    def func_with_list_return() -> List[int]:
        """
        A function that returns a list of integers.

        Returns:
            list: A list of integers
        """
        return [1, 2, 3]

    sql_body = generate_sql_function_body(func_with_list_return, "Function with list return", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_list_return()
    RETURNS ARRAY<INTEGER>
    LANGUAGE PYTHON
    COMMENT 'Function with list return'
    AS $$
return [1, 2, 3]
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_map_return():
    def func_with_map_return() -> Dict[str, int]:
        """
        A function that returns a map with string keys and integer values.

        Returns:
            dict: A map with string keys and integer values
        """
        return {"a": 1, "b": 2}

    sql_body = generate_sql_function_body(func_with_map_return, "Function with map return", "test_catalog", "test_schema")
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_map_return()
    RETURNS MAP<STRING, INTEGER>
    LANGUAGE PYTHON
    COMMENT 'Function with map return'
    AS $$
return {"a": 1, "b": 2}
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_invalid_list_type():
    def func_with_invalid_list(a: List[Any]) -> str:
        """
        A function that accepts a list of any type.

        Args:
            a: A list of any type
        
        Returns:
            str: A string representation of the list
        """
        return str(a)

    with pytest.raises(ValueError, match=re.escape("Error in parameter 'a': type typing.List[typing.Any] is not supported")):
        generate_sql_function_body(func_with_invalid_list, "Function with invalid list", "test_catalog", "test_schema")


def test_function_with_invalid_map_type():
    def func_with_invalid_map(a: Dict[str, Any]) -> str:
        """
        A function that accepts a map with string keys and any values.

        Args:
            a: A map with string keys and any values
        
        Returns:
            str: A string representation of the map
        """
        return str(a)

    with pytest.raises(ValueError, match=re.escape("Error in parameter 'a': type typing.Dict[str, typing.Any] is not supported")):
        generate_sql_function_body(func_with_invalid_map, "Function with invalid map", "test_catalog", "test_schema")


def test_function_with_invalid_list_return():
    def func_with_invalid_list_return() -> List[Any]:
        """
        A function that returns a list of any type.

        Returns:
            list: A list of any type
        """
        return [1, "string", True]

    with pytest.raises(ValueError, match=re.escape("Error in return type: typing.List[typing.Any] is not supported")):
        generate_sql_function_body(func_with_invalid_list_return, "Function with invalid list return", "test_catalog", "test_schema")


def test_function_with_invalid_map_return():
    def func_with_invalid_map_return() -> Dict[str, Any]:
        """
        A function that returns a map with string keys and any values.

        Returns:
            dict: A map with string keys and any values
        """
        return {"a": 1, "b": "string"}

    with pytest.raises(ValueError, match=re.escape("Error in return type: typing.Dict[str, typing.Any] is not supported")):
        generate_sql_function_body(func_with_invalid_map_return, "Function with invalid map return", "test_catalog", "test_schema")

def test_function_with_dict_list_input():
    def func_with_dict_list(a: Dict[str, List[str]]) -> str:
        """
        A function that accepts a dictionary with string keys and list of string values.

        Args:
            a: A dictionary with string keys and list of string values
        
        Returns:
            str: A string representation of the dictionary
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_dict_list, "Function with Dict[str, List[str]] input", "test_catalog", "test_schema")

    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_dict_list(a MAP<STRING, ARRAY<STRING>> COMMENT 'A dictionary with string keys and list of string values')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with Dict[str, List[str]] input'
    AS $$
return str(a)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_list_of_dict_input():
    def func_with_list_of_map(a: List[Dict[str, int]]) -> str:
        """
        A function that accepts a list of maps with string keys and integer values.

        Args:
            a: A list of maps with string keys and integer values
        
        Returns:
            str: A string representation of the list of maps
        """
        return str(a)

    sql_body = generate_sql_function_body(func_with_list_of_map, "Function with List[Dict[str, int]] input", "test_catalog", "test_schema")

    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_list_of_map(a ARRAY<MAP<STRING, INTEGER>> COMMENT 'A list of maps with string keys and integer values')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with List[Dict[str, int]] input'
    AS $$
return str(a)
    $$;
    """
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_heavily_nested_structure():
    def func_with_heavily_nested(a: List[Dict[str, List[Dict[str, int]]]]) -> str:
        """
        A function that accepts a heavily nested structure of lists and dictionaries.

        Args:
            a: A list of dictionaries where the key is a string and the value is a list of dictionaries
               with string keys and integer values.
        
        Returns:
            str: A string representation of the nested structure
        """
        return str(a)

    sql_body = generate_sql_function_body(
        func_with_heavily_nested,
        "Function with heavily nested structure List[Dict[str, List[Dict[str, int]]]]",
        "test_catalog",
        "test_schema"
    )

    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_heavily_nested(a ARRAY<MAP<STRING, ARRAY<MAP<STRING, INTEGER>>>> COMMENT 'A list of dictionaries where the key is a string and the value is a list of dictionaries with string keys and integer values.')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with heavily nested structure List[Dict[str, List[Dict[str, int]]]]'
    AS $$
return str(a)
    $$;
    """
    
    assert sql_body.strip() == expected_sql.strip()

def test_function_with_extra_docstring_params_ignored():
    def func_with_extra_param_in_docstring(a: int) -> str:
        """
        A function with extra parameter in docstring.

        Args:
            a: The first argument
            b: An extra parameter not in function signature
        
        Returns:
            str: The string representation of the first argument
        """
        return str(a)

    # We expect the generated SQL to ignore 'b' since it's not in the function signature
    sql_body = generate_sql_function_body(
        func_with_extra_param_in_docstring,
        "Function with extra docstring parameter",
        "test_catalog",
        "test_schema"
    )
    
    expected_sql = """
    CREATE OR REPLACE FUNCTION test_catalog.test_schema.func_with_extra_param_in_docstring(a INTEGER COMMENT 'The first argument')
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT 'Function with extra docstring parameter'
    AS $$
return str(a)
    $$;
    """
    
    assert sql_body.strip() == expected_sql.strip()


def test_function_with_plain_list_type():
    def func_with_plain_list_type(a: List) -> str:
        """
        A function with a plain List as a parameter type.

        Args:
            a: A plain list without inner types

        Returns:
            str: A string representation of the list
        """
        return str(a)

    with pytest.raises(ValueError, match="Error in parameter 'a': type typing.List is not supported."):
        generate_sql_function_body(
            func_with_plain_list_type,
            "Function with plain List type",
            "test_catalog",
            "test_schema"
        )

def test_function_with_plain_dict_type():
    def func_with_plain_dict_type(a: Dict) -> str:
        """
        A function with a plain Dict as a parameter type.

        Args:
            a: A plain dict without inner types

        Returns:
            str: A string representation of the dict
        """
        return str(a)

    with pytest.raises(ValueError, match="Error in parameter 'a': type typing.Dict is not supported."):
        generate_sql_function_body(
            func_with_plain_dict_type,
            "Function with plain Dict type",
            "test_catalog",
            "test_schema"
        )

def test_function_with_plain_list_return_type():
    def func_with_plain_list_return() -> List:
        """
        A function with a plain List as a return type.

        Returns:
            list: A plain list without inner types
        """
        return [1, 2, 3]

    with pytest.raises(ValueError, match="Error in return type: typing.List is not supported."):
        generate_sql_function_body(
            func_with_plain_list_return,
            "Function with plain List return type",
            "test_catalog",
            "test_schema"
        )

def test_function_with_plain_dict_return_type():
    def func_with_plain_dict_return() -> Dict:
        """
        A function with a plain Dict as a return type.

        Returns:
            dict: A plain dict without inner types
        """
        return {"key": "value"}

    with pytest.raises(ValueError, match="Error in return type: typing.Dict is not supported."):
        generate_sql_function_body(
            func_with_plain_dict_return,
            "Function with plain Dict return type",
            "test_catalog",
            "test_schema"
        )