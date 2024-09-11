## Unity Catalog AI library

The Unity Catalog AI library provides convenient APIs to interact with Unity Catalog functions, including functions creation, retrieval and execution.
The library includes clients for interacting with both the Open-Source Unity Catalog server and the Databricks-managed Unity Catalog service.

## Installation

```sh
# install from the source
pip install git+ssh://git@github.com/serena-ruan/unitycatalog-ai.git
```
> [!NOTE]
> Once this package is published to PyPI, users can install via `pip install unitycatalog-ai`


## Get started

### Databricks-managed UC
To use Databricks-managed Unity Catalog with this package, follow the [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html#authentication-for-the-databricks-cli) to authenticate to your workspace and ensure that your access token has workspace-level privilege for managing UC functions.

#### Prerequisites
- Install databricks-sdk package with `pip install databricks-sdk`.
- For accessing the UC functions in Databricks, please create a SQL warehouse following [this instruction](https://docs.databricks.com/en/compute/sql-warehouse/create.html), and save the warehouse id.
- [Optional] Install databricks-connect package with `pip install databricks-connect`.
    - This package is only required for creating UC functions using sql body.
    - If you want to use [serverless compute](https://docs.databricks.com/en/compute/use-compute.html#use-serverless-compute) for function creation, please make sure you have python>=3.10 and databricks-connect==15.1.0.
    - If you want to use [your own cluster](https://docs.databricks.com/en/compute/use-compute.html#create-new-compute-using-a-policy) for function creation, please make sure the cluster is up running and pass the cluster_id when creating DatabricksFunctionClient.

#### Client initialization
```python
from unitycatalog.ai.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient(
    warehouse_id="..." # replace with the warehouse_id
    cluster_id="..." # optional, only pass when you want to use cluster for function creation
)
```

#### Create a UC function
Create a UC function with SQL string should follow [this syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#create-function-sql-and-python).

```python
# make sure you have privilege in the corresponding catalog and schema for function creation
CATALOG = "..."
SCHEMA = "..."
func_name = "test"
sql_body = f"""CREATE FUNCTION {CATALOG}.{SCHEMA}.{func_name}(s string)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  return s
$$
"""

function_info = client.create_function(sql_function_body=sql_body)
```

#### Retrieve a UC function
The client also provides API to get the UC function information details. Note that the function name passed in must be the full name in the format of `<catalog>.<schema>.<function_name>`.

```python
full_func_name = f"{CATALOG}.{SCHEMA}.{func_name}"
client.get_function(full_func_name)
```

#### List UC functions
To get a list of functions stored in a catalog and schema, you can use list API with wildcards to do so.

```python
client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=5)
```

#### Execute a UC function
Parameters passed into execute_function must be a dictionary that maps to the input params defined by the UC function.

```python
result = client.execute_function(full_func_name, {"s": "some_string"})
assert result.value == "some_string"
```
