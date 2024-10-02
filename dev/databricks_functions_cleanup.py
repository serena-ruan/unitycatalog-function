import sys

import click
from databricks.sdk import WorkspaceClient


@click.command(help="Clean up functions in a schema")
@click.option(
    "--catalog",
    required=True,
    help=f"The catalog to clean up functions from",
)
@click.option(
    "--schema",
    required=True,
    help=f"The schema to clean up functions from",
)
def cleanup_functions(catalog: str, schema: str):
    client = WorkspaceClient()
    failed_deletions = {}
    function_infos = client.functions.list(catalog_name=catalog, schema_name=schema)
    for function_info in function_infos:
        try:
            client.functions.delete(function_info.full_name)
        except Exception as e:
            failed_deletions[function_info.full_name] = str(e)

    if failed_deletions:
        sys.stderr.write(f"Failed to delete the following functions: {failed_deletions}")
        sys.exit(1)


if __name__ == "__main__":
    cleanup_functions()
