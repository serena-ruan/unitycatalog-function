import sys

import click
from databricks.sdk import WorkspaceClient

from ucai.test_utils.function_utils import CATALOG


@click.command(help="Clean up functions in a schema")
@click.option(
    "--schema",
    required=True,
    help=f"The schema to clean up functions from, under catalog {CATALOG}",
)
def cleanup_function(schema: str):
    client = WorkspaceClient()
    failed_deletions = []
    function_infos = client.functions.list(catalog_name=CATALOG, schema_name=schema)
    for function_info in function_infos:
        try:
            client.functions.delete(function_info.full_name)
        except Exception:
            failed_deletions.append(function_info.full_name)

    if failed_deletions:
        print(f"Failed to delete the following functions: {failed_deletions}")  # noqa: T201
        sys.exit(1)


if __name__ == "__main__":
    cleanup_function()
