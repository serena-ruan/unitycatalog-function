import logging
from typing import TYPE_CHECKING

_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def get_default_databricks_workspace_client() -> "WorkspaceClient":
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()
    except ImportError as e:
        raise ImportError(
            "Could not import databricks-sdk python package. "
            "If you want to use databricks backend then "
            "please install it with `pip install databricks-sdk`."
        ) from e
    except Exception as e:
        _logger.warning(
            "Failed to initialize databricks workspace client "
            f"Falling back to NoOpClient. Error: {e}"
        )
    return NoOpClient()


class NoOpStatementExecutionAPI:
    def execute_statement(self, *args, **kwargs):
        from databricks.sdk.service.sql import (
            ResultData,
            ResultManifest,
            StatementResponse,
            StatementState,
            StatementStatus,
        )

        return StatementResponse(
            manifest=ResultManifest(truncated=False),
            result=ResultData(data_array=[["null"]]),
            status=StatementStatus(state=StatementState.SUCCEEDED),
        )


class NoOpAPIClient:
    def do(self, *args, **kwargs):
        return {}


class NoOpFunctionsAPI:
    _api: NoOpAPIClient = NoOpAPIClient()

    def get(self, *args, **kwargs):
        from databricks.sdk.service.catalog import FunctionInfo

        return FunctionInfo()


class NoOpWarehouse:
    enable_serverless_compute: bool = True


class NoOpWarehousesAPI:
    def get(self, *args, **kwargs):
        return NoOpWarehouse()


class NoOpClient:
    statement_execution: NoOpStatementExecutionAPI = NoOpStatementExecutionAPI()
    functions: NoOpFunctionsAPI = NoOpFunctionsAPI()
    warehouses: NoOpWarehousesAPI = NoOpWarehousesAPI()
