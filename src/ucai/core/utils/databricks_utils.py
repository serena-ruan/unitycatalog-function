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


class _NoOpStatementExecutionAPI:
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
            result=ResultData(data_array=[[]]),
            status=StatementStatus(state=StatementState.SUCCEEDED),
        )


class _NoOpAPIClient:
    def do(self, *args, **kwargs):
        return {}


class _NoOpFunctionsAPI:
    _api: _NoOpAPIClient = _NoOpAPIClient()

    def get(self, *args, **kwargs):
        from databricks.sdk.service.catalog import FunctionInfo

        return FunctionInfo()


class _NoOpWarehouse:
    enable_serverless_compute: bool = True


class _NoOpWarehousesAPI:
    def get(self, *args, **kwargs):
        return _NoOpWarehouse()


# This is a no-op client that can be used as a fallback when the databricks
# WorkspaceClient fails to initialize. It mocks the behavior of WorkspaceClient
# and its methods used in the UCAI codebase, but actually does nothing.
class NoOpClient:
    statement_execution: _NoOpStatementExecutionAPI = _NoOpStatementExecutionAPI()
    functions: _NoOpFunctionsAPI = _NoOpFunctionsAPI()
    warehouses: _NoOpWarehousesAPI = _NoOpWarehousesAPI()
