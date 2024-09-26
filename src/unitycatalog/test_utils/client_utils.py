import logging
import os
from contextlib import contextmanager

import pytest

from unitycatalog.ai.client import set_uc_function_client
from unitycatalog.ai.databricks import DatabricksFunctionClient

USE_SERVERLESS = "USE_SERVERLESS"

_logger = logging.getLogger(__name__)


def requires_databricks(test_func):
    return pytest.mark.skipif(
        os.environ.get("TEST_IN_DATABRICKS", "false").lower() != "true",
        reason="This function test relies on connecting to a databricks workspace",
    )(test_func)


# TODO: CI -- two test cases
# 1. python 3.10 with databricks-connect 15.1.0, no cluster_id, use serverless
# 2. python 3.9 with databricks-sdk, with cluster_id
@pytest.fixture
def client() -> DatabricksFunctionClient:
    # with mock.patch(
    #     "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
    #     return_value=mock.Mock(),
    # ):
    #     return DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")
    return DatabricksFunctionClient(warehouse_id="63f9f8ed4cedd92b")


@pytest.fixture
def serverless_client() -> DatabricksFunctionClient:
    return DatabricksFunctionClient()


def get_client() -> DatabricksFunctionClient:
    # with mock.patch(
    #     "unitycatalog.ai.databricks.get_default_databricks_workspace_client",
    #     return_value=mock.Mock(),
    # ):
    if os.environ.get(USE_SERVERLESS, "false").lower() == "true":
        return DatabricksFunctionClient()
    else:
        return DatabricksFunctionClient(warehouse_id="63f9f8ed4cedd92b")
        return DatabricksFunctionClient(warehouse_id="warehouse_id", cluster_id="cluster_id")


@contextmanager
def set_default_client(client: DatabricksFunctionClient):
    try:
        set_uc_function_client(client)
        yield
    finally:
        set_uc_function_client(None)
