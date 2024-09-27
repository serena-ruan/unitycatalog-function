import os
from contextlib import contextmanager
from unittest import mock

import pytest

from ucai.core.client import set_uc_function_client
from ucai.core.databricks import DatabricksFunctionClient

USE_SERVERLESS = "USE_SERVERLESS"


def requires_databricks(test_func):
    return pytest.mark.skipif(
        os.environ.get("TEST_IN_DATABRICKS", "false").lower() != "true",
        reason="This function test relies on connecting to a databricks workspace",
    )(test_func)


# TODO: CI -- only support python 3.10, test with databricks-connect 15.1.0 + serverless
@pytest.fixture
def client() -> DatabricksFunctionClient:
    with mock.patch(
        "ucai.core.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        return DatabricksFunctionClient(warehouse_id="warehouse_id")


@pytest.fixture
def serverless_client() -> DatabricksFunctionClient:
    return DatabricksFunctionClient()


def get_client() -> DatabricksFunctionClient:
    with mock.patch(
        "ucai.core.databricks.get_default_databricks_workspace_client",
        return_value=mock.Mock(),
    ):
        if os.environ.get(USE_SERVERLESS, "false").lower() == "true":
            return DatabricksFunctionClient()
        else:
            return DatabricksFunctionClient(warehouse_id="warehouse_id")


@contextmanager
def set_default_client(client: DatabricksFunctionClient):
    try:
        set_uc_function_client(client)
        yield
    finally:
        set_uc_function_client(None)
