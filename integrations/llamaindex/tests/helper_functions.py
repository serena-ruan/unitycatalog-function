import pytest
import os


def requires_databricks(test_func):
    return pytest.mark.skipif(
        os.environ.get("TEST_IN_DATABRICKS", "false").lower() != "true",
        reason="This function test relies on connecting to a databricks workspace",
    )(test_func)