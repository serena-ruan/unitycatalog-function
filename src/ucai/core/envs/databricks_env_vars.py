import os


class _EnvironmentVariable:
    def __init__(self, name, default_value):
        self.name = name
        self.default_value = str(default_value)

    def get(self):
        return os.getenv(self.name, self.default_value)

    def set(self, value):
        os.environ[self.name] = str(value)

    def remove(self):
        os.environ.pop(self.name, None)


EXECUTE_FUNCTION_WAIT_TIMEOUT = _EnvironmentVariable("EXECUTE_FUNCTION_WAIT_TIMEOUT", "30s")
EXECUTE_FUNCTION_ROW_LIMIT = _EnvironmentVariable("EXECUTE_FUNCTION_ROW_LIMIT", "100")
EXECUTE_FUNCTION_BYTE_LIMIT = _EnvironmentVariable("EXECUTE_FUNCTION_BYTE_LIMIT", "4096")
UNITYCATALOG_AI_CLIENT_EXECUTION_TIMEOUT = _EnvironmentVariable("UNITYCATALOG_AI_CLIENT_EXECUTION_TIMEOUT", "120")
UC_AI_CLIENT_EXECUTION_RESULT_ROW_LIMIT = _EnvironmentVariable("UC_AI_CLIENT_EXECUTION_RESULT_ROW_LIMIT", "100")
