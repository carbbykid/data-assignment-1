import pytest 
import os 
from decouple import Config, RepositoryEnv

@pytest.fixture(scope="session")
def env_config():
    conftest_dir  = os.path.dirname(os.path.abspath(__file__))
    env_file_path = os.path.join(conftest_dir, ".test.env")
    
    return Config(RepositoryEnv(env_file_path))

@pytest.fixture(scope="function")
def compare_schema():
    import pyarrow as pa
    return pa.schema([
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("timestamp", pa.timestamp("ms")),
        ("user_id", pa.int32()),
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("day", pa.int32()),
        ("location", pa.string()),
        ("device", pa.string()),
        ("ip_address", pa.string()),
        ("event_attribute", pa.list_(
            pa.struct([
                ("key", pa.string()),
                ("int_value", pa.int32()),
                ("float_value", pa.float32()),
                ("string_value", pa.string()),
                ("bool_value", pa.bool_())
            ])
        )),
    ])
