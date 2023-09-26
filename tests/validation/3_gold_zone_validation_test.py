import pytest 
import pandas as pd 

import pyarrow.dataset as ds

@pytest.fixture(scope="session")
def dataset_path(env_config):  
    BUCKET_NAME = env_config.get("BUCKET_NAME")
    DESTINATION_PREFIX = env_config.get("EVENT_GOLD_ZONE_PREFIX")

    return f"gs://{BUCKET_NAME}/{DESTINATION_PREFIX}"


def test_is_matching_parquet_schema(dataset_path,compare_schema):
    parquet_dataset =  ds.dataset(dataset_path, 
                                format="parquet",
                                partitioning="hive")
    schema = parquet_dataset.schema.remove_metadata() 

    assert len(schema) == len(compare_schema)
    for s in schema:
        assert s in compare_schema
        
def secret_test_gold_zone_1(dataset_path):
    assert True

def secret_test_gold_zone_2(dataset_path):
    assert True 
        

        
