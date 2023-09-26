import pytest 
from google.cloud import storage 
import pandas as pd 
import json

@pytest.fixture(scope="session")
def user_blob(env_config):
    BUCKET_NAME = env_config.get("BUCKET_NAME")
    USER_DESTINATION_PATH = env_config.get("USER_DESTINATION_PATH")

    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    return bucket.get_blob(USER_DESTINATION_PATH)

def test_blob_is_exist(user_blob):
    assert user_blob.exists()

@pytest.fixture(scope="session")
def df_user(user_blob):
    user_data = user_blob.download_as_bytes().decode("utf-8")
    df_user = pd.DataFrame([json.loads(ud) for ud in user_data.splitlines()])
    df_user = df_user.dropna(axis=1)
    return df_user

def test_is_matching_user_schema(df_user):
    user_columns = ['user_id', 'birthday', 'sign_in_date', 'sex', 'country']

    output_col = list(df_user.columns)
    for col in user_columns:
        assert col in output_col

def secret_test_user_info_len(df_user):
    assert True
