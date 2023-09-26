import pytest 
from google.cloud import storage
import re 

@pytest.fixture(scope="session")
def list_blobs_event(env_config):     
    BUCKET_NAME = env_config.get("BUCKET_NAME")
    EVENT_DESTINATION_PREFIX = env_config.get("EVENT_BRONZE_ZONE_PREFIX")
    
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    return bucket.list_blobs(prefix=EVENT_DESTINATION_PREFIX)

def test_matching_event_partern(list_blobs_event):
    pattern = r"bronze-zone/event_info/\d{4}/\d{2}/\d{2}/[A-Za-z0-9\-]+\.json"
    for blob in list_blobs_event:
        assert re.match(pattern, blob.name)


