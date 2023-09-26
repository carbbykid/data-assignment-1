import pytest 
from batch_job.onprem_batch_job.upload_event import encode_destination_path

test_data = [ 
    ("./data/2018-01-01/hello.json", "mmo-zone", "mmo-zone/2018/01/01/hello.json"),
    ("/Users/aaron/Desktop/data/2018-01-01/hello.json", "brone-zone", "brone-zone/2018/01/01/hello.json"),
    ("../../../data/2023-09-10/different_name.json", "gold-zone", "gold-zone/2023/09/10/different_name.json")
]

@pytest.mark.parametrize("local_file_path,,destination_prefix,expected",test_data)
def test_encode_destination_path(local_file_path,destination_prefix,expected):     
    result = encode_destination_path(local_file_path,destination_prefix)
    assert result == expected

