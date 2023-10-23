from typing import List
import pandas as pd
import pyarrow as pa
from pyarrow import json as pj
from pyarrow import parquet as pq

from decouple import Config, RepositoryEnv
from google.cloud import storage
from loguru import logger
import json


def list_file_in_bucket(bucket_name: str, prefix: str) -> List[storage.Blob]:
    """
    Hàm này dùng để trả về một list các blob
    từ google cloud storage có uri bắt đầu ở dạng
        gs://bucket_name/prefix/*

    Args:
        bucket_name (str): Tên bucket
        prefix (str): prefix

    Returns:
        List[storage.Blob]: List các blob của bucket google cloud storage

    Ví dụ:
        Storage gs://mmo_adventure có chứa 3 file:
                - gs://mmo_adventure/bronze-zone/user_info/user.json
                - gs://mmo_adventure/bronze-zone/user_info/another/user.json
                - gs://mmo_adventure/bronze-zone/event/2023/08/09/event.json

        Ta cần lấy tất cả các file ở folder event:
            gs://mmo_adventure/bronze-zone/event/*

        bucket_name = "mmo_adventure"
        prefix = "bronze-zone/event"
        >> list_file_in_bucket("mmo_adventure","bronze-zone/event")

        Kết quả mong muốn:
        [
            Blob(blob_name = gs://mmo_adventure/bronze-zone/event/2023/08/09/event.json,...),
        ]
    """
    storage_client = storage.Client()
    list_file = []
    # TODO BEGIN CODE
    for blob in storage_client.list_blobs(bucket_name,prefix=prefix):
        list_file.append(blob)
    # TODO END
    return list_file


def _transform_event_attribute(event: dict) -> list:
    """
        Hàm này nhận một dictionary event_attribute 
        và trả về một list các dictionary theo tiêu chí sau:
            Trước khi biến đổi:
            event = {
                    "revenue": 123.0,
                    "transaction_id":"3124wfdb6332asdc1332"
            }
            Sau khi biến đổi:
            "event_attribute" : [{
                            {
                                "key": "revenue",
                                "int_value": None,
                                "float_value": 123, <- float
                                "string_value": None,
                                "bool_value": None,
                            },
                            {
                                "key": "transaction_id",
                                "int_value": None,
                                "float_value": None,
                                "string_value": "3124wfdb6332asdc1332", <-
                                "bool_value": None,
                            }]
                        }

            Các kiểu event_type có thể có 
                Nếu event_type = purchase
                    { ...
                        "event_attribute" : [{
                            {
                                "key": "revenue",
                                "int_value": None,
                                "float_value": 123, <- float
                                "string_value": None,
                                "bool_value": None,
                            },
                            {
                                "key": "transaction_id",
                                "int_value": None,
                                "float_value": None,
                                "string_value": "3124wfdb6332asdc1332", <-
                                "bool_value": None,
                            }]
                        }
                Nếu event_type = play
                    { ...,
                        "event_attribute" : [
                        {
                            "key": "play_time",
                            "int_value": 100,
                            "float_value": None,
                            "string_value": None,
                            "bool_value": None,
                        }]
                    }
                Nếu event_type = view
                    { ...,
                        "event_attribute" : [{
                        {
                            "key": "creative_id",
                            "int_value": 1,
                            "float_value": None,
                            "string_value": None,
                            "bool_value": None,
                        },
                        {
                            "key": "view_time",
                            "int_value": 26,
                            "float_value": None,
                            "string_value": None,
                            "bool_value": None,
                        },
                        {
                            "key": "is_click",
                            "int_value": None,
                            "float_value": None,
                            "string_value": None,
                            "bool_value": True,
                        }]
                    }
                Nếu event_type = log_in hoặc log_out
                    {...,
                        "event_attribute": []
                    }

    Args:
        event (dict): dictionary của event_attribute

    Returns:
        list: _description_
    """
    transformed_data = []
    # TODO: Begin
    # isBool = (isinstance(False,int) and type(False) != bool)s
    if event:
        transformed_data = [
            { "key":key ,"int_value": (isinstance(value,int) and type(value) != bool) and value or None, "float_value": isinstance(value,float) and value or None,"string_value": isinstance(value,str) and value or None, "bool_value": value if isinstance(value, bool) else None} if key is not None else [] for key, value in event.items()]
    else:
        transformed_data = event
    # TODO: End
    return transformed_data


def extract_transform_load_event_to_parquet(
    blob: storage.Blob,
    bucket_name: str,
    destination_prefix: str,
    schema: pa.Schema,
) -> None:
    """
    Hàm này nhận 1 object blob của folder event_info
    và thực hiện các bước sau

        - Đọc nội dung của object blob đó.
        - Parse nội dung của object blob từ json line.
        - Biến đổi event_attribute theo hàm _transform_event_attribute
        - Load data thành file parquet partition theo year,month,day dựa trên timestamp:
            ví dụ timestamp = "2023-08-09 12:00:00" -> ghi vào partition: 
                gs://bucket_name/destination_prefix/year=2023/month=8/day=9

    Args:
        blob (storage.Blob): Object blob của google cloud storage
        bucket_name (str): Tên bucket
        destination_prefix (str): prefix
        schema (pa.Schema): Schema của file parquet
    Returns:
        None

    Ví dụ:
        Input:
            Blob(blob_name = gs://mmo_adventure/bronze-zone/2023/08/09/event.json,...)
            bucket_name = "mmo_adventure"
            destination_prefix = "gold-zone/event_info"

        Kết quả mong muốn:
            Ghi ra file parquet:
                - gs://mmo_adventure/gold-zone/event_info/year=2023/month=8/day=9/something_have_timestamp_2023_08_09_12_00_00.parquet
                - gs://mmo_adventure/gold-zone/event_info/year=2023/month=8/day=9/something_also_have_timestamp_2023_08_09_12_00_00.parquet
    """
    # TODO: Begin
    data = blob.download_as_bytes()
    parsed_data = []
    for line in data.decode('utf-8').split("\n"):
        if line:
            # convert string json to dict
            event=json.loads(line)
            parsed_data.append({**event,**{"event_attribute":_transform_event_attribute(event["event_attribute"])}})
            # attribute_of_line = pd.read_json(line, lines=True).to_dict()["event_attribute"]
        else: 
            continue
        
    ### CONVERT JSON TO DATAFRAME: tao file json -> create schema -> dung pyarrow.json.read_json(jsonfile, schema) -> table -> table.to_pandas -> dataframe
    ### CONVERT DATAFRAME to TABLE: pa.Table.from_pandas(df) -> dataframe
    # Convert list dict to list json  and write file
    with open("dict_to_json.json", "w",encoding="utf-8") as outfile: 
        for item in parsed_data:
            outfile.write(json.dumps(item) + "\n")

    parse_opt = pj.ParseOptions(
        explicit_schema = schema
    )
    # How to read json with pyarrow from json string, don't need write file
    table = pj.read_json("./dict_to_json.json",parse_options=parse_opt)

    # pq.write_table(table,"table.parquet", compression="snappy")

    # How to read parquet from table, don't need write file
    # pd.read_parquet("./table.parquet",engine = "pyarrow")

    # Convert Pyarrow sang pandas
    df = table.to_pandas(types_mapper = pd.ArrowDtype)
    
    df['datetime'] = df['timestamp'].str.split(" ").map(lambda x: x[0])
    df['year'] = df['datetime'].str.split("-").map(lambda x: x[0]).map(int)
    df['month'] = df['datetime'].str.split("-").map(lambda x: x[1]).map(int)
    df['day'] = df['datetime'].str.split("-").map(lambda x: x[2]).map(int)

    new_schema = pa.schema([
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
    
    new_table = pa.Table.from_pandas(df, schema=new_schema)

    gcs = pa.fs.GcsFileSystem(anonymous=False)
    pq.write_to_dataset(new_table,
                                root_path=f'{bucket_name}/{destination_prefix}',
                                partition_cols=['year','month','day'],
                                filesystem=gcs)
    # TODO: End


if __name__ == "__main__":
    DOTENV_FILE = "./.env"
    env_config = Config(RepositoryEnv(DOTENV_FILE))

    BUCKET_NAME = env_config.get("BUCKET_NAME")
    SOURCE_PREFIX = env_config.get("EVENT_SOURCE_PREFIX")
    DESTINATION_PREFIX = env_config.get("EVENT_GOLD_ZONE_PREFIX")
    """
        Tạo schema
    """
    schema = pa.schema([
    #TODO: Begin 
        ('event_id', pa.string()),
        ('event_type', pa.string()),
        ('timestamp',  pa.string()),
        ('user_id', pa.int32()),
        ('location', pa.string()),
        ('device', pa.string()),
        ('ip_address', pa.string()),
        ('event_attribute', pa.list_(pa.struct([
            ('key', pa.string()),
            ('int_value', pa.int32()),
            ('float_value', pa.float32()),
            ('string_value', pa.string()),
            ('bool_value', pa.bool_())
    ])))
    #TODO: End 
    ])

    for blob in list_file_in_bucket(bucket_name=BUCKET_NAME, prefix=SOURCE_PREFIX):
        logger.info(f"Process file {blob.name}")
        extract_transform_load_event_to_parquet(
            blob=blob,
            bucket_name=BUCKET_NAME,
            destination_prefix=DESTINATION_PREFIX,
            schema=schema
        )