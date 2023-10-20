import json
from decouple import Config, RepositoryEnv
import datetime
import psycopg2

from typing import List

from google.cloud import storage
from psycopg2.extras import DictCursor


def datetime_serializer(obj) -> str:
    """
    Hàm hỗ trợ parse dữ liệu datetime sang json
    """
    if isinstance(obj, (datetime.datetime,datetime.date)):
        return obj.isoformat()

def get_user_info(dbconfig: dict) -> List[dict]:
    """
    Lấy các dữ liệu user_info
    từ bảng user_info trong Postgres

    Args:
        dbconfig (dict): config của database

    Returns:
        List[dict] có dạng
        [
            {
                "user_id" : 1,
                "birthday": "1990-01-01",
                "sign_in_date": "2023-01-01",
                "sex": "Male",
                "country": "Vietnam"
            },
            ....
        ]
    """

    connection = psycopg2.connect(**dbconfig)
    result = None
    try:
        # Get connection PostgreSQL
        cursor = connection.cursor(cursor_factory=DictCursor)
        #TODO: Begin
        # Query sql
        cursor.execute("SELECT * FROM user_info")
        users_data = []
        # Convert data to list[Dict]
        for row in cursor:
            user={"user_id":row[0], "birthday":row[1].strftime("%Y-%m-%d"), "sign_in_date": row[2].strftime("%Y-%m-%d"), "sex": row[3], "country": row[4]}
            users_data.append(user)
        result = users_data
        #TODO: End 
    except Exception as e:
        print(e)
        raise Exception

    finally:
        cursor.close()
        connection.close()

    return result


def upload_from_string(data: str, bucket_name: str, destination_path: str) -> None:
    """
    Upload dữ liệu dạng string của user_info
    chuỗi json theo dòng 
    lên folder GS có dạng

    gs://bucket_name/destination_path

    Args:
        data (str): chuỗi json theo dòng
        bucket_name (str): tên bucket trên gcs
        destination_path (str): tên blob chứa file user_info.json

    Ví dụ: 
        data = "
            {"user_id" : 1,"birthday": "1990-01-01","sign_in_date": "2023-01-01","sex": "Male","country": "Vietnam"} \n
            {"user_id" : 2,"birthday": "1990-01-01","sign_in_date": "2023-01-02","sex": "Male","country": "Lao"} \n
        "
        bucket_name = "mmo_adventure_event_processing"
        destination_path = "bronze-zone/user_info/user_info.json"
    
    Sau khi upload cần phải có: 
        gs://mmo_adventure_event_processing/bronze-zone/user_info/user_info.json
    File user_info.json phải có định dạng:
        {"user_id" : 1,"birthday": "1990-01-01","sign_in_date": "2023-01-01","sex": "Male","country": "Vietnam"} \n
        {"user_id" : 2,"birthday": "1990-01-01","sign_in_date": "2023-01-02","sex": "Male","country": "Lao"} \n
    """
    # Get client cloud
    client = storage.Client()
    #TODO: Begin
    # Get bucket or create bucket
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket = client.create_bucket(bucket_name)
    # Create blob
    blob = bucket.blob(destination_path)
    is_existed = blob.exists()
    if not is_existed:
        # Upload string json to blob and replace into file json    
        blob.upload_from_string(data,content_type='application/json')
    #TODO: End

if __name__ == "__main__":
    """
    Lấy data từ bảng user_info
    và đẩy lên GS
    """
    DOTENV_FILE = ".env"
    env_config = Config(RepositoryEnv(DOTENV_FILE))
    BUCKET_NAME = env_config.get("BUCKET_NAME")
    USER_DESTINATION_PATH = env_config.get("USER_DESTINATION_PATH")

    dbconfig = {
        "host": env_config.get("HOST"),
        "port": env_config.get("DB_PORT"),
        "user": env_config.get("DB_USER"),
        "password": env_config.get("PASSWORD"),
        "database": env_config.get("DB"),
    }
    user_info = get_user_info(dbconfig)
    #dumps key object to string, and handle data is't object to iso time, prehension, join array to string
    data = "\n".join([json.dumps(u, default=datetime_serializer) for u in user_info])

    upload_from_string(data=data, bucket_name=BUCKET_NAME, destination_path=USER_DESTINATION_PATH)
