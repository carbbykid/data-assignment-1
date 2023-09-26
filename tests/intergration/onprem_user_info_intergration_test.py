from batch_job.onprem_batch_job.snapshot_user_info import get_user_info

def test_get_user_info(env_config):     
    import pandas as pd  
    
    dbconfig = {
        "host": env_config.get("HOST"),
        "port": env_config.get("DB_PORT"),
        "user": env_config.get("DB_USER"),
        "password": env_config.get("PASSWORD"),
        "database": env_config.get("DB"),
    }
    user_info = get_user_info(dbconfig)
    # Check len of user_info 

    assert len(user_info) == 100
    user_columns = ['user_id', 'birthday', 'sign_in_date', 'sex', 'country']
    df_user = pd.DataFrame(user_info)
    df_user = df_user.dropna(axis=1)

    # Check output of user_info
    output_col = list(df_user.columns)
    for col in user_columns:
        assert col in output_col
