import os

def load_data(df, config, logger):
    table_name = config['tables']['target_report']
    write_mode = config.get('load_settings',{}).get('mode','overwrite')


    logger.info(f"Starting load into {table_name}")
    jdbc_url = config['database']['url']
    properties = {
        "user":os.getenv("DB_USER"),
        "password":os.getenv("DB_PASSWORD"),
        "driver":os.getenv("DB_DRIVER")
    }

    print(f"Loading data into {table_name}")
    df.write.jdbc(url = jdbc_url, table = table_name, mode = write_mode, properties=properties)

    print(f"Successfully loaded data into {table_name}")
    logger.info("Successfully loaded data")