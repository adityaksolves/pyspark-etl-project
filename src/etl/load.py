import os

def load_data(df, table_name, logger):
    logger.info(f"Starting load into {table_name}")
    jdbc_url = os.getenv("DB_URL")
    properties = {
        "user":os.getenv("DB_USER"),
        "password":os.getenv("DB_PASSWORD"),
        "driver":os.getenv("DB_DRIVER")
    }

    print(f"Loading data into {table_name}")
    df.write.jdbc(url = jdbc_url, table = table_name, mode = "overwrite", properties=properties)

    print(f"Successfully loaded data into {table_name}")
    logger.info("Successfully loaded data")