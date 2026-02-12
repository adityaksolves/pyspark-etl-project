
import os
import sys
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import logging
load_dotenv()

def load_config(env="dev"):
    with open(f"configs/{env}_config.yaml","r") as f:
        return yaml.safe_load(f)

def get_logger():
    app_name = os.getenv("APP_NAME","ETL Pipeline")
    logger = logging.getLogger(app_name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

def get_spark_session(config):
    return SparkSession.builder \
        .appName(config['project']['name']) \
        .config("spark.executor.memory", config['spark_params']['spark.executor.memory']) \
        .config("spark.sql.shuffle.partitions", config['spark_params']['spark.sql.shuffle.partitions']) \
        .config("spark.jars", os.getenv('JAR_PATH')) \
        .getOrCreate()

def log_metadata(spark, config, status_code, status_text, error_message=None):
    jdbc_url = config['database']['url']

    target_table = config['tables']['metadata_table']

    properties = {
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASSWORD'),
        "driver": config['database']['driver']
    }

    schema = StructType([
        StructField("run_timestamp", TimestampType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("status_text", StringType(), True),
        StructField("error_message", StringType(), True)
    ])

    log_data = [(datetime.now(), status_code, status_text, error_message)]
    log_df = spark.createDataFrame(log_data, schema=schema)

    log_df.write.jdbc(url = jdbc_url, table=target_table, mode = "append", properties=properties)




