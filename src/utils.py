
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import logging
load_dotenv()

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


def get_spark_session():
    app_name = os.getenv('APP_NAME')
    jar_path = os.getenv('JAR_PATH')

    return SparkSession.builder\
        .appName(app_name)\
        .config("spark.jars", jar_path)\
        .getOrCreate()

def log_metadata(spark, status_code, status_text, error_message=None):
    jdbc_url = os.getenv('DB_URL')

    properties = {
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASSWORD'),
        "driver": os.getenv('DB_DRIVER')
    }

    schema = StructType([
        StructField("run_timestamp", TimestampType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("status_text", StringType(), True),
        StructField("error_message", StringType(), True)
    ])

    log_data = [(datetime.now(), status_code, status_text, error_message)]
    log_df = spark.createDataFrame(log_data, schema=schema)

    log_df.write.jdbc(url = jdbc_url, table="public.pipeline_run_metadata", mode = "append", properties=properties)




