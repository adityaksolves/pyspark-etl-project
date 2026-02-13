import os
import sys
from src.utils import get_spark_session, log_metadata, get_logger, load_config
from src.etl.extract import extract_data
from src.etl.transform import transform_data
from src.etl.load import load_data

def run_pipeline():
    #env is selected to dev by default and hence our original data is safe
    env = os.getenv("ENV","dev")
    config = load_config(env)

    logger = get_logger()
    spark = get_spark_session(config)

    try:
        logger.info(f"Starting Pipeline in {env} mode...")

        #Extract
        sales_df , product_df = extract_data(spark, config)
        logger.info("Data Extracted")

        #Transform
        report_df = transform_data(sales_df, product_df)
        logger.info("Transformation Completed")

        #Load
        load_data(report_df, config ,logger)
        logger.info("Data Loaded into DB")

        #Logging Metadata
        log_metadata(spark,config, config['status_codes']['success_code'], config['status_codes']['success_msg'])
        logger.info("Pipeline Finished Successfully")

    except Exception as e:
        logger.error(f"Pipeline Failed : {str(e)}")
        log_metadata(spark, config,config['status_codes']['failed_code'], "FAILED",str(e))
        sys.exit(1)

    finally:
        logger.info("Stopping Spark Session.")
        spark.stop()

if __name__ == "__main__":
    run_pipeline()


