import sys
from src.utils import get_spark_session, log_metadata, get_logger
from src.etl.extract import extract_data
from src.etl.transform import transform_data
from src.etl.load import load_data

def run_pipeline():
    logger = get_logger()
    spark = get_spark_session()

    try:
        logger.info("Starting Pipeline ...")
        #Extract
        sales_df , product_df = extract_data(spark)
        logger.info("Data Extracted")

        #Transform
        report_df = transform_data(sales_df, product_df)
        logger.info("Transformation Completed")

        #Load
        load_data(report_df, "public.category_sales_report", logger)
        logger.info("Data Loaded into DB")

        #Logging Metadata
        log_metadata(spark, 0 , "SUCCESS")
        logger.info("Pipeline Finished Successfully")

    except Exception as e:
        logger.error(f"Pipeline Failed : {str(e)}")
        log_metadata(spark, 2 , "FAILED",str(e))
        sys.exit(1)

    finally:
        logger.info("Stopping Spark Session.")
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
