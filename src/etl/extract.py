import os
def extract_data(spark, config):
    url = config['database']['url']
    properties ={
        "user":os.getenv("DB_USER"),
        "password":os.getenv("DB_PASSWORD"),
        "driver":os.getenv("DB_DRIVER")
    }

    #Reading sales table
    sales_df = spark.read.jdbc(url=url, table =config['tables']['source_sales'],properties = properties)

    #Reading Products Table
    product_df = spark.read.jdbc(url=url, table = config['tables']['source_products'],properties = properties)

    return sales_df, product_df