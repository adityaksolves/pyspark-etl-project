import os
def extract_data(spark):
    url = os.getenv("DB_URL")
    properties ={
        "user":os.getenv("DB_USER"),
        "password":os.getenv("DB_PASSWORD"),
        "driver":os.getenv("DB_DRIVER")
    }

    #Reading sales table
    sales_df = spark.read.jdbc(url=url, table = "public.sales_transactions",properties = properties)

    #Reading Products Table
    product_df = spark.read.jdbc(url=url, table = "public.product_master",properties = properties)

    return sales_df, product_df