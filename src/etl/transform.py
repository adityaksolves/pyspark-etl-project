from pyspark.sql import functions as F

def transform_data(sales_df, product_df):
    joined_df = sales_df.join(product_df,on="product_id", how="inner")

    df_with_total = joined_df.withColumn("total_amount",F.col("quantity")*F.col("price"))

    report_df = df_with_total.groupBy("category")\
        .agg(F.sum("total_amount").alias("total_sales"))\
        .withColumn("load_timestamp",F.current_timestamp())

    return report_df


