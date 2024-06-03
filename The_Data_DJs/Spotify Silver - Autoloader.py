# Databricks notebook source
checkpoint_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/checkpoint/"
bronze_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/bronze_table"
silver_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/silver_table"
checkpoint_path_silver = "dbfs:/FileStore/The_Data_DJs/Auto_loader/checkpoints_silver/"

# COMMAND ----------

bronze_df = (spark.readStream
             .format("delta")
             .load(bronze_table_path))

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning the Data
# MAGIC
# MAGIC 1. drop `Unnamed: 0`
# MAGIC 2. rename `_c0` to `id`
# MAGIC 3. change `rank` to integer
# MAGIC 4. change `date` to date format
# MAGIC 5. drop `url` 
# MAGIC 6. change data type of `streams` to integer
# MAGIC 7. change data type of `popularity` and `duration_ms` into integer
# MAGIC 8. change data type of `explicit` to boolean
# MAGIC 9. change data type of `release_date` to date
# MAGIC 10. drop `available_market`
# MAGIC 11. change data type of `af_danceability` and `af_energy` to float

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace

def data_cleaning(df):
    df = df.withColumn("rank", col("rank").cast("integer")) \
            .withColumn("date", to_date("date", "yyyy-MM-dd")) \
            .withColumn("streams", col("streams").cast("integer")) \
            .withColumn("popularity", col("popularity").cast("integer")) \
            .withColumn("duration_ms", col("duration_ms").cast("integer")) \
            .withColumn("explicit", col("explicit").cast("boolean")) \
            .withColumn("release_date", to_date("release_date", "yyyy-MM-dd")) \
            .drop("url") \
            .drop("available_markets") \
            .withColumn("af_danceability", col("af_danceability").cast("float")) \
            .withColumn("af_energy", col("af_energy").cast("float"))
    return df

bronze_df_cleaned = data_cleaning(bronze_df)
display(bronze_df_cleaned)

# COMMAND ----------

def remove_null_stream(df):
    """
    remove all rows where stream is null
    """
    df = df.filter(col("streams").isNotNull())
    return df


silver_df = remove_null_stream(bronze_df_cleaned)

# COMMAND ----------

silver_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/silver_table"
checkpoint_path_silver = "dbfs:/FileStore/The_Data_DJs/Auto_loader/checkpoints_silver/"

(silver_df.writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_path_silver)
   .outputMode("append")
   .start(silver_table_path))

# COMMAND ----------

display(silver_df.selectExpr("count(*) as total_rows"))

# COMMAND ----------


