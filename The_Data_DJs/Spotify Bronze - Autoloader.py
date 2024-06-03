# Databricks notebook source
# Path to the checkpoint directory
input_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/raw/"
checkpoint_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/checkpoint/"
bronze_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/bronze_table"
silver_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/silver_table"
checkpoint_path_silver = "dbfs:/FileStore/The_Data_DJs/Auto_loader/checkpoints_silver/"

# COMMAND ----------

# # Delete the Delta table directory
# dbutils.fs.rm(checkpoint_path, recurse=True)
# dbutils.fs.rm(bronze_table_path, recurse=True)
# dbutils.fs.rm(silver_table_path, recurse=True)
# dbutils.fs.rm(checkpoint_path_silver, recurse=True)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable

# Define the schema for your data
schema = StructType([
    StructField("index", StringType(), True),
    StructField("Unnamed0", StringType(), True),
    StructField("title", StringType(), True),
    StructField("rank", StringType(), True),
    StructField("date", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("url", StringType(), True),
    StructField("region", StringType(), True),
    StructField("chart", StringType(), True),
    StructField("trend", StringType(), True),
    StructField("streams", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("album", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("duration_ms", StringType(), True),
    StructField("explicit", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("available_markets", StringType(), True),
    StructField("af_danceability", StringType(), True),
    StructField("af_energy", StringType(), True),
    StructField("af_key", StringType(), True),
    StructField("af_loudness", StringType(), True),
    StructField("af_mode", StringType(), True),
    StructField("af_speechiness", StringType(), True),
    StructField("af_acousticness", StringType(), True),
    StructField("af_instrumentalness", StringType(), True),
    StructField("af_liveness", StringType(), True),
    StructField("af_valence", StringType(), True),
    StructField("af_tempo", StringType(), True),
    StructField("af_time_signature", StringType(), True),
])

# Read data using Auto Loader
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("header", "true")  # Ignore the first row (header row)
          .schema(schema)
          .load(input_path))

# Write to Delta Lake table
(df.writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_path)
   .outputMode("append")
   .start(bronze_table_path))

# COMMAND ----------

bronze_df = (spark.readStream
             .format("delta")
             .load(bronze_table_path))
             
# Display the data
display(bronze_df)

# COMMAND ----------

display(df.selectExpr("count(*) as total_rows"))

# 1.65 million

# COMMAND ----------


