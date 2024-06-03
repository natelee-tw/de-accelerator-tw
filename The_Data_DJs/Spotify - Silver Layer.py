# Databricks notebook source
# MAGIC %md
# MAGIC # Spotify - Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df

Bronze_dir = "dbfs:///FileStore/The_Data_DJs/Spotify/Bronze/"    
raw_df = read_parquet(Bronze_dir)

display(raw_df)

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
    df = df.drop("Unnamed: 0") \
            .withColumnRenamed("_c0", "id") \
            .withColumn("rank", col("rank").cast("integer")) \
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

df = data_cleaning(raw_df)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out null data
# MAGIC We found that `streams` is important data for analysis, so we decided to filter out the rows that are missing `streams` data.

# COMMAND ----------

def remove_null_stream(df):
    """
    remove all rows where stream is null
    """
    df = df.filter(col("streams").isNotNull())
    return df

def null_streams(df):
    df = df.filter(col("streams").isNull())
    return df  

df_cleaned = remove_null_stream(df)
df_fail = null_streams(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

# write to silver dbfs

def write(input_df: DataFrame):
    out_dir = "dbfs:///FileStore/The_Data_DJs/Spotify/Silver/"
    
    mode_name = "overwrite"
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)

write(df_cleaned)
