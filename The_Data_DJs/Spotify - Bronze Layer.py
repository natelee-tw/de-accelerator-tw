# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Spotify - Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Data
# MAGIC For this project, we used Spotify Charts data from Kaggle and extracted them into five CSV files: 
# MAGIC 1. Indonesia
# MAGIC 2. Thailand
# MAGIC 3. Singapore
# MAGIC 4. Malaysia
# MAGIC 5. Vietnam
# MAGIC
# MAGIC So in this Bronze layer, we will create a dataframe that combines the data from these five countries.

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

spark = SparkSession.builder \
    .appName("CSV Ingestion") \
    .getOrCreate()

def create_dataframe() -> DataFrame:
    csv_files = [
        "file:///Workspace/Shared/The_Data_DJs/Data/spotify_data_Indonesia.csv",
        "file:///Workspace/Shared/The_Data_DJs/Data/spotify_data_Thailand.csv",
        "file:///Workspace/Shared/The_Data_DJs/Data/spotify_data_Singapore.csv",
        "file:///Workspace/Shared/The_Data_DJs/Data/spotify_data_Malaysia.csv",
        "file:///Workspace/Shared/The_Data_DJs/Data/spotify_data_Vietnam.csv"
    ]

    dfs = [spark.read.csv(file, header=True, inferSchema=True) for file in csv_files]

    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)

    return combined_df

combined_df = create_dataframe()

combined_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

def write(input_df: DataFrame):
    out_dir = "dbfs:///FileStore/The_Data_DJs/Spotify/Bronze/"
    
    mode_name = "overwrite"
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)

write(combined_df)
    

