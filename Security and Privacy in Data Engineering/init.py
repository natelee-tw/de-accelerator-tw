# Databricks notebook source
# MAGIC %pip install wget ff3 pandas datafuzz

# COMMAND ----------

# Clear out existing working directory

current_user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split("@")[0]
working_directory=f"/FileStore/{current_user}/dataSecurity"
dbutils.fs.rm(working_directory, True)

# COMMAND ----------

# Function to download files to DBFS

import os
import wget
import sys
import shutil

sys.stdout.fileno = lambda: False # prevents AttributeError: 'ConsoleBuffer' object has no attribute 'fileno'   

def clean_remake_dir(dir):
    if os.path.isdir(local_tmp_dir): shutil.rmtree(local_tmp_dir)
    os.makedirs(local_tmp_dir)
    

def download_to_local_dir(local_dir, target_dir, url, filename_parsing_lambda):
    filename = (filename_parsing_lambda)(url)
    tmp_path = f"{local_dir}/{filename}"
    target_path = f"{target_dir}/{filename}"
    if os.path.exists(tmp_path):
        os.remove(tmp_path) 
    
    saved_filename = wget.download(url, out = tmp_path)
    
    if target_path.endswith(".zip"):
        with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
            zip_ref.extractall(local_dir)

    dbutils.fs.cp(f"file:{local_dir}/", target_dir, True)
    
    return target_path

# COMMAND ----------

local_tmp_dir = f"{os.getcwd()}/{current_user}/dataSecurity/tmp"
clean_remake_dir(local_tmp_dir)

urls = [
    "https://raw.githubusercontent.com/data-derp/exercise-data-security/master/data/air_quality.csv"
]
    
# target_directory = f"/FileStore/{current_user}/dataSecurity"
    
for url in urls:    
    download_to_local_dir(local_tmp_dir, f"{working_directory}/data", url, lambda y: y.split("/")[-1])
    

# COMMAND ----------

dbutils.fs.ls(f"{working_directory}/data")

# COMMAND ----------


