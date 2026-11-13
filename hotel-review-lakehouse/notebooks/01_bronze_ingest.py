# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: load the raw CSV
# MAGIC Load every CSV file in the raw volume. Keep the data as it is (all columns are strings)
# MAGIC and add 2 columns to know when and from which file it was loaded.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))  # so we can import from src/

from pyspark.sql import functions as F
from src.config import RAW_DIR, T_BRONZE

# COMMAND ----------

raw = (spark.read
       .option("header", True)
       .option("quote", '"')
       .option("escape", '"')
       .option("pathGlobFilter", "*.csv")   # only .csv files
       .csv(RAW_DIR))

bronze = (raw
          .withColumn("_ingest_time", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))

# COMMAND ----------

(bronze.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(T_BRONZE))

print("rows in bronze:", spark.table(T_BRONZE).count())
display(spark.table(T_BRONZE).groupBy("_source_file").count())

# COMMAND ----------

display(spark.table(T_BRONZE).limit(10))
