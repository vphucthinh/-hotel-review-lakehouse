# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean data, check quality, tag aspects
# MAGIC - fix types, remove placeholders ("No Negative"), remove duplicates
# MAGIC - bad rows go to a quarantine table
# MAGIC - each review gets aspect mentions (room, staff, location...)

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.config import T_BRONZE, T_QUARANTINE, T_SILVER, T_SILVER_ASPECTS
from src.transforms import (add_score_label, clean_reviews, remove_duplicates, split_valid_invalid,
                            tag_aspects)

# COMMAND ----------

bronze = spark.table(T_BRONZE)

clean = clean_reviews(bronze)
valid, invalid = split_valid_invalid(clean)   # check quality first
valid = remove_duplicates(valid)              # then remove duplicates
valid = add_score_label(valid)

valid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SILVER)
invalid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_QUARANTINE)

# COMMAND ----------

# data quality report
n_bronze = bronze.count()
n_valid = spark.table(T_SILVER).count()
n_invalid = spark.table(T_QUARANTINE).count()
print("bronze:", n_bronze, "| valid:", n_valid, "| quarantine:", n_invalid,
      "| duplicates removed:", n_bronze - n_valid - n_invalid)

display(spark.table(T_QUARANTINE).groupBy("dq_reason").count())

# stop the pipeline if something is clearly wrong
assert spark.table(T_SILVER).filter("review_id IS NULL").count() == 0, "review_id is null"
assert n_invalid / n_bronze < 0.05, "more than 5% bad rows, check the source file"

# COMMAND ----------

aspects = tag_aspects(spark.table(T_SILVER))
aspects.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SILVER_ASPECTS)

display(spark.table(T_SILVER_ASPECTS).groupBy("aspect", "polarity").count().orderBy("aspect"))
