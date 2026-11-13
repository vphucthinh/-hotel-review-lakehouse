# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: star schema for reporting
# MAGIC - dimensions: hotel, date, aspect
# MAGIC - facts: one row per review, one row per aspect mention

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.config import (T_DIM_ASPECT, T_DIM_DATE, T_DIM_HOTEL, T_FACT_ASPECT, T_FACT_REVIEW,
                        T_SENTIMENT, T_SILVER, T_SILVER_ASPECTS)


def save(sql, table):
    spark.sql(sql).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(table, spark.table(table).count())

# COMMAND ----------

save(f"""
SELECT xxhash64(hotel_name)  AS hotel_key,
       hotel_name,
       first(hotel_address)  AS hotel_address,
       first(country)        AS country,
       avg(hotel_avg_score)  AS hotel_avg_score,
       avg(lat)              AS lat,
       avg(lng)              AS lng
FROM {T_SILVER}
GROUP BY hotel_name
""", T_DIM_HOTEL)

save(f"""
SELECT DISTINCT
       CAST(date_format(review_date, 'yyyyMMdd') AS INT) AS date_key,
       review_date                                      AS full_date,
       year(review_date)                                AS year,
       quarter(review_date)                             AS quarter,
       month(review_date)                               AS month,
       date_format(review_date, 'MMM')                  AS month_name,
       dayofweek(review_date)                           AS day_of_week
FROM {T_SILVER}
""", T_DIM_DATE)

save(f"""
SELECT DISTINCT xxhash64(aspect) AS aspect_key, aspect
FROM {T_SILVER_ASPECTS}
""", T_DIM_ASPECT)

# COMMAND ----------

save(f"""
SELECT s.review_id,
       xxhash64(s.hotel_name)                             AS hotel_key,
       CAST(date_format(s.review_date, 'yyyyMMdd') AS INT) AS date_key,
       s.reviewer_nationality,
       s.reviewer_score,
       s.score_label,
       s.neg_word_count,
       s.pos_word_count,
       m.model_label,
       m.model_score
FROM {T_SILVER} s
LEFT JOIN {T_SENTIMENT} m ON s.review_id = m.review_id
""", T_FACT_REVIEW)

save(f"""
SELECT a.review_id,
       f.hotel_key,
       f.date_key,
       xxhash64(a.aspect) AS aspect_key,
       a.polarity
FROM {T_SILVER_ASPECTS} a
JOIN {T_FACT_REVIEW} f ON a.review_id = f.review_id
""", T_FACT_ASPECT)

# COMMAND ----------

# simple checks
assert spark.table(T_FACT_REVIEW).count() == spark.table(T_SILVER).count(), "fact_review lost rows"
missing_hotel = spark.sql(f"""
    SELECT count(*) AS n FROM {T_FACT_REVIEW} f
    LEFT JOIN {T_DIM_HOTEL} h ON f.hotel_key = h.hotel_key
    WHERE h.hotel_key IS NULL""").first()["n"]
assert missing_hotel == 0, "some reviews have no hotel in dim_hotel"
print("gold checks passed")
