"""Cleaning and aspect-tagging functions used by the notebooks.

They only take and return Spark DataFrames, so they can be tested
locally with pytest without Databricks.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# raw column name -> clean column name
RENAME_MAP = {
    "Hotel_Address": "hotel_address",
    "Hotel_Name": "hotel_name",
    "Average_Score": "hotel_avg_score",
    "Review_Date": "review_date_raw",
    "Reviewer_Nationality": "reviewer_nationality",
    "Negative_Review": "negative_review",
    "Positive_Review": "positive_review",
    "Review_Total_Negative_Word_Counts": "neg_word_count",
    "Review_Total_Positive_Word_Counts": "pos_word_count",
    "Reviewer_Score": "reviewer_score",
    "lat": "lat",
    "lng": "lng",
}

# simple keyword lists for each aspect
ASPECT_KEYWORDS = {
    "room": ["room", "bed", "bathroom", "shower", "pillow", "air con", "aircon"],
    "staff": ["staff", "reception", "receptionist", "service", "friendly", "helpful", "rude"],
    "location": ["location", "metro", "station", "walk", "close to", "central", "area"],
    "food": ["breakfast", "food", "restaurant", "coffee", "dinner", "bar"],
    "price": ["price", "expensive", "value", "money", "cheap", "overpriced"],
    "cleanliness": ["clean", "cleaning", "dirty", "dust", "dusty", "smell", "smelly", "hygiene"],
    "noise": ["noise", "noisy", "quiet", "loud"],
}


def rename_columns(df: DataFrame) -> DataFrame:
    cols = [F.col(old).alias(new) for old, new in RENAME_MAP.items() if old in df.columns]
    return df.select(*cols)


def clean_reviews(df: DataFrame) -> DataFrame:
    df = rename_columns(df)

    # placeholder texts mean "empty"
    df = (df
          .withColumn("negative_review", F.trim(F.col("negative_review")))
          .withColumn("positive_review", F.trim(F.col("positive_review")))
          .withColumn("negative_review",
                      F.when(F.lower(F.col("negative_review")).isin("no negative", "nothing", "n a", ""), F.lit(""))
                       .otherwise(F.col("negative_review")))
          .withColumn("positive_review",
                      F.when(F.lower(F.col("positive_review")).isin("no positive", "nothing", "n a", ""), F.lit(""))
                       .otherwise(F.col("positive_review"))))

    # types (try_cast / try_to_timestamp give NULL instead of an error for bad values)
    df = (df
          .withColumn("review_date",
                      F.to_date(F.try_to_timestamp(F.col("review_date_raw"), F.lit("M/d/yyyy"))))
          .withColumn("reviewer_score", F.expr("try_cast(reviewer_score as double)"))
          .withColumn("hotel_avg_score", F.expr("try_cast(hotel_avg_score as double)"))
          .withColumn("neg_word_count", F.expr("try_cast(neg_word_count as int)"))
          .withColumn("pos_word_count", F.expr("try_cast(pos_word_count as int)"))
          .withColumn("lat", F.expr("try_cast(lat as double)"))
          .withColumn("lng", F.expr("try_cast(lng as double)"))
          .withColumn("reviewer_nationality", F.trim(F.col("reviewer_nationality")))
          .drop("review_date_raw"))

    # country is the last part of the address
    df = df.withColumn(
        "country",
        F.when(F.col("hotel_address").endswith("United Kingdom"), F.lit("United Kingdom"))
         .otherwise(F.element_at(F.split(F.trim(F.col("hotel_address")), " "), -1)))

    # id for each review (same content -> same id)
    df = df.withColumn("review_id", F.sha2(F.concat_ws("||", "hotel_name", "review_date",
                                                       "reviewer_nationality", "negative_review",
                                                       "positive_review"), 256))
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["review_id"])


def split_valid_invalid(df: DataFrame):
    """Data quality rules. Bad rows go to a quarantine table with a reason."""
    reason = (F.when(F.col("hotel_name").isNull(), "missing hotel_name")
               .when(F.col("review_date").isNull(), "bad review_date")
               .when(F.col("reviewer_score").isNull(), "missing reviewer_score")
               .when((F.col("reviewer_score") < 0) | (F.col("reviewer_score") > 10), "score out of range")
               .when((F.col("negative_review") == "") & (F.col("positive_review") == ""), "empty review"))
    df = df.withColumn("dq_reason", reason)
    valid = df.filter(F.col("dq_reason").isNull()).drop("dq_reason")
    invalid = df.filter(F.col("dq_reason").isNotNull())
    return valid, invalid


def add_score_label(df: DataFrame) -> DataFrame:
    """Label from the reviewer's own score. Used to check the AI model."""
    return df.withColumn(
        "score_label",
        F.when(F.col("reviewer_score") >= 7, "positive")
         .when(F.col("reviewer_score") <= 5, "negative")
         .otherwise("neutral"))


def tag_aspects(df: DataFrame) -> DataFrame:
    """One row per (review, aspect, polarity).

    If an aspect word is in the negative text -> negative mention,
    if it is in the positive text -> positive mention.
    """
    rows = []
    for aspect, words in ASPECT_KEYWORDS.items():
        pattern = r"\b(" + "|".join(words) + r")s?\b"  # whole word, plural allowed
        for side, polarity in [("negative_review", "negative"), ("positive_review", "positive")]:
            rows.append(
                df.filter(F.lower(F.col(side)).rlike(pattern))
                  .select("review_id", F.lit(aspect).alias("aspect"), F.lit(polarity).alias("polarity")))
    result = rows[0]
    for r in rows[1:]:
        result = result.unionByName(r)
    return result.dropDuplicates()
