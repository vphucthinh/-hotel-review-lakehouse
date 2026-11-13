# Change these if you use another catalog / schema name
CATALOG = "workspace"
SCHEMA = "hotel_lakehouse"
VOLUME = "raw"

DB = f"{CATALOG}.{SCHEMA}"
# folder with the raw CSV files (every .csv in it is loaded)
RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"

T_BRONZE = f"{DB}.bronze_reviews"
T_SILVER = f"{DB}.silver_reviews"
T_QUARANTINE = f"{DB}.silver_reviews_quarantine"
T_SILVER_ASPECTS = f"{DB}.silver_review_aspects"
T_SENTIMENT = f"{DB}.silver_review_sentiment"

T_DIM_HOTEL = f"{DB}.gold_dim_hotel"
T_DIM_DATE = f"{DB}.gold_dim_date"
T_DIM_ASPECT = f"{DB}.gold_dim_aspect"
T_FACT_REVIEW = f"{DB}.gold_fact_review"
T_FACT_ASPECT = f"{DB}.gold_fact_review_aspect"

# AI step
MODEL_NAME = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
SAMPLE_SIZE = 3000
