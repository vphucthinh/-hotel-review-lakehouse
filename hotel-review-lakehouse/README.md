# Hotel Review Intelligence – Databricks Lakehouse

Automated pipeline that loads hotel reviews, cleans and checks them, runs a pretrained
sentiment model, and builds a star schema for a BI dashboard.

## Architecture

```
Hotel_Reviews.csv (Kaggle)
        |  01_bronze_ingest
        v
bronze_reviews  (raw, all strings)
        |  02_silver_clean  -> data quality rules -> silver_reviews_quarantine
        v
silver_reviews  +  silver_review_aspects (room, staff, location, ...)
        |  03_ai_sentiment  (Hugging Face model, logged in MLflow)
        v
silver_review_sentiment
        |  04_gold_star_schema
        v
gold_dim_hotel  gold_dim_date  gold_dim_aspect
gold_fact_review  gold_fact_review_aspect
        |
        v
Databricks SQL dashboard (05_dashboard_queries.sql)
```

Orchestrated with a Databricks Lakeflow Job (bronze -> silver -> ai_sentiment -> gold). The job starts
automatically when a new CSV file lands in the raw volume (file arrival trigger). Job definition:
`resources/hotel_review_job.yml`. Unit tests run on every push with GitHub Actions.

## Tech
Databricks (Free Edition), PySpark, Delta Lake, Unity Catalog, MLflow, Hugging Face Transformers,
Databricks SQL, pytest, GitHub Actions.

## Project structure

| Path | What it does |
| --- | --- |
| `src/config.py` | catalog, schema, table names, model name |
| `src/transforms.py` | cleaning, data quality rules, aspect tagging (pure PySpark functions) |
| `notebooks/00_setup.sql` | create schema and volume |
| `notebooks/01_bronze_ingest.py` | CSV -> bronze table |
| `notebooks/02_silver_clean.py` | clean, quality check, quarantine, aspects |
| `notebooks/03_ai_sentiment.py` | sentiment model on a sample, MLflow tracking |
| `notebooks/04_gold_star_schema.py` | dimensions and facts |
| `notebooks/05_dashboard_queries.sql` | queries for the dashboard |
| `tests/` | pytest unit tests for `src/transforms.py` |
| `sample_data/` | 500 fake rows with the same columns as the real file, for testing |
| `resources/hotel_review_job.yml` | job definition: 4 tasks + file arrival trigger |
| `.github/workflows/ci.yml` | run tests on every push |

## Run tests locally
```
pip install -r requirements-dev.txt   # needs Java 17+
pytest -v
```

## Data
515K Hotel Reviews Data in Europe (Kaggle). Download `Hotel_Reviews.csv` and upload it to the
volume `/Volumes/workspace/hotel_lakehouse/raw/`. Every `.csv` file in this folder is loaded,
so new batches can be added as new files (e.g. `reviews_2026_11.csv`).

## Results
(fill in after running: number of rows per layer, model accuracy / F1, dashboard screenshot)
