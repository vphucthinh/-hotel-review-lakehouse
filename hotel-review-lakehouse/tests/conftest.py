import os
import sys

import pytest
from pyspark.sql import SparkSession

# make "src" importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("tests")
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    yield spark
    spark.stop()
