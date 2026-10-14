"""Shared local Spark fixture and workbook-derived DataFrames."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from mobilite.cleaning import clean_deplacements
from mobilite.config import load_config
from mobilite.ingestion import convert_workbook, load_deplacement
from mobilite.spark_session import create_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create one project-configured Spark session for integration tests."""
    session = create_spark(load_config("config/config.yaml"))
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze(spark: SparkSession) -> str:
    """Convert the real workbook into cached bronze Parquet once."""
    path = Path("data/bronze")
    source = Path("data/raw/donnees_deplacement_PSpark 2026.xlsx")
    convert_workbook(str(source), str(path))
    return str(path)


@pytest.fixture(scope="session")
def raw_df(spark: SparkSession, bronze: str):
    """Load the full main table with its declared schema."""
    return load_deplacement(spark, bronze)


@pytest.fixture(scope="session")
def silver_df(raw_df):
    """Apply real cleaning transformations to the full table."""
    return clean_deplacements(raw_df)
