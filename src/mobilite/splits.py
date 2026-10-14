"""Deterministic household-group holdout and cross-validation folds."""

from pyspark.sql import DataFrame, functions as F


def split_households(df: DataFrame, seed: int = 42) -> tuple[DataFrame, DataFrame]:
    """Split by household hash to prevent household-member leakage."""
    bucket = F.pmod(F.xxhash64("num_mena", F.lit(seed)), F.lit(100))
    return df.filter(bucket < 80), df.filter(bucket >= 80)


def add_household_fold(df: DataFrame, folds: int = 3) -> DataFrame:
    """Assign a deterministic fold shared by every person in the same household."""
    if folds < 2:
        raise ValueError("folds must be at least 2")
    return df.withColumn("household_fold", F.pmod(F.xxhash64("num_mena"), F.lit(folds)))
