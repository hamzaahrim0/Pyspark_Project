"""Household-group split correctness and determinism."""

from pyspark.sql import functions as F


from mobilite.splits import add_household_fold, split_households


def test_household_split_is_disjoint_reproducible_and_80_20(raw_df) -> None:
    train, test = split_households(raw_df, 42)
    repeated_train, repeated_test = split_households(raw_df, 42)
    train_households = train.select("num_mena").distinct()
    test_households = test.select("num_mena").distinct()
    assert train_households.join(test_households, "num_mena", "inner").count() == 0
    assert train_households.exceptAll(repeated_train.select("num_mena").distinct()).count() == 0
    assert test_households.exceptAll(repeated_test.select("num_mena").distinct()).count() == 0
    train_household_count = train_households.count()
    all_household_count = raw_df.select("num_mena").distinct().count()
    train_fraction = train_household_count / all_household_count
    assert abs(train_fraction - 0.8) <= 0.02


def test_cross_validation_fold_is_shared_by_household(raw_df) -> None:
    folded = add_household_fold(raw_df, 3)
    conflicts = folded.groupBy("num_mena").agg(F.countDistinct("household_fold").alias("folds"))
    assert conflicts.filter(F.col("folds") != 1).count() == 0
