"""Real weighted OD pivot reconciliation acceptance test."""

from mobilite.analytics import assert_od_reconciliation
from mobilite.ingestion import load_od_reference


def test_od_matrix_matches_excel_cells_and_total(spark, silver_df, bronze) -> None:
    actual = silver_df.groupBy("strate_depart", "strate_arrivee").sum("poids_extra")
    actual = actual.withColumnRenamed("sum(poids_extra)", "poids_total")
    reference = load_od_reference(spark, bronze)
    result = assert_od_reconciliation(actual, reference)
    assert abs(result["spark_total"] - 6_699_228.017) <= 6_699_228.017e-9
    assert result["mismatched_cells"] == 0
