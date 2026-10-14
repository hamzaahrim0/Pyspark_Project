"""Acceptance checks for workbook ingestion."""


def test_ingestion_counts_and_key(raw_df) -> None:
    assert raw_df.count() == 45_605
    assert len(raw_df.columns) == 33
    assert raw_df.select("num_mena", "num_individu", "num_deplacement").distinct().count() == 45_605
    assert raw_df.select("num_mena").distinct().count() == 3_127
    assert raw_df.select("num_mena", "num_individu").distinct().count() == 11_457
