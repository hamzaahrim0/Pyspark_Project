"""Acceptance checks for time and transport invariants."""

from pyspark.sql import functions as F


def test_hours_normalized(silver_df) -> None:
    assert silver_df.filter(~F.col("heure_depart_h").between(0, 23)).count() == 0
    assert silver_df.filter(~F.col("heure_arrivee_h").between(0, 23)).count() == 0


def test_duration_and_leg_counts(silver_df) -> None:
    delta = F.col("heure_arrivee_min_total") - F.col("heure_depart_min_total")
    assert silver_df.filter(F.abs(delta - F.col("duree_mn")) > 1).count() == 0
    modes = sum(F.col(f"mode_{i}").isNotNull().cast("int") for i in range(1, 5))
    assert silver_df.filter(F.col("nb_trajets") != modes).count() == 0


def test_unknown_strata_are_flagged(silver_df) -> None:
    assert (
        silver_df.filter(
            (~F.col("strate_connue_depart")) & (F.col("famille_zone_depart") != "AUTRE_INCONNU")
        ).count()
        == 0
    )
    assert (
        silver_df.filter(
            (~F.col("strate_connue_arrivee")) & (F.col("famille_zone_arrivee") != "AUTRE_INCONNU")
        ).count()
        == 0
    )


def test_non_unique_quartier_mapping_is_preserved(spark, raw_df) -> None:
    from mobilite.quality import build_dim_quartier

    dimension = build_dim_quartier(raw_df)
    mappings = {
        row.strate
        for row in dimension.filter(F.col("lieu_quartier") == 399).select("strate").collect()
    }
    assert mappings == {"zex", "NA1_r"}


def test_missing_walking_weight_uses_household_weight(raw_df, silver_df) -> None:
    missing_source = raw_df.filter(F.col("nb_trajets_apied_pondere").isNull()).select(
        "num_mena", "num_individu", "num_deplacement", "poids_extra", "moyen_transport"
    )
    assert missing_source.count() == 1
    assert silver_df.filter(F.col("nb_trajets_apied_pondere").isNull()).count() == 0
    imputed = silver_df.join(
        missing_source.select("num_mena", "num_individu", "num_deplacement"),
        ["num_mena", "num_individu", "num_deplacement"],
    ).first()
    assert imputed.moyen_transport == "APied"
    assert abs(imputed.nb_trajets_apied_pondere - imputed.poids_extra) < 1e-9
