"""Silver transformations expressed with native Spark functions."""

from pyspark.sql import DataFrame, functions as F


def add_minutes(df: DataFrame, source: str, target: str) -> DataFrame:
    """Convert HHMM (including next-day values) to elapsed minutes."""
    return df.withColumn(target, F.floor(F.col(source) / 100) * 60 + F.pmod(F.col(source), 100))


def clean_deplacements(df: DataFrame) -> DataFrame:
    """Apply documented time, strata, and anomaly flags without dropping records."""
    df = add_minutes(df, "heure_depart_hhmm", "heure_depart_min_total")
    df = add_minutes(df, "heure_arrivee_hhmm", "heure_arrivee_min_total")
    return (
        df.withColumn(
            "heure_depart_h", F.pmod(F.floor(F.col("heure_depart_min_total") / 60), 24).cast("int")
        )
        .withColumn(
            "heure_arrivee_h",
            F.pmod(F.floor(F.col("heure_arrivee_min_total") / 60), 24).cast("int"),
        )
        .withColumn("depart_apres_minuit", F.col("heure_depart_min_total") >= 1440)
        .withColumn("cout_manquant", F.col("cout").isNull())
        .withColumn("cout_aberrant", (F.col("cout") > 10000) | (F.col("cout") < 0))
        .withColumn("duree_mn_aberrant", (F.col("duree_mn") > 180) | (F.col("duree_mn") < 0))
        .withColumn("strate_connue_depart", F.col("strate_depart").rlike("^[DGPR][0-9]{2}$"))
        .withColumn("strate_connue_arrivee", F.col("strate_arrivee").rlike("^[DGPR][0-9]{2}$"))
        .withColumn(
            "famille_zone_depart",
            F.when(F.col("strate_connue_depart"), F.substring("strate_depart", 1, 1)).otherwise(
                "AUTRE_INCONNU"
            ),
        )
        .withColumn(
            "famille_zone_arrivee",
            F.when(F.col("strate_connue_arrivee"), F.substring("strate_arrivee", 1, 1)).otherwise(
                "AUTRE_INCONNU"
            ),
        )
        .withColumn(
            "nb_trajets_apied_pondere",
            F.when(
                F.col("nb_trajets_apied_pondere").isNull() & (F.col("moyen_transport") == "APied"),
                F.col("poids_extra"),
            ).otherwise(F.coalesce("nb_trajets_apied_pondere", F.lit(0.0))),
        )
    )
