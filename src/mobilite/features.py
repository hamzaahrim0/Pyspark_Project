"""Leakage-safe ex-ante feature construction."""

from collections.abc import Iterable

from pyspark.sql import DataFrame, Window, functions as F

MOTIF_GROUPS = {
    "TravailHabituel": "Travail",
    "AutreMotifTravail": "Travail",
    "TravailAmbulant": "Travail",
    "RechercheTravail": "Travail",
    "Etudes": "Études",
    "AutreMotifEtudes": "Études",
    "AchatsAlimentaires": "Achats",
    "AchatsNonAlimentaires": "Achats",
    "VisiteAmis": "Loisirs/Social",
    "VisiteFamille": "Loisirs/Social",
    "VisitesVoisins": "Loisirs/Social",
    "SportsLoisirs": "Loisirs/Social",
    "RepasExterieur": "Loisirs/Social",
    "Ceremonies": "Loisirs/Social",
    "Religion": "Loisirs/Social",
    "Associations": "Loisirs/Social",
    "RetourDomicile": "Retour domicile",
    "Sante": "Santé/Administratif/Services",
    "DemarchesAdministratives": "Santé/Administratif/Services",
    "Services": "Santé/Administratif/Services",
    "Accompagnement": "Accompagnement",
    "Autre": "Autre",
    "AutreMotifMenage": "Autre",
    "ApprovisionnementEau": "Autre",
}
MODE_FEATURES = [
    "heure_depart_h",
    "tranche_horaire",
    "jour_enquete",
    "is_weekend",
    "depart_apres_minuit",
    "age",
    "tranche_age",
    "sexe",
    "position_menage",
    "taille_menage_obs",
    "rang_deplacement",
    "is_premier_depl",
    "is_dernier_depl",
    "motif_precedent",
    "nb_depl_individu",
    "motif",
    "motif_groupe",
    "strate_depart",
    "strate_arrivee",
    "famille_zone_depart",
    "famille_zone_arrivee",
    "meme_strate",
    "meme_famille_zone",
    "od_pair",
    "od_freq",
    "od_duree_mediane",
    "od_part_marche",
]
FORBIDDEN_PREFIXES = ("mode_", "nb_trajets_")
FORBIDDEN = {"cout", "nb_trajets", "moyen_transport", "famille_modale", "duree_mn", "log1p_duree"}


def assert_motif_coverage(values: Iterable[str]) -> None:
    """Raise when observed purpose modalities are missing from the explicit map."""
    missing = sorted(set(values) - MOTIF_GROUPS.keys())
    if missing:
        raise ValueError(f"Unmapped motif modalities: {missing}")


def assert_no_leakage(features: list[str], include_duration: bool = False) -> None:
    """Raise if post-choice information enters mode-choice features."""
    banned = FORBIDDEN - ({"duree_mn", "log1p_duree"} if include_duration else set())
    hits = [name for name in features if name in banned or name.startswith(FORBIDDEN_PREFIXES)]
    if hits:
        raise ValueError(f"Target leakage in features: {hits}")


def add_motif_group(df: DataFrame) -> DataFrame:
    """Map every observed source purpose explicitly and reject missing mappings."""
    unmapped = (
        df.filter(F.col("motif").isNull() | ~F.col("motif").isin(*MOTIF_GROUPS.keys()))
        .limit(1)
        .count()
    )
    if unmapped:
        observed = [row.motif for row in df.select("motif").distinct().collect()]
        assert_motif_coverage(observed)
    motif_map = F.create_map(
        *[item for pair in MOTIF_GROUPS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    mapped = df.withColumn("motif_groupe", motif_map[F.col("motif")])
    if mapped.filter(F.col("motif_groupe").isNull()).limit(1).count():
        raise ValueError("Explicit motif mapping produced a null motif_groupe")
    return mapped


def engineer_features(df: DataFrame) -> DataFrame:
    """Add ex-ante time, person, sequence, purpose, and OD features."""
    df = add_motif_group(df)
    household = df.groupBy("num_mena").agg(
        F.countDistinct(F.struct("num_mena", "num_individu")).alias("taille_menage_obs")
    )
    window = Window.partitionBy("num_mena", "num_individu").orderBy(
        "heure_depart_min_total", "num_deplacement"
    )
    individual = Window.partitionBy("num_mena", "num_individu")
    return (
        df.join(F.broadcast(household), "num_mena")
        .withColumn("rang_deplacement", F.row_number().over(window))
        .withColumn("nb_depl_individu", F.count("*").over(individual))
        .withColumn("motif_precedent", F.lag("motif").over(window))
        .withColumn("is_premier_depl", F.col("rang_deplacement") == 1)
        .withColumn("is_dernier_depl", F.col("rang_deplacement") == F.col("nb_depl_individu"))
        .withColumn(
            "tranche_horaire",
            F.when(F.col("heure_depart_h").between(6, 8), "pointe_matin")
            .when(F.col("heure_depart_h").between(16, 19), "pointe_soir")
            .when(F.col("heure_depart_h").between(9, 15), "journee")
            .otherwise("soiree_nuit"),
        )
        .withColumn("is_weekend", F.col("jour_enquete").isin("Samedi", "Dimanche"))
        .withColumn(
            "tranche_age",
            F.when(F.col("age") < 18, "mineur")
            .when(F.col("age") < 35, "18_34")
            .when(F.col("age") < 60, "35_59")
            .otherwise("60_plus"),
        )
        .withColumn("meme_strate", F.col("strate_depart") == F.col("strate_arrivee"))
        .withColumn(
            "meme_famille_zone", F.col("famille_zone_depart") == F.col("famille_zone_arrivee")
        )
        .withColumn("od_pair", F.concat_ws("→", "strate_depart", "strate_arrivee"))
        .withColumn(
            "famille_modale",
            F.when(F.col("moyen_transport") == "APied", "Marche")
            .when(F.col("moyen_transport").isin("Tata", "CarRapide", "DDD", "NN"), "Collectif")
            .when(F.col("moyen_transport").isin("Taxi", "Clando"), "Taxi/Clando")
            .when(F.col("moyen_transport").isin("Voiture", "Moto"), "Individuel motorisé")
            .when(F.col("moyen_transport") == "Intermodal", "Intermodal")
            .otherwise("Autre"),
        )
        .withColumn("log1p_duree", F.log1p("duree_mn"))
    )


def add_train_od_statistics(train: DataFrame, test: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Create out-of-fold training OD statistics and full-training test statistics."""
    from functools import reduce

    keys = ["strate_depart", "strate_arrivee"]
    fold_col = "_od_fold"
    folded = train.withColumn(fold_col, F.pmod(F.xxhash64("num_mena"), F.lit(3)))
    train_parts = []
    for fold in range(3):
        validation = folded.filter(F.col(fold_col) == fold).drop(fold_col)
        source = folded.filter(F.col(fold_col) != fold).drop(fold_col)
        stats = source.groupBy(*keys).agg(
            F.count("*").alias("od_freq"),
            F.expr("percentile_approx(duree_mn, 0.5, 10000)").alias("od_duree_mediane"),
            F.avg((F.col("moyen_transport") == "APied").cast("double")).alias("od_part_marche"),
        )
        train_parts.append(validation.join(F.broadcast(stats), keys, "left"))
    train_with_stats = reduce(lambda left, right: left.unionByName(right), train_parts)
    full_stats = train.groupBy(*keys).agg(
        F.count("*").alias("od_freq"),
        F.expr("percentile_approx(duree_mn, 0.5, 10000)").alias("od_duree_mediane"),
        F.avg((F.col("moyen_transport") == "APied").cast("double")).alias("od_part_marche"),
    )
    test_with_stats = test.join(F.broadcast(full_stats), keys, "left")
    median_duration = train.approxQuantile("duree_mn", [0.5], 0.01)[0]
    walking_share = train.agg(F.avg((F.col("moyen_transport") == "APied").cast("double"))).first()[
        0
    ]

    def fill_missing(frame: DataFrame) -> DataFrame:
        return (
            frame.withColumn("od_freq", F.coalesce("od_freq", F.lit(0)).cast("double"))
            .withColumn(
                "od_duree_mediane",
                F.coalesce("od_duree_mediane", F.lit(median_duration)).cast("double"),
            )
            .withColumn(
                "od_part_marche", F.coalesce("od_part_marche", F.lit(walking_share)).cast("double")
            )
        )

    return fill_missing(train_with_stats), fill_missing(test_with_stats)


DURATION_FEATURES = MODE_FEATURES + ["moyen_transport"]


def assert_duration_no_leakage(features: list[str]) -> None:
    """Reject cost, leg-level modes, and post-duration fields in duration predictors."""
    banned = [
        name
        for name in features
        if name == "cout"
        or name in {"nb_trajets", "duree_mn", "log1p_duree"}
        or name.startswith(("mode_", "nb_trajets_"))
    ]
    if banned:
        raise ValueError(f"Target leakage in duration features: {banned}")
