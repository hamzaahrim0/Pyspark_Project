"""Spark-based quality checks, summary tables, and generated dictionary."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import NumericType

from .ingestion import MAPPING


def quality_summary(df: DataFrame) -> list[dict]:
    """Calculate null/cardinality/range/IQR metrics in a small number of Spark passes."""
    total = df.count()
    numeric_names = [
        field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)
    ]
    quantile_values = df.approxQuantile(numeric_names, [0.25, 0.75], 0.01)
    quantiles = {
        name: values if len(values) == 2 else [None, None]
        for name, values in zip(numeric_names, quantile_values)
    }
    expressions = []
    for index, name in enumerate(df.columns):
        column = F.col(name)
        expressions += [
            F.count(F.when(column.isNull(), 1)).alias(f"null_{index}"),
            F.countDistinct(column).alias(f"cardinality_{index}"),
        ]
        if name in numeric_names:
            expressions.extend(
                [F.min(column).alias(f"minimum_{index}"), F.max(column).alias(f"maximum_{index}")]
            )
    aggregated = df.agg(*expressions).first()
    outlier_expressions = []
    for index, name in enumerate(numeric_names):
        q1, q3 = quantiles[name]
        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            outlier_expressions.append(
                F.sum(
                    F.when(
                        (F.col(name) < q1 - 1.5 * iqr) | (F.col(name) > q3 + 1.5 * iqr), 1
                    ).otherwise(0)
                ).alias(f"iqr_{index}")
            )
        if name == "cout":
            outlier_expressions.append(
                F.sum(F.when(F.col(name) > 10000, 1).otherwise(0)).alias(f"business_{index}")
            )
        elif name == "duree_mn":
            outlier_expressions.append(
                F.sum(F.when(F.col(name) > 180, 1).otherwise(0)).alias(f"business_{index}")
            )
    outliers = df.agg(*outlier_expressions).first() if outlier_expressions else None
    outlier_values = outliers.asDict() if outliers is not None else {}
    result = []
    for index, name in enumerate(df.columns):
        null_count = aggregated[f"null_{index}"] or 0
        structural_count = 0
        if name in {"mode_2", "mode_3", "mode_4"}:
            position = int(name[-1])
            structural_count = min(
                df.filter((F.col("nb_trajets") < position) & F.col(name).isNull()).count(),
                null_count,
            )
        q1, q3 = quantiles.get(name, [None, None])
        iqr_count = 0
        business_count = 0
        if outliers is not None and name in numeric_names:
            numeric_index = numeric_names.index(name)
            iqr_count = outlier_values.get(f"iqr_{numeric_index}") or 0
            business_count = outlier_values.get(f"business_{numeric_index}") or 0
        result.append(
            {
                "column": name,
                "null_pct": 100.0 * null_count / total if total else 0.0,
                "structural_nulls": int(structural_count),
                "true_nulls": int(null_count - structural_count),
                "cardinality": aggregated[f"cardinality_{index}"],
                "minimum": str(aggregated[f"minimum_{index}"] if name in numeric_names else ""),
                "maximum": str(aggregated[f"maximum_{index}"] if name in numeric_names else ""),
                "q1": q1 if q1 is not None else 0.0,
                "q3": q3 if q3 is not None else 0.0,
                "outliers_iqr": int(iqr_count),
                "outliers_business": int(business_count),
            }
        )
    return result


def _assertion(name: str, passed: bool, observed: object, expected: str) -> dict:
    """Represent one named acceptance check."""
    return {
        "assertion": name,
        "status": "OK" if passed else "KO",
        "observed": str(observed),
        "expected": expected,
    }


def build_dim_quartier(df: DataFrame) -> DataFrame:
    """Build the observed quartier-to-stratum relation without forcing uniqueness."""
    locations = (
        df.select(
            F.col("lieu_depart").alias("lieu_quartier"),
            F.col("strate_depart").alias("strate"),
        )
        .unionByName(
            df.select(
                F.col("lieu_arrivee").alias("lieu_quartier"),
                F.col("strate_arrivee").alias("strate"),
            )
        )
        .distinct()
    )
    counts = locations.groupBy("lieu_quartier").agg(F.countDistinct("strate").alias("nb_strates"))
    return locations.join(counts, "lieu_quartier").withColumn(
        "quartier_univoque", F.col("nb_strates") == 1
    )


def quality_assertions(df: DataFrame) -> list[dict]:
    """Run named invariants over the complete current Spark table."""
    total = df.count()
    distinct_key = df.select("num_mena", "num_individu", "num_deplacement").distinct().count()
    mode_count = sum(F.col(f"mode_{i}").isNotNull().cast("int") for i in range(1, 5))
    invalid_modes = df.filter(F.col("nb_trajets") != mode_count).count()
    duration_errors = df.filter(
        F.abs(
            (F.col("heure_arrivee_min_total") - F.col("heure_depart_min_total")) - F.col("duree_mn")
        )
        > 1
    ).count()
    household_weight_errors = (
        df.groupBy("num_mena")
        .agg(F.countDistinct("poids_extra").alias("weights"))
        .filter(F.col("weights") > 1)
        .count()
    )
    quartier_errors = (
        df.select(F.col("lieu_depart").alias("lieu"), F.col("strate_depart").alias("strate"))
        .unionByName(
            df.select(F.col("lieu_arrivee").alias("lieu"), F.col("strate_arrivee").alias("strate"))
        )
        .groupBy("lieu")
        .agg(F.countDistinct("strate").alias("strata"))
        .filter(F.col("strata") > 1)
        .count()
    )
    intermodal_errors = df.filter(
        (F.col("moyen_transport") == "Intermodal") & (F.col("nb_trajets") < 2)
    ).count()
    apied_errors = df.filter(
        (F.col("moyen_transport") == "APied") & (F.col("nb_trajets") != 1)
    ).count()
    unknown_bad = df.filter(
        (F.col("strate_connue_depart") & (F.col("famille_zone_depart") == "AUTRE_INCONNU"))
        | (F.col("strate_connue_arrivee") & (F.col("famille_zone_arrivee") == "AUTRE_INCONNU"))
    ).count()
    return [
        _assertion(
            "key_unique",
            total == distinct_key,
            f"{distinct_key}/{total}",
            "une clé par déplacement",
        ),
        _assertion(
            "mode_count_matches_nb_trajets",
            invalid_modes == 0,
            invalid_modes,
            "0 ligne incohérente",
        ),
        _assertion(
            "duration_matches_elapsed_time",
            duration_errors == 0,
            duration_errors,
            "écart ≤ 1 minute",
        ),
        _assertion(
            "weight_constant_per_household",
            household_weight_errors == 0,
            household_weight_errors,
            "0 ménage à poids multiples",
        ),
        _assertion(
            "quarter_to_stratum_one_to_one",
            quartier_errors == 0,
            quartier_errors,
            "0 quartier associé à plusieurs strates",
        ),
        _assertion(
            "intermodal_has_multiple_trips",
            intermodal_errors == 0,
            intermodal_errors,
            "Intermodal implique nb_trajets ≥ 2",
        ),
        _assertion(
            "walking_has_single_trip",
            apied_errors == 0,
            apied_errors,
            "APied implique nb_trajets = 1",
        ),
        _assertion(
            "known_strata_have_known_family",
            unknown_bad == 0,
            unknown_bad,
            "strates connues dans D/G/P/R",
        ),
    ]


def write_dictionary(df: DataFrame, bronze: str, docs: str, quality: Iterable[dict]) -> None:
    """Generate the source-to-target dictionary with observed type/cardinality/null rates."""
    spark = df.sparkSession
    definitions = spark.read.parquet(str(Path(bronze) / "explication.parquet"))
    quality_frame = spark.createDataFrame(list(quality))
    rows = []
    for source, target in MAPPING.items():
        field = df.schema[target]
        rows.append((source, target, field.dataType.simpleString()))
    mapping_df = spark.createDataFrame(rows, ["nom_source", "nom_cible", "type"])
    mapping_df = mapping_df.join(definitions, "nom_source", "left").join(
        quality_frame, mapping_df.nom_cible == quality_frame.column, "left"
    )
    descriptions = {
        "poids_extra": "Poids d’extrapolation du ménage (inféré)",
        "lieu_depart": "Code quartier de départ",
        "lieu_arrivee": "Code quartier d’arrivée",
        "cout": "Coût déclaré, unité à confirmer",
        "moyen_transport": "Mode retenu après analyse des temps",
        "strate_arrivee": "Strate d’arrivée; colonne source mal orthographiée",
        "nb_trajets_apied_pondere": "Nombre pondéré de tronçons à pied (inféré)",
    }
    rows_md = []
    for row in mapping_df.collect():
        original = row.definition or descriptions.get(
            row.nom_cible, "Définition inférée; à confirmer"
        )
        if not row.definition and row.nom_cible not in descriptions:
            original = f"{original} (inféré)"
        rows_md.append(
            f"| {row.nom_source} | {row.nom_cible} | {row.type} | {original} | "
            f"{row.cardinality} | {row.null_pct:.4f}% |"
        )
    content = (
        "# Dictionnaire des données\n\n| Nom source | Nom cible | Type | Signification | Cardinalité | Nuls (%) |\n|---|---|---|---|---:|---:|\n"
        + "\n".join(rows_md)
        + "\n"
    )
    Path(docs, "dictionnaire_donnees.md").write_text(content, encoding="utf-8")


def write_quality(df: DataFrame, outputs: str, docs: str, bronze: str) -> list[dict]:
    """Export computed per-column quality metrics and named rule statuses."""
    Path(outputs).mkdir(parents=True, exist_ok=True)
    Path(docs).mkdir(parents=True, exist_ok=True)
    summary = quality_summary(df)
    assertions = quality_assertions(df)
    spark = df.sparkSession
    spark.createDataFrame(summary).coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{outputs}/dq_columns"
    )
    spark.createDataFrame(assertions).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(f"{outputs}/dq_assertions")
    report = "# Rapport de qualité\n\n" + "\n".join(
        f"- `{item['assertion']}` : **{item['status']}** — observé {item['observed']}; attendu {item['expected']}."
        for item in assertions
    )
    Path(docs, "data_quality_report.md").write_text(report + "\n", encoding="utf-8")
    write_dictionary(df, bronze, docs, summary)
    return assertions
