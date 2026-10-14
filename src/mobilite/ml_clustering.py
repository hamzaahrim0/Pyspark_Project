"""Weighted individual segmentation with Spark KMeans, stability, and PCA."""

from __future__ import annotations

import itertools
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, functions as F


def build_person_table(df: DataFrame) -> DataFrame:
    """Aggregate observed displacement attributes to one record per person."""
    collective = F.col("moyen_transport").isin("Tata", "CarRapide", "DDD", "NN")
    grouped = df.groupBy("num_mena", "num_individu").agg(
        F.first("poids_extra").alias("poids_extra"),
        F.first("age").alias("age"),
        F.first("sexe").alias("sexe"),
        F.count("*").alias("nb_deplacements"),
        F.avg((F.col("moyen_transport") == "APied").cast("double")).alias("part_marche"),
        F.avg(collective.cast("double")).alias("part_collectif"),
        F.avg("duree_mn").alias("duree_moyenne"),
        F.sum("duree_mn").alias("duree_totale"),
        F.countDistinct("motif").alias("nb_motifs_distincts"),
        F.avg((F.col("motif_groupe") == "Travail").cast("double")).alias("part_motif_travail"),
        F.avg((F.col("motif_groupe") == "Études").cast("double")).alias("part_motif_etudes"),
        F.avg((F.col("motif_groupe") == "Achats").cast("double")).alias("part_motif_achats"),
        F.avg((F.col("motif_groupe") == "Loisirs/Social").cast("double")).alias(
            "part_motif_loisirs"
        ),
        F.min("heure_depart_min_total").alias("premier_depart_min"),
        F.max("heure_arrivee_min_total").alias("dernier_retour_min"),
    )
    return grouped.withColumn(
        "heure_premier_depart", F.pmod(F.floor(F.col("premier_depart_min") / 60), 24).cast("double")
    ).withColumn(
        "heure_dernier_retour", F.pmod(F.floor(F.col("dernier_retour_min") / 60), 24).cast("double")
    )


def _adjusted_rand(left: DataFrame, right: DataFrame) -> float:
    """Compute adjusted Rand index from a small Spark contingency aggregate."""
    joined = left.select("num_mena", "num_individu", F.col("prediction").alias("a")).join(
        right.select("num_mena", "num_individu", F.col("prediction").alias("b")),
        ["num_mena", "num_individu"],
    )
    cells = joined.groupBy("a", "b").count().collect()
    rows = {}
    cols = {}
    total = 0
    sum_cells = 0
    for cell in cells:
        n = int(cell["count"])
        rows[cell.a] = rows.get(cell.a, 0) + n
        cols[cell.b] = cols.get(cell.b, 0) + n
        total += n
        sum_cells += n * (n - 1) / 2
    sum_rows = sum(n * (n - 1) / 2 for n in rows.values())
    sum_cols = sum(n * (n - 1) / 2 for n in cols.values())
    total_pairs = total * (total - 1) / 2
    expected = sum_rows * sum_cols / total_pairs if total_pairs else 0.0
    maximum = (sum_rows + sum_cols) / 2
    return (sum_cells - expected) / (maximum - expected) if maximum != expected else 1.0


def fit_clusters(df: DataFrame, outputs: str, seed: int = 42) -> dict:
    """Fit weighted KMeans for k=2..10, choose by silhouette, and save diagnostics."""
    persons = build_person_table(df)
    categorical = StringIndexer(inputCol="sexe", outputCol="sexe_index", handleInvalid="keep")
    indexed = categorical.fit(persons).transform(persons)
    input_columns = [
        "nb_deplacements",
        "part_marche",
        "part_collectif",
        "duree_moyenne",
        "duree_totale",
        "nb_motifs_distincts",
        "part_motif_travail",
        "part_motif_etudes",
        "part_motif_achats",
        "part_motif_loisirs",
        "heure_premier_depart",
        "heure_dernier_retour",
        "age",
        "sexe_index",
    ]
    assembler = VectorAssembler(
        inputCols=input_columns, outputCol="raw_features", handleInvalid="keep"
    )
    raw = assembler.transform(indexed)
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withMean=True, withStd=True
    )
    prepared = scaler.fit(raw).transform(raw).cache()
    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="prediction", weightCol="poids_extra"
    )
    scores = []
    models = {}
    for k in range(2, 11):
        model = KMeans(k=k, seed=seed, featuresCol="features", weightCol="poids_extra").fit(
            prepared
        )
        assignments = model.transform(prepared)
        silhouette = evaluator.evaluate(assignments)
        wssse = float(model.summary.trainingCost)
        scores.append({"k": k, "silhouette": silhouette, "wssse": wssse})
        models[k] = model
    selected = max(scores, key=lambda row: row["silhouette"])
    k_best = selected["k"]
    final_model = models[k_best]
    root = Path(outputs)
    for subdirectory in ["models", "metrics", "predictions", "figures", "tables"]:
        (root / subdirectory).mkdir(parents=True, exist_ok=True)
    final_model.write().overwrite().save(str(root / "models" / "clustering"))
    assignments = final_model.transform(prepared).cache()
    assignments.write.mode("overwrite").parquet(str(root / "predictions" / "clusters.parquet"))
    profiles = assignments.groupBy("prediction").agg(
        *[
            (F.sum(F.col(name) * F.col("poids_extra")) / F.sum("poids_extra")).alias(
                f"weighted_{name}"
            )
            for name in input_columns
            if name != "sexe_index"
        ],
        F.sum("poids_extra").alias("cluster_weight"),
        F.count("*").alias("persons_raw"),
    )
    profile_rows = profiles.collect()
    mean_walk = sum(row.weighted_part_marche for row in profile_rows) / len(profile_rows)
    names = []
    for row in profile_rows:
        if row.weighted_part_marche >= mean_walk:
            name = "Marche dominante"
        elif row.weighted_part_collectif >= 0.25:
            name = "Usagers du collectif"
        elif row.weighted_nb_deplacements <= 2:
            name = "Mobilité observée réduite"
        else:
            name = "Mobilité diversifiée"
        names.append((row.prediction, name))
    name_df = df.sparkSession.createDataFrame(names, ["prediction", "nom_cluster"])
    profiles.join(name_df, "prediction").coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(str(root / "tables" / "cluster_profiles"))
    profile_pd = profiles.orderBy("prediction").toPandas()
    feature_names = [column for column in profile_pd.columns if column.startswith("weighted_")]
    figure, axis = plt.subplots(figsize=(12, 6))
    axis.imshow(profile_pd[feature_names].to_numpy(), aspect="auto")
    axis.set_xticks(
        range(len(feature_names)), [x.removeprefix("weighted_") for x in feature_names], rotation=75
    )
    axis.set_yticks(range(len(profile_pd)), profile_pd.prediction)
    figure.tight_layout()
    figure.savefig(root / "figures" / "cluster_profiles_heatmap.png", dpi=140)
    plt.close(figure)
    score_df = df.sparkSession.createDataFrame(scores)
    score_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(root / "tables" / "cluster_scores")
    )
    figure, axis = plt.subplots(figsize=(8, 4))
    axis.plot([x["k"] for x in scores], [x["wssse"] for x in scores], marker="o")
    axis.set(xlabel="k", ylabel="WSSSE", title="Coude KMeans")
    figure.tight_layout()
    figure.savefig(root / "figures" / "cluster_elbow.png", dpi=140)
    plt.close(figure)
    pca_model = PCA(k=2, inputCol="features", outputCol="pca_features").fit(prepared)
    pca = (
        pca_model.transform(assignments).select("prediction", "pca_features").limit(5000).toPandas()
    )
    coordinates = pca.pca_features.apply(lambda vector: vector.toArray())
    figure, axis = plt.subplots(figsize=(8, 6))
    for cluster in sorted(pca.prediction.unique()):
        selected_points = [
            coordinates.iloc[i] for i in range(len(pca)) if pca.prediction.iloc[i] == cluster
        ]
        axis.scatter(
            [point[0] for point in selected_points],
            [point[1] for point in selected_points],
            s=8,
            label=str(cluster),
            alpha=0.5,
        )
    axis.legend(title="Cluster")
    axis.set(title="Projection PCA 2D", xlabel="PC1", ylabel="PC2")
    figure.tight_layout()
    figure.savefig(root / "figures" / "clusters_pca.png", dpi=140)
    plt.close(figure)
    stability = []
    seed_models = []
    for alternate_seed in [seed, seed + 1, seed + 2]:
        alternate = KMeans(
            k=k_best, seed=alternate_seed, featuresCol="features", weightCol="poids_extra"
        ).fit(prepared)
        seed_models.append(
            alternate.transform(prepared).select("num_mena", "num_individu", "prediction")
        )
    for left, right in itertools.combinations(range(3), 2):
        stability.append(
            {
                "seed_left": seed + left,
                "seed_right": seed + right,
                "adjusted_rand_index": _adjusted_rand(seed_models[left], seed_models[right]),
            }
        )
    results = {
        "k": k_best,
        "silhouette": selected["silhouette"],
        "scores": scores,
        "stability": stability,
        "cluster_names": names,
    }
    (root / "metrics" / "clustering.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    assignments.unpersist()
    prepared.unpersist()
    return results
