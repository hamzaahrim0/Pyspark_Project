"""Leakage-checked A1/A2 Spark ML mode-choice classifiers."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, Window, functions as F

from .features import MODE_FEATURES, add_train_od_statistics, assert_no_leakage
from .splits import add_household_fold
from .ml_metrics import classification_metrics
from .splits import split_households

NUMERIC_FEATURES = {
    "age",
    "heure_depart_h",
    "taille_menage_obs",
    "rang_deplacement",
    "nb_depl_individu",
    "od_freq",
    "od_duree_mediane",
    "od_part_marche",
}


def _evaluate(predictions: DataFrame, labels: list[str]) -> dict:
    """Calculate raw and survey-weighted classification results."""
    return classification_metrics(predictions, labels, "poids_extra")


def _save_confusion(metrics: dict, scenario: str, root: Path) -> None:
    """Save raw and normalized confusion heatmaps from small matrices."""
    raw = metrics["unweighted"]["confusion_matrix"]
    labels = [item["label"] for item in metrics["unweighted"]["per_class"]]
    row_sums = [sum(row) for row in raw]
    normalized = [
        [value / row_sums[i] if row_sums[i] else 0.0 for value in row] for i, row in enumerate(raw)
    ]
    for matrix, label in [(raw, "brute"), (normalized, "normalisee")]:
        figure, axis = plt.subplots(figsize=(9, 7))
        image = axis.imshow(matrix, aspect="auto")
        axis.set_xticks(range(len(labels)), labels, rotation=90)
        axis.set_yticks(range(len(labels)), labels)
        axis.set_title(f"Matrice de confusion {label} — {scenario}")
        figure.colorbar(image, ax=axis)
        figure.tight_layout()
        figure.savefig(root / "figures" / f"mode_choice_{scenario}_confusion_{label}.png", dpi=140)
        plt.close(figure)


def _group_cv_rf(
    source: DataFrame, target: str, features: list[str], numeric: set[str], config: dict
) -> tuple[list[dict], int]:
    """Tune a compact RF depth grid with three household-held-out folds."""
    categorical = [name for name in features if name not in numeric]
    folded = add_household_fold(source, 3)
    depths = sorted(set([min(4, config["ml"]["max_depth"]), config["ml"]["max_depth"]]))
    grid = []
    for depth in depths:
        fold_scores = []
        for fold in range(3):
            fold_train = folded.filter(F.col("household_fold") != fold).drop("household_fold")
            fold_valid = folded.filter(F.col("household_fold") == fold).drop("household_fold")
            fold_train, fold_valid = add_train_od_statistics(fold_train, fold_valid)
            for column in categorical:
                fold_train = fold_train.withColumn(column, F.col(column).cast("string"))
                fold_valid = fold_valid.withColumn(column, F.col(column).cast("string"))
            target_indexer = StringIndexer(
                inputCol=target, outputCol="label", handleInvalid="keep"
            ).fit(fold_train)
            fold_train = target_indexer.transform(fold_train)
            fold_valid = target_indexer.transform(fold_valid)
            counts = fold_train.groupBy("label").count()
            totals = counts.agg(F.sum("count").alias("n"), F.count("*").alias("k")).first()
            counts = counts.withColumn(
                "class_weight", F.lit(float(totals.n)) / (F.lit(float(totals.k)) * F.col("count"))
            )
            fold_train = fold_train.join(counts.select("label", "class_weight"), "label", "left")
            encoded = [f"{column}_idx" for column in categorical]
            indexers = [
                StringIndexer(inputCols=categorical, outputCols=encoded, handleInvalid="keep")
            ]
            numeric_columns = sorted(numeric & set(features))
            tree_categories = [name for name in encoded if name != "od_pair_idx"]
            stages = indexers + [
                OneHotEncoder(
                    inputCols=["od_pair_idx"],
                    outputCols=["od_pair_oh"],
                    handleInvalid="keep",
                ),
                VectorAssembler(
                    inputCols=numeric_columns + tree_categories + ["od_pair_oh"],
                    outputCol="features",
                    handleInvalid="keep",
                ),
            ]
            stages += [
                RandomForestClassifier(
                    labelCol="label",
                    featuresCol="features",
                    weightCol="class_weight",
                    numTrees=min(10, config["ml"]["max_trees"]),
                    maxDepth=depth,
                    maxBins=64,
                    seed=config["seed"],
                )
            ]
            fitted = Pipeline(stages=stages).fit(fold_train)
            prediction = fitted.transform(fold_valid)
            fold_scores.append(
                classification_metrics(prediction, list(target_indexer.labels))["unweighted"][
                    "macro_f1"
                ]
            )
        grid.append(
            {
                "max_depth": depth,
                "mean_macro_f1": sum(fold_scores) / len(fold_scores),
                "fold_scores": fold_scores,
            }
        )
    best_depth = max(grid, key=lambda row: row["mean_macro_f1"])["max_depth"]
    return grid, best_depth


def fit_mode_scenario(df: DataFrame, outputs: str, config: dict, scenario: str) -> dict:
    """Train baselines, multinomial LR, RF, and decision tree for A1 or A2."""
    target = config["ml"].get("target", "moyen_transport")
    if target not in {"moyen_transport", "famille_modale"}:
        raise ValueError("ml.target must be moyen_transport or famille_modale")
    features = MODE_FEATURES + (["duree_mn"] if scenario == "A2" else [])
    assert_no_leakage(features, include_duration=(scenario == "A2"))
    train, test = split_households(df, config["seed"])
    numeric = NUMERIC_FEATURES | ({"duree_mn"} if scenario == "A2" else set())
    cv_results, selected_depth = _group_cv_rf(train, target, features, numeric, config)
    train, test = add_train_od_statistics(train, test)
    categorical = [name for name in features if name not in numeric]
    for column in categorical:
        train = train.withColumn(column, F.col(column).cast("string"))
        test = test.withColumn(column, F.col(column).cast("string"))
    label_model = StringIndexer(inputCol=target, outputCol="label", handleInvalid="keep").fit(train)
    train = label_model.transform(train)
    test = label_model.transform(test)
    labels = list(label_model.labels)
    class_counts = train.groupBy("label").count()
    totals = class_counts.agg(F.sum("count").alias("total"), F.count("*").alias("classes")).first()
    class_counts = class_counts.withColumn(
        "class_weight",
        F.lit(float(totals.total) / (float(totals.classes) * 1.0)) / F.col("count"),
    )
    train = train.join(class_counts.select("label", "class_weight"), "label", "left")
    if config["ml"].get("use_survey_weight", False):
        train = train.withColumn("class_weight", F.col("class_weight") * F.col("poids_extra"))
    majority = train.groupBy(target).count().orderBy(F.desc("count"), F.asc(target)).first()[target]
    target_to_index = {name: i for i, name in enumerate(labels)}
    root = Path(outputs)
    (root / "models").mkdir(parents=True, exist_ok=True)
    (root / "predictions").mkdir(parents=True, exist_ok=True)
    (root / "metrics").mkdir(parents=True, exist_ok=True)
    (root / "figures").mkdir(parents=True, exist_ok=True)
    results: dict = {
        "scenario": scenario,
        "target": target,
        "features": features,
        "group_cv": cv_results,
        "selected_rf_depth": selected_depth,
        "models": {},
    }

    baseline = test.withColumn("prediction", F.lit(float(target_to_index[majority])))
    results["models"]["baseline_majority"] = _evaluate(baseline, labels)
    conditional = (
        train.groupBy("motif_groupe", "famille_zone_depart", target)
        .count()
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("motif_groupe", "famille_zone_depart").orderBy(
                    F.desc("count"), F.asc(target)
                )
            ),
        )
        .filter(F.col("rank") == 1)
        .select(
            "motif_groupe", "famille_zone_depart", F.col(target).alias("conditional_prediction")
        )
    )
    condition_test = test.join(
        F.broadcast(conditional), ["motif_groupe", "famille_zone_depart"], "left"
    )
    condition_expr = F.create_map(
        *[
            item
            for pair in target_to_index.items()
            for item in (F.lit(pair[0]), F.lit(float(pair[1])))
        ]
    )
    condition_test = condition_test.withColumn(
        "prediction",
        F.coalesce(
            condition_expr[F.col("conditional_prediction")], F.lit(float(target_to_index[majority]))
        ),
    )
    results["models"]["baseline_motif_zone"] = _evaluate(condition_test, labels)

    indexed_columns = [f"{column}_idx" for column in categorical]
    indexers = [
        StringIndexer(inputCols=categorical, outputCols=indexed_columns, handleInvalid="keep")
    ]
    encoders = [
        OneHotEncoder(
            inputCols=indexed_columns,
            outputCols=[f"{column}_oh" for column in categorical],
            handleInvalid="keep",
        )
    ]
    numeric_inputs = sorted(numeric & set(features))
    lr_assembler = VectorAssembler(
        inputCols=numeric_inputs + [f"{column}_oh" for column in categorical],
        outputCol="raw_features",
        handleInvalid="keep",
    )
    tree_od_encoder = OneHotEncoder(
        inputCols=["od_pair_idx"], outputCols=["od_pair_oh"], handleInvalid="keep"
    )
    tree_assembler = VectorAssembler(
        inputCols=numeric_inputs
        + [column for column in indexed_columns if column != "od_pair_idx"]
        + ["od_pair_oh"],
        outputCol="raw_features",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withMean=False, withStd=True
    )
    estimators = {
        "logistic_regression": LogisticRegression(
            labelCol="label",
            featuresCol="features",
            weightCol="class_weight",
            family="multinomial",
            elasticNetParam=0.5,
            maxIter=50,
            regParam=0.05,
        ),
        "random_forest": RandomForestClassifier(
            labelCol="label",
            featuresCol="raw_features",
            weightCol="class_weight",
            numTrees=config["ml"]["max_trees"],
            maxDepth=selected_depth,
            maxBins=64,
            seed=config["seed"],
        ),
        "decision_tree": DecisionTreeClassifier(
            labelCol="label",
            featuresCol="raw_features",
            weightCol="class_weight",
            maxDepth=min(config["ml"]["max_depth"], 8),
            maxBins=64,
            seed=config["seed"],
        ),
    }
    predictions_by_model = {}
    for name, estimator in estimators.items():
        if name == "logistic_regression":
            model_stages = indexers + encoders + [lr_assembler, scaler, estimator]
        else:
            model_stages = indexers + [tree_od_encoder, tree_assembler, estimator]
        pipeline = Pipeline(stages=model_stages)
        fitted = pipeline.fit(train)
        predictions = fitted.transform(test)
        predictions_by_model[name] = predictions
        metrics = _evaluate(predictions, labels)
        results["models"][name] = metrics
        fitted.write().overwrite().save(str(root / "models" / f"mode_choice_{scenario}_{name}"))
        if name == "random_forest":
            importance = fitted.stages[-1].featureImportances.toArray().tolist()
            ordered = sorted(enumerate(importance), key=lambda item: item[1], reverse=True)
            (root / "metrics" / f"mode_choice_{scenario}_feature_importance.json").write_text(
                json.dumps(
                    [{"feature_index": i, "importance": value} for i, value in ordered], indent=2
                ),
                encoding="utf-8",
            )
            top = ordered[:20]
            figure, axis = plt.subplots(figsize=(9, 5))
            axis.bar([f"f{i}" for i, _ in reversed(top)], [value for _, value in reversed(top)])
            axis.set_title(f"Importance RF — {scenario}")
            figure.tight_layout()
            figure.savefig(
                root / "figures" / f"mode_choice_{scenario}_feature_importance.png", dpi=140
            )
            plt.close(figure)
        if name == "logistic_regression":
            coefficients = fitted.stages[-1].coefficientMatrix.toArray().tolist()
            (root / "metrics" / f"mode_choice_{scenario}_lr_coefficients.json").write_text(
                json.dumps(coefficients), encoding="utf-8"
            )

    best_name = max(
        results["models"], key=lambda name: results["models"][name]["unweighted"]["macro_f1"]
    )
    results["best_model"] = best_name
    best_predictions = (
        condition_test
        if best_name == "baseline_motif_zone"
        else baseline if best_name == "baseline_majority" else None
    )
    if best_predictions is None:
        best_predictions = predictions_by_model[best_name]
    best_predictions.select(
        "num_mena", "num_individu", "num_deplacement", target, "label", "prediction"
    ).write.mode("overwrite").parquet(str(root / "predictions" / f"mode_choice_{scenario}.parquet"))
    best_raw = results["models"][best_name]["unweighted"]["confusion_matrix"]
    matrix_rows = [
        {
            "actual": labels[i],
            "predicted": labels[j],
            "count": best_raw[i][j],
            "normalized": best_raw[i][j] / sum(best_raw[i]) if sum(best_raw[i]) else 0.0,
        }
        for i in range(len(labels))
        for j in range(len(labels))
    ]
    df.sparkSession.createDataFrame(matrix_rows).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(str(root / "tables" / f"mode_choice_{scenario}_confusion"))
    segment_results = {}
    for segment in ["motif_groupe", "tranche_age"]:
        segment_results[segment] = {}
        for row in best_predictions.groupBy(segment).count().select(segment).collect():
            value = row[segment]
            subset = best_predictions.filter(F.col(segment).eqNullSafe(value))
            segment_results[segment][str(value)] = _evaluate(subset, labels)
    results["error_by_segment"] = segment_results
    _save_confusion(results["models"][best_name], scenario, root)
    path = root / "metrics" / f"mode_choice_{scenario}.json"
    path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return results


def fit_mode_model(df: DataFrame, outputs: str, config: dict) -> dict:
    """Train both A1 ex-ante and A2 duration-known mode-choice scenarios."""
    return {
        "A1": fit_mode_scenario(df, outputs, config, "A1"),
        "A2": fit_mode_scenario(df, outputs, config, "A2"),
    }
