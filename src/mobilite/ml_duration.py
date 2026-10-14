"""Train-only-winsorized Spark duration models and raw/weighted evaluations."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, functions as F

from .features import DURATION_FEATURES, add_train_od_statistics, assert_duration_no_leakage
from .ml_mode_choice import NUMERIC_FEATURES
from .splits import split_households


def _regression_metrics(df: DataFrame) -> dict:
    """Calculate raw and survey-weighted regression metrics from Spark sums."""
    pred_minutes = F.exp(F.col("prediction")) - 1.0
    observed_minutes = F.col("duree_mn").cast("double")
    log_error = F.col("prediction") - F.log1p(observed_minutes)
    minute_error = pred_minutes - observed_minutes
    aggregates = df.agg(
        F.count("*").alias("n"),
        F.sum(log_error * log_error).alias("log_sse"),
        F.sum(F.abs(log_error)).alias("log_sae"),
        F.sum(F.log1p(observed_minutes)).alias("log_sum"),
        F.sum(F.log1p(observed_minutes) * F.log1p(observed_minutes)).alias("log_sumsq"),
        F.sum(minute_error * minute_error).alias("minute_sse"),
        F.sum(F.abs(minute_error)).alias("minute_sae"),
        F.sum(observed_minutes).alias("minute_sum"),
        F.sum(observed_minutes * observed_minutes).alias("minute_sumsq"),
        F.sum(F.col("poids_extra")).alias("weight"),
        F.sum(F.col("poids_extra") * log_error * log_error).alias("weighted_log_sse"),
        F.sum(F.col("poids_extra") * F.abs(log_error)).alias("weighted_log_sae"),
        F.sum(F.col("poids_extra") * F.log1p(observed_minutes)).alias("weighted_log_sum"),
        F.sum(F.col("poids_extra") * F.log1p(observed_minutes) * F.log1p(observed_minutes)).alias(
            "weighted_log_sumsq"
        ),
        F.sum(F.col("poids_extra") * minute_error * minute_error).alias("weighted_minute_sse"),
        F.sum(F.col("poids_extra") * F.abs(minute_error)).alias("weighted_minute_sae"),
        F.sum(F.col("poids_extra") * observed_minutes).alias("weighted_minute_sum"),
        F.sum(F.col("poids_extra") * observed_minutes * observed_minutes).alias(
            "weighted_minute_sumsq"
        ),
    ).first()

    def metrics(
        n, log_sse, log_sae, log_sum, log_sumsq, minute_sse, minute_sae, minute_sum, minute_sumsq
    ):
        mean_log = log_sum / n if n else 0.0
        mean_minute = minute_sum / n if n else 0.0
        log_sst = log_sumsq - n * mean_log * mean_log
        minute_sst = minute_sumsq - n * mean_minute * mean_minute
        return {
            "rmse_log": (log_sse / n) ** 0.5 if n else 0.0,
            "mae_log": log_sae / n if n else 0.0,
            "r2_log": 1.0 - log_sse / log_sst if log_sst else 0.0,
            "rmse_minutes": (minute_sse / n) ** 0.5 if n else 0.0,
            "mae_minutes": minute_sae / n if n else 0.0,
            "r2_minutes": 1.0 - minute_sse / minute_sst if minute_sst else 0.0,
        }

    return {
        "unweighted": metrics(
            aggregates.n,
            aggregates.log_sse,
            aggregates.log_sae,
            aggregates.log_sum,
            aggregates.log_sumsq,
            aggregates.minute_sse,
            aggregates.minute_sae,
            aggregates.minute_sum,
            aggregates.minute_sumsq,
        ),
        "survey_weighted": metrics(
            aggregates.weight,
            aggregates.weighted_log_sse,
            aggregates.weighted_log_sae,
            aggregates.weighted_log_sum,
            aggregates.weighted_log_sumsq,
            aggregates.weighted_minute_sse,
            aggregates.weighted_minute_sae,
            aggregates.weighted_minute_sum,
            aggregates.weighted_minute_sumsq,
        ),
    }


def _evaluate_model(predictions: DataFrame) -> dict:
    """Add log prediction to minutes and calculate aggregate metrics."""
    return _regression_metrics(predictions)


def fit_duration_model(df: DataFrame, outputs: str, config: dict) -> dict:
    """Train baselines and LR/RF/GBT on log duration with train p99 winsorization."""
    train, test = split_households(df, config["seed"])
    train, test = add_train_od_statistics(train, test)
    p99 = train.approxQuantile("duree_mn", [0.99], 0.001)[0]
    train = train.withColumn("label", F.log1p(F.least("duree_mn", F.lit(p99))))
    test = test.withColumn("label", F.log1p("duree_mn"))
    numeric = NUMERIC_FEATURES
    features = DURATION_FEATURES
    assert_duration_no_leakage(features)
    categorical = [column for column in features if column not in numeric]
    for column in categorical:
        train = train.withColumn(column, F.col(column).cast("string"))
        test = test.withColumn(column, F.col(column).cast("string"))
    global_median = train.approxQuantile("duree_mn", [0.5], 0.001)[0]
    global_log_median = __import__("math").log1p(global_median)
    mode_medians = train.groupBy("moyen_transport").agg(
        F.expr("percentile_approx(duree_mn, 0.5, 10000)").alias("mode_median")
    )
    od_medians = train.groupBy("strate_depart", "strate_arrivee").agg(
        F.expr("percentile_approx(duree_mn, 0.5, 10000)").alias("od_median")
    )
    baseline_specs = {
        "baseline_global_median": None,
        "baseline_mode_median": mode_medians,
        "baseline_od_median": od_medians,
    }
    results: dict = {"training_p99_minutes": p99, "models": {}}
    for name, stats in baseline_specs.items():
        if name == "baseline_global_median":
            predictions = test.withColumn("prediction", F.lit(global_log_median))
        elif name == "baseline_mode_median":
            predictions = test.join(F.broadcast(stats), "moyen_transport", "left").withColumn(
                "prediction", F.log1p(F.coalesce("mode_median", F.lit(global_median)))
            )
        else:
            predictions = test.join(F.broadcast(stats), ["strate_depart", "strate_arrivee"], "left")
            predictions = predictions.join(F.broadcast(mode_medians), "moyen_transport", "left")
            fallback = F.coalesce("mode_median", F.lit(global_median))
            predictions = predictions.withColumn(
                "prediction", F.log1p(F.coalesce("od_median", fallback))
            )
        results["models"][name] = _evaluate_model(predictions)

    indexed_inputs = [f"{column}_idx" for column in categorical]
    indexers = [
        StringIndexer(inputCols=categorical, outputCols=indexed_inputs, handleInvalid="keep")
    ]
    numeric_inputs = sorted(numeric & set(features))
    tree_od_encoder = OneHotEncoder(
        inputCols=["od_pair_idx"], outputCols=["od_pair_oh"], handleInvalid="keep"
    )
    tree_assembler = VectorAssembler(
        inputCols=numeric_inputs
        + [column for column in indexed_inputs if column != "od_pair_idx"]
        + ["od_pair_oh"],
        outputCol="raw_features",
        handleInvalid="keep",
    )
    root = Path(outputs)
    for subdirectory in ["models", "metrics", "predictions", "figures", "tables"]:
        (root / subdirectory).mkdir(parents=True, exist_ok=True)
    regressors = {
        "linear_regression": LinearRegression(
            labelCol="label", featuresCol="features", maxIter=50, regParam=0.05
        ),
        "random_forest": RandomForestRegressor(
            labelCol="label",
            featuresCol="raw_features",
            numTrees=config["ml"]["max_trees"],
            maxDepth=config["ml"]["max_depth"],
            maxBins=64,
            seed=config["seed"],
        ),
        "gradient_boosted_trees": GBTRegressor(
            labelCol="label",
            featuresCol="raw_features",
            maxIter=30,
            maxDepth=min(config["ml"]["max_depth"], 5),
            maxBins=64,
            seed=config["seed"],
        ),
    }
    predictions_by_model = {}
    for name, estimator in regressors.items():
        stages = indexers.copy()
        if name == "linear_regression":
            stages += [
                OneHotEncoder(
                    inputCols=indexed_inputs,
                    outputCols=[f"{column}_oh" for column in categorical],
                    handleInvalid="keep",
                )
            ]
            lr_assembler = VectorAssembler(
                inputCols=numeric_inputs + [f"{column}_oh" for column in categorical],
                outputCol="raw_features",
                handleInvalid="keep",
            )
            stages += [
                lr_assembler,
                StandardScaler(
                    inputCol="raw_features", outputCol="features", withMean=False, withStd=True
                ),
            ]
        else:
            stages += [tree_od_encoder, tree_assembler]
        stages += [estimator]
        model = Pipeline(stages=stages).fit(train)
        predictions = model.transform(test).cache()
        predictions_by_model[name] = predictions
        results["models"][name] = _evaluate_model(predictions)
        model.write().overwrite().save(str(root / "models" / f"duration_{name}"))
        residuals = predictions.withColumn("pred_minutes", F.exp("prediction") - 1.0).withColumn(
            "residual_minutes", F.col("pred_minutes") - F.col("duree_mn")
        )
        residuals.select(
            "num_mena",
            "num_individu",
            "num_deplacement",
            "moyen_transport",
            "duree_mn",
            "pred_minutes",
            "residual_minutes",
            "poids_extra",
        ).write.mode("overwrite").parquet(str(root / "predictions" / f"duration_{name}.parquet"))
        per_mode = residuals.groupBy("moyen_transport").agg(
            F.avg(F.abs("residual_minutes")).alias("mae_minutes"),
            (
                F.sum(F.abs(F.col("residual_minutes") * F.col("poids_extra")))
                / F.sum("poids_extra")
            ).alias("weighted_mae_minutes"),
        )
        per_mode.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(root / "tables" / f"duration_mae_by_mode_{name}")
        )
        sample = (
            residuals.select("duree_mn", "pred_minutes", "residual_minutes").limit(5000).toPandas()
        )
        figure, axes = plt.subplots(1, 2, figsize=(11, 4))
        axes[0].scatter(sample.duree_mn, sample.pred_minutes, s=8, alpha=0.35)
        axes[0].set(xlabel="Observé (min)", ylabel="Prédit (min)", title="Prédit vs observé")
        axes[1].hist(sample.residual_minutes, bins=30)
        axes[1].set(title="Résidus (min)")
        figure.tight_layout()
        figure.savefig(root / "figures" / f"duration_{name}.png", dpi=140)
        plt.close(figure)
        predictions.unpersist()
    best = max(
        results["models"], key=lambda key: results["models"][key]["unweighted"]["mae_minutes"] * -1
    )
    results["best_model"] = best
    (root / "metrics" / "duration.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return results
