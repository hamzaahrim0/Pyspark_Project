"""Command line entry point for the complete Spark workflow."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from .analytics import run_analytics
from .cleaning import clean_deplacements
from .config import load_config
from .features import engineer_features
from .ingestion import (
    convert_workbook,
    load_deplacement,
    load_od_reference,
    load_strate_dimension,
)
from .ml_clustering import fit_clusters
from .ml_duration import fit_duration_model
from .ml_mode_choice import fit_mode_model
from .quality import build_dim_quartier, write_quality
from .reporting import build_report
from .spark_session import create_spark

LOG = logging.getLogger("mobilite")
STAGES = ["ingest", "clean", "features", "analytics", "ml_mode", "ml_duration", "cluster", "report"]
DATA_STAGES = {"clean", "features", "analytics", "ml_mode", "ml_duration", "cluster"}


def _parse_stages(value: str) -> list[str]:
    """Parse a single stage, comma-separated stages, or all."""
    chosen = (
        STAGES if value == "all" else [part for part in value.replace("|", ",").split(",") if part]
    )
    unknown = sorted(set(chosen) - set(STAGES))
    if unknown:
        raise ValueError(f"Unknown stages {unknown}; expected all or {STAGES}")
    return chosen


def main() -> None:
    """Run selected stages and transparently satisfy their data dependencies."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--stages", default="all")
    parser.add_argument("--sample", type=int, help="Limit to this many households")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    config = load_config(args.config)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    chosen = _parse_stages(args.stages)
    paths = config["paths"]
    if args.sample is not None and args.sample < 1:
        parser.error("--sample must be a positive household count")
    spark = create_spark(config)
    try:
        import pandas
        import pyspark

        LOG.info(
            "Spark=%s PySpark=%s pandas=%s seed=%s JAVA_HOME=%s",
            spark.version,
            pyspark.__version__,
            pandas.__version__,
            config["seed"],
            os.environ["JAVA_HOME"],
        )
        if "ingest" in chosen:
            convert_workbook(paths["source"], paths["bronze"])
            load_strate_dimension(spark, paths["bronze"]).write.mode("overwrite").parquet(
                str(Path(paths["bronze"]) / "dim_strate_commune")
            )
        if not DATA_STAGES.intersection(chosen):
            if "report" in chosen:
                build_report("outputs/metrics", "reports/rapport_final.md")
            return
        bronze = Path(paths["bronze"])
        source = Path(paths["source"])
        main_parquet = bronze / "deplacement.parquet"
        if not main_parquet.exists() or source.stat().st_mtime > main_parquet.stat().st_mtime:
            convert_workbook(paths["source"], paths["bronze"])
        dimension_path = bronze / "dim_strate_commune"
        if not dimension_path.exists():
            load_strate_dimension(spark, paths["bronze"]).write.mode("overwrite").parquet(
                str(dimension_path)
            )
        frame = load_deplacement(spark, paths["bronze"])
        dim_quartier = build_dim_quartier(frame)
        dim_quartier.write.mode("overwrite").parquet("data/silver/dim_quartier")
        if args.sample:
            households = frame.select("num_mena").distinct().orderBy("num_mena").limit(args.sample)
            frame = frame.join(households, "num_mena", "left_semi")
        silver = clean_deplacements(frame)
        silver.write.mode("overwrite").parquet(paths["silver"])
        assertions = write_quality(
            silver, str(Path(paths["outputs"]) / "tables"), "docs", paths["bronze"]
        )
        failed = [
            item["assertion"]
            for item in assertions
            if item["status"] != "OK" and item["assertion"] != "quarter_to_stratum_one_to_one"
        ]
        if failed:
            raise AssertionError(f"Silver quality checks failed: {failed}")
        for item in assertions:
            if item["status"] == "KO":
                LOG.warning(
                    "Quality assertion KO: %s; observed=%s", item["assertion"], item["observed"]
                )
        if not (set(chosen) & {"features", "analytics", "ml_mode", "ml_duration", "cluster"}):
            if "report" in chosen:
                build_report("outputs/metrics", "reports/rapport_final.md")
            return
        gold = engineer_features(silver)
        gold.write.mode("overwrite").parquet(paths["gold"])
        if "analytics" in chosen:
            reference = load_od_reference(spark, paths["bronze"]) if args.sample is None else None
            analytics_metrics = run_analytics(gold, paths["outputs"], reference)
            metric_path = Path(paths["outputs"]) / "metrics" / "analytics.json"
            metric_path.parent.mkdir(parents=True, exist_ok=True)
            metric_path.write_text(
                json.dumps(analytics_metrics, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        if "ml_mode" in chosen:
            fit_mode_model(gold, paths["outputs"], config)
        if "ml_duration" in chosen:
            fit_duration_model(gold, paths["outputs"], config)
        if "cluster" in chosen:
            fit_clusters(gold, paths["outputs"], config["seed"])
        if "report" in chosen:
            build_report("outputs/metrics", "reports/rapport_final.md")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
