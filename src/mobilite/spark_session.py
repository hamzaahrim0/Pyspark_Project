"""Spark session factory with project-scoped Java selection."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_spark(config: dict) -> SparkSession:
    """Create local Spark after selecting and validating a supported JDK."""
    settings = config["spark"]
    java_home = settings.get("java_home") or os.environ.get("JAVA_HOME")
    if not java_home:
        project_jdk = Path.home() / ".jdks" / "temurin-17"
        if (project_jdk / "bin" / "java").is_file():
            java_home = str(project_jdk)
    if not java_home:
        raise RuntimeError("JAVA_HOME is unset; configure a Java 11 or 17 JDK for this project.")
    resolved = str(Path(os.path.expandvars(os.path.expanduser(java_home))).resolve())
    java = Path(resolved, "bin", "java")
    if not java.is_file():
        raise RuntimeError(f"JAVA_HOME does not contain bin/java: {resolved}")
    os.environ["JAVA_HOME"] = resolved
    os.environ["PATH"] = f"{Path(resolved, 'bin')}:{os.environ.get('PATH', '')}"
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(settings["master"])
        .appName("mobilite-spark")
        .config("spark.sql.shuffle.partitions", settings["shuffle_partitions"])
        .config("spark.driver.memory", settings["driver_memory"])
        .config("spark.sql.session.timeZone", settings["timezone"])
        .config("spark.sql.adaptive.enabled", str(settings["adaptive"]).lower())
    )
    return builder.getOrCreate()
