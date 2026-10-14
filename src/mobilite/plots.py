"""Plot small Spark aggregates after collecting only bounded result sets."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, functions as F


def plot_bar(
    df: DataFrame, category: str, value: str, title: str, target: Path, limit: int = 30
) -> None:
    """Render a bar chart from a bounded aggregate DataFrame."""
    local = df.orderBy(F.desc(value)).limit(limit).toPandas()
    figure, axis = plt.subplots(figsize=(10, 5))
    axis.bar(local[category].astype(str), local[value])
    axis.set_title(title)
    axis.tick_params(axis="x", labelrotation=70)
    figure.tight_layout()
    figure.savefig(target, dpi=140)
    plt.close(figure)
