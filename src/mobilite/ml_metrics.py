"""Classification metrics computed from small Spark confusion aggregates."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def classification_metrics(
    predictions: DataFrame, labels: list[str], weight_col: str | None = None
) -> dict:
    """Compute accuracy, macro-F1, weighted-F1, and per-class rates."""
    weight = F.col(weight_col) if weight_col else F.lit(1.0)
    cells = (
        predictions.groupBy("label", "prediction")
        .agg(F.count("*").alias("count"), F.sum(weight).alias("weight_sum"))
        .collect()
    )
    n_labels = len(labels)
    raw = [[0.0] * n_labels for _ in range(n_labels)]
    weighted = [[0.0] * n_labels for _ in range(n_labels)]
    for cell in cells:
        actual, predicted = int(cell.label), int(cell.prediction)
        if 0 <= actual < n_labels and 0 <= predicted < n_labels:
            raw[actual][predicted] += float(cell["count"])
            weighted[actual][predicted] += float(cell.weight_sum)

    def summarize(matrix: list[list[float]]) -> dict:
        per_class = []
        total = sum(map(sum, matrix))
        for index, label in enumerate(labels):
            tp = matrix[index][index]
            actual = sum(matrix[index])
            predicted = sum(row[index] for row in matrix)
            precision = tp / predicted if predicted else 0.0
            recall = tp / actual if actual else 0.0
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            per_class.append(
                {
                    "label": label,
                    "precision": precision,
                    "recall": recall,
                    "f1": f1,
                    "support": actual,
                }
            )
        macro_f1 = sum(item["f1"] for item in per_class) / n_labels if n_labels else 0.0
        support = sum(item["support"] for item in per_class)
        weighted_f1 = (
            sum(item["f1"] * item["support"] for item in per_class) / support if support else 0.0
        )
        accuracy = sum(matrix[i][i] for i in range(n_labels)) / total if total else 0.0
        return {
            "accuracy": accuracy,
            "macro_f1": macro_f1,
            "weighted_f1": weighted_f1,
            "per_class": per_class,
            "confusion_matrix": matrix,
        }

    return {"unweighted": summarize(raw), "survey_weighted": summarize(weighted)}
