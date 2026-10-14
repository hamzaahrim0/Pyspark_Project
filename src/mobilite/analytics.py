"""Weighted and unweighted descriptive analysis built on Spark aggregates."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Window, functions as F

from .plots import plot_bar


def _write_csv(df: DataFrame, path: Path) -> None:
    """Write a Spark aggregate as a CSV directory."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))


def assert_od_reconciliation(observed: DataFrame, reference: DataFrame) -> dict[str, float]:
    """Check weighted OD total and every cell against the workbook pivot."""
    keys = ["strate_depart", "strate_arrivee"]
    actual = observed.groupBy(*keys).agg(F.sum("poids_total").alias("actual"))
    expected = reference.groupBy(*keys).agg(F.sum("poids_total").alias("expected"))
    cells = actual.join(expected, keys, "full").fillna(0.0, subset=["actual", "expected"])
    mismatch = cells.filter(
        F.abs(F.col("actual") - F.col("expected"))
        > F.lit(1e-9) * F.greatest(F.abs(F.col("expected")), F.lit(1.0))
    ).count()
    actual_total = actual.agg(F.sum("actual")).first()[0] or 0.0
    expected_total = expected.agg(F.sum("expected")).first()[0] or 0.0
    if expected_total == 0 or abs(actual_total - expected_total) > 1e-9 * abs(expected_total):
        raise AssertionError(
            f"OD total mismatch: Spark={actual_total:.12f}, workbook={expected_total:.12f}"
        )
    if mismatch:
        raise AssertionError(f"OD matrix differs in {mismatch} cells")
    return {
        "spark_total": float(actual_total),
        "reference_total": float(expected_total),
        "mismatched_cells": float(mismatch),
    }


def run_analytics(df: DataFrame, outputs: str, reference_od: DataFrame | None = None) -> dict:
    """Produce the six required analyses, paired raw/weighted tables and figures."""
    root = Path(outputs)
    tables = root / "tables"
    figures = root / "figures"
    tables.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    result: dict[str, object] = {}

    # 1. Modal shares overall and by requested population segments.
    segments = [
        None,
        "tranche_horaire",
        "tranche_age",
        "sexe",
        "motif_groupe",
        "famille_zone_depart",
    ]
    for target in ["moyen_transport", "famille_modale"]:
        for segment in segments:
            group = [target] + ([segment] if segment else [])
            base = df.groupBy(*group).agg(
                F.count("*").alias("n_brut"), F.sum("poids_extra").alias("n_pondere")
            )
            window = Window.partitionBy(*([segment] if segment else []))
            base = base.withColumn("part_brute", F.col("n_brut") / F.sum("n_brut").over(window))
            base = base.withColumn(
                "part_ponderee", F.col("n_pondere") / F.sum("n_pondere").over(window)
            )
            label = f"modal_{target}_{segment or 'global'}"
            _write_csv(base, tables / label)
            if segment is None:
                plot_bar(
                    base,
                    target,
                    "part_ponderee",
                    f"Part pondérée — {target}",
                    figures / f"{label}.png",
                )

    # 2. Hourly departures, raw and weighted by mode.
    hourly = df.groupBy("heure_depart_h", "moyen_transport").agg(
        F.count("*").alias("n_brut"), F.sum("poids_extra").alias("n_pondere")
    )
    _write_csv(hourly, tables / "profil_horaire")
    hourly_plot = hourly.toPandas()
    hourly_pivot = hourly_plot.pivot_table(
        index="heure_depart_h",
        columns="moyen_transport",
        values="n_pondere",
        aggfunc="sum",
        fill_value=0,
    )
    figure, axis = plt.subplots(figsize=(11, 5))
    hourly_pivot.sort_index().plot(ax=axis)
    axis.set(title="Départs pondérés par heure et mode", xlabel="Heure", ylabel="Poids extrapolé")
    figure.tight_layout()
    figure.savefig(figures / "profil_horaire_par_mode.png", dpi=140)
    plt.close(figure)
    plot_bar(
        hourly.groupBy("heure_depart_h").agg(F.sum("n_pondere").alias("n_pondere")),
        "heure_depart_h",
        "n_pondere",
        "Départs pondérés par heure",
        figures / "profil_horaire.png",
    )

    # 3. Duration distribution and mode/purpose summaries; omit anomalous costs.
    duration = df.groupBy("moyen_transport").agg(
        F.count("*").alias("n_brut"),
        F.avg("duree_mn").alias("duree_moyenne_brute"),
        F.expr("percentile_approx(duree_mn, 0.5, 10000)").alias("mediane_mn"),
        F.expr("percentile_approx(duree_mn, 0.9, 10000)").alias("p90_mn"),
        (F.sum(F.col("duree_mn") * F.col("poids_extra")) / F.sum("poids_extra")).alias(
            "duree_moyenne_ponderee"
        ),
        F.sum("poids_extra").alias("n_pondere"),
    )
    _write_csv(duration, tables / "duree_par_mode")
    purpose_duration = df.groupBy("motif_groupe").agg(
        F.count("*").alias("n_brut"),
        F.sum("poids_extra").alias("n_pondere"),
        F.avg("duree_mn").alias("duree_moyenne_brute"),
        (F.sum(F.col("duree_mn") * F.col("poids_extra")) / F.sum("poids_extra")).alias(
            "duree_moyenne_ponderee"
        ),
    )
    _write_csv(purpose_duration, tables / "duree_par_motif")
    cost = (
        df.filter(~F.col("cout_aberrant") & F.col("cout").isNotNull())
        .groupBy("moyen_transport")
        .agg(
            F.avg("cout").alias("cout_moyen_brut"),
            (F.sum(F.col("cout") * F.col("poids_extra")) / F.sum("poids_extra")).alias(
                "cout_moyen_pondere"
            ),
        )
    )
    _write_csv(cost, tables / "cout_par_mode")
    plot_bar(
        cost,
        "moyen_transport",
        "cout_moyen_pondere",
        "Coût moyen pondéré par mode",
        figures / "cout_par_mode.png",
    )
    duration_distribution = (
        df.groupBy("duree_mn")
        .agg(F.count("*").alias("n_brut"), F.sum("poids_extra").alias("n_pondere"))
        .orderBy("duree_mn")
    )
    _write_csv(duration_distribution, tables / "distribution_duree")
    distribution_plot = duration_distribution.toPandas()
    figure, axis = plt.subplots(figsize=(10, 4))
    axis.plot(distribution_plot.duree_mn, distribution_plot.n_brut, label="Brut")
    axis.plot(distribution_plot.duree_mn, distribution_plot.n_pondere, label="Pondéré")
    axis.set(xlabel="Durée (minutes)", ylabel="Effectif / poids", title="Distribution des durées")
    axis.legend()
    figure.tight_layout()
    figure.savefig(figures / "distribution_duree.png", dpi=140)
    plt.close(figure)
    plot_bar(
        duration,
        "moyen_transport",
        "duree_moyenne_ponderee",
        "Durée moyenne pondérée",
        figures / "duree_par_mode.png",
    )

    # 4. Purpose profile and observed mobility ratio with its denominator limitation.
    purpose = df.groupBy("motif_groupe").agg(
        F.count("*").alias("n_brut"),
        F.sum("poids_extra").alias("n_pondere"),
        F.avg("duree_mn").alias("duree_moyenne_brute"),
        (F.sum(F.col("duree_mn") * F.col("poids_extra")) / F.sum("poids_extra")).alias(
            "duree_moyenne_ponderee"
        ),
    )
    purpose_totals = purpose.agg(
        F.sum("n_brut").alias("total_brut"), F.sum("n_pondere").alias("total_pondere")
    ).first()
    purpose = purpose.withColumn("part_brute", F.col("n_brut") / F.lit(purpose_totals.total_brut))
    purpose = purpose.withColumn(
        "part_ponderee", F.col("n_pondere") / F.lit(purpose_totals.total_pondere)
    )
    _write_csv(purpose, tables / "motifs")
    plot_bar(
        purpose, "motif_groupe", "part_ponderee", "Part pondérée des motifs", figures / "motifs.png"
    )
    people = df.select("num_mena", "num_individu", "poids_extra").dropDuplicates(
        ["num_mena", "num_individu"]
    )
    person_weight = (
        people.agg(F.sum("poids_extra").alias("individuals_weighted")).first().individuals_weighted
    )
    movement_weight = (
        df.agg(F.sum("poids_extra").alias("movements_weighted")).first().movements_weighted
    )
    mobility = float(movement_weight / person_weight) if person_weight else 0.0
    result["observed_mobility_rate"] = mobility
    result["mobility_rate_limitation"] = (
        "Les individus sans déplacement sont absents du fichier observé."
    )

    # 5. Global and family OD matrices, with a real workbook reconciliation.
    od = df.groupBy("strate_depart", "strate_arrivee").agg(
        F.sum("poids_extra").alias("poids_total")
    )
    _write_csv(od, tables / "matrice_od")
    od_family = df.groupBy("famille_modale", "strate_depart", "strate_arrivee").agg(
        F.sum("poids_extra").alias("poids_total")
    )
    _write_csv(od_family, tables / "matrice_od_famille_modale")
    if reference_od is not None:
        result["od_reconciliation"] = assert_od_reconciliation(od, reference_od)
    top_strata = (
        df.groupBy("strate_depart")
        .agg(F.sum("poids_extra").alias("weight"))
        .orderBy(F.desc("weight"))
        .limit(20)
        .select("strate_depart")
        .toPandas()["strate_depart"]
        .tolist()
    )
    heat = od.filter(
        F.col("strate_depart").isin(top_strata) & F.col("strate_arrivee").isin(top_strata)
    ).toPandas()
    pivot = heat.pivot(
        index="strate_depart", columns="strate_arrivee", values="poids_total"
    ).fillna(0)
    figure, axis = plt.subplots(figsize=(10, 8))
    image = axis.imshow(pivot.to_numpy(), aspect="auto")
    axis.set_xticks(range(len(pivot.columns)), pivot.columns, rotation=90)
    axis.set_yticks(range(len(pivot.index)), pivot.index)
    figure.colorbar(image, ax=axis, label="Poids extrapolé")
    figure.tight_layout()
    figure.savefig(figures / "matrice_od_heatmap.png", dpi=140)
    plt.close(figure)

    # 6. Top flows, intrazone share, and intermodal share, raw and weighted.
    flows = (
        df.groupBy("strate_depart", "strate_arrivee")
        .agg(F.count("*").alias("n_brut"), F.sum("poids_extra").alias("n_pondere"))
        .orderBy(F.desc("n_pondere"))
    )
    top_flows = flows.limit(15)
    _write_csv(top_flows, tables / "top15_flux_od")
    flow_plot = top_flows.withColumn(
        "od", F.concat_ws("→", "strate_depart", "strate_arrivee")
    ).select("od", "n_pondere")
    plot_bar(flow_plot, "od", "n_pondere", "15 principaux flux OD", figures / "top15_flux_od.png")
    totals = df.agg(
        F.sum("poids_extra").alias("weighted_total"),
        F.count("*").alias("raw_total"),
        F.sum(F.when(F.col("meme_strate"), F.col("poids_extra")).otherwise(0)).alias(
            "intra_weight"
        ),
        F.sum(F.when(F.col("meme_strate"), 1).otherwise(0)).alias("intra_raw"),
        F.sum(
            F.when(F.col("moyen_transport") == "Intermodal", F.col("poids_extra")).otherwise(0)
        ).alias("intermodal_weight"),
        F.sum(F.when(F.col("moyen_transport") == "Intermodal", 1).otherwise(0)).alias(
            "intermodal_raw"
        ),
    ).first()
    result.update(
        {
            "intra_strate_share_raw": float(totals.intra_raw / totals.raw_total),
            "intra_strate_share_weighted": float(totals.intra_weight / totals.weighted_total),
            "intermodal_share_raw": float(totals.intermodal_raw / totals.raw_total),
            "intermodal_share_weighted": float(totals.intermodal_weight / totals.weighted_total),
        }
    )
    mode_rows = (
        df.groupBy("moyen_transport")
        .agg(F.count("*").alias("n"), F.sum("poids_extra").alias("weight"))
        .collect()
    )
    mode_total = sum(row.weight for row in mode_rows)
    mode_raw_total = sum(row.n for row in mode_rows)
    top_mode = max(mode_rows, key=lambda row: row.weight)
    peak = (
        df.groupBy("heure_depart_h")
        .agg(F.sum("poids_extra").alias("weight"))
        .orderBy(F.desc("weight"))
        .first()
    )
    top_purpose = purpose.orderBy(F.desc("n_pondere")).first()
    weighted_duration = (
        df.agg(
            (F.sum(F.col("duree_mn") * F.col("poids_extra")) / F.sum("poids_extra")).alias("mean")
        )
        .first()
        .mean
    )
    valid_cost = (
        df.filter(~F.col("cout_aberrant") & F.col("cout").isNotNull())
        .agg((F.sum(F.col("cout") * F.col("poids_extra")) / F.sum("poids_extra")).alias("mean"))
        .first()
        .mean
    )
    insights = [
        {
            "label": f"Mode dominant ({top_mode.moyen_transport}), part pondérée",
            "value": f"{100 * top_mode.weight / mode_total:.2f}%",
        },
        {
            "label": f"Mode dominant ({top_mode.moyen_transport}), part brute",
            "value": f"{100 * top_mode.n / mode_raw_total:.2f}%",
        },
        {
            "label": "Part des déplacements intra-strate pondérée",
            "value": f"{100 * result['intra_strate_share_weighted']:.2f}%",
        },
        {
            "label": "Part des déplacements intra-strate brute",
            "value": f"{100 * result['intra_strate_share_raw']:.2f}%",
        },
        {
            "label": "Part modale intermodale pondérée",
            "value": f"{100 * result['intermodal_share_weighted']:.2f}%",
        },
        {
            "label": "Heure de départ la plus active (pondérée)",
            "value": f"{peak.heure_depart_h} h ({peak.weight:.2f} déplacements extrapolés)",
        },
        {
            "label": f"Motif le plus fréquent ({top_purpose.motif_groupe}), déplacements pondérés",
            "value": f"{top_purpose.n_pondere:.2f}",
        },
        {"label": "Durée moyenne pondérée", "value": f"{weighted_duration:.2f} minutes"},
    ]
    if valid_cost is not None:
        insights.append(
            {
                "label": "Coût moyen pondéré par mode (hors valeurs aberrantes)",
                "value": f"{valid_cost:.2f} unités déclarées",
            }
        )
    if "od_reconciliation" in result:
        insights.append(
            {
                "label": "Total OD pondéré réconcilié avec le pivot",
                "value": f"{result['od_reconciliation']['spark_total']:.3f}",
            }
        )
    result["insights"] = insights[:10]
    return result
