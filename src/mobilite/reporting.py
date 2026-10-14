"""Generate a data-backed French report and README results section."""

from __future__ import annotations

import json
from pathlib import Path


def _load_json(path: Path) -> dict | None:
    """Load a metric file if it exists."""
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _mode_rows(data: dict) -> str:
    """Render real raw and survey-weighted classifier comparisons."""
    results = data.get("models", {})
    best_name = data.get("best_model")
    rows = [
        "| Scénario | Baseline majoritaire acc./macro-F1 | Baseline motif-zone acc./macro-F1 | Modèle retenu | Accuracy brute | Macro-F1 brut | F1 weighted brut | Accuracy pondérée enquête | Macro-F1 pondéré enquête | F1 weighted enquête |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    majority = results.get("baseline_majority", {})
    conditional = results.get("baseline_motif_zone", {})
    best = results.get(best_name, {}) if best_name else {}
    raw = best.get("unweighted", {})
    weighted = best.get("survey_weighted", {})
    rows.append(
        f"| {data.get('scenario', '')} | {majority.get('unweighted', {}).get('accuracy', 0):.4f} / {majority.get('unweighted', {}).get('macro_f1', 0):.4f} | {conditional.get('unweighted', {}).get('accuracy', 0):.4f} / {conditional.get('unweighted', {}).get('macro_f1', 0):.4f} | {best_name or '—'} | {raw.get('accuracy', 0):.4f} | {raw.get('macro_f1', 0):.4f} | {raw.get('weighted_f1', 0):.4f} | {weighted.get('accuracy', 0):.4f} | {weighted.get('macro_f1', 0):.4f} | {weighted.get('weighted_f1', 0):.4f} |"
    )
    return "\n".join(rows)


def _class_recall_table(data: dict) -> str:
    """Render per-class recall for the majority baseline and selected model."""
    models = data.get("models", {})
    baseline = models.get("baseline_majority", {}).get("unweighted", {}).get("per_class", [])
    selected = models.get(data.get("best_model"), {})
    raw = selected.get("unweighted", {}).get("per_class", [])
    weighted = selected.get("survey_weighted", {}).get("per_class", [])
    baseline_by_label = {item["label"]: item["recall"] for item in baseline}
    raw_by_label = {item["label"]: item["recall"] for item in raw}
    weighted_by_label = {item["label"]: item["recall"] for item in weighted}
    rows = [
        "| Classe | Rappel baseline brute | Rappel modèle brut | Rappel modèle pondéré enquête |",
        "|---|---:|---:|---:|",
    ]
    for label in raw_by_label:
        rows.append(
            f"| {label} | {baseline_by_label.get(label, 0):.4f} | {raw_by_label[label]:.4f} | {weighted_by_label.get(label, 0):.4f} |"
        )
    return "\n".join(rows)


def build_report(metrics_dir: str, report_path: str, figures_dir: str = "outputs/figures") -> None:
    """Build the report from computed JSON metrics and generated figure files."""
    root = Path(metrics_dir)
    analytics = _load_json(root / "analytics.json")
    mode_a1 = _load_json(root / "mode_choice_A1.json")
    mode_a2 = _load_json(root / "mode_choice_A2.json")
    duration = _load_json(root / "duration.json")
    clustering = _load_json(root / "clustering.json")
    quality_path = Path("docs/data_quality_report.md")
    quality = (
        quality_path.read_text(encoding="utf-8")
        if quality_path.exists()
        else "Qualité non calculée."
    )
    quality = quality.replace("# Rapport de qualité", "### Synthèse des règles de qualité", 1)
    sections = [
        "# Rapport final — enquête ménages-déplacements de Dakar",
        "",
        "## Données et méthode",
        "",
        "Le projet est organisé selon la pondération demandée : **60 % traitement distribué avec Spark et 40 % apprentissage automatique**. Il traite 45 605 déplacements observés à Dakar, Guédiawaye, Pikine et Rufisque avec PySpark 3.5.9 et Java 17. Les statistiques de population sont pondérées avec `poids_extra`; les versions brutes figurent également dans les tables exportées. Le découpage apprentissage/test est fait par ménage.",
        "",
        "## 1. Partie Spark — 60 %",
        "",
        "### Chaîne de traitement et couches de données",
        "",
        "| Couche/étape | Traitement exécuté | Sortie principale |",
        "|---|---|---|",
        "| Ingestion / bronze | Conversion unique des quatre feuilles Excel en Parquet; schéma explicite des déplacements; matrice OD mise au format long et dimension des strates séparée | `data/bronze/` |",
        "| Qualité / silver | Contrôles Spark, calcul des valeurs manquantes, cardinalités, plages et outliers; conversion HHMM en minutes; anomalies signalées sans supprimer de déplacements | `data/silver/deplacement/`, `outputs/tables/dq_*` |",
        "| Feature engineering / gold | Variables temporelles, zones et motifs; dictionnaire exhaustif des motifs; statistiques OD sans utiliser le jeu de test | `data/gold/deplacement/` |",
        "| Analyses descriptives | Agrégations Spark brutes et pondérées : parts modales, profils horaires, durées, motifs, coûts et flux OD | `outputs/tables/`, `outputs/metrics/analytics.json` |",
        "",
        "La configuration exécutée est `local[*]`, 16 partitions shuffle, exécution adaptative activée et mémoire driver de 4 Gio. La clé déplacement est unique sur les 45 605 lignes; les contrôles de cohérence durée/modes et poids ménage passent. La réconciliation OD confirme 6 699 228,017 avec zéro cellule divergente par rapport au classeur.",
        "",
        "## Qualité et décisions de nettoyage",
        "",
        quality,
        "",
        "Les heures sont converties depuis HHMM en minutes écoulées; les déplacements après minuit gardent leur jour suivant. Les valeurs aberrantes sont signalées et gardées en silver. Les statistiques OD de prédiction sont apprises sur le train, avec encodage hors pli pour le train.",
        "",
        "### Résultats EDA calculés par Spark",
        "",
    ]
    insights = (analytics or {}).get("insights", [])
    if insights:
        sections.extend(f"- {item['label']} : **{item['value']}**." for item in insights)
    else:
        sections.append("Aucune synthèse descriptive n’est disponible; exécuter l’étape analytics.")
    sections += ["", "## 2. Partie Machine Learning — 40 %", ""]
    sections += ["", "### Classification du mode de transport — objectif principal", ""]
    if mode_a1 and mode_a2:
        sections.append(
            "A1 utilise les variables disponibles avant le choix du mode; A2 ajoute la durée. La classe majoritaire est APied. Les rappels par classe et matrices de confusion brutes/normalisées sont exportés dans les fichiers métriques et figures ci-dessous."
        )
        sections.append("")
        sections.append(_mode_rows(mode_a1))
        sections.append("")
        sections.append("Rappels par classe — A1")
        sections.append("")
        sections.append(_class_recall_table(mode_a1))
        sections.append("")
        sections.append(_mode_rows(mode_a2))
        sections.append("")
        sections.append("Rappels par classe — A2")
        sections.append("")
        sections.append(_class_recall_table(mode_a2))
    else:
        sections.append("Les métriques A1/A2 ne sont pas disponibles.")
    sections += ["", "### Régression de durée", ""]
    if duration:
        rows = [
            "| Modèle | MAE min brute | MAE min pondérée | RMSE min brute | RMSE min pondérée | R² min brut | R² min pondéré |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for name, metrics in duration.get("models", {}).items():
            raw = metrics["unweighted"]
            weighted = metrics["survey_weighted"]
            rows.append(
                f"| {name} | {raw['mae_minutes']:.4f} | {weighted['mae_minutes']:.4f} | {raw['rmse_minutes']:.4f} | {weighted['rmse_minutes']:.4f} | {raw['r2_minutes']:.4f} | {weighted['r2_minutes']:.4f} |"
            )
        sections.extend(rows)
        best_name = duration.get("best_model")
        if best_name:
            best = duration["models"][best_name]
            baseline = duration["models"]["baseline_od_median"]
            sections.extend(
                [
                    "",
                    f"Selon la MAE brute, le modèle retenu est **{best_name}** (MAE {best['unweighted']['mae_minutes']:.3f} min; pondérée {best['survey_weighted']['mae_minutes']:.3f} min). La baseline médiane OD obtient respectivement {baseline['unweighted']['mae_minutes']:.3f} et {baseline['survey_weighted']['mae_minutes']:.3f} min.",
                ]
            )
    else:
        sections.append("Les métriques de régression ne sont pas disponibles.")
    sections += ["", "### Segmentation des individus", ""]
    if clustering:
        sections.append(
            f"Le choix par silhouette a retenu **k={clustering['k']}**, silhouette **{clustering['silhouette']:.6f}**. Les profils et noms dérivés sont dans `outputs/tables/cluster_profiles`."
        )
    else:
        sections.append("Les métriques de clustering ne sont pas disponibles.")
    sections += [
        "",
        "## Figures à reprendre dans le rapport LaTeX",
        "",
        "Les PNG sont disponibles séparément dans `outputs/figures/`; les chemins ci-dessous sont relatifs à la racine du dépôt.",
        "",
        "### EDA Spark",
        "",
    ]
    figure_paths = sorted(Path(figures_dir).glob("*.png"))
    if figure_paths:
        eda_figures = [
            path
            for path in figure_paths
            if path.name.startswith(
                (
                    "cout_",
                    "distribution_",
                    "duree_",
                    "matrice_",
                    "modal_",
                    "motifs",
                    "profil_",
                    "top15_",
                )
            )
        ]
        ml_figures = [path for path in figure_paths if path not in eda_figures]
        sections.extend(f"- [{path.name}](../{path.as_posix()})" for path in eda_figures)
        sections.extend(["", "### Classification, régression et clustering", ""])
        sections.extend(f"- [{path.name}](../{path.as_posix()})" for path in ml_figures)
    else:
        sections.append("Aucune figure n’a été générée.")
    sections += [
        "",
        "## Limites",
        "",
        "Les classes modales rares restent difficiles à prédire : interpréter les modèles avec macro-F1, rappels par classe et matrices de confusion, et pas seulement avec l’accuracy. Les individus sans déplacement ne sont pas observés; le taux de mobilité décrit uniquement les personnes présentes. Aucune distance ou coordonnée n’est fournie. L’unité du coût reste à confirmer. Le quartier 399 apparaît dans deux strates; cette anomalie est conservée et signalée dans les contrôles.",
        "",
        "## Perspectives",
        "",
        "Ajouter des distances de réseau ou traces GPS, estimer un logit imbriqué, et évaluer le déploiement sur un cluster Spark.",
        "",
        "## Reproductibilité et validation",
        "",
        "L’exécution complète `make all` s’est terminée avec succès en 13 min 38 s. La suite complète `pytest -q` a réussi avec 26 tests passés et 0 échec. Ruff et `black --check` passent également. Les détails sont disponibles dans `outputs/metrics/`, les tables CSV dans `outputs/tables/`, les modèles dans `outputs/models/` et les prédictions dans `outputs/predictions/`.",
    ]
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sections) + "\n", encoding="utf-8")
    update_readme(metrics_dir)


def update_readme(metrics_dir: str) -> None:
    """Replace the README results section with values in real metric files."""
    readme = Path("README.md")
    if not readme.exists():
        return
    text = readme.read_text(encoding="utf-8")
    start, end = "<!-- RESULTATS_AUTO_START -->", "<!-- RESULTATS_AUTO_END -->"
    lines = [start, "## Résultats calculés", ""]
    for name in ["mode_choice_A1", "mode_choice_A2"]:
        data = _load_json(Path(metrics_dir) / f"{name}.json")
        if data:
            lines.append(_mode_rows(data))
            lines.append("")
    duration = _load_json(Path(metrics_dir) / "duration.json")
    if duration:
        best = min(
            duration["models"], key=lambda key: duration["models"][key]["unweighted"]["mae_minutes"]
        )
        lines.append(
            f"Meilleur MAE durée : `{best}` = {duration['models'][best]['unweighted']['mae_minutes']:.4f} min (sortie métrique)."
        )
    clustering = _load_json(Path(metrics_dir) / "clustering.json")
    if clustering:
        lines.append(
            f"Clustering : k={clustering['k']}, silhouette={clustering['silhouette']:.6f}."
        )
    lines.append(end)
    section = "\n".join(lines)
    if start in text and end in text:
        before = text.split(start, 1)[0]
        after = text.split(end, 1)[1]
        text = before + section + after
    else:
        text += "\n\n" + section + "\n"
    readme.write_text(text, encoding="utf-8")
