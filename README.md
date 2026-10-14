# Mobilité à Dakar — pipeline Spark et apprentissage automatique

Projet d’analyse de l’enquête ménages-déplacements de Dakar, Guédiawaye, Pikine et Rufisque. La présentation est structurée autour de **Spark (60 %)** — ingestion, qualité, transformations, EDA — et du **machine learning (40 %)**, dont la prédiction du mode de transport.

## Lancement

Prérequis : Python 3.10 ou plus récent et Java 11 ou 17. Depuis la racine du dépôt :

```sh
make setup
make all
```

Le Makefile choisit le JDK via `JAVA_HOME` (par défaut `~/.jdks/temurin-17`). La configuration Spark se trouve dans `config/config.yaml`.

Pour un test rapide sur 200 ménages :

```sh
.venv/bin/python -m mobilite.run_pipeline --stages all --sample 200
```

Étapes disponibles : `ingest`, `clean`, `features`, `analytics`, `ml_mode`, `ml_duration`, `cluster` et `report`. Cibles Makefile : `setup`, `data`, `clean`, `features`, `analytics`, `ml`, `report`, `test` et `all`.

## Organisation

| Emplacement | Contenu |
|---|---|
| `data/raw/` | Classeur source Excel |
| `data/bronze/`, `data/silver/`, `data/gold/` | Couches Parquet produites par le pipeline |
| `outputs/figures/` | Figures EDA, classification, durée et clustering |
| `outputs/metrics/` | Métriques JSON brutes et pondérées |
| `outputs/tables/` | Agrégats EDA, qualité et profils de clusters |
| `reports/rapport_final.md` | Rapport de résultats à adapter pour LaTeX |
| `docs/SPEC.md` | Cahier des charges du projet |

Le classeur brut n’est pas inclus dans ce dépôt Git. Pour réexécuter le pipeline et les tests d’intégration, placer le fichier autorisé `donnees_deplacement_PSpark 2026.xlsx` sous `data/raw/` avec le nom attendu. Les fichiers agrégés de métriques, tables et figures sont fournis; les Parquet ligne par ligne, prédictions et modèles entraînés sont régénérables et exclus du dépôt.

Le classeur est converti en Parquet à l’ingestion; les transformations, contrôles de qualité, agrégations et modèles sont exécutés avec Spark. Les déplacements après minuit sont conservés, les anomalies sont signalées sans supprimer de lignes et les statistiques d’apprentissage OD sont calculées sans accéder au jeu de test.

## Limites connues

Le quartier 399 est lié à deux strates dans les données et cette anomalie est signalée dans les contrôles. Les classes modales rares sont moins bien prédites; consulter les rappels par classe et les baselines, pas uniquement l’accuracy. Les coordonnées/distances ne sont pas disponibles, l’unité du coût reste à confirmer et les individus sans déplacement sont absents de l’enquête observée.


<!-- RESULTATS_AUTO_START -->
## Résultats calculés

| Scénario | Baseline majoritaire acc./macro-F1 | Baseline motif-zone acc./macro-F1 | Modèle retenu | Accuracy brute | Macro-F1 brut | F1 weighted brut | Accuracy pondérée enquête | Macro-F1 pondéré enquête | F1 weighted enquête |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| A1 | 0.6882 / 0.0741 | 0.6882 / 0.0778 | random_forest | 0.6611 | 0.2106 | 0.6637 | 0.6428 | 0.2073 | 0.6487 |

| Scénario | Baseline majoritaire acc./macro-F1 | Baseline motif-zone acc./macro-F1 | Modèle retenu | Accuracy brute | Macro-F1 brut | F1 weighted brut | Accuracy pondérée enquête | Macro-F1 pondéré enquête | F1 weighted enquête |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| A2 | 0.6882 / 0.0741 | 0.6882 / 0.0778 | random_forest | 0.6730 | 0.2277 | 0.6748 | 0.6575 | 0.2246 | 0.6613 |

Meilleur MAE durée : `random_forest` = 8.2231 min (sortie métrique).
Clustering : k=9, silhouette=0.317980.
<!-- RESULTATS_AUTO_END -->
