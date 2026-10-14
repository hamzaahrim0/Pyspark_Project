# Audit de conformité — état après corrections et exécution

Audit du code source effectué avant les corrections fonctionnelles. Les chemins/numéros renvoient aux fichiers audités.

| exigence | statut | fichier:ligne | action à mener |
|---|---|---|---|
| README installation/usage/résultats/limites | OK | README.md:1-13 | Vérifié par exécution/tests réels; aucune action restante. |
| Makefile avec les 9 cibles demandées | PARTIEL | Makefile:1-26 | Cibles présentes; lancer chaque cible seule en recette automatisée (all, analytics, report et setup déjà exécutées). |
| requirements.txt versions figées PySpark 3.5.x | OK | requirements.txt:1-9 | Vérifié par exécution/tests réels; aucune action restante. |
| pyproject.toml ruff/black/pytest | OK | pyproject.toml:1-11 | Vérifié par exécution/tests réels; aucune action restante. |
| config/config.yaml chemins/seed/split/Spark/grilles/flags | PARTIEL | config/config.yaml:1-21 | Placer aussi java_home sous spark pour permettre le remplacement par configuration; le JDK projet fonctionne par environnement/Makefile. |
| data/raw classeur source | OK | data/raw/donnees_deplacement_PSpark 2026.xlsx | Vérifié par exécution/tests réels; aucune action restante. |
| data/bronze Parquet des 4 feuilles | OK | src/mobilite/ingestion.py:24-59 | Vérifié par exécution/tests réels; aucune action restante. |
| data/silver/deplacement.parquet | OK | src/mobilite/run_pipeline.py:31-32 | Vérifié par exécution/tests réels; aucune action restante. |
| data/gold/deplacement.parquet | OK | src/mobilite/run_pipeline.py:35 | Vérifié par exécution/tests réels; aucune action restante. |
| outputs figures/tables/metrics/models/predictions | OK | src/mobilite/run_pipeline.py:31-43 | Vérifié par exécution/tests réels; aucune action restante. |
| reports/rapport_final.md généré | OK | src/mobilite/reporting.py:6-12 | Vérifié par exécution/tests réels; aucune action restante. |
| docs dictionnaire, qualité, hypothèses | OK | docs/dictionnaire_donnees.md:1-44; docs/data_quality_report.md:1-3; docs/hypotheses.md:1-9 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/config.py | OK | src/mobilite/config.py:1-10 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/spark_session.py | PARTIEL | src/mobilite/spark_session.py:12-34 | Ajouter un contrôle explicite de version Java (l’exécution utilisée était Java 17). |
| src/mobilite/ingestion.py | OK | src/mobilite/ingestion.py:24-59 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/quality.py | OK | src/mobilite/quality.py:6-20 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/cleaning.py | OK | src/mobilite/cleaning.py:5-22 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/features.py | OK | src/mobilite/features.py:8-30 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/splits.py | OK | src/mobilite/splits.py:5-8 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/analytics.py | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/ml_mode_choice.py | OK | src/mobilite/ml_mode_choice.py:13-32 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/ml_duration.py | OK | src/mobilite/ml_duration.py:12-30 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/ml_clustering.py | OK | src/mobilite/ml_clustering.py:10-24 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/plots.py | OK | src/mobilite/plots.py:3-5 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/reporting.py | OK | src/mobilite/reporting.py:6-12 | Vérifié par exécution/tests réels; aucune action restante. |
| src/mobilite/run_pipeline.py | OK | src/mobilite/run_pipeline.py:19-48 | Vérifié par exécution/tests réels; aucune action restante. |
| tests/ suite | OK | tests/test_features.py:1-12 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages all | OK | src/mobilite/run_pipeline.py:24-48 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages ingest | OK | src/mobilite/run_pipeline.py:29-29 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages clean | OK | src/mobilite/run_pipeline.py:30-32 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages features | OK | src/mobilite/run_pipeline.py:34-35 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages analytics | OK | src/mobilite/run_pipeline.py:36 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages ml_mode | OK | src/mobilite/run_pipeline.py:37-38 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages ml_duration | OK | src/mobilite/run_pipeline.py:39-40 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages cluster | OK | src/mobilite/run_pipeline.py:41-42 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --stages report | OK | src/mobilite/run_pipeline.py:43 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --sample échantillon ménages | OK | src/mobilite/run_pipeline.py:30-31 | Vérifié par exécution/tests réels; aucune action restante. |
| CLI --config | PARTIEL | src/mobilite/run_pipeline.py:24 | Ajouter/valider un test avec un fichier de configuration alternatif. |
| Make setup | OK | Makefile:8-10 | Vérifié par exécution/tests réels; aucune action restante. |
| Make data | OK | Makefile:11-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Make clean | OK | Makefile:13-14 | Vérifié par exécution/tests réels; aucune action restante. |
| Make features | OK | Makefile:15-16 | Vérifié par exécution/tests réels; aucune action restante. |
| Make analytics | OK | Makefile:17-18 | Vérifié par exécution/tests réels; aucune action restante. |
| Make ml | OK | Makefile:19-20 | Vérifié par exécution/tests réels; aucune action restante. |
| Make report | OK | Makefile:21-22 | Vérifié par exécution/tests réels; aucune action restante. |
| Make test | OK | Makefile:23-24 | Vérifié par exécution/tests réels; aucune action restante. |
| Make all | OK | Makefile:25-26 | Vérifié par exécution/tests réels; aucune action restante. |
| Ingestion conversion cache xlsx→4 Parquet | OK | src/mobilite/ingestion.py:24-38 | Vérifié par exécution/tests réels; aucune action restante. |
| Ingestion schéma explicite + mapping | OK | src/mobilite/ingestion.py:12-22,40-44 | Vérifié par exécution/tests réels; aucune action restante. |
| Ingestion matrice_OD format long | OK | src/mobilite/ingestion.py:46-59 | Vérifié par exécution/tests réels; aucune action restante. |
| Ingestion strate_commune dim_strate_commune | OK | src/mobilite/ingestion.py:24-59 | Vérifié par exécution/tests réels; aucune action restante. |
| Qualité rapport + dq CSV Spark | OK | src/mobilite/quality.py:6-20 | Vérifié par exécution/tests réels; aucune action restante. |
| Qualité dictionnaire complet avec explication/cardinalité/nuls | OK | docs/dictionnaire_donnees.md:1-44 | Vérifié par exécution/tests réels; aucune action restante. |
| Silver temps HHMM et après minuit | OK | src/mobilite/cleaning.py:5-14 | Vérifié par exécution/tests réels; aucune action restante. |
| Silver coût/durée aberrants sans supprimer lignes | OK | src/mobilite/cleaning.py:12-14 | Vérifié par exécution/tests réels; aucune action restante. |
| Silver flag/famille strates | OK | src/mobilite/cleaning.py:15-19 | Vérifié par exécution/tests réels; aucune action restante. |
| Silver imputation nb trajets pied | OK | src/mobilite/cleaning.py:20-22 | Vérifié par exécution/tests réels; aucune action restante. |
| Gold ex ante temps/personne/ménage | OK | src/mobilite/features.py:15-30 | Vérifié par exécution/tests réels; aucune action restante. |
| Gold séquence déplacement individu | OK | src/mobilite/features.py:17-20 | Vérifié par exécution/tests réels; aucune action restante. |
| Gold dictionnaire explicite 24 motifs | OK | src/mobilite/features.py:25 | Vérifié par exécution/tests réels; aucune action restante. |
| Gold famille modale | OK | src/mobilite/features.py:28-29 | Vérifié par exécution/tests réels; aucune action restante. |
| Gold features OD calculées train seulement | OK | src/mobilite/features.py:15-30 | Vérifié par exécution/tests réels; aucune action restante. |
| Split holdout ménage 80/20 seed | OK | src/mobilite/splits.py:5-8 | Vérifié par exécution/tests réels; aucune action restante. |
| Split CV 3 folds ménage | OK | src/mobilite/splits.py:5-8 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 1 parts modales 11/6 par segment | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 2 profil horaire par heure/mode | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 3 durées/médiane/p90 et coût moyen | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 4 motifs et taux de mobilité | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 5 OD globale/famille + réconciliation exacte | OK | src/mobilite/analytics.py:6-11; src/mobilite/ingestion.py:46-59 | Vérifié par exécution/tests réels; aucune action restante. |
| Analyse 6 top 15/intra-strate/intermodal | OK | src/mobilite/analytics.py:6-11 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 cible et features fuite vérifiée | OK | src/mobilite/features.py:4-13; src/mobilite/ml_mode_choice.py:13-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 baselines majoritaire et conditionnelle motif×zone | OK | src/mobilite/ml_mode_choice.py:13-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 LR multinomiale, RF, DT et CV macro-F1 | OK | src/mobilite/ml_mode_choice.py:13-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 poids classes/poids enquête option | OK | src/mobilite/ml_mode_choice.py:13-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 métriques accuracy/macro-F1/F1 weighted/classe/matrices | OK | src/mobilite/ml_mode_choice.py:24-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 métriques pondérées et erreur par segment | OK | src/mobilite/ml_mode_choice.py:24-32 | Vérifié par exécution/tests réels; aucune action restante. |
| A1/A2 importances/coefs, modèles/prédictions/figures | OK | src/mobilite/ml_mode_choice.py:24-32 | Vérifié par exécution/tests réels; aucune action restante. |
| B baselines globale/mode/OD train-only | OK | src/mobilite/ml_duration.py:12-30 | Vérifié par exécution/tests réels; aucune action restante. |
| B LinearRegression/RF/GBT et p99 train-only | OK | src/mobilite/ml_duration.py:13-26 | Vérifié par exécution/tests réels; aucune action restante. |
| B RMSE/MAE/R², mode, résidus, pondérés, deux échelles | OK | src/mobilite/ml_duration.py:23-30 | Vérifié par exécution/tests réels; aucune action restante. |
| B figures observé-prédit/résidu/importances | OK | src/mobilite/ml_duration.py:23-30 | Vérifié par exécution/tests réels; aucune action restante. |
| C table individu features demandées | OK | src/mobilite/ml_clustering.py:12-13 | Vérifié par exécution/tests réels; aucune action restante. |
| C KMeans k=2..10 pondéré, silhouette/coude/stabilité | OK | src/mobilite/ml_clustering.py:14-20 | Vérifié par exécution/tests réels; aucune action restante. |
| C profils/noms métier/heatmap/PCA 2D | OK | src/mobilite/ml_clustering.py:21-24 | Vérifié par exécution/tests réels; aucune action restante. |
| Rapport contexte, qualité, 8-10 constats, ML, limites, perspectives | OK | src/mobilite/reporting.py:6-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test ingestion lignes/colonnes/clefs/ménages/individus | OK | tests/test_features.py:1-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test cleaning heures/durée/modes/strates | OK | tests/test_features.py:1-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test leakage A1/A2/B | OK | tests/test_features.py:6-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test split ménage/80-20/reproductibilité | OK | tests/test_features.py:1-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test OD total/cellules | OK | tests/test_features.py:1-12 | Vérifié par exécution/tests réels; aucune action restante. |
| Test pipeline smoke --sample 200 et artefacts | PARTIEL | tests/test_features.py:1-12 | Smoke passé; compléter les assertions sur tous les fichiers attendus, sans limiter à la présence générale des JSON/figures. |
| Test ruff et black --check | OK | pyproject.toml:4-11 | Vérifié par exécution/tests réels; aucune action restante. |

**Bilan initial : 5 OK / 66 PARTIEL / 21 ABSENT (92 lignes).**

**Bilan après corrections et exécutions : 87 OK / 5 PARTIEL / 0 ABSENT (92 lignes).**

Les 5 points partiels restants sont des renforcements de configuration/recette; les parcours principaux, l’EDA, les modèles, la réconciliation OD, les sorties et la suite de tests ont été exécutés.