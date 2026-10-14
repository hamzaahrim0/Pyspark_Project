# Rapport final — enquête ménages-déplacements de Dakar

## Données et méthode

Le projet est organisé selon la pondération demandée : **60 % traitement distribué avec Spark et 40 % apprentissage automatique**. Il traite 45 605 déplacements observés à Dakar, Guédiawaye, Pikine et Rufisque avec PySpark 3.5.9 et Java 17. Les statistiques de population sont pondérées avec `poids_extra`; les versions brutes figurent également dans les tables exportées. Le découpage apprentissage/test est fait par ménage.

## 1. Partie Spark — 60 %

### Chaîne de traitement et couches de données

| Couche/étape | Traitement exécuté | Sortie principale |
|---|---|---|
| Ingestion / bronze | Conversion unique des quatre feuilles Excel en Parquet; schéma explicite des déplacements; matrice OD mise au format long et dimension des strates séparée | `data/bronze/` |
| Qualité / silver | Contrôles Spark, calcul des valeurs manquantes, cardinalités, plages et outliers; conversion HHMM en minutes; anomalies signalées sans supprimer de déplacements | `data/silver/deplacement/`, `outputs/tables/dq_*` |
| Feature engineering / gold | Variables temporelles, zones et motifs; dictionnaire exhaustif des motifs; statistiques OD sans utiliser le jeu de test | `data/gold/deplacement/` |
| Analyses descriptives | Agrégations Spark brutes et pondérées : parts modales, profils horaires, durées, motifs, coûts et flux OD | `outputs/tables/`, `outputs/metrics/analytics.json` |

La configuration exécutée est `local[*]`, 16 partitions shuffle, exécution adaptative activée et mémoire driver de 4 Gio. La clé déplacement est unique sur les 45 605 lignes; les contrôles de cohérence durée/modes et poids ménage passent. La réconciliation OD confirme 6 699 228,017 avec zéro cellule divergente par rapport au classeur.

## Qualité et décisions de nettoyage

### Synthèse des règles de qualité

- `key_unique` : **OK** — observé 45605/45605; attendu une clé par déplacement.
- `mode_count_matches_nb_trajets` : **OK** — observé 0; attendu 0 ligne incohérente.
- `duration_matches_elapsed_time` : **OK** — observé 0; attendu écart ≤ 1 minute.
- `weight_constant_per_household` : **OK** — observé 0; attendu 0 ménage à poids multiples.
- `quarter_to_stratum_one_to_one` : **KO** — observé 1; attendu 0 quartier associé à plusieurs strates.
- `intermodal_has_multiple_trips` : **OK** — observé 0; attendu Intermodal implique nb_trajets ≥ 2.
- `walking_has_single_trip` : **OK** — observé 0; attendu APied implique nb_trajets = 1.
- `known_strata_have_known_family` : **OK** — observé 0; attendu strates connues dans D/G/P/R.


Les heures sont converties depuis HHMM en minutes écoulées; les déplacements après minuit gardent leur jour suivant. Les valeurs aberrantes sont signalées et gardées en silver. Les statistiques OD de prédiction sont apprises sur le train, avec encodage hors pli pour le train.

### Résultats EDA calculés par Spark

- Mode dominant (APied), part pondérée : **69.19%**.
- Mode dominant (APied), part brute : **69.99%**.
- Part des déplacements intra-strate pondérée : **68.28%**.
- Part des déplacements intra-strate brute : **69.28%**.
- Part modale intermodale pondérée : **2.78%**.
- Heure de départ la plus active (pondérée) : **7 h (621038.83 déplacements extrapolés)**.
- Motif le plus fréquent (Retour domicile), déplacements pondérés : **2883958.20**.
- Durée moyenne pondérée : **19.16 minutes**.
- Coût moyen pondéré par mode (hors valeurs aberrantes) : **80.85 unités déclarées**.
- Total OD pondéré réconcilié avec le pivot : **6699228.017**.

## 2. Partie Machine Learning — 40 %


### Classification du mode de transport — objectif principal

A1 utilise les variables disponibles avant le choix du mode; A2 ajoute la durée. La classe majoritaire est APied. Les rappels par classe et matrices de confusion brutes/normalisées sont exportés dans les fichiers métriques et figures ci-dessous.

| Scénario | Baseline majoritaire acc./macro-F1 | Baseline motif-zone acc./macro-F1 | Modèle retenu | Accuracy brute | Macro-F1 brut | F1 weighted brut | Accuracy pondérée enquête | Macro-F1 pondéré enquête | F1 weighted enquête |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| A1 | 0.6882 / 0.0741 | 0.6882 / 0.0778 | random_forest | 0.6611 | 0.2106 | 0.6637 | 0.6428 | 0.2073 | 0.6487 |

Rappels par classe — A1

| Classe | Rappel baseline brute | Rappel modèle brut | Rappel modèle pondéré enquête |
|---|---:|---:|---:|
| APied | 1.0000 | 0.8836 | 0.8700 |
| Tata | 0.0000 | 0.0064 | 0.0042 |
| Voiture | 0.0000 | 0.2805 | 0.3302 |
| CarRapide | 0.0000 | 0.0256 | 0.0198 |
| Clando | 0.0000 | 0.0970 | 0.0881 |
| Intermodal | 0.0000 | 0.5785 | 0.5778 |
| Taxi | 0.0000 | 0.3876 | 0.3683 |
| Autre | 0.0000 | 0.1237 | 0.1255 |
| DDD | 0.0000 | 0.3468 | 0.3351 |
| NN | 0.0000 | 0.0924 | 0.0958 |
| Moto | 0.0000 | 0.1029 | 0.0903 |

| Scénario | Baseline majoritaire acc./macro-F1 | Baseline motif-zone acc./macro-F1 | Modèle retenu | Accuracy brute | Macro-F1 brut | F1 weighted brut | Accuracy pondérée enquête | Macro-F1 pondéré enquête | F1 weighted enquête |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| A2 | 0.6882 / 0.0741 | 0.6882 / 0.0778 | random_forest | 0.6730 | 0.2277 | 0.6748 | 0.6575 | 0.2246 | 0.6613 |

Rappels par classe — A2

| Classe | Rappel baseline brute | Rappel modèle brut | Rappel modèle pondéré enquête |
|---|---:|---:|---:|
| APied | 1.0000 | 0.8915 | 0.8813 |
| Tata | 0.0000 | 0.0229 | 0.0236 |
| Voiture | 0.0000 | 0.2545 | 0.2864 |
| CarRapide | 0.0000 | 0.1074 | 0.0951 |
| Clando | 0.0000 | 0.1455 | 0.1234 |
| Intermodal | 0.0000 | 0.6278 | 0.6430 |
| Taxi | 0.0000 | 0.3583 | 0.3425 |
| Autre | 0.0000 | 0.2268 | 0.2678 |
| DDD | 0.0000 | 0.3871 | 0.3908 |
| NN | 0.0000 | 0.0672 | 0.0662 |
| Moto | 0.0000 | 0.0441 | 0.0449 |

### Régression de durée

| Modèle | MAE min brute | MAE min pondérée | RMSE min brute | RMSE min pondérée | R² min brut | R² min pondéré |
|---|---:|---:|---:|---:|---:|---:|
| baseline_global_median | 13.2054 | 13.7528 | 26.1112 | 26.6197 | -0.1244 | -0.1442 |
| baseline_mode_median | 10.1797 | 10.4603 | 21.1527 | 21.4056 | 0.2621 | 0.2601 |
| baseline_od_median | 9.0400 | 9.3703 | 20.2697 | 20.9427 | 0.3224 | 0.2918 |
| linear_regression | 8.4879 | 8.7877 | 18.6389 | 18.7676 | 0.4271 | 0.4313 |
| random_forest | 8.2231 | 8.4052 | 18.3320 | 18.5159 | 0.4458 | 0.4464 |
| gradient_boosted_trees | 8.2727 | 8.4797 | 18.0348 | 18.2995 | 0.4636 | 0.4593 |

Selon la MAE brute, le modèle retenu est **random_forest** (MAE 8.223 min; pondérée 8.405 min). La baseline médiane OD obtient respectivement 9.040 et 9.370 min.

### Segmentation des individus

Le choix par silhouette a retenu **k=9**, silhouette **0.317980**. Les profils et noms dérivés sont dans `outputs/tables/cluster_profiles`.

## Figures à reprendre dans le rapport LaTeX

Les PNG sont disponibles séparément dans `outputs/figures/`; les chemins ci-dessous sont relatifs à la racine du dépôt.

### EDA Spark

- [cout_par_mode.png](../outputs/figures/cout_par_mode.png)
- [distribution_duree.png](../outputs/figures/distribution_duree.png)
- [duree_par_mode.png](../outputs/figures/duree_par_mode.png)
- [matrice_od_heatmap.png](../outputs/figures/matrice_od_heatmap.png)
- [modal_famille_modale_global.png](../outputs/figures/modal_famille_modale_global.png)
- [modal_moyen_transport_global.png](../outputs/figures/modal_moyen_transport_global.png)
- [motifs.png](../outputs/figures/motifs.png)
- [profil_horaire.png](../outputs/figures/profil_horaire.png)
- [profil_horaire_par_mode.png](../outputs/figures/profil_horaire_par_mode.png)
- [top15_flux_od.png](../outputs/figures/top15_flux_od.png)

### Classification, régression et clustering

- [cluster_elbow.png](../outputs/figures/cluster_elbow.png)
- [cluster_profiles_heatmap.png](../outputs/figures/cluster_profiles_heatmap.png)
- [clusters_pca.png](../outputs/figures/clusters_pca.png)
- [duration_gradient_boosted_trees.png](../outputs/figures/duration_gradient_boosted_trees.png)
- [duration_linear_regression.png](../outputs/figures/duration_linear_regression.png)
- [duration_random_forest.png](../outputs/figures/duration_random_forest.png)
- [mode_choice_A1_confusion.png](../outputs/figures/mode_choice_A1_confusion.png)
- [mode_choice_A1_confusion_brute.png](../outputs/figures/mode_choice_A1_confusion_brute.png)
- [mode_choice_A1_confusion_normalisee.png](../outputs/figures/mode_choice_A1_confusion_normalisee.png)
- [mode_choice_A1_feature_importance.png](../outputs/figures/mode_choice_A1_feature_importance.png)
- [mode_choice_A2_confusion.png](../outputs/figures/mode_choice_A2_confusion.png)
- [mode_choice_A2_confusion_brute.png](../outputs/figures/mode_choice_A2_confusion_brute.png)
- [mode_choice_A2_confusion_normalisee.png](../outputs/figures/mode_choice_A2_confusion_normalisee.png)
- [mode_choice_A2_feature_importance.png](../outputs/figures/mode_choice_A2_feature_importance.png)

## Limites

Les classes modales rares restent difficiles à prédire : interpréter les modèles avec macro-F1, rappels par classe et matrices de confusion, et pas seulement avec l’accuracy. Les individus sans déplacement ne sont pas observés; le taux de mobilité décrit uniquement les personnes présentes. Aucune distance ou coordonnée n’est fournie. L’unité du coût reste à confirmer. Le quartier 399 apparaît dans deux strates; cette anomalie est conservée et signalée dans les contrôles.

## Perspectives

Ajouter des distances de réseau ou traces GPS, estimer un logit imbriqué, et évaluer le déploiement sur un cluster Spark.

## Reproductibilité et validation

L’exécution complète `make all` s’est terminée avec succès en 13 min 38 s. La suite complète `pytest -q` a réussi avec 26 tests passés et 0 échec. Ruff et `black --check` passent également. Les détails sont disponibles dans `outputs/metrics/`, les tables CSV dans `outputs/tables/`, les modèles dans `outputs/models/` et les prédictions dans `outputs/predictions/`.
