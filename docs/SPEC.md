Tu es un ingénieur data/ML senior. Livre de bout en bout, en une seule passe et sans me poser de question, un projet PySpark complet : ingestion → qualité des données → nettoyage → feature engineering → analyses descriptives → 3 modèles de machine learning (Spark MLlib) → tests → rapport final.
Règles de conduite :
- Tu exécutes réellement tout ce que tu écris (installation, pipeline, tests). Tu corriges jusqu'à ce que tout passe. Tu ne livres pas de code non exécuté.
- Si une information manque, tu fais l'hypothèse la plus raisonnable, tu l'appliques et tu la consignes dans docs/hypotheses.md (une hypothèse = une ligne : décision, justification, impact).
- Tu n'inventes jamais un chiffre : toute métrique du rapport est lue depuis les fichiers produits par le pipeline.
- Documentation et rapport en français ; identifiants de code, noms de colonnes et commentaires techniques en anglais snake_case.
Enquête ménages-déplacements de la région de Dakar (Dakar, Guédiawaye, Pikine, Rufisque). Une ligne = un déplacement d'un individu. Un déplacement va d'une origine vers une destination pour un motif ; il est composé de 1 à 4 trajets (tronçons), chacun avec son mode.
Le classeur Excel contient 4 feuilles :
Volumétrie : 3 127 ménages, 11 457 individus (≈ 4 déplacements/individu, max 18), âge 11–95 ans (23 % de mineurs), sexe équilibré, jours d'enquête Mardi→Dimanche (pas de Lundi).
Colonnes de deplacement (nom d'origine → nom cible snake_case) :
Distribution de moyen_transport (déséquilibrée) : APied 70,0 % · Tata 8,4 % · Voiture 4,2 % · CarRapide 4,2 % · Clando 3,2 % · Intermodal 2,7 % · Taxi 2,7 % · DDD 1,3 % · Autre 1,3 % · NN 1,2 % · Moto 0,9 %. Intermodal n'apparaît que si nb_trajets ≥ 2 ; APied a toujours nb_trajets = 1.
1. Heures > 24h : 270 départs et 335 arrivées ont une valeur ≥ 2400 (max 3130) → déplacement après minuit, jour suivant. Convertir en minutes totales depuis minuit (HH*60+MM, peut dépasser 1440), dériver heure_depart_h (modulo 24) et un indicateur depart_apres_minuit. Ne jamais traiter HHMM comme un nombre.
2. Fuite de données (target leakage) pour prédire moyen_transport : mode_1..4, nb_trajets, tous les nb_trajets_*_pondere et cout encodent directement la cible (ex. coût = 0 ⇒ marche). Ils sont interdits comme variables explicatives. Prévoir un test automatique qui échoue si une de ces colonnes entre dans une liste de features.
3. lieu_depart n'est PAS le « code commune » de la feuille strate_commune (ex. la 1ʳᵉ ligne a lieu_depart=10 et strate D01, alors que le code commune 10 = HLM/D10). Ne pas joindre lieu_* sur code commune. Vérifier empiriquement que quartier → strate est 1-pour-1 (c'est le cas : 219 quartiers pour 46 strates, jusqu'à 13 quartiers par strate) et construire dim_quartier depuis les données.
4. Strates absentes du dictionnaire : zex, NA_d, NA_g, NA_p, NA_region (départ et arrivée) et NA1_r (arrivée). Les conserver, ajouter strate_connue (booléen), et regrouper dans la famille de zone AUTRE_INCONNU. Ne pas supposer leur signification (documenter « zex = probablement zone extérieure »).
5. Poids d'enquête : toute statistique descriptive destinée à décrire la population doit être pondérée par poids_extra (et affichée à côté de la version brute). Les ménages étant l'unité d'échantillonnage, un individu hérite du poids de son ménage.
6. Fuite entre train et test : les membres d'un même ménage se ressemblent. Séparer train/test par num_mena (jamais par ligne).
7. Déséquilibre de classes : la précision seule est trompeuse (une classe majoritaire à 70 %). Rapporter macro-F1, rappel par classe, matrice de confusion.
8. Unités / outliers : cout et duree_mn ont des valeurs extrêmes → les signaler (colonnes *_aberrant), ne pas les supprimer de la couche silver ; winsorisation uniquement à l'entraînement de la régression.
9. Colonnes mal nommées (Dextination_…, espaces et parenthèses dans Duree (mn) et Moyen de transport) : renommer proprement à l'ingestion et consigner le mapping dans docs/dictionnaire_donnees.md.
10. num_deplacement semble chronologique : le vérifier ; sinon trier par heure_depart_min_total pour toutes les fonctions fenêtre (lag/lead).
- Python ≥ 3.10, environnement virtuel .venv, requirements.txt aux versions figées. PySpark ≥ 3.5 (vérifie java -version ; Java 11 ou 17 requis ; si Java est absent, tente de l'installer ; si c'est impossible, n'écris pas de version pandas de remplacement : livre tout le code et les tests, et signale le blocage en tête du message final).
- Spark en mode local (local[*]), mais code écrit pour passer à l'échelle : API DataFrame / Spark SQL, aucune boucle Python sur des lignes, aucun .collect() ni .toPandas() sur des données volumineuses (autorisé uniquement sur de petits agrégats pour tracer des graphiques), jointures broadcast pour les petites dimensions, UDF Python évitées au profit des fonctions natives.
- Config Spark adaptée à 45 k lignes : spark.sql.shuffle.partitions=16, spark.sql.adaptive.enabled=true, spark.driver.memory=4g, spark.sql.session.timeZone=UTC. Tout est paramétrable dans config/config.yaml.
- pandas + openpyxl : uniquement pour la conversion unique Excel → Parquet (Spark ne lit pas le .xlsx nativement) et pour le tracé matplotlib de petits agrégats.
- Reproductibilité : seed=42 partout, versions loguées au démarrage, chemins relatifs et configurables (pas de chemin absolu en dur), pipeline idempotent (relançable sans effet de bord).
- Temps total cible : < 20 minutes sur un portable 4 cœurs / 8 Go (grilles d'hyperparamètres volontairement petites, numTrees ≤ 100, maxDepth ≤ 12).
- Qualité de code : type hints, docstrings, logging (pas de print), fonctions pures DataFrame -> DataFrame, modules courts, ruff + black configurés et passants.
```
projet_mobilite_spark/
├── README.md                      # installation, usage, résultats clés, limites
├── Makefile                       # setup | data | clean | features | analytics | ml | report | test | all
├── requirements.txt
├── pyproject.toml                 # config ruff/black/pytest
├── config/config.yaml             # chemins, seed, ratios de split, grilles, flags
├── data/{raw,bronze,silver,gold}/ # raw = xlsx ; bronze/silver/gold = Parquet
├── outputs/{figures,tables,metrics,models,predictions}/
├── reports/rapport_final.md
├── docs/{dictionnaire_donnees.md, data_quality_report.md, hypotheses.md}
├── src/mobilite/
│   ├── config.py  spark_session.py  ingestion.py  quality.py  cleaning.py
│   ├── features.py  splits.py  analytics.py
│   ├── ml_mode_choice.py  ml_duration.py  ml_clustering.py
│   ├── plots.py  reporting.py  run_pipeline.py
└── tests/
```
CLI : python -m mobilite.run_pipeline --stages all|ingest|clean|features|analytics|ml_mode|ml_duration|cluster|report [--sample N] [--config config/config.yaml]. L'option --sample N (échantillon de ménages, pas de lignes) sert aux tests de fumée rapides.
- Convertir les 4 feuilles en Parquet dans data/bronze/ (une seule fois, mise en cache si le Parquet est plus récent que le .xlsx).
- Charger deplacement dans Spark avec un schéma explicite (pas d'inferSchema), renommer selon le mapping de la section 1.
- matrice_OD : parser correctement le tableau croisé (en-tête à la 4ᵉ ligne, ignorer les lignes vides et « Total général ») et le stocker au format long strate_depart, strate_arrivee, poids_total, pour servir de référence de contrôle.
- strate_commune → dim_strate_commune (renommage snake_case ; ne pas la joindre sur lieu_*).
Produire docs/data_quality_report.md + outputs/tables/dq_*.csv, calculés par Spark, avec au minimum : % de nuls par colonne (en distinguant NaN structurels de mode_2..4 et vrais manquants), cardinalités, clés uniques, plages de valeurs, outliers (règle IQR + seuils métier), cohérence duree_mn vs heures (écart ≤ 1 min), cohérence nb_trajets vs nombre de modes non nuls, poids_extra constant par ménage, correspondance quartier → strate, strates hors dictionnaire, couples (Intermodal ⇔ nb_trajets ≥ 2). Chaque règle = une assertion nommée avec statut OK/KO dans le rapport. Générer aussi docs/dictionnaire_donnees.md complet : nom d'origine, nom cible, type, signification (reprendre la feuille explication, compléter par inférence en marquant « (inféré) », et « à confirmer » quand c'est incertain), cardinalité, % nuls.
Traiter les pièges de la section 2 : minutes totales / heure modulo 24 / depart_apres_minuit ; cout (NaN → conservé nul + cout_manquant, extrêmes → cout_aberrant) ; duree_mn_aberrant (> p99 ou > 180 min, au choix documenté) ; strate_connue_depart/arrivee ; famille_zone_depart/arrivee = première lettre de la strate parmi {D, G, P, R} sinon AUTRE_INCONNU ; remplissage du NaN de nb_trajets_apied_pondere (0 si moyen_transport ≠ APied, sinon documenter) ; typage correct. Sortie : data/silver/deplacement.parquet, mis en cache pour la suite.
Features ex ante (connues avant ou au moment du choix du mode) :
Variables cibles et auxiliaires : moyen_transport (11 classes), famille_modale (variante à 6 classes : Marche = APied ; Collectif = Tata+CarRapide+DDD+NN ; Taxi/Clando = Taxi+Clando ; Individuel motorisé = Voiture+Moto ; Intermodal ; Autre), duree_mn, log1p_duree. lieu_depart/arrivee (haute cardinalité, 219) : exclus du modèle principal, testés en ablation documentée.
- Hold-out 80/20 par ménage, déterministe : abs(xxhash64(num_mena, seed)) % 100 < 80.
- Validation croisée 3 plis par groupe de ménages (pmod(xxhash64(num_mena), 3)) écrite à la main (boucle sur les plis + grilles réduites) car CrossValidator de MLlib ne gère pas les groupes.
- Vérifier : aucun num_mena à la fois en train et en test ; proportions 80/20 ± 2 points.
Toutes en version brute et pondérée, exportées en CSV + figures PNG :
1. Part modale (11 classes et 6 familles), globale et par tranche horaire, âge, sexe, motif, famille de zone.
2. Profil horaire des départs (par heure et par mode ; identifier les heures de pointe).
3. Durée : distribution, médiane/p90 par mode et par motif ; coût moyen par mode (hors NaN/aberrants).
4. Motifs : part, durée moyenne ; taux de mobilité (déplacements pondérés / individus pondérés, en précisant la limite : seuls les individus ayant ≥ 1 déplacement sont présents).
5. Matrice OD pondérée strate × strate (globale + par famille modale) ; réconciliation obligatoire avec la feuille matrice_OD : total = 6 699 228,017 et égalité cellule par cellule (tolérance relative 1e-9). Figure : heatmap des 20 strates les plus actives.
6. Top 15 flux OD, part des déplacements intra-strate (≈ 69 % en brut — à vérifier), part des déplacements intermodaux.
Tâche A — Classification du mode de transport (ml_mode_choice.py)
- Cible : moyen_transport (11 classes) ; variante famille_modale (6 classes) via un paramètre de config.
- Deux scénarios : A1 « choix ex ante » = features de 5.4 uniquement ; A2 « durée connue » = A1 + duree_mn. Jamais cout, mode_*, nb_trajets*.
- Prétraitement : StringIndexer(handleInvalid="keep") + OneHotEncoder pour LR ; indexation seule pour les arbres avec maxBins ≥ cardinalité maximale des catégorielles indexées (strates ≈ 47 → maxBins=64) ; VectorAssembler ; StandardScaler pour LR.
- Modèles : baselines (classe majoritaire ; mode majoritaire par motif_groupe × famille_zone appris sur le train) ; LogisticRegression multinomiale (elastic-net) ; RandomForestClassifier ; DecisionTreeClassifier (interprétable). OneVsRest(GBTClassifier) en option désactivée par défaut (GBT MLlib n'est pas multiclasse).
- Déséquilibre : essayer un weightCol de poids de classe équilibrés (n / (k · n_c)) ; flag de config pour ajouter ou non poids_extra à l'entraînement (défaut : non).
- Tuning : grilles petites via la CV par groupe (5.5) optimisant le macro-F1.
- Évaluation sur le test : accuracy, macro-F1, F1 pondéré, rappel/précision par classe, matrice de confusion (brute et normalisée), et mêmes métriques pondérées par poids_extra (weightCol de l'évaluateur). Importance des variables (RF) / coefficients (LR). Analyse d'erreur par segment (mode, motif, tranche d'âge).
- Sorties : modèle, prédictions test (Parquet), outputs/metrics/mode_choice_*.json, CSV, figures.
Tâche B — Régression de la durée d'un déplacement (ml_duration.py)
- Cible : log1p(duree_mn) ; entraînement avec winsorisation au p99 ; évaluation en échelle log et en minutes (non winsorisé).
- Features : A1 + moyen_transport (le mode est connu quand on estime la durée) + stats OD du train (od_duree_mediane).
- Modèles : baselines (médiane globale ; médiane par mode ; médiane par couple OD du train avec repli sur le mode) ; LinearRegression ; RandomForestRegressor ; GBTRegressor.
- Métriques : RMSE, MAE, R², MAE par mode, résidus ; versions pondérées. Figures : prédit vs observé, distribution des résidus, importances.
Tâche C — Segmentation non supervisée des individus (ml_clustering.py)
- Table individu (≈ 11 457 lignes) : nb de déplacements, part de marche, part de transport collectif, durée moyenne et totale, nb de motifs distincts, parts de motifs (travail, études, achats, loisirs), heure du 1ᵉʳ départ et du dernier retour, âge, sexe, poids du ménage.
- StandardScaler → KMeans pour k = 2…10 (weightCol = poids du ménage) ; choisir k par silhouette (ClusteringEvaluator) + coude (WSSSE) et lisibilité métier ; stabilité (3 seeds).
- Profil de chaque cluster (moyennes pondérées, tableau + heatmap) ; proposer un nom métier pour chaque cluster dérivé des chiffres réels ; projection PCA 2D (PCA MLlib) pour la visualisation.
Généré automatiquement à partir des JSON/CSV/figures : 1) contexte et données ; 2) qualité et décisions de nettoyage ; 3) 8–10 enseignements descriptifs chiffrés (pondérés) ; 4) résultats ML des tâches A, B, C avec tableaux comparatifs modèles vs baselines et figures ; 5) limites honnêtes (échantillon, pas de coordonnées/distances, unité du coût incertaine, individus sans déplacement absents, classes rares, résultat si les modèles ne battent pas les baselines) ; 6) perspectives (distance réseau, données GPS, modèle logit imbriqué, déploiement cluster). Interdit : texte de remplissage ou chiffres écrits à la main.
Prérequis (Java/Python), installation, make all, description des couches bronze/silver/gold, commandes par étape, structure des sorties, résultats clés (lus depuis les métriques), limites, pistes d'extension.
1. test_ingestion : 45 605 lignes, 33 colonnes, clé (num_mena, num_individu, num_deplacement) unique, 3 127 ménages, 11 457 individus.
2. test_cleaning : plus aucune heure ≥ 24 après normalisation ; duree_mn = arrivée − départ (en minutes totales) sur 100 % des lignes ; nb_trajets = nombre de modes non nuls ; strates inconnues bien flaguées.
3. test_leakage : aucune colonne interdite (mode_*, nb_trajets*, cout, et la cible) dans les features de A1/A2/B.
4. test_split : intersection vide des num_mena train/test ; proportions 80/20 ± 2 pts ; reproductibilité (même seed ⇒ même découpage).
5. test_od_matrix : total Spark = 6 699 228,017 (tolérance relative 1e-9) et égalité cellule par cellule avec la feuille matrice_OD.
6. test_pipeline_smoke : --stages all --sample 200 s'exécute sans erreur et produit tous les artefacts attendus.
7. ruff et black --check passent.
Critères de livraison : make all tourne de bout en bout depuis un clone propre ; tous les fichiers listés en section 4 existent ; chaque modèle est comparé à ses baselines avec les métriques réelles ; aucun TODO ni code mort.
- Découpage aléatoire par ligne ; calcul de statistiques/encodages cible sur l'ensemble des données avant le split.
- Utiliser cout, mode_*, nb_trajets* comme features du modèle de mode.
- .toPandas() ou .collect() sur les données de déplacement complètes ; remplacer Spark par pandas pour le traitement ou le ML.
- Supprimer silencieusement des lignes : toute exclusion est comptée, loguée et documentée.
- Chiffres, noms de clusters ou conclusions écrits sans appui dans les sorties du pipeline.
- Joindre lieu_depart/lieu_arrivee sur code commune.
1. Arbre du dépôt et commande unique pour tout relancer.
2. Résultat des tests (nombre passés / échoués) et durée totale du pipeline.
3. Tableau récapitulatif des métriques réelles : baseline vs meilleur modèle pour A1, A2, B ; k retenu et silhouette pour C.
4. Les 5 hypothèses les plus structurantes et les limites principales.
5. Tout blocage rencontré (ex. Java) et ce qui reste à faire côté utilisateur.