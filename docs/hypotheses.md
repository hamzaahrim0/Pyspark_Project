# Hypothèses documentées

- Les valeurs HHMM supérieures à 2400 représentent des minutes au-delà du jour civil ; conversion par heures et minutes, sans correction des heures invalides, car les heures source sont documentées comme HHMM.
- Le seuil d’anomalie du coût est fixé à 10 000 unités (inféré) ; unité monétaire à confirmer, et 99 999 constitue une valeur aberrante manifeste.
- La durée est signalée aberrante au-delà de 180 minutes (choix métier) ; aucune ligne n’est supprimée.
- `zex` désigne probablement une zone extérieure (à confirmer) ; toute strate ne correspondant pas à D/G/P/R est classée `AUTRE_INCONNU`.
- La valeur manquante observée de trajets à pied pondérés appartient à un déplacement `APied` avec un seul tronçon ; elle est imputée à `poids_extra` (122,1333), le poids logique d’un trajet à pied, tandis que les autres absences structurelles sont à zéro.
- Les 24 modalités de motif sont affectées explicitement aux huit familles métier par dictionnaire; toute modalité inattendue provoque une erreur, et aucune sortie nulle n’est admise.
- Le split par hash de ménage cible environ 80/20 et peut varier de quelques points selon la distribution des identifiants.

- La relation quartier→strate n’est pas strictement 1-pour-1 dans le classeur réel : Spark a observé le quartier 399 lié à `zex` et `NA1_r`; la dimension conserve les deux associations et l’assertion reste KO au lieu de choisir arbitrairement une strate.
