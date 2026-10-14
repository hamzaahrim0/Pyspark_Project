# Rapport de qualité

- `key_unique` : **OK** — observé 45605/45605; attendu une clé par déplacement.
- `mode_count_matches_nb_trajets` : **OK** — observé 0; attendu 0 ligne incohérente.
- `duration_matches_elapsed_time` : **OK** — observé 0; attendu écart ≤ 1 minute.
- `weight_constant_per_household` : **OK** — observé 0; attendu 0 ménage à poids multiples.
- `quarter_to_stratum_one_to_one` : **KO** — observé 1; attendu 0 quartier associé à plusieurs strates.
- `intermodal_has_multiple_trips` : **OK** — observé 0; attendu Intermodal implique nb_trajets ≥ 2.
- `walking_has_single_trip` : **OK** — observé 0; attendu APied implique nb_trajets = 1.
- `known_strata_have_known_family` : **OK** — observé 0; attendu strates connues dans D/G/P/R.
