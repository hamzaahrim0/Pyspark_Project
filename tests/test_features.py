"""Leakage and explicit-purpose dictionary checks."""

import pytest

from mobilite.features import MOTIF_GROUPS, assert_motif_coverage, assert_no_leakage

EXPECTED_MOTIFS = {
    "TravailHabituel",
    "AutreMotifTravail",
    "TravailAmbulant",
    "RechercheTravail",
    "Etudes",
    "AutreMotifEtudes",
    "AchatsAlimentaires",
    "AchatsNonAlimentaires",
    "VisiteAmis",
    "VisiteFamille",
    "VisitesVoisins",
    "SportsLoisirs",
    "RepasExterieur",
    "Ceremonies",
    "Religion",
    "Associations",
    "RetourDomicile",
    "Sante",
    "DemarchesAdministratives",
    "Services",
    "Accompagnement",
    "Autre",
    "AutreMotifMenage",
    "ApprovisionnementEau",
}


def test_motif_dictionary_is_exhaustive_and_non_null() -> None:
    assert set(MOTIF_GROUPS) == EXPECTED_MOTIFS
    assert all(MOTIF_GROUPS.values())
    assert_motif_coverage(EXPECTED_MOTIFS)


def test_unmapped_motif_fails() -> None:
    with pytest.raises(ValueError, match="Unmapped"):
        assert_motif_coverage(["motif_inconnu"])


def test_safe_features_pass() -> None:
    assert_no_leakage(["age", "motif", "heure_depart_h"])


@pytest.mark.parametrize(
    "column", ["cout", "mode_1", "nb_trajets", "nb_trajets_apied_pondere", "moyen_transport"]
)
def test_leakage_rejected(column: str) -> None:
    with pytest.raises(ValueError):
        assert_no_leakage([column])


@pytest.mark.spark
def test_motif_group_spark_mapping_has_no_null(spark) -> None:
    from mobilite.features import add_motif_group

    rows = [(motif,) for motif in sorted(EXPECTED_MOTIFS)]
    mapped = add_motif_group(spark.createDataFrame(rows, ["motif"]))
    assert mapped.filter(mapped.motif_groupe.isNull()).count() == 0
    assert mapped.select("motif_groupe").distinct().count() == 8
