"""Feature-level target leakage guards for all supervised scenarios."""

import pytest

from mobilite.features import (
    DURATION_FEATURES,
    MODE_FEATURES,
    assert_duration_no_leakage,
    assert_no_leakage,
)


def test_a1_a2_and_duration_predictors_are_safe() -> None:
    assert_no_leakage(MODE_FEATURES)
    assert_no_leakage(MODE_FEATURES + ["duree_mn"], include_duration=True)
    assert_duration_no_leakage(DURATION_FEATURES)


@pytest.mark.parametrize(
    "bad", ["cout", "mode_1", "nb_trajets", "nb_trajets_nn_pondere", "duree_mn"]
)
def test_mode_choice_leakage_is_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        assert_no_leakage([bad])


def test_duration_leakage_is_rejected() -> None:
    with pytest.raises(ValueError):
        assert_duration_no_leakage(["age", "cout"])
