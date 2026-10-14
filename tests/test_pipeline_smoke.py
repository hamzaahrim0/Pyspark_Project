"""Full --sample smoke test over 200 households."""

import os
import subprocess
import sys
from pathlib import Path


def test_pipeline_smoke_all_sample_200() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = "src"
    environment["JAVA_HOME"] = str(Path.home() / ".jdks" / "temurin-17")
    environment["TMPDIR"] = "/dev/shm"
    subprocess.run(
        [sys.executable, "-m", "mobilite.run_pipeline", "--stages", "all", "--sample", "200"],
        env=environment,
        check=True,
        timeout=1_200,
    )
    assert Path("data/silver/deplacement").exists()
    assert Path("data/gold/deplacement").exists()
    assert Path("outputs/metrics/mode_choice_A1.json").exists()
    assert Path("outputs/metrics/mode_choice_A2.json").exists()
    assert Path("outputs/metrics/duration.json").exists()
    assert Path("outputs/metrics/clustering.json").exists()
    assert list(Path("outputs/figures").glob("*.png"))
