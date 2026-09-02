"""Session factory and the JDK guard."""

from __future__ import annotations

from pathlib import Path

import pytest

from fuel_lakehouse import spark as spark_module


def test_unsupported_java_is_refused(monkeypatch) -> None:
    monkeypatch.setattr(spark_module, "_java_major_version", lambda: 25)
    with pytest.raises(RuntimeError) as err:
        spark_module._check_java()
    message = str(err.value)
    assert "Java 25" in message
    assert "JAVA_HOME" in message


@pytest.mark.parametrize("version", [17, 21])
def test_supported_java_passes(monkeypatch, version: int) -> None:
    monkeypatch.setattr(spark_module, "_java_major_version", lambda: version)
    spark_module._check_java()


def test_undetectable_java_does_not_block(monkeypatch) -> None:
    # Better to let Spark complain than to refuse on a failed probe.
    monkeypatch.setattr(spark_module, "_java_major_version", lambda: None)
    spark_module._check_java()


def test_java_home_wins_over_path(monkeypatch, tmp_path: Path) -> None:
    java = tmp_path / "bin" / "java"
    java.parent.mkdir()
    java.touch()
    monkeypatch.setenv("JAVA_HOME", str(tmp_path))
    assert spark_module._java_binary() == str(java)


def test_falls_back_to_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("JAVA_HOME", str(tmp_path / "missing"))
    assert spark_module._java_binary() == "java"


def test_delta_round_trip(spark, tmp_path: Path) -> None:
    path = str(tmp_path / "table")
    spark.createDataFrame([(1, "GASOLINA")], "id int, product string").write.format("delta").save(
        path
    )

    result = spark.read.format("delta").load(path)
    assert result.count() == 1
    assert result.first()["product"] == "GASOLINA"
