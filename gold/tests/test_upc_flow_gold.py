"""upc_flow support in gold: survey_id placeholder, Latest folder name from the environment."""
import pandas as pd

from ris_kpi import config
from ris_kpi.common import storage
from ris_kpi.common.db_ids import fill_survey_id_placeholders, survey_id_placeholder


def test_placeholder_is_stable_and_readable():
    a = survey_id_placeholder("N/01002/01001/GJ", "1", "2")
    assert a == survey_id_placeholder("N/01002/01001/GJ", "1", "2")          # same survey -> same text (overwrite)
    assert a.startswith("N_01002_01001_GJ_P1_C2_") and len(a.rsplit("_", 1)[1]) == 12
    assert a != survey_id_placeholder("N/01002/01001/GJ", "1", "1")


def test_fill_only_missing_and_only_when_enabled():
    df = pd.DataFrame({"_upc": ["N/1", "N/1"], "_phase": ["1", "2"], "_cycle": ["1", "1"], "survey_id": ["s1", None]})
    assert fill_survey_id_placeholders(df, False)["survey_id"].isna().sum() == 1
    out = fill_survey_id_placeholders(df, True)
    assert out["survey_id"].iloc[0] == "s1" and out["survey_id"].iloc[1] == survey_id_placeholder("N/1", "2", "1")


def test_empty_latest_folder_writes_into_the_root(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LATEST_FOLDER", "")
    assert str(storage.latest_location(tmp_path, "t")) == str(tmp_path / "t.csv")
    storage.save_run(pd.DataFrame({"a": [1]}), tmp_path, "t", pd.Timestamp("2026-11-26").date())
    assert (tmp_path / "t.csv").is_file() and (tmp_path / "year=2026/month=11/day=26/t.csv").is_file()
    assert storage.find_latest(tmp_path, "t") == tmp_path / "year=2026/month=11/day=26/t.csv"
