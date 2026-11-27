"""Surveys / silver folders, settings, and whole runs on an in-memory S3."""
from datetime import date

import pandas as pd
import pytest

from conftest import cases, master_tables, references, silver_tables
from status_update import config, pipeline, surveys
from status_update.config import Settings

BUCKET = "s3://dhms-new-stgae"
SETTINGS = dict(ris_input_dir=f"{BUCKET}/DHMS/NSV/Gold/Latest_v2", dic_root=f"{BUCKET}/NSV/DHMS/DIC_Staging",
                silver_root=f"{BUCKET}/DHMS/NSV/Silver", silver_subfolder="dlm_kpi_new",
                output_dir=f"{BUCKET}/DHMS/NSV/Gold/Latest_v2/status_update", drop_rows_without_ids=True,
                survey_id_fallback=True)


def settings(**overrides):
    return Settings(**{**SETTINGS, **overrides})


def put_silver(fake, upc="N/9", phase="1", cycle="1", zone="C"):
    folder = f"{SETTINGS['silver_root']}/Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc.replace('/', '_')}/dlm_kpi_new"
    for name, df in silver_tables(upc, phase, cycle, zone).items():
        fake.put_df(f"{folder}/{name}.parquet", df)


def put_ris(fake, combos):
    df = pd.DataFrame(combos, columns=["upc", "phase", "cycle"]).assign(total_defects_ca="1")
    fake.put_df(f"{SETTINGS['ris_input_dir']}/ris_view_data.csv", df)
    fake.put_df(f"{SETTINGS['ris_input_dir']}/ris_trends_data.csv", df.head(1))


def put_pull(fake, day, statuses):
    folder = f"{SETTINGS['dic_root']}/year=2026/month=11/day={day:02d}"
    fake.put_df(f"{folder}/udm_defect_reference.parquet", references())
    fake.put_df(f"{folder}/dlm_defect_case.csv", cases(statuses))


@pytest.fixture
def world(fake_s3):
    """N/9 phase 1 cycle 1 in Zone=C (+ an unrelated survey in Zone=A); the pothole case closed on the 25th."""
    put_silver(fake_s3)
    put_silver(fake_s3, upc="N/1", zone="A")
    put_ris(fake_s3, [("N/9", "1", "1"), ("N/1", "1", "1"), ("N/9", "2", "1")])   # N/9 phase 2: no silver folder
    put_pull(fake_s3, 24, ["DEFECT_ASSIGNED"] * 3)
    put_pull(fake_s3, 25, ["DEFECT_ASSIGNED", "CLOSED", "DEFECT_ASSIGNED"])
    for name, df in master_tables().items():
        fake_s3.put_df(f"{SETTINGS['dic_root']}/Latest/{name}.csv", df)
    return fake_s3


def out(fake, name):
    return fake.read_df(f"{SETTINGS['output_dir']}/{name}.csv")


def test_run_updates_the_changed_survey(world):
    result = pipeline.run(settings())
    assert result["changed"]["upc"].tolist() == ["N/9"]
    rep = out(world, "updated_surveys")
    assert rep[["upc", "phase", "zone", "result"]].fillna("-").values.tolist() == [
        ["N/9", "1", "C", "recalculated"], ["N/9", "2", "-", "no silver folder"]]
    pothole = out(world, "master_pothole")
    assert pothole[["defect_reference_id", "current_status", "final_status"]].values.tolist() == [
        ["r2", "CLOSED", "Closed"]]
    view = out(world, "ris_view_data").set_index(["defect_type", "status"])
    assert view.loc[("Pothole", "close"), "total_defects_ca"] == "1"
    assert view.loc[("Pothole", "open"), "total_defects_ca"] == "0"
    assert view.loc[("Roughness", "open"), "affected_length_ca_m"] == "20.0"    # parent + first CA row breached
    assert set(view["upc"]) == {"N/9"} and set(view["survey_id"]) == {"sv9"}
    assert view["id"].is_unique and (view["created_at"] == view["updated_at"]).all()
    trends = out(world, "ris_trends_data")
    assert len(trends) == 3 and set(trends["survey_id"]) == {"sv9"}
    inter = out(world, config.INTERMEDIATE_NAME)
    assert inter["total_status"].dropna().unique().tolist() == ["Open"]
    changed = out(world, "changed_upcs")
    assert changed[["upc", "case_changed", "in_ris_data", "recalculated", "latest_pull", "previous_pull"]].values \
        .tolist() == [["N/9", "1", "True", "True", "2026-11-25", "2026-11-24"]]


def test_no_change_removes_the_previous_tables(world):
    pipeline.run(settings())
    assert world.has(f"{SETTINGS['output_dir']}/ris_view_data.csv")
    put_pull(world, 26, ["DEFECT_ASSIGNED", "CLOSED", "DEFECT_ASSIGNED"])      # same as the 25th
    result = pipeline.run(settings())
    assert result["changed"].empty and result["tables"] == {}
    for name in config.OUTPUT_TABLES:
        assert not world.has(f"{SETTINGS['output_dir']}/{name}.csv")
    assert out(world, "changed_upcs").empty and out(world, "updated_surveys").empty
    assert world.has(f"{SETTINGS['output_dir']}/changed_upcs.csv")


def test_dry_run_writes_nothing(world):
    result = pipeline.run(settings(), dry_run=True)
    assert len(result["tables"]["ris_view_data"]) == 12
    assert not [k for k in world.objects if "status_update" in k]


def test_given_dates(world):
    put_pull(world, 26, ["CLOSED"] * 3)
    result = pipeline.run(settings(), latest_date=date(2026, 11, 25), previous_date=date(2026, 11, 24))
    assert result["changed"]["case_changed"].tolist() == [1]


def test_survey_in_several_zones_uses_every_folder(world):
    put_silver(world, zone="D")
    folders = surveys.find_silver_folders(SETTINGS["silver_root"], pd.DataFrame(
        {"upc": ["N/9", "N/5"], "phase": ["1", "1"], "cycle": ["1", "1"]}), "dlm_kpi_new")
    assert folders[["upc", "zone"]].fillna("-").values.tolist() == [["N/9", "C"], ["N/9", "D"], ["N/5", "-"]]


def test_ris_surveys_normalised_and_matched(fake_s3):
    fake_s3.put_df(f"{SETTINGS['ris_input_dir']}/ris_view_data.csv",
                   pd.DataFrame({"upc": ["N_9", "N/9"], "phase": ["1.0", "1"], "cycle": ["1", "1"]}))
    found = surveys.ris_surveys(SETTINGS["ris_input_dir"])                     # ris_trends_data missing: logged
    assert found[["upc", "phase", "cycle", "in_tables"]].values.tolist() == [["N/9", "1", "1", "ris_view_data"]]
    changed = pd.DataFrame({"upc": ["N/9", "N/7"]})
    assert surveys.matched_surveys(found, changed)["upc"].tolist() == ["N/9"]
    assert changed["in_ris_data"].tolist() == [True, False]
    with pytest.raises(FileNotFoundError):
        surveys.ris_surveys(f"{BUCKET}/nothing")


def test_settings_from_env(monkeypatch, tmp_path):
    for key in list(config.DEFAULTS) + ["S3_GOLD_OUTPUT_DIR", "UPC_FLOW_GOLD_DIR", "S3_DB_TABLES_DIR",
                                        "S3_SILVER_INPUT_DIR", "STATUS_WFM_GOLD_DIR", "STATUS_NSV_GOLD_DIR"]:
        monkeypatch.delenv(key, raising=False)
    env = tmp_path / ".env"
    env.write_text("S3_GOLD_OUTPUT_DIR=s3://x/Gold/\nS3_DB_TABLES_DIR=s3://x/DIC_Staging/Latest\n"
                   "STATUS_SILVER_SUBFOLDER=\nSURVEY_ID_FALLBACK=no\n")
    wfm = Settings.from_env(env, source="wfm")
    assert wfm.ris_input_dir == "s3://x/Gold/Latest" and wfm.output_dir == "s3://x/Gold/Latest/status_update"
    assert wfm.dic_root == "s3://x/DIC_Staging" and wfm.silver_subfolder == "" and wfm.survey_id_fallback is False
    assert wfm.push is True and wfm.survey_id_placeholder is False and wfm.drop_rows_without_ids is True
    nsv = Settings.from_env(env, source="nsv")
    assert nsv.ris_input_dir == "s3://x/Gold/Latest_v2" and nsv.output_dir == "s3://x/Gold/Latest_v2/status_update"
    assert nsv.push is False and nsv.survey_id_placeholder is True
    monkeypatch.setenv("STATUS_NSV_GOLD_DIR", "local_ris")
    assert Settings.from_env(env, source="nsv").ris_input_dir == config.ROOT_DIR / "local_ris"
    monkeypatch.setenv("DROP_ROWS_WITHOUT_IDS", "maybe")
    with pytest.raises(ValueError):
        Settings.from_env(env)
    monkeypatch.setenv("STATUS_PUSH", "wfm,xyz")
    with pytest.raises(ValueError):
        config.names("STATUS_PUSH")


def test_survey_without_ravelling(world):
    """New-format surveys have no master_ravelling: read as an empty table, not an error."""
    for key in [k for k in world.objects if k.endswith("master_ravelling.parquet")]:
        del world.objects[key]
    result = pipeline.run(settings(), dry_run=True)
    assert result["tables"]["master_ravelling"].empty
    view = result["tables"]["ris_view_data"].set_index(["defect_type", "status"])
    assert "Ravelling" not in view.index.get_level_values("defect_type")      # no ravelling grain, as in the gold run
    assert view.loc[("Pothole", "close"), "total_defects_ca"] == 1
