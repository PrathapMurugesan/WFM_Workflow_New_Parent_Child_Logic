"""DB pulls (dated folders, table files) and the changed UPCs between two pulls."""
from datetime import date

import pandas as pd
import pytest

from status_update import changes, dic

ROOT = "s3://b/NSV/DHMS/DIC_Staging"


def put_pull(fake, day_folder, udm=None, dlm=None, udm_ext=".csv"):
    udm = udm if udm is not None else pd.DataFrame({"id": ["r1"], "defect_collection": ["x"], "defect_id": ["d"],
                                                    "upc": ["N/1"], "defect_status": ["IN_PROGRESS"]})
    dlm = dlm if dlm is not None else pd.DataFrame({"id": ["c1"], "defect_reference_id": ["r1"],
                                                    "current_status": ["DEFECT_ASSIGNED"], "upc": ["N/1"]})
    fake.put_df(f"{ROOT}/{day_folder}/udm_defect_reference{udm_ext}", udm)
    fake.put_df(f"{ROOT}/{day_folder}/dlm_defect_case.csv", dlm)


def test_snapshot_days_and_pick_the_two_newest(fake_s3):
    put_pull(fake_s3, "year=2026/month=10/day=31")
    put_pull(fake_s3, "year=2026/month=11/day=3")                 # no zero padding
    put_pull(fake_s3, "year=2026/month=11/day=25", udm_ext=".parquet")
    fake_s3.put(f"{ROOT}/year=2026/month=11/day=26/udm_defect_reference.csv", "id\n")   # no dlm_defect_case: skipped
    fake_s3.put(f"{ROOT}/Latest/master_upc_project.csv", "id\n")
    fake_s3.put(f"{ROOT}/year=2026/month=11/notes/readme.txt", "x")
    days = dic.snapshot_days(ROOT)
    assert sorted(days) == [date(2026, 10, 31), date(2026, 11, 3), date(2026, 11, 25), date(2026, 11, 26)]
    latest, previous = dic.pick_snapshots(ROOT)
    assert (latest.day, previous.day) == (date(2026, 11, 25), date(2026, 11, 3))
    assert latest.files["udm_defect_reference"][0].endswith("day=25/udm_defect_reference.parquet")
    latest, previous = dic.pick_snapshots(ROOT, date(2026, 11, 3), date(2026, 10, 31))   # given days
    assert (latest.day, previous.day) == (date(2026, 11, 3), date(2026, 10, 31))
    with pytest.raises(FileNotFoundError):
        dic.pick_snapshots(ROOT, date(2026, 11, 26))               # that day has no dlm_defect_case
    with pytest.raises(FileNotFoundError):
        dic.pick_snapshots(ROOT, date(2026, 10, 31))               # nothing before it


def test_table_files_direct_or_part_files(fake_s3):
    folder = f"{ROOT}/year=2026/month=11/day=25"
    fake_s3.put(f"{folder}/udm_defect_reference.csv", "id\n1\n")
    fake_s3.put(f"{folder}/udm_defect_reference.parquet", b"x")
    fake_s3.put(f"{folder}/dlm_defect_case/part-0001.csv", "id\n1\n")
    fake_s3.put(f"{folder}/dlm_defect_case/part-0000.csv", "id\n2\n")
    assert dic.table_files(folder, "udm_defect_reference") == [f"{folder}/udm_defect_reference.parquet"]
    assert dic.table_files(folder, "dlm_defect_case") == [f"{folder}/dlm_defect_case/part-0000.csv",
                                                          f"{folder}/dlm_defect_case/part-0001.csv"]
    assert dic.table_files(folder, "survey_data_map") == []


def test_other_db_tables_from_latest_only(fake_s3):
    put_pull(fake_s3, "year=2026/month=11/day=25")
    fake_s3.put_df(f"{ROOT}/year=2026/month=11/day=25/master_survey_system.csv",     # ignored: dated copy
                   pd.DataFrame({"id": ["day"], "system_code": ["NSV"]}))
    fake_s3.put_df(f"{ROOT}/Latest/master_survey_system.csv", pd.DataFrame({"id": ["latest"], "system_code": ["NSV"]}))
    fake_s3.put_df(f"{ROOT}/Latest/survey_data_map.parquet",
                   pd.DataFrame({"survey_schedule_id": ["s"], "upc": ["N/1"], "phase": ["1"], "cycle": ["1"]}))
    db = dic.load_db_tables(ROOT, ["master_survey_system", "survey_data_map"])
    assert db["master_survey_system"]["id"].tolist() == ["latest"]
    assert db["survey_data_map"]["survey_schedule_id"].tolist() == ["s"]
    with pytest.raises(FileNotFoundError):
        dic.load_db_tables(ROOT, ["master_defect_type"])
    fake_s3.put_df(f"{ROOT}/Latest/master_defect_type.csv", pd.DataFrame({"id": ["x"]}))
    with pytest.raises(ValueError, match="no column"):
        dic.load_db_tables(ROOT, ["master_defect_type"])


# --- changed UPCs ------------------------------------------------------------------------------------------------

def udm(rows):
    return pd.DataFrame(rows, columns=["id", "upc", "defect_status"])


def dlm(rows):
    return pd.DataFrame(rows, columns=["id", "defect_reference_id", "current_status", "upc"])


def test_changed_upcs_from_both_tables():
    udm_prev = udm([("r1", "N/1", "IN_PROGRESS"), ("r2", "N/2", "IN_PROGRESS"), ("r3", "N/3", "IN_PROGRESS"),
                    ("r4", "N/4", "IN_PROGRESS"), ("r5", "N_5", "IN_PROGRESS")])
    udm_new = udm([("r1", "N/1", "IN_PROGRESS"), ("r2", "N/2", " CLOSED"),             # r2 status changed
                   ("r3", "N/3", "IN_PROGRESS"), ("r5", "N_5", "IN_PROGRESS "),         # r4 removed; r5: spaces only
                   ("r6", "N/6", None)])                                                # r6 new
    case_prev = dlm([("c1", "r1", "DEFECT_ASSIGNED", "N/1"), ("c3", "r3", "DEFECT_ASSIGNED", None),
                     ("c7", "r7", "DEFECT_ASSIGNED", "N/7")])
    case_new = dlm([("c1", "r1", "DEFECT_ASSIGNED", "N/1"), ("c3", "r3", "CLOSED", None),   # no upc: via r3 -> N/3
                    ("c8", "r1", "CLOSED", "N_8")])                                         # c7 removed, c8 new
    out = changes.changed_upcs(udm_new, udm_prev, case_new, case_prev).set_index("upc")
    assert sorted(out.index) == ["N/2", "N/3", "N/4", "N/6", "N/7", "N/8"]      # N/1, N/5 unchanged
    assert out.loc["N/2", "udm_status_changed"] == 1 and out.loc["N/4", "udm_removed"] == 1
    assert out.loc["N/6", "udm_new"] == 1 and out.loc["N/3", "case_changed"] == 1
    assert out.loc["N/7", "case_removed"] == 1 and out.loc["N/8", "case_new"] == 1
    assert out[changes.COUNT_COLUMNS].to_numpy().sum() == 6


def test_no_change():
    u = udm([("r1", "N/1", "IN_PROGRESS")])
    c = dlm([("c1", "r1", "DEFECT_ASSIGNED", "N/1")])
    out = changes.changed_upcs(u, u.copy(), c, c.copy())
    assert out.empty and list(out.columns) == ["upc"] + changes.COUNT_COLUMNS
