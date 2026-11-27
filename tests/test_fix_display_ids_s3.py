"""tools/fix_display_ids_s3.py on local folders: plan, dry run, apply (silver parquet + csv, image_uploads, gold)."""
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

import fix_display_ids_s3 as fix  # noqa: E402


def test_plan_shifts_only_overlapping_surveys_in_written_order():
    surveys = {"A": (1, ["D/060426/N/00001", "D/060426/N/00003", "D/060426/N/00003/0001", "D/070426/N/00001"]),
               "B": (2, ["D/060426/N/00001", "D/060426/N/00002"]),
               "C": (3, ["D/060426/N/00010"]),                   # after A and B (shifted B ends at 5): kept
               "D": (4, ["D/080426/N/00001"])}
    assert fix.plan(surveys) == {"B": {"060426": 3}}
    assert fix.new_id("D/060426/N/00001/0002", {"060426": 3}) == "D/060426/N/00004/0002"
    assert fix.new_id("D/070426/N/00001", {"060426": 3}) == "D/070426/N/00001"
    assert fix.new_id("D/060426/N/99999", {"060426": 3}) == "D/060426/N/100002"
    assert fix.new_id(None, {"060426": 3}) is None and fix.new_id("x", {"060426": 3}) == "x"


def put_survey(silver, upc, ids, written):
    base = silver / "Zone=B" / "Phase=2" / "Cycle=1" / f"UPC={upc}"
    df = pd.DataFrame({"id": [f"{upc}_{i}" for i in range(len(ids))], "upc_code": [upc] * len(ids),
                       "display_defect_id": ids, "breach_flag": ["1"] * len(ids)})
    for t in ("master_roughness_parent", "master_pothole"):
        f = base / "dlm_kpi_new" / f"{t}.parquet"
        f.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(f, index=False)
        df.to_csv(base / "dlm_kpi_new" / f"{t}.csv", index=False)
        os.utime(f, (written, written))
    pd.DataFrame({"image_name": ["a.jpg"], "display_defect_id": [ids[0]], "url": ["https://cdn/x"]}).to_csv(
        base / "dlm_kpi_new" / "image_uploads.csv", index=False)
    return base


def test_dry_run_then_apply(tmp_path, capsys):
    silver, gold = tmp_path / "Silver", tmp_path / "Gold"
    now = time.time()
    put_survey(silver, "N_08009_04002_GJ", ["D/060426/N/00001", "D/060426/N/00002", "D/060426/N/00002/0001"], now - 100)
    b = put_survey(silver, "N_08009_05001_GJ", ["D/060426/N/00001", "D/060426/N/00002", None], now)
    stacked = pd.DataFrame({"upc": ["N/08009/04002/GJ", "N/08009/05001/GJ", "N/08009/05001/GJ"],
                            "phase": ["2", "2.0", "2"], "cycle": ["1"] * 3,
                            "display_defect_id": ["D/060426/N/00001", "D/060426/N/00001", "D/060426/N/00002"]})
    for folder in ("Latest", "year=2026/month=11/day=9", "year=2026/month=11/day=26"):
        (gold / folder).mkdir(parents=True)
        stacked.to_csv(gold / folder / "master_pothole.csv", index=False)
    args = ["--silver-root", str(silver), "--gold-dir", str(gold / "Latest"), "--out", str(tmp_path / "out")]

    assert fix.main(args) == 0                                                # dry run
    plan = pd.read_csv(tmp_path / "out" / "renumbered_surveys.csv", dtype=str)
    assert plan[["survey", "old_from", "new_from", "new_to"]].values.tolist() == [
        ["Zone=B/Phase=2/Cycle=1/UPC=N_08009_05001_GJ", "1", "3", "4"]]
    assert pd.read_parquet(b / "dlm_kpi_new" / "master_pothole.parquet")["display_defect_id"].iloc[0] == "D/060426/N/00001"

    assert fix.main(args + ["--apply"]) == 0
    for t in ("master_roughness_parent", "master_pothole"):
        assert pd.read_parquet(b / "dlm_kpi_new" / f"{t}.parquet")["display_defect_id"].tolist() == [
            "D/060426/N/00003", "D/060426/N/00004", None]
        assert pd.read_csv(b / "dlm_kpi_new" / f"{t}.csv", dtype=str)["display_defect_id"].tolist()[:2] == [
            "D/060426/N/00003", "D/060426/N/00004"]
    assert pd.read_csv(b / "dlm_kpi_new" / "image_uploads.csv")["display_defect_id"].tolist() == ["D/060426/N/00003"]
    kept = silver / "Zone=B/Phase=2/Cycle=1/UPC=N_08009_04002_GJ/dlm_kpi_new/master_pothole.parquet"
    assert pd.read_parquet(kept)["display_defect_id"].tolist()[0] == "D/060426/N/00001"     # first survey untouched
    for folder, changed in (("Latest", True), ("year=2026/month=11/day=26", True), ("year=2026/month=11/day=9", False)):
        ids = pd.read_csv(gold / folder / "master_pothole.csv", dtype=str)["display_defect_id"].tolist()
        assert ids == (["D/060426/N/00001", "D/060426/N/00003", "D/060426/N/00004"] if changed else
                       stacked["display_defect_id"].tolist())
    backups = list((silver / "_backup_display_ids").rglob("master_pothole.parquet"))
    assert len(backups) == 1 and pd.read_parquet(backups[0])["display_defect_id"].iloc[0] == "D/060426/N/00001"
    assert fix.main(args) == 0                                                # nothing left to fix
    assert pd.read_csv(tmp_path / "out" / "renumbered_surveys.csv").empty
    assert pd.read_csv(tmp_path / "out" / "files.csv").empty
