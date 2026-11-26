"""upc_flow rules with a local downloads folder and a fake run.py (no AWS, no DB)."""
import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from upc_flow import run as flow  # noqa: E402


def downloads(tmp_path):
    raw = tmp_path / "raw"
    for z, p, c, u in [("Zone B", "Phase 1", "Cycle 1", "N_01002_01001_GJ"),
                       ("Zone B", "Phase 1", "Cycle 2", "N_01002_01001_GJ"),
                       ("Zone B", "Phase 2", "Cycle 1", "N-08009-04001-GJ"),
                       ("Zone D", "Phase 1", "Cycle 1", "N_99999_00000_XX")]:
        (raw / z / p / c / u / "Road" / "Deliverable 1 - RAMS Data").mkdir(parents=True)
    return raw


def test_read_list_csv_text_and_cli(tmp_path):
    f = tmp_path / "l.csv"
    f.write_text("upc,phase,cycle,zone\nN/01002/01001/GJ,1,1.0,\nn_08009_04001_gj,,,b\n", encoding="utf-8")
    rows = flow.read_list(f, ["N-02001-12001-GJ"])
    assert rows == [{"upc": "N/01002/01001/GJ", "phase": "1", "cycle": "1", "zone": ""},
                    {"upc": "N/08009/04001/GJ", "phase": "", "cycle": "", "zone": "B"},
                    {"upc": "N/02001/12001/GJ", "phase": "", "cycle": "", "zone": ""}]
    t = tmp_path / "l.txt"
    t.write_text("N/01002/01001/GJ\nN/01002/01001/GJ\n", encoding="utf-8")
    assert len(flow.read_list(t)) == 1


def test_find_surveys_filters_phase_cycle_and_reports_missing(tmp_path):
    raw = downloads(tmp_path)
    wanted = flow.read_list(None, []) + [
        {"upc": "N/01002/01001/GJ", "phase": "1", "cycle": "2", "zone": ""},
        {"upc": "N/08009/04001/GJ", "phase": "", "cycle": "", "zone": ""},
        {"upc": "N/01002/01001/GJ", "phase": "3", "cycle": "1", "zone": ""},     # not in the downloads
    ]
    found, missing = flow.find_surveys(str(raw), wanted)
    assert [(s["upc"], s["zone"], s["phase"], s["cycle"]) for s in found] == [
        ("N/01002/01001/GJ", "B", "1", "2"), ("N/08009/04001/GJ", "B", "2", "1")]
    assert missing == [{"upc": "N/01002/01001/GJ", "phase": "3", "cycle": "1", "zone": ""}]


def test_upc_without_phase_finds_every_cycle(tmp_path):
    found, _ = flow.find_surveys(str(downloads(tmp_path)), flow.read_list(None, ["N/01002/01001/GJ"]))
    assert [(s["phase"], s["cycle"]) for s in found] == [("1", "1"), ("1", "2")]


FAKE = textwrap.dedent("""
    import json, os, sys, time
    from pathlib import Path
    a = sys.argv[1:]
    stage = a[a.index("--only") + 1]
    upc = a[a.index("--upc") + 1] if "--upc" in a else "-"
    rec = Path(os.environ["FAKE_RECORD"])
    calls = json.loads(rec.read_text()) if rec.exists() else []
    calls.append({"args": a, "gold_dir": os.environ.get("S3_GOLD_OUTPUT_DIR"),
                  "latest": os.environ.get("RIS_KPI_LATEST_FOLDER"), "ph": os.environ.get("SURVEY_ID_PLACEHOLDER")})
    rec.write_text(json.dumps(calls))
    codes = json.loads(os.environ.get("FAKE_CODES", "{}"))
    code = codes.get(f"{stage}:{upc}", 0)
    d = Path(os.environ["LOG_DIR"]) / f"run_{time.time_ns()}"
    d.mkdir(parents=True)
    why = "warning (exit 1) - scheduled runs stop on warnings" if code == 1 else f"exit code {code}"
    extra = os.environ.get("FAKE_LOG_" + stage.upper(), "")
    (d / "pipeline.log").write_text(extra + (f"!! STOPPED: {stage} {upc}: {why}\\n" if code else "Every step OK\\n"), encoding="utf-8")
    sys.exit(code)
""")


@pytest.fixture
def fake(tmp_path, monkeypatch):
    script = tmp_path / "fake_run.py"
    script.write_text(FAKE, encoding="utf-8")
    monkeypatch.setattr(flow, "RUN_PY", script)
    monkeypatch.setenv("FAKE_RECORD", str(tmp_path / "calls.json"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("RAW_S3_ROOT", str(downloads(tmp_path)))
    monkeypatch.setenv("S3_GOLD_OUTPUT_DIR", "s3://b/DHMS/NSV/Gold")
    monkeypatch.delenv("UPC_FLOW_GOLD_DIR", raising=False)
    return tmp_path


def calls(tmp_path):
    return json.loads((tmp_path / "calls.json").read_text())


def test_stages_per_survey_then_gold_to_latest_v2_never_push(fake):
    code = flow.main(["--upc", "N/01002/01001/GJ"])
    assert code == 0
    c = calls(fake)
    stages = [x["args"][x["args"].index("--only") + 1] for x in c]
    assert stages == ["bronze", "silver", "images", "bronze", "silver", "images", "gold"]
    assert all("--no-write" in x["args"] and "--scheduled" in x["args"] for x in c)
    assert not any(s.startswith("push") for s in stages)
    gold = c[-1]
    assert gold["gold_dir"] == "s3://b/DHMS/NSV/Gold/Latest_v2" and gold["latest"] == "" and gold["ph"] == "True"
    assert all(x["gold_dir"] == "s3://b/DHMS/NSV/Gold" for x in c[:-1])          # other stages: normal folders


def test_warning_passes_failure_skips_survey_but_not_others(fake, monkeypatch):
    monkeypatch.setenv("FAKE_CODES", json.dumps({"bronze:N/01002/01001/GJ": 1, "silver:N/08009/04001/GJ": 3}))
    code = flow.main(["--upc", "N/01002/01001/GJ", "--upc", "N/08009/04001/GJ"])
    assert code == 1
    c = calls(fake)
    stages = [(x["args"][x["args"].index("--only") + 1],
               x["args"][x["args"].index("--upc") + 1] if "--upc" in x["args"] else "-") for x in c]
    assert ("images", "N/01002/01001/GJ") in stages                        # bronze warning passed
    assert ("images", "N/08009/04001/GJ") not in stages                    # silver failed -> survey stopped
    gold_list = Path(c[-1]["args"][c[-1]["args"].index("--surveys") + 1]).read_text()
    assert "N/01002/01001/GJ" in gold_list and "N/08009/04001/GJ" not in gold_list
    res = (sorted((fake / "logs" / "upc_flow").iterdir())[-1] / "results.csv").read_text()
    assert "warnings" in res and "failed" in res


def test_dry_run_and_skip(fake):
    assert flow.main(["--upc", "N/01002/01001/GJ", "--dry-run"]) == 0
    assert not (fake / "calls.json").exists()
    assert flow.main(["--upc", "N/08009/04001/GJ", "--skip", "images", "--gold-dir", "s3://b/x"]) == 0
    c = calls(fake)
    assert [x["args"][x["args"].index("--only") + 1] for x in c] == ["bronze", "silver", "gold"]
    assert c[-1]["gold_dir"] == "s3://b/x"


def test_stage_env_lets_the_environment_override_env_file(monkeypatch):
    from pipeline.settings import Settings
    s = Settings({"S3_GOLD_OUTPUT_DIR": "s3://b/Gold", "OTHER": "x"})
    monkeypatch.setenv("S3_GOLD_OUTPUT_DIR", "s3://b/Gold/Latest_v2")
    env = s.stage_env()
    assert env["S3_GOLD_OUTPUT_DIR"] == "s3://b/Gold/Latest_v2" and env["OTHER"] == "x"


def test_read_nsv_tracker_columns(tmp_path):
    import pandas as pd
    df = pd.DataFrame({"S. No.": [201, 228, 367, 999], "Zone": ["A", "B", "C", "A"],
                       "Phase": ["Phase 2", "Phase 2", "Phase 3", "Phase 3"],
                       "Cycle": ["Cycle 1", "Cycle 1", "Cycle 1", "Cycle 1"],
                       "UPC code": ["N/02007/11001/HR", "N/02001/\n04001/RJ", "", "not a upc"],
                       "UPC as per contract": ["", "", "N/02008/01001/MP", ""]})
    x = tmp_path / "tracker.xlsx"
    df.to_excel(x, index=False)
    rows = flow.read_list(x)
    assert rows == [{"upc": "N/02007/11001/HR", "phase": "2", "cycle": "1", "zone": "A"},
                    {"upc": "N/02001/04001/RJ", "phase": "2", "cycle": "1", "zone": "B"},
                    {"upc": "N/02008/01001/MP", "phase": "3", "cycle": "1", "zone": "C"}]
    c = tmp_path / "tracker.csv"
    df.to_csv(c, index=False)
    assert flow.read_list(c) == rows


def test_limit_and_zone(fake):
    assert flow.main(["--upc", "N/01002/01001/GJ", "--limit", "1"]) == 0
    c = calls(fake)
    assert len([x for x in c if x["args"][x["args"].index("--only") + 1] == "bronze"]) == 1


def test_silver_with_failed_tables_stops_the_survey(fake, monkeypatch):
    monkeypatch.setenv("FAKE_CODES", json.dumps({"silver:N/08009/04001/GJ": 1}))
    monkeypatch.setenv("FAKE_LOG_SILVER", "UPCs: 1 | table runs FAILED: 1 | DQ FAIL: 0\n"
                                          "N_08009_04001_GJ (load) FAILED 0 0 OutOfBoundsDatetime: 202-07-14\n")
    assert flow.main(["--upc", "N/08009/04001/GJ"]) == 1
    stages = [x["args"][x["args"].index("--only") + 1] for x in calls(fake)]
    assert stages == ["bronze", "silver"]                     # no images, no gold
    res = (sorted((fake / "logs" / "upc_flow").iterdir())[-1] / "results.csv").read_text()
    assert "table run(s) FAILED" in res and "OutOfBoundsDatetime" in res


def test_dropped_dates_reported_and_survey_goes_on(fake, monkeypatch):
    monkeypatch.setenv("FAKE_LOG_SILVER", "  WARNING: 1 date value(s) unreadable or out of 1900-2100 → empty, "
                                          "e.g. ['202-07-14']\nUPCs: 1 | table runs FAILED: 0 | DQ FAIL: 0\n")
    assert flow.main(["--upc", "N/08009/04001/GJ"]) == 0
    stages = [x["args"][x["args"].index("--only") + 1] for x in calls(fake)]
    assert stages == ["bronze", "silver", "images", "gold"]          # moved forward
    res = (sorted((fake / "logs" / "upc_flow").iterdir())[-1] / "results.csv").read_text(encoding="utf-8")
    assert "warnings" in res and "202-07-14" in res
