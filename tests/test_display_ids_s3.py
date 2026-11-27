"""silver display ids: a new survey continues after the numbers other surveys of the same date have in S3 silver."""
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "silver"))

from common import display_ids  # noqa: E402

ROOT = "s3://b/DHMS/NSV/Silver"
OTHER = "DHMS/NSV/Silver/Zone=B/Phase=2/Cycle=1/UPC=N_08009_04002_GJ/dlm_kpi_new/master_roughness_parent.parquet"
OWN = "DHMS/NSV/Silver/Zone=B/Phase=2/Cycle=1/UPC=N_08009_05001_GJ/dlm_kpi_new/master_pothole.parquet"


@pytest.fixture
def prj(tmp_path, monkeypatch):
    out_root = tmp_path / "silver"
    out_dir = out_root / "Zone=B" / "Phase=2" / "Cycle=1" / "UPC=N_08009_05001_GJ"
    out_dir.mkdir(parents=True)
    monkeypatch.setenv("SILVER_S3_ROOT", ROOT)
    monkeypatch.delenv("DISPLAY_ID_S3_CHECK", raising=False)
    display_ids._S3_IDS.clear()
    return SimpleNamespace(out_dir=str(out_dir), output_root=str(out_root), upc="N_08009_05001_GJ")


def fake_s3(monkeypatch, files):
    monkeypatch.setattr(display_ids, "_s3_display_ids", lambda root, folder, tables: files)


def test_counter_continues_after_other_surveys_in_s3(prj, monkeypatch):
    fake_s3(monkeypatch, {OTHER: ["D/060426/N/00001", "D/060426/N/00229", "D/060426/N/00229/0003",
                                  "D/070426/N/00900"],
                          OWN: ["D/060426/N/05000"]})                       # its own earlier run: left out
    assert display_ids._max_counter_other_upcs(prj, "060426", ["master_roughness_parent"], "dlm_kpi_new") == 229
    assert display_ids._max_counter_other_upcs(prj, "010101", ["master_roughness_parent"], "dlm_kpi_new") == 0


def test_local_folder_still_counts(prj, monkeypatch):
    fake_s3(monkeypatch, {OTHER: ["D/060426/N/00010"]})
    other = Path(prj.output_root) / "Zone=A" / "Phase=1" / "Cycle=1" / "UPC=N_1" / "dlm_kpi_new"
    other.mkdir(parents=True)
    pd.DataFrame({"upc_code": ["N_1"], "display_defect_id": ["D/060426/N/00042"]}).to_parquet(
        other / "master_pothole.parquet")
    assert display_ids._max_counter_other_upcs(prj, "060426", ["master_pothole"], "dlm_kpi_new") == 42


def test_s3_not_readable_stops_and_switch_off(prj, monkeypatch):
    def boom(root, folder, tables):
        raise PermissionError("AccessDenied")
    monkeypatch.setattr(display_ids, "_s3_display_ids", boom)
    with pytest.raises(RuntimeError, match="cannot read the ids"):
        display_ids._max_counter_other_upcs(prj, "060426", ["master_pothole"], "dlm_kpi_new")
    monkeypatch.setenv("DISPLAY_ID_S3_CHECK", "0")
    assert display_ids._max_counter_other_upcs(prj, "060426", ["master_pothole"], "dlm_kpi_new") == 0
    monkeypatch.delenv("DISPLAY_ID_S3_CHECK")
    monkeypatch.delenv("SILVER_S3_ROOT")                                   # local runs: no S3 look
    assert display_ids._max_counter_other_upcs(prj, "060426", ["master_pothole"], "dlm_kpi_new") == 0
