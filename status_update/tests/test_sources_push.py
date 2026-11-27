"""Two sources (wfm pushed, nsv S3 only), the DB id / delete plan, and the gold write-back (no AWS, no DB)."""
import pandas as pd
import pytest

from status_update import config, pipeline, push
from status_update import run as cli

U, S, SS = "U1", "SV1", "SRC1"


def view_rows(ids, statuses, counts, created="2026-11-27 01:30:00", survey=(U, S, SS)):
    n = len(ids)
    return pd.DataFrame({"id": ids, "upc": ["N/9"] * n, "phase": ["1"] * n, "cycle": ["1"] * n,
                         "upc_id": [survey[0]] * n, "survey_id": [survey[1]] * n, "survey_source_id": [survey[2]] * n,
                         "defect_detected_on": ["2026-01-05"] * n, "defect_type_id": ["dt-pot"] * n,
                         "status": statuses, "total_defects_ca": counts, "created_at": [created] * n,
                         "updated_at": [created] * n})


def test_plan_ids_keeps_db_ids_and_deletes_rows_no_longer_produced():
    db = view_rows(["db-open", "db-dup"], ["open", "open"], ["1", "1"], created="2026-11-20 10:00:00")
    db = pd.concat([db, view_rows(["db-other"], ["open"], ["5"], survey=("U2", "SV2", SS))])
    new = view_rows(["new-open", "new-close"], ["open", "close"], ["0", "1"])
    # the DB holds the open row twice (ambiguous): neither is matched -> both deleted, the rebuilt open row inserted
    out, stale, report = push.plan_ids(new, db[db["upc_id"] == U], "ris_view")
    assert out["id"].tolist() == ["new-open", "new-close"] and sorted(stale) == ["db-dup", "db-open"]
    single = db[db["id"] != "db-dup"]
    out, stale, report = push.plan_ids(new, single[single["upc_id"] == U], "ris_view")
    assert out["id"].tolist() == ["db-open", "new-close"] and stale == []
    assert out.loc[0, "created_at"] == "2026-11-20 10:00:00" and out.loc[0, "total_defects_ca"] == "0"
    assert report["matched"] == 1
    out, stale, _ = push.plan_ids(new, pd.DataFrame(), "ris_view")
    assert out["id"].tolist() == ["new-open", "new-close"] and stale == []


def test_scope_triples_and_mask():
    df = pd.concat([view_rows(["a"], ["open"], ["1"]), view_rows(["b"], ["open"], ["1"], survey=("u1 ", "sv1", "")),
                    view_rows(["c"], ["open"], ["1"], survey=("U3", "SV3", "SRC3"))])
    assert push.scope_triples(df) == [("u1", "sv1", "src1"), ("u3", "sv3", "src3")]
    mask = push.in_scope(df.reset_index(drop=True), [("u1", "sv1", "src1")])
    assert mask.tolist() == [True, False, False]


def test_write_back_replaces_the_pushed_surveys_in_latest_and_the_newest_dated_copy(tmp_path):
    gold = tmp_path / "Gold"
    other = view_rows(["keep"], ["open"], ["3"], survey=("U2", "SV2", SS))
    old = pd.concat([view_rows(["db-open"], ["open"], ["1"]), other]).assign(extra_col="x")
    for folder in ("Latest", "year=2026/month=11/day=26", "year=2026/month=11/day=20"):
        (gold / folder).mkdir(parents=True)
        old.to_csv(gold / folder / "ris_view_data.csv", index=False)
    pushed = view_rows(["db-open", "new-close"], ["open", "close"], ["0", "1"])
    push.write_back(gold / "Latest", {config.RIS_VIEW_NAME: pushed})
    for folder, changed in (("Latest", True), ("year=2026/month=11/day=26", True), ("year=2026/month=11/day=20", False)):
        df = pd.read_csv(gold / folder / "ris_view_data.csv", dtype=str, keep_default_na=False)
        if changed:
            assert sorted(df["id"]) == ["db-open", "keep", "new-close"]
            assert df.set_index("id").loc["db-open", "total_defects_ca"] == "0"
            assert df.set_index("id").loc["keep", "extra_col"] == "x" and list(df.columns) == list(old.columns)
        else:
            assert sorted(df["id"]) == ["db-open", "keep"]                   # older history untouched


@pytest.fixture
def two_sources(monkeypatch, tmp_path):
    for key in ["STATUS_SOURCES", "STATUS_PUSH", "S3_GOLD_OUTPUT_DIR", "UPC_FLOW_GOLD_DIR", "STATUS_WFM_GOLD_DIR",
                "STATUS_NSV_GOLD_DIR"]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(config, "ENV_FILE", tmp_path / "none.env")
    monkeypatch.setenv("S3_GOLD_OUTPUT_DIR", "s3://b/Gold")
    calls = {"run": [], "push": [], "back": []}

    def fake_run(settings, latest, previous, dry_run=False, run_time=None):
        from ris_kpi import config as gold_config
        calls["run"].append((settings.source, str(settings.ris_input_dir), settings.survey_id_placeholder, dry_run))
        return {"changed": pd.DataFrame({"upc": ["N/9"]}),
                "surveys": pd.DataFrame({"result": ["recalculated"]}),
                "tables": {config.RIS_VIEW_NAME: view_rows(["n1"], ["open"], ["1"])}}

    monkeypatch.setattr(pipeline, "run", fake_run)
    monkeypatch.setattr(push, "push", lambda tables, dry_run=False, db_name=None:
                        calls["push"].append((sorted(tables), dry_run, db_name)) or tables)
    monkeypatch.setattr(push, "write_back", lambda gold_dir, pushed: calls["back"].append(str(gold_dir)))
    return calls


def test_both_sources_wfm_pushed_nsv_s3_only(two_sources):
    assert cli.main(["--db", "dhms_staging"]) == 0
    assert two_sources["run"] == [("wfm", "s3://b/Gold/Latest", False, False),
                                  ("nsv", "s3://b/Gold/Latest_v2", True, False)]
    assert two_sources["push"] == [([config.RIS_VIEW_NAME], False, "dhms_staging")]
    assert two_sources["back"] == ["s3://b/Gold/Latest"]


def test_dry_run_and_no_push(two_sources):
    assert cli.main(["--dry-run", "--source", "wfm"]) == 0
    assert two_sources["push"] == [([config.RIS_VIEW_NAME], True, None)] and two_sources["back"] == []
    assert cli.main(["--no-push"]) == 0
    assert len(two_sources["push"]) == 1


def test_a_failed_source_does_not_stop_the_other(two_sources, monkeypatch):
    def boom(tables, dry_run=False, db_name=None):
        raise RuntimeError("DB checks failed")
    monkeypatch.setattr(push, "push", boom)
    assert cli.main([]) == 1
    assert [r[0] for r in two_sources["run"]] == ["wfm", "nsv"]


def test_placeholder_switched_per_source_and_restored(monkeypatch):
    from ris_kpi import config as gold_config
    seen = []
    monkeypatch.setattr(pipeline, "run_source", lambda *a, **k: seen.append(gold_config.SURVEY_ID_PLACEHOLDER))
    before = gold_config.SURVEY_ID_PLACEHOLDER
    pipeline.run(config.Settings.from_env(source="nsv"))
    pipeline.run(config.Settings.from_env(source="wfm"))
    assert seen == [True, False] and gold_config.SURVEY_ID_PLACEHOLDER == before
