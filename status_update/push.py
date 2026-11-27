"""The rebuilt ris_view_data / ris_trends_data of a pushed source (STATUS_PUSH, default wfm) -> the DB, then -> the
source's gold files.

DB (both tables in ONE transaction, committed only when everything succeeds):
  1. DB rows of the rebuilt surveys: same upc_id + survey_id + survey_source_id (SCOPE), read from the live DB.
  2. ids: a rebuilt row whose key matches a DB row takes that row's id / created_at -> UPDATE; the others keep their
     new id -> INSERT. Key = the one of `ris-kpi sync-db-ids` (gold/src/ris_kpi/tools/db_ids_sync.py TABLES):
     ris_view   survey_source_id, upc_id, defect_detected_on, survey_id, defect_type_id, status
     ris_trends upc_id, survey_id, survey_source_id, defect_detected_on, pavement_type, the 4 category columns
  3. DB rows of those surveys that no rebuilt row matches -> DELETE (e.g. the 'open' row of a defect type whose cases
     all closed, or a duplicate row; push_gold never deletes, so they would stay forever).
  4. Before anything is written, push_gold's checks run on the rows (columns in both, types, NOT NULL, foreign keys):
     any problem -> nothing written.

Gold files (after the commit, write_back): in <gold folder>/<table>.csv and, for a .../Latest folder, the newest dated
copy (the one the next gold run reads), the rows of the pushed surveys are replaced by the pushed rows (with their DB
ids). So the next gold run, sync-db-ids and push_gold start from what the DB has.
"""
import importlib.util
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

from status_update import config

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SCOPE = ["upc_id", "survey_id", "survey_source_id"]
# output table -> (push_gold / sync-db-ids name, DB table)
TABLES = {config.RIS_VIEW_NAME: ("ris_view", "ris_view_kpi"), config.RIS_TRENDS_NAME: ("ris_trends", "ris_trends_kpi")}


def _key_parts(df):
    return [df[c].astype("string").fillna("").str.strip().str.lower() for c in SCOPE]


def scope_triples(df):
    """Sorted (upc_id, survey_id, survey_source_id) of the rows (lower case; rows missing one of them left out)."""
    if df is None or df.empty or any(c not in df.columns for c in SCOPE):
        return []
    parts = pd.concat(_key_parts(df), axis=1)
    parts = parts[(parts != "").all(axis=1)]
    return sorted(set(map(tuple, parts.to_numpy().tolist())))


def in_scope(df, triples):
    """Boolean mask: rows of df whose (upc_id, survey_id, survey_source_id) is one of triples."""
    if df.empty or not triples or any(c not in df.columns for c in SCOPE):
        return pd.Series(False, index=df.index)
    keys = pd.Series(list(zip(*_key_parts(df))), index=df.index)
    return keys.isin(set(triples))


def plan_ids(new, db_rows, name):
    """(new rows with the DB id / created_at of the matching DB rows, [DB ids to delete], copy report).
    name: 'ris_view' / 'ris_trends'."""
    from ris_kpi.tools.db_ids_sync import TABLES as SYNC_TABLES
    from ris_kpi.tools.db_ids_sync import copy_db_ids
    new = new.copy()
    for col in ("id", "created_at"):
        new[col] = new[col].astype("string")
    if db_rows is None or db_rows.empty:
        return new, [], {"matched": 0, "not_in_db": len(new), "only_in_db": 0}
    out, report = copy_db_ids(new, db_rows, SYNC_TABLES[name]["key"], f"{name} (status update)")
    kept = set(out["id"].astype("string").str.lower())
    db_ids = db_rows["id"].astype("string")
    stale = db_ids[~db_ids.str.lower().isin(kept)].tolist()
    return out, stale, report


# --- DB -----------------------------------------------------------------------------------------------------------

def _push_gold():
    """push_gold/push_to_db.py as a module (its column / type / FK checks and write_table)."""
    spec = importlib.util.spec_from_file_location("push_to_db", ROOT / "push_gold" / "push_to_db.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def db_env(db_name=None):
    """DB_* of the repository .env into os.environ (password from .env or Secrets Manager, as run.py's pushes)."""
    from pipeline.settings import load_settings, resolve_db_password
    s = load_settings(db_name)
    problem = resolve_db_password(s)
    if problem:
        raise RuntimeError(f"DB password: {problem}")
    env = s.stage_env()
    for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA"):
        if env.get(key):
            os.environ[key] = str(env[key])
    return os.environ.get("DB_SCHEMA") or "public"


def fetch_db_rows(cur, schema, table, triples):
    """Every column (as text) of the DB rows of the surveys in triples."""
    from psycopg2 import sql
    if not triples:
        return pd.DataFrame()
    u, s, ss = (list(x) for x in zip(*triples))
    cur.execute(sql.SQL("SELECT t.* FROM {}.{} t JOIN unnest(%s::text[], %s::text[], %s::text[]) AS k(u, s, ss) "
                        "ON lower(t.upc_id::text) = k.u AND lower(t.survey_id::text) = k.s "
                        "AND lower(t.survey_source_id::text) = k.ss").format(sql.Identifier(schema),
                                                                              sql.Identifier(table)), (u, s, ss))
    cols = [d[0] for d in cur.description]
    rows = [[None if v is None else str(v) for v in r] for r in cur.fetchall()]
    return pd.DataFrame(rows, columns=cols, dtype="string")


def push(tables, dry_run=False, db_name=None):
    """{output table: pushed rows (with DB ids)} - empty when there was nothing to push. Raises when the DB rejects
    anything (nothing written)."""
    import psycopg2
    from psycopg2 import sql
    pg = _push_gold()
    schema = db_env(db_name)
    settings = pg.db_settings()
    log.info("DB push%s: %s@%s/%s schema %s", " (DRY RUN)" if dry_run else "", settings["user"], settings["host"],
             settings["dbname"], schema)
    conn = psycopg2.connect(connect_timeout=15, application_name="kpi_status_update", **settings)
    plans, pushed = [], {}
    try:
        with conn.cursor() as cur, tempfile.TemporaryDirectory(prefix="status_update_") as tmp:
            for out_name, (name, db_table) in TABLES.items():
                df = tables.get(out_name)
                if df is None or df.empty:
                    log.info("%s: no rows - not pushed", out_name)
                    continue
                triples = scope_triples(df)
                db_rows = fetch_db_rows(cur, schema, db_table, triples)
                prepared, stale, report = plan_ids(df, db_rows, name)
                log.info("%s -> %s.%s: %s surveys, %s rebuilt rows, %s DB rows of those surveys; %s matched (UPDATE), "
                         "%s new (INSERT), %s DB rows no longer produced (DELETE)", out_name, schema, db_table,
                         len(triples), len(df), len(db_rows), report["matched"], report["not_in_db"], len(stale))
                path = Path(tmp) / f"{out_name}.csv"
                prepared.to_csv(path, index=False)
                plan = pg.plan_table(cur, name, pg.TABLES[name], schema, str(path))
                if plan is None:
                    raise RuntimeError(f"{db_table}: cannot be pushed (see the lines above)")
                plan.update(stale=stale, out_name=out_name)
                plans.append(plan)
                pushed[out_name] = prepared
            conn.rollback()                                       # the plan only read
            bad = [p["table"] for p in plans if not p["ok"]]
            if bad:
                raise RuntimeError(f"DB checks failed for {bad} - nothing written")
            if dry_run:
                log.info("DRY RUN - nothing written to the DB")
                return pushed
            for p in plans:
                deleted = 0
                if p["stale"]:
                    cur.execute(sql.SQL("DELETE FROM {}.{} WHERE id::text = ANY(%s)").format(
                        sql.Identifier(schema), sql.Identifier(p["table"])), (p["stale"],))
                    deleted = cur.rowcount
                updated, inserted = pg.write_table(cur, p)
                log.info("%s.%s: %s updated, %s inserted, %s deleted", schema, p["table"], updated, inserted, deleted)
            conn.commit()
            log.info("DB push committed")
            return pushed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- gold files ---------------------------------------------------------------------------------------------------

def gold_files(gold_dir, name):
    """The gold files of a source that hold <name>: <gold_dir>/<name>.csv and, when gold_dir is a .../Latest folder,
    the newest dated copy next to it (year=/month=/day=), as `ris-kpi sync-db-ids` updates them."""
    from ris_kpi.common.io import exists
    text = str(gold_dir).replace("\\", "/").rstrip("/")
    if text.endswith("/Latest"):
        from ris_kpi.tools.db_ids_sync import run_data_locations
        root = text[: -len("/Latest")] if text.startswith("s3://") else Path(gold_dir).parent
        return run_data_locations(root, name)
    location = f"{text}/{name}.csv" if text.startswith("s3://") else Path(gold_dir) / f"{name}.csv"
    return [location] if exists(location) else []


def write_back(gold_dir, pushed):
    """In every gold file of the pushed tables: rows of the pushed surveys replaced by the pushed rows."""
    from ris_kpi.common import storage as gold_storage
    from ris_kpi.tools.db_ids_sync import write_text
    for out_name, rows in pushed.items():
        triples = scope_triples(rows)
        for location in gold_files(gold_dir, out_name):
            saved = gold_storage.read_text(location, empty_as_null=False)
            drop = in_scope(saved, triples)
            new = rows.reindex(columns=saved.columns).astype("string").fillna("")
            extra = sorted(set(rows.columns) - set(saved.columns))
            out = pd.concat([saved[~drop], new], ignore_index=True)
            write_text(out, location)
            log.info("%s: %s rows of %s surveys replaced by %s pushed rows -> %s rows%s", location, int(drop.sum()),
                     len(triples), len(new), len(out), f" (columns not in the file: {extra})" if extra else "")
