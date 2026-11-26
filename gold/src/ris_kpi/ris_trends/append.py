"""Only new surveys: which silver surveys are not in ris_trends_data yet, and appending their rows.

A survey is one upc + phase + cycle + detected_on combination (SURVEY_COLUMNS). The surveys already in the newest
saved ris_trends_data (common/storage.py: latest year=/month=/day= folder, else latest/) are skipped; the silver
tables are filtered to the other ("new") surveys before any calculation, the RIS trends are built for those only and
their rows are appended to the table (created_at = updated_at = this run). Rows already in it are never changed (no
update, unlike ris_view_data). The result is saved under the run date (year=/month=/day=) and copied to latest/.

    no saved ris_trends_data -> every silver survey is new (the table is built from scratch)
    no new survey            -> nothing to calculate, nothing is written (also when every new survey's rows are
                                dropped for a missing id, e.g. no survey_data_map row yet: tried again next run)

Every ris_trends calculation is per survey (kpi_metrics / categories GRAIN, pci CHAINAGE_GRAIN), so filtering the
silver tables first gives the same rows as building every survey and keeping the new ones.
"""
import logging
from datetime import datetime

import pandas as pd

from ris_kpi.common.db_ids import survey_key
from ris_kpi.common.storage import find_latest, read_text

log = logging.getLogger(__name__)

# ris_trends_data columns of the survey grain
SURVEY_COLUMNS = ["upc", "phase", "cycle", "defect_detected_on"]
# silver date column per table (the others use detected_on)
SILVER_DATE_COLUMNS = {"master_chainage": "defect_detected_on"}
# one survey has 3 rows per pavement type, told apart by roughness_irc_category (Good / Poor / Fair)
ROW_COLUMNS = SURVEY_COLUMNS + ["pavement_type", "roughness_irc_category"]


def load_existing(root, name):
    """The newest saved <name> under root as text (written back unchanged), or None when there is none."""
    location = find_latest(root, name)
    if location is None:
        log.info("No saved %s in %s — every survey is new", name, root)
        return None
    df = read_text(location)
    missing = [c for c in SURVEY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{location} has no {missing} column(s) (written by an older version) — delete it so it is "
                         "rebuilt for every survey")
    log.info("Saved %s (%s) — %s rows, %s surveys", name, location, f"{len(df):,}",
             f"{survey_key(df, SURVEY_COLUMNS).nunique():,}")
    return df


def keep_new_surveys(tables, existing):
    """Silver tables filtered to the surveys not in `existing` (all rows when existing is None).
    Returns (tables, number of new surveys)."""
    surveys = set() if existing is None else set(survey_key(existing, SURVEY_COLUMNS))
    out, new = {}, set()
    for name, df in tables.items():
        date_col = SILVER_DATE_COLUMNS.get(name, "detected_on")
        key = survey_key(df, ["upc", "phase", "cycle", date_col])
        keep = ~key.isin(surveys).fillna(False).astype(bool)
        out[name] = df[keep.to_numpy()]
        new |= set(key[keep].dropna())
        log.info("  %-30s new surveys: %s of %s rows", name, f"{int(keep.sum()):,}", f"{len(df):,}")
    if new:
        log.info("New surveys (not in ris_trends_data): %s\n  %s", len(new), "\n  ".join(sorted(new)))
    return out, len(new)


def _row_key(df):
    """survey key + pavement_type + roughness_irc_category: one ris_trends_data row of a survey."""
    extra = [df[c].astype("string").str.strip().fillna("") for c in ROW_COLUMNS[len(SURVEY_COLUMNS):]]
    return survey_key(df, SURVEY_COLUMNS).str.cat(extra, sep="|")


def surveys_in(tables):
    """Number of surveys (upc + phase + cycle + date) in the silver tables."""
    keys = set()
    for name, df in tables.items():
        keys |= set(survey_key(df, ["upc", "phase", "cycle", SILVER_DATE_COLUMNS.get(name, "detected_on")]).dropna())
    return len(keys)


def refresh_ris_trends_data(existing, created, run_time=None):
    """existing ris_trends_data with the surveys of `created` replaced by their new rows: a new row that matches a
    saved row (ROW_COLUMNS) keeps its id and created_at (the DB push updates it instead of adding a second row);
    updated_at = this run. Surveys not in `created` stay as they are."""
    run_time = run_time or datetime.now()
    created = created.assign(created_at=run_time, updated_at=run_time)
    if existing is None or existing.empty:
        log.info("ris_trends_data: %s rows, file built from scratch", f"{len(created):,}")
        return created.reset_index(drop=True)
    surveys = set(survey_key(created, SURVEY_COLUMNS))
    redo = survey_key(existing, SURVEY_COLUMNS).isin(surveys).to_numpy()
    old = existing[redo]
    if not old.empty and all(c in old.columns for c in ROW_COLUMNS):
        saved = dict(zip(_row_key(old), zip(old["id"], old["created_at"])))
        keys = _row_key(created)
        hit = keys.isin(set(saved)).to_numpy()
        created["created_at"] = created["created_at"].astype(object)      # saved text kept as written
        created.loc[hit, "id"] = [saved[k][0] for k in keys[hit]]
        created.loc[hit, "created_at"] = [saved[k][1] for k in keys[hit]]
    n_kept = int(created["id"].isin(set(old["id"])).sum()) if "id" in old.columns else 0
    columns = list(created.columns) + [c for c in existing.columns if c not in created.columns]
    log.info("ris_trends_data: %s surveys recalculated (%s saved rows replaced, %s rows written, %s keep their id); "
             "%s rows of other surveys unchanged", len(surveys), f"{int(redo.sum()):,}", f"{len(created):,}",
             f"{n_kept:,}", f"{int((~redo).sum()):,}")
    return pd.concat([existing[~redo].reindex(columns=columns), created.reindex(columns=columns)], ignore_index=True)


def append_ris_trends_data(existing, created, run_time=None):
    """existing ris_trends_data (None = no file) + the new surveys' rows (created_at = updated_at = this run)."""
    run_time = run_time or datetime.now()
    created = created.assign(created_at=run_time, updated_at=run_time)
    if existing is None or existing.empty:
        log.info("ris_trends_data: %s rows inserted, file built from scratch", f"{len(created):,}")
        return created.reset_index(drop=True)
    overlap = survey_key(created, SURVEY_COLUMNS).isin(set(survey_key(existing, SURVEY_COLUMNS)))
    if overlap.any():
        raise AssertionError(f"{int(overlap.sum())} new rows belong to surveys already in ris_trends_data")
    columns = list(created.columns) + [c for c in existing.columns if c not in created.columns]
    log.info("ris_trends_data: %s existing rows kept, %s rows inserted", f"{len(existing):,}", f"{len(created):,}")
    return pd.concat([existing.reindex(columns=columns), created.reindex(columns=columns)], ignore_index=True)
