"""The DB export (DIC_Staging): one folder per daily pull, plus Latest/.

    <DIC_ROOT>/year=2026/month=11/day=26/udm_defect_reference.csv     (or .parquet; or part files in a
                                                                      udm_defect_reference/ folder of the day)
    <DIC_ROOT>/Latest/<table>.csv|.parquet

pick_snapshots: the newest day that has both udm_defect_reference and dlm_defect_case ("latest") and the newest day
before it that has both ("previous"; the immediately previous pull available, whatever the gap). Both days can be
given instead (--latest-date / --previous-date), e.g. to process a day a failed run missed. The dated folders are used
only for udm_defect_reference / dlm_defect_case (comparison + status of the latest day); every other DB table
(master_upc_project, survey_data_map, ...) is read from <DIC_ROOT>/Latest/ only (load_db_tables).

Every DB table is read with all columns as text and empty cells as null, like ris_kpi/common/db_tables.py.
"""
import logging
import re
from dataclasses import dataclass
from datetime import date

import pandas as pd

from status_update import config
from status_update.storage import join, list_dirs, list_files, name_of, read_text_table, suffix_of

log = logging.getLogger(__name__)

TABLE_SUFFIXES = (".parquet", ".csv")
COMPARED_TABLES = [config.UDM_DEFECT_REFERENCE, config.DLM_DEFECT_CASE]

# Columns used from each table; a table without them stops the run before any work is done
REQUIRED_COLUMNS = {
    "udm_defect_reference": ["id", "defect_collection", "defect_id", "upc", "defect_status"],
    "dlm_defect_case": ["id", "defect_reference_id", "current_status", "upc"],
    "master_upc_project": ["id", "upc", "length_km", "lane_length_km"],
    "master_survey_system": ["id", "system_code"],
    "survey_data_map": ["survey_schedule_id", "upc", "phase", "cycle"],
    "master_defect_type": ["id", "survey_system", "asset_type", "defect_name"],
    "wfm_survey_schedule": ["id", "upc", "current_status"],   # SURVEY_ID_FALLBACK only
}

_PART = re.compile(r"^(year|month|day)=(\d+)$")


def _value(folder, level):
    m = _PART.match(folder)
    return int(m.group(2)) if m and m.group(1) == level else None


def snapshot_days(root):
    """{date: folder} of every <root>/year=/month=/day= folder (zero padding optional)."""
    days = {}
    for y in list_dirs(root):
        year = _value(y, "year")
        if year is None:
            continue
        for m in list_dirs(join(root, y)):
            month = _value(m, "month")
            if month is None:
                continue
            for d in list_dirs(join(root, y, m)):
                day = _value(d, "day")
                if day is None:
                    continue
                try:
                    days[date(year, month, day)] = join(root, y, m, d)
                except ValueError:
                    log.warning("Not a date folder: %s", join(root, y, m, d))
    return days


def table_files(folder, table):
    """[<folder>/<table>.parquet|.csv] (parquet first), else the part files of <folder>/<table>/; [] when none."""
    files = list_files(folder)
    for suffix in TABLE_SUFFIXES:
        direct = [f for f in files if name_of(f) == f"{table}{suffix}" and _parent_name(f, folder) == ""]
        if direct:
            return direct
    sub = str(join(folder, table)).replace("\\", "/") + "/"
    parts = [f for f in files if str(f).replace("\\", "/").startswith(sub) and suffix_of(f) in TABLE_SUFFIXES]
    for suffix in TABLE_SUFFIXES:  # one format only
        chosen = [f for f in parts if suffix_of(f) == suffix]
        if chosen:
            return chosen
    return []


def _parent_name(location, folder):
    """Sub-path between folder and the file name ('' when the file is directly in folder)."""
    rel = str(location).replace("\\", "/")[len(str(folder).replace("\\", "/")):].strip("/")
    return rel.rsplit("/", 1)[0] if "/" in rel else ""


def read_table(folder, table, files=None):
    """<table> of one export folder as text (None when the folder does not have it); required columns checked."""
    files = table_files(folder, table) if files is None else files
    if not files:
        return None
    df = pd.concat([read_text_table(f) for f in files], ignore_index=True)
    missing = [c for c in REQUIRED_COLUMNS.get(table, []) if c not in df.columns]
    if missing:
        raise ValueError(f"{table} ({files[0]}) has no column(s) {', '.join(missing)}")
    log.info("Loaded %-22s %9s rows from %s%s", table, f"{len(df):,}", files[0],
             f" (+{len(files) - 1} part files)" if len(files) > 1 else "")
    return df


@dataclass
class Snapshot:
    day: date
    folder: object
    files: dict      # table -> files


def _snapshot(day, folder):
    files = {table: table_files(folder, table) for table in COMPARED_TABLES}
    return Snapshot(day, folder, files) if all(files.values()) else None


def pick_snapshots(root, latest_day=None, previous_day=None):
    """(latest, previous) Snapshot: the two newest days with both compared tables, or the given days."""
    days = snapshot_days(root)
    if not days:
        raise FileNotFoundError(f"No year=/month=/day= folder under {root}")
    log.info("DB export days under %s: %s (newest %s)", root, len(days), max(days))

    def given(day, label):
        if day not in days:
            raise FileNotFoundError(f"{label} {day}: no folder for that day under {root}")
        snap = _snapshot(day, days[day])
        if snap is None:
            raise FileNotFoundError(f"{label} {day}: {days[day]} does not have both {COMPARED_TABLES}")
        return snap

    def newest(before=None):
        for day in sorted(days, reverse=True):
            if before is not None and day >= before:
                continue
            snap = _snapshot(day, days[day])
            if snap is not None:
                return snap
            log.warning("%s: no %s — day skipped", days[day], " / ".join(COMPARED_TABLES))
        return None

    latest = given(latest_day, "--latest-date") if latest_day else newest()
    if latest is None:
        raise FileNotFoundError(f"No day under {root} has both {COMPARED_TABLES}")
    previous = given(previous_day, "--previous-date") if previous_day else newest(before=latest.day)
    if previous is None:
        raise FileNotFoundError(f"No day before {latest.day} under {root} has both {COMPARED_TABLES} — nothing to "
                                "compare with")
    if previous.day >= latest.day:
        raise ValueError(f"--previous-date {previous.day} must be before the latest date {latest.day}")
    log.info("Comparing the pull of %s (%s) with the pull of %s (%s)", latest.day, latest.folder, previous.day,
             previous.folder)
    return latest, previous


def load_db_tables(root, tables):
    """{table: DataFrame} from <root>/Latest/ (the DB tables other than the two compared ones)."""
    latest = join(root, config.DIC_LATEST_FOLDER)
    out = {}
    for table in tables:
        df = read_table(latest, table)
        if df is None:
            raise FileNotFoundError(f"No {table}.(parquet|csv) in {latest}")
        out[table] = df
    return out
