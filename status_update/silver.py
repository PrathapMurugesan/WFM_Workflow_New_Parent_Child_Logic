"""Silver tables of the surveys to recalculate, read from their folders (surveys.find_silver_folders) and stacked.

As ris_kpi/common/silver.py: every column read as text, then the key columns normalised (upc '/' form, phase / cycle
'2' not '2.0', detected_on yyyy-mm-dd, text keys trimmed, pavement type spellings unified); columns missing in some
files stay empty (logged). A table is the file <table>.parquet / .csv / .xlsx inside the survey folder (any depth; one
format per folder, parquet first: silver writes each table as .parquet and .csv). A survey folder without a table is
logged; no file of a table at all stops the run, except for config.OPTIONAL_SILVER_TABLES (master_ravelling: an empty
table, as new-format surveys have none).
"""
import logging
from pathlib import Path

import pandas as pd

from status_update import config
from ris_kpi.common.keys import to_str_id, upc_slash
from status_update.storage import local_file, name_of

log = logging.getLogger(__name__)

SUFFIXES = (".parquet", ".csv", ".xlsx")
TEXT_KEYS = ("zone", "source_system", "lane", "threshold_name", "pavement_type", "defect_type")
# Columns of an optional silver table that no survey of the run has (ris_kpi common/silver.EMPTY_TABLE_COLUMNS)
EMPTY_TABLE_COLUMNS = ["id", "display_defect_id", "upc", "zone", "phase", "cycle", "source_system", "detected_on", "lane",
                       "threshold_name", "pavement_type", "parent_id", "breach_flag", "start_chainage", "end_chainage",
                       "metric_value", "threshold_value", "pci"]


def _read(location):
    """One silver file, every column as text (ris_kpi/common/silver._read)."""
    with local_file(location) as path:
        suffix = Path(path).suffix.lower()
        if suffix == ".parquet":
            return pd.read_parquet(path).astype("string")
        if suffix == ".xlsx":
            return pd.read_excel(path, dtype=str)
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)


def normalise(df):
    """Key columns in one form across CSV / parquet sources (ris_kpi/common/silver.normalise)."""
    df = df.copy()
    if "upc" in df.columns:
        df["upc"] = upc_slash(df["upc"].astype("string").str.strip())
    for col in ("phase", "cycle"):
        if col in df.columns:
            df[col] = to_str_id(df[col].astype("string").str.strip())
    for col in TEXT_KEYS:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    if "pavement_type" in df.columns:
        df["pavement_type"] = df["pavement_type"].replace(config.PAVEMENT_TYPE_ALIASES)
    if "detected_on" in df.columns:
        df["detected_on"] = df["detected_on"].astype("string").str.strip().str[:10]
    return df


def table_file(files, table, folder):
    """The file of <table> among a folder's files (parquet, else csv, else xlsx), None when there is none."""
    by_suffix = {}
    for f in files:
        for suffix in SUFFIXES:
            if name_of(f) == f"{table}{suffix}":
                by_suffix.setdefault(suffix, []).append(f)
    for suffix in SUFFIXES:
        if suffix in by_suffix:
            chosen = by_suffix[suffix]
            others = [s for s in by_suffix if s != suffix]
            if len(chosen) > 1:
                log.warning("%s: several %s files in %s — %s used", table, suffix, folder, chosen[0])
            elif others:
                log.info("%s: %s read, %s not (same table in the same folder)", table, name_of(chosen[0]), others)
            return chosen[0]
    return None


def load_tables(folders, tables=config.SILVER_TABLES):
    """{table: stacked, normalised DataFrame} from every folder of `folders` (surveys.find_silver_folders rows with a
    zone)."""
    out = {}
    folders = folders[folders["zone"].notna()]
    for table in tables:
        frames = []
        for row in folders.itertuples(index=False):
            location = table_file(row.files, table, row.folder)
            if location is None:
                log.warning("No %s in %s (that survey has no rows of this table)", table, row.folder)
                continue
            df = _read(location)
            log.info("Loaded %-30s rows: %9s  (%s)", table, f"{len(df):,}", location)
            frames.append((location, df))
        if not frames and table in config.OPTIONAL_SILVER_TABLES:
            log.warning("No %s in any survey folder (new-format surveys have none): read as an empty table", table)
            out[table] = pd.DataFrame(columns=EMPTY_TABLE_COLUMNS, dtype="string")
            continue
        if not frames:
            raise FileNotFoundError(f"No {table}.parquet / .csv / .xlsx in any survey folder: "
                                    + ", ".join(str(f) for f in folders["folder"]))
        all_cols = set().union(*(df.columns for _, df in frames))
        for location, df in frames:
            missing = sorted(all_cols - set(df.columns))
            if missing:
                log.warning("%s: %s has no %s (left empty)", table, location, missing)
        out[table] = normalise(pd.concat([df for _, df in frames], ignore_index=True))
    return out
