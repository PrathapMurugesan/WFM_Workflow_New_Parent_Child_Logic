"""Which surveys to recalculate, and where their silver tables are.

1. ris_surveys: every upc + phase + cycle in <RIS_INPUT_DIR>/ris_view_data.csv and ris_trends_data.csv (only those
   three columns are read; a missing file is logged, both missing stops the run).
2. matched_surveys: the ones whose upc had a DB change (changes.changed_upcs).
3. find_silver_folders: the zone is not known, so every <SILVER_ROOT>/Zone=*/ is tried:
       <SILVER_ROOT>/Zone=<z>/Phase=<phase>/Cycle=<cycle>/UPC=<upc with _>/<SILVER_SUBFOLDER>/
   a survey found in no zone is logged and skipped; found in several zones, every folder is used (logged).
"""
import logging

import pandas as pd

from status_update import config
from ris_kpi.common.keys import to_str_id, upc_slash
from status_update.storage import exists, join, list_dirs, list_files, read_text_table

log = logging.getLogger(__name__)

KEYS = ["upc", "phase", "cycle"]


def _keys(df):
    return pd.DataFrame({"upc": upc_slash(df["upc"].astype("string").str.strip()),
                         "phase": to_str_id(df["phase"].astype("string").str.strip()),
                         "cycle": to_str_id(df["cycle"].astype("string").str.strip())}).replace("", pd.NA)


def ris_surveys(ris_input_dir):
    """upc + phase + cycle of the RIS tables, with the tables each one is in (column 'in_tables')."""
    frames = []
    for name in config.RIS_INPUT_TABLES:
        location = join(ris_input_dir, f"{name}.csv")
        if not exists(location):
            log.warning("No %s — its surveys are not considered", location)
            continue
        keys = _keys(read_text_table(location, columns=KEYS)).dropna().drop_duplicates()
        log.info("%s: %s upc + phase + cycle combinations (%s)", name, len(keys), location)
        frames.append(keys.assign(in_tables=name))
    if not frames:
        raise FileNotFoundError(f"Neither {' nor '.join(config.RIS_INPUT_TABLES)}.csv in {ris_input_dir}")
    surveys = pd.concat(frames, ignore_index=True)
    return (surveys.groupby(KEYS, as_index=False)["in_tables"].agg(lambda s: ", ".join(sorted(set(s))))
            .sort_values(KEYS).reset_index(drop=True))


def matched_surveys(surveys, changed):
    """Surveys of the RIS tables whose upc is in changed (changes.changed_upcs); changed gets 'in_ris_data'."""
    upcs = set(changed["upc"])
    matched = surveys[surveys["upc"].isin(upcs)].reset_index(drop=True)
    changed["in_ris_data"] = changed["upc"].isin(set(surveys["upc"]))
    not_in = changed.loc[~changed["in_ris_data"], "upc"].tolist()
    if not_in:
        log.info("UPCs with a DB change but not in the RIS tables (skipped): %s", not_in)
    log.info("Surveys to recalculate: %s\n%s", len(matched), matched.to_string(index=False) if len(matched) else "")
    return matched


def survey_folder(silver_root, zone, upc, phase, cycle, subfolder):
    return join(silver_root, zone, f"Phase={phase}", f"Cycle={cycle}", f"UPC={upc.replace('/', '_')}", subfolder)


def find_silver_folders(silver_root, surveys, subfolder):
    """surveys + zone / folder / files (one row per folder found; surveys found nowhere get zone = None)."""
    zones = [z for z in list_dirs(silver_root) if z.startswith("Zone=")]
    log.info("Silver zones under %s: %s", silver_root, zones)
    rows = []
    for s in surveys.itertuples(index=False):
        found = []
        for zone in zones:
            folder = survey_folder(silver_root, zone, s.upc, s.phase, s.cycle, subfolder)
            files = list_files(folder)
            if files:
                found.append({"upc": s.upc, "phase": s.phase, "cycle": s.cycle, "zone": zone.split("=", 1)[1],
                              "folder": folder, "files": files})
        if not found:
            log.warning("No silver folder for upc %s phase %s cycle %s in any zone (%s) — skipped", s.upc, s.phase,
                        s.cycle, survey_folder(silver_root, "Zone=*", s.upc, s.phase, s.cycle, subfolder))
            rows.append({"upc": s.upc, "phase": s.phase, "cycle": s.cycle, "zone": None, "folder": None,
                         "files": []})
        elif len(found) > 1:
            log.warning("upc %s phase %s cycle %s is in %s zones — every folder is used: %s", s.upc, s.phase,
                        s.cycle, len(found), [f["zone"] for f in found])
        rows.extend(found)
    return pd.DataFrame(rows, columns=KEYS + ["zone", "folder", "files"])
