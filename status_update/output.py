"""OUTPUT_DIR: one CSV per table, overwritten by every run (no dated copies).

    master_roughness_parent / master_rutting_parent / master_ravelling / master_pothole / master_cracking_as_per_ca
                            silver rows of the recalculated surveys + defect_reference_id / current_status / final_status
    intermediate_10m_data   ris_view_data   ris_trends_data       of the recalculated surveys
    changed_upcs            every UPC with a DB change: counts per change kind, in_ris_data, recalculated
    updated_surveys         the upc + phase + cycle recalculated (or skipped: no silver folder), zone, silver folder

The two reports are written on every run. When nothing was recalculated, the tables of the previous run are deleted
(only the file names above), so OUTPUT_DIR never holds an older run's tables next to this run's reports.
"""
import logging

import pandas as pd

from status_update import config
from status_update.storage import delete, exists, join, write_csv

log = logging.getLogger(__name__)


def reports(changed, folders, tables, latest_day, previous_day, run_time):
    """{changed_upcs, updated_surveys} DataFrames."""
    done = set(folders.loc[folders["zone"].notna(), "upc"])
    changed_report = changed.assign(recalculated=changed["upc"].isin(done), latest_pull=str(latest_day),
                                    previous_pull=str(previous_day), run_time=str(run_time))
    surveys_report = pd.DataFrame({
        "upc": folders["upc"], "phase": folders["phase"], "cycle": folders["cycle"], "zone": folders["zone"],
        "silver_folder": folders["folder"].astype("string"),
        "in_tables": folders["in_tables"] if "in_tables" in folders else pd.NA,
        "result": folders["zone"].notna().map({True: "recalculated", False: "no silver folder"}),
        "latest_pull": str(latest_day), "previous_pull": str(previous_day), "run_time": str(run_time)})
    return {config.CHANGED_UPCS_NAME: changed_report, config.UPDATED_SURVEYS_NAME: surveys_report}


def write(output_dir, report_tables, tables):
    for name, df in {**tables, **report_tables}.items():
        write_csv(df, output_dir, name)
    stale = [join(output_dir, f"{name}.csv") for name in config.OUTPUT_TABLES if name not in tables]
    for location in stale:
        if exists(location):
            delete(location)
            log.info("Deleted %s (previous run; nothing recalculated in this run)", location)
