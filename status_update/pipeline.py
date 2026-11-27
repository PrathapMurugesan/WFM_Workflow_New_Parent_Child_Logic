"""The daily KPI status update, start to end (run.py calls run()).

 1. DB pulls: the newest day under DIC_ROOT with udm_defect_reference + dlm_defect_case, and the newest day before it
    (dic.pick_snapshots)
 2. Changed UPCs: new / removed / changed rows of both tables between the two pulls (changes.changed_upcs)
 3. Surveys: upc + phase + cycle of RIS_INPUT_DIR ris_view_data / ris_trends_data whose upc changed
    (surveys.matched_surveys)
 4. Silver: their folders, every Zone=* tried (surveys.find_silver_folders), the tables read and stacked (silver.py)
 5. Gold, built fresh for those surveys (calculate.py): silver tables with status -> intermediate_10m_data ->
    ris_view_data; ris_trends_data
 6. Output: every table to OUTPUT_DIR/<table>.csv, overwriting the previous run's files (output.py)

No changed UPC / no matching survey / no silver folder: only the two reports are written and the previous run's
tables are removed from OUTPUT_DIR (so they are never taken for this run's result).
"""
import logging
import time
from datetime import datetime

import pandas as pd

from status_update import calculate, changes, config, dic, output, silver, surveys

log = logging.getLogger("status_update")


def run(settings, latest_date=None, previous_date=None, dry_run=False, run_time=None):
    """run_source with the gold code's survey_id placeholder switched as the source needs (settings.survey_id_placeholder:
    on for the NSV downloads surveys of Latest_v2, which upc_flow gave placeholders; off for the WFM surveys)."""
    from ris_kpi import config as gold_config
    before = gold_config.SURVEY_ID_PLACEHOLDER
    gold_config.SURVEY_ID_PLACEHOLDER = bool(getattr(settings, "survey_id_placeholder", False))
    try:
        return run_source(settings, latest_date, previous_date, dry_run, run_time)
    finally:
        gold_config.SURVEY_ID_PLACEHOLDER = before


def run_source(settings, latest_date=None, previous_date=None, dry_run=False, run_time=None):
    """Returns {"changed": changed_upcs report, "surveys": updated_surveys report, "tables": {name: DataFrame}}."""
    run_time = run_time or datetime.now()
    start = time.time()
    log.info("Settings: %s", settings.describe())

    # --- 1. The two newest DB pulls, 2. changed UPCs ---
    latest, previous = dic.pick_snapshots(settings.dic_root, latest_date, previous_date)
    udm_latest = dic.read_table(latest.folder, config.UDM_DEFECT_REFERENCE, latest.files[config.UDM_DEFECT_REFERENCE])
    case_latest = dic.read_table(latest.folder, config.DLM_DEFECT_CASE, latest.files[config.DLM_DEFECT_CASE])
    changed = changes.changed_upcs(
        udm_latest, dic.read_table(previous.folder, config.UDM_DEFECT_REFERENCE,
                                   previous.files[config.UDM_DEFECT_REFERENCE]),
        case_latest, dic.read_table(previous.folder, config.DLM_DEFECT_CASE, previous.files[config.DLM_DEFECT_CASE]))

    # --- 3. Surveys of the RIS tables with a changed UPC, 4. their silver folders ---
    folders = pd.DataFrame(columns=surveys.KEYS + ["zone", "folder", "files", "in_tables"])
    if changed.empty:
        log.info("No defect status change between %s and %s", previous.day, latest.day)
        changed["in_ris_data"] = pd.Series(dtype=bool)
    else:
        matched = surveys.matched_surveys(surveys.ris_surveys(settings.ris_input_dir), changed)
        if len(matched):
            folders = surveys.find_silver_folders(settings.silver_root, matched, settings.silver_subfolder)
            folders = folders.merge(matched, on=surveys.KEYS, how="left")

    # --- 5. Gold for those surveys ---
    tables = {}
    usable = folders[folders["zone"].notna()]
    if len(usable):
        silver_tables = silver.load_tables(usable)
        db_tables = dic.load_db_tables(settings.dic_root, calculate.db_tables_needed(settings.survey_id_fallback))
        status = calculate.silver_with_status(silver_tables, udm_latest, case_latest)
        intermediate = calculate.intermediate(status, silver_tables, db_tables["master_upc_project"])
        tables = {**status,
                  config.INTERMEDIATE_NAME: intermediate,
                  config.RIS_VIEW_NAME: calculate.ris_view(status, intermediate, db_tables, settings, run_time),
                  config.RIS_TRENDS_NAME: calculate.ris_trends(silver_tables, db_tables, settings, run_time)}
    else:
        log.info("No survey to recalculate")

    # --- 6. Reports + tables -> OUTPUT_DIR ---
    reports = output.reports(changed, folders, tables, latest.day, previous.day, run_time)
    if dry_run:
        log.info("DRY RUN — nothing written to %s. Would write: %s", settings.output_dir,
                 ", ".join(f"{name} ({len(df):,} rows)" for name, df in {**reports, **tables}.items()))
    else:
        output.write(settings.output_dir, reports, tables)
    log.info("KPI status update done in %.1fs — %s UPC(s) changed, %s survey folder(s) recalculated%s",
             time.time() - start, len(changed), len(usable), " (dry run)" if dry_run else "")
    return {"changed": reports[config.CHANGED_UPCS_NAME], "surveys": reports[config.UPDATED_SURVEYS_NAME],
            "tables": tables}
