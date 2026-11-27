"""The gold calculations for the surveys to recalculate, built fresh (as for a new survey in ris_kpi), in memory.

    silver_with_status  ris_kpi ris_view step 1 (silver_status.py, UPDATE): every row of the status tables gets
                        defect_reference_id / current_status / final_status from udm_defect_reference /
                        dlm_defect_case of the latest pull (gold/status.py)
    intermediate        ris_kpi ris_view step 2 (intermediate.py): intermediate_10m_data from the status tables and
                        the silver as_per_ca / child_10m tables
    ris_view            ris_kpi ris_view step 3 (view.py): counts / affected lengths per status, DB ids;
                        id = new uuid4, created_at = updated_at = this run
    ris_trends          ris_kpi ris_trends (pipeline.py): KPIs + categories from the silver parent tables, DB ids;
                        id = new uuid4, created_at = updated_at = this run (no status column: same values as the gold
                        run unless the silver data changed)

Each step reads its inputs in the form the gold step reads them from its saved CSV (storage.as_text), so the values
are the same as a gold run of these surveys.
"""
import logging

from status_update import config
from ris_kpi.ris_trends import db_ids as trends_db_ids
from ris_kpi.ris_view import db_ids as view_db_ids
from ris_kpi.ris_trends.categories import build_categories
from ris_kpi.ris_view.loaders import num
from ris_kpi.ris_view.intermediate_10m import build_intermediate_10m, ca_only, cracking_10m, line_defect_10m
from ris_kpi.ris_trends.kpi_metrics import build_kpis, check_pci_sources
from ris_kpi.ris_view.project_master import filter_project_upcs, load_project_master
from ris_kpi.ris_view.status import add_status, case_status
from ris_kpi.ris_trends.pipeline import FINAL_COLUMNS as TRENDS_COLUMNS
from ris_kpi.ris_trends.pipeline import combine
from status_update.gold_parts import keep_pavement_types
from status_update.gold_parts import trends_filter_project_upcs
from ris_kpi.ris_view.view_table import build_view
from status_update.silver import normalise
from status_update.storage import as_text

log = logging.getLogger(__name__)

# defect_type -> silver status table whose breached rows are counted (ris_kpi ris_view/view.py COUNT_TABLES)
COUNT_TABLES = {
    "Roughness": config.ROUGHNESS_TABLES["parent"],
    "Rutting": config.RUTTING_TABLES["parent"],
    "Cracking": config.CRACKING_TABLES["ca"],
    "Pothole": config.POTHOLE_TABLE,
    "Ravelling": config.RAVELLING_TABLE,
}


def db_tables_needed(survey_id_fallback):
    """DB tables of the ris_view / ris_trends id columns (besides the two compared tables)."""
    return list(dict.fromkeys(view_db_ids.id_db_tables(survey_id_fallback)
                              + trends_db_ids.id_db_tables(survey_id_fallback)))


def silver_with_status(silver, udm_defect_reference, dlm_defect_case):
    """{status table: silver rows + defect_reference_id / current_status / final_status}."""
    cases = case_status(dlm_defect_case)
    return {table: add_status(silver[table], collection, udm_defect_reference, cases, silver_key=config.STATUS_KEY)
            for table, collection in config.STATUS_TABLES.items()}


def _saved_form(status):
    """Status tables as the later gold steps read them back (ris_view/loaders.load_status_table)."""
    return {name: normalise(as_text(df)) for name, df in status.items()}


def intermediate(status, silver, master_upc_project):
    """intermediate_10m_data (text, as saved) of the surveys."""
    rough, rut, crack = config.ROUGHNESS_TABLES, config.RUTTING_TABLES, config.CRACKING_TABLES
    saved = _saved_form(status)
    tables = {name: saved[name] for name in (rough["parent"], rut["parent"], crack["ca"])}
    for name in config.INTERMEDIATE_SILVER_TABLES:
        tables[name] = silver[name]
    tables = filter_project_upcs(tables, load_project_master(master_upc_project), True)

    child = lambda name: ca_only(tables[name], config.IRC_ONLY_THRESHOLD, name)
    rough_10m = line_defect_10m(tables[rough["parent"]], tables[rough["ca"]], child(rough["child_10m"]), "roughness")
    rut_10m = line_defect_10m(tables[rut["parent"]], tables[rut["ca"]], child(rut["child_10m"]), "rutting")
    crack_10m = cracking_10m(tables[crack["ca"]], child(crack["child_10m"]))
    return as_text(build_intermediate_10m(rough_10m, rut_10m, crack_10m)
                   .rename(columns={"detected_on": "defect_detected_on"}))


def _intermediate_in_memory(df):
    """intermediate_10m_data as ris_view/loaders.load_intermediate reads it."""
    df = df.rename(columns={"defect_detected_on": "detected_on"})
    for col in ("start_chainage", "end_chainage", "length_m"):
        df[col] = num(df[col])
    for col in [c for c in df.columns if c.endswith("_flag")]:
        df[col] = num(df[col]).fillna(0).astype(int)
    return df


def ris_view(status, intermediate_10m, db_tables, settings, run_time):
    """ris_view_data rows of the surveys (ris_view_db_ids.FINAL_COLUMNS)."""
    saved = _saved_form(status)
    tables = {defect: saved[name] for defect, name in COUNT_TABLES.items()}
    project = load_project_master(db_tables["master_upc_project"])
    tables = filter_project_upcs(tables, project, True)
    inter = filter_project_upcs({config.INTERMEDIATE_NAME: _intermediate_in_memory(intermediate_10m)}, project,
                                True)[config.INTERMEDIATE_NAME]
    rows = build_view(tables, inter, project)
    rows = view_db_ids.drop_rows_without_ids(view_db_ids.add_db_ids(rows, db_tables, settings.survey_id_fallback),
                                             drop=settings.drop_rows_without_ids)
    out = view_db_ids.finalize(rows.assign(created_at=run_time, updated_at=run_time))
    log.info("ris_view_data — %s rows × %s cols", f"{len(out):,}", out.shape[1])
    return out


def ris_trends(silver, db_tables, settings, run_time):
    """ris_trends_data rows of the surveys (TRENDS_COLUMNS + created_at / updated_at)."""
    rough_name, rut_name = config.ROUGHNESS_TABLES["parent"], config.RUTTING_TABLES["parent"]
    tables = {name: keep_pavement_types(silver[name], name) for name in (rough_name, rut_name)}
    tables = trends_filter_project_upcs(tables, db_tables["master_upc_project"])
    roughness, rutting = tables[rough_name], tables[rut_name]
    check_pci_sources(roughness, rutting)
    kpis = build_kpis(roughness, rutting, roughness, config.CA_THRESHOLD_NAME)   # PCI from master_roughness_parent
    rows = combine(kpis, build_categories(roughness, rutting, config.CA_THRESHOLD_NAME))
    rows = trends_db_ids.drop_rows_without_ids(trends_db_ids.add_db_ids(rows, db_tables, settings.survey_id_fallback),
                                               drop=settings.drop_rows_without_ids)
    out = rows[TRENDS_COLUMNS].assign(created_at=run_time, updated_at=run_time).reset_index(drop=True)
    log.info("ris_trends_data — %s rows × %s cols", f"{len(out):,}", out.shape[1])
    return out
