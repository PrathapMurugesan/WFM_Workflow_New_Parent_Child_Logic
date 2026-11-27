"""Settings of the KPI status update, read from the repository .env (one settings file for every stage).

An environment variable of the same name wins over the .env value; a key that is missing or empty in both gets the
default below. Every location is an S3 URI (EC2 runs) or a local folder (testing without AWS access).

    STATUS_DIC_ROOT        DB export: <root>/year=YYYY/month=MM/day=DD/<table>.csv|.parquet (one folder per daily pull)
                           and <root>/Latest/. Default: S3_DB_TABLES_DIR without its '/Latest'
    STATUS_SILVER_ROOT     silver tables: <root>/Zone=*/Phase=<p>/Cycle=<c>/UPC=<upc>/<STATUS_SILVER_SUBFOLDER>/.
                           Default: S3_SILVER_INPUT_DIR
    STATUS_SILVER_SUBFOLDER  folder inside the UPC folder (default dlm_kpi_new; empty = the UPC folder itself)
    DROP_ROWS_WITHOUT_IDS  True: ris_view_data / ris_trends_data rows with an empty upc_id / survey_id /
                           survey_source_id are dropped (logged); same key as the gold run
    SURVEY_ID_FALLBACK     True: several survey_schedule_ids for one upc + phase + cycle -> the one picked by
                           wfm_survey_schedule; same key as the gold run

Sources: the gold folders whose surveys are checked, each with its own output folder <gold folder>/status_update:

    wfm   STATUS_WFM_GOLD_DIR  default <S3_GOLD_OUTPUT_DIR>/Latest      surveys of the WFM cron (wfm_flow), pushed to
                                                                        the DB by push_gold -> the result is pushed too
    nsv   STATUS_NSV_GOLD_DIR  default UPC_FLOW_GOLD_DIR, else          surveys of the NSV downloads (upc_flow), never
                               <S3_GOLD_OUTPUT_DIR>/Latest_v2           pushed -> S3 only; survey_id placeholders on
    STATUS_SOURCES  sources that run (default wfm,nsv)     STATUS_PUSH  sources whose result goes to the DB (default wfm)

AWS credentials are not set here: boto3 finds them itself (the EC2 instance role, or `aws configure` locally).
"""
import os
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT_DIR / ".env"
GOLD_DEFAULT = "s3://dhms-new-stgae/DHMS/NSV/Gold"

DEFAULTS = {
    "STATUS_DIC_ROOT": "s3://dhms-new-stgae/NSV/DHMS/DIC_Staging",
    "STATUS_SILVER_ROOT": "s3://dhms-new-stgae/DHMS/NSV/Silver",
    "STATUS_SILVER_SUBFOLDER": "dlm_kpi_new",
    "DROP_ROWS_WITHOUT_IDS": "True",
    "SURVEY_ID_FALLBACK": "True",
    "STATUS_SOURCES": "wfm,nsv",
    "STATUS_PUSH": "wfm",
}
OUTPUT_SUBFOLDER = "status_update"          # <gold folder of the source>/status_update/
# source -> (key of its gold folder, survey_id placeholder for surveys without one)
SOURCES = {"wfm": ("STATUS_WFM_GOLD_DIR", False), "nsv": ("STATUS_NSV_GOLD_DIR", True)}


def _env(key):
    return (os.getenv(key) or "").strip()


def default_location(key):
    """Default of a location key, from the gold run's own keys when they are set."""
    gold = _env("S3_GOLD_OUTPUT_DIR").rstrip("/") or GOLD_DEFAULT
    if key == "STATUS_WFM_GOLD_DIR":
        return f"{gold}/Latest"
    if key == "STATUS_NSV_GOLD_DIR":
        return _env("UPC_FLOW_GOLD_DIR").rstrip("/") or f"{gold}/Latest_v2"
    if key == "STATUS_DIC_ROOT" and _env("S3_DB_TABLES_DIR"):
        db = _env("S3_DB_TABLES_DIR").rstrip("/")
        return db[: -len("/Latest")] if db.endswith("/Latest") else db
    if key == "STATUS_SILVER_ROOT" and _env("S3_SILVER_INPUT_DIR"):
        return _env("S3_SILVER_INPUT_DIR").rstrip("/")
    return DEFAULTS[key]


def names(key):
    """'wfm, nsv' -> ['wfm', 'nsv'] (unknown names stop the run)."""
    out = [x.strip().lower() for x in (_env(key) if key in os.environ else DEFAULTS[key]).split(",") if x.strip()]
    bad = [x for x in out if x not in SOURCES]
    if bad:
        raise ValueError(f"{key}: unknown source(s) {bad} (known: {list(SOURCES)})")
    return out


# --- Tables -------------------------------------------------------------------------------------------------------
# DB tables compared between the two newest daily pulls (changed UPCs) and used for the status
UDM_DEFECT_REFERENCE, DLM_DEFECT_CASE = "udm_defect_reference", "dlm_defect_case"
# DB export folder holding the newest copy of every table (used for a table missing in the dated folder)
DIC_LATEST_FOLDER = "Latest"
# RIS tables read from RIS_INPUT_DIR (only their upc / phase / cycle columns)
RIS_INPUT_TABLES = ["ris_view_data", "ris_trends_data"]

# Silver tables (file names without extension), as in ris_kpi/config.py (gold of wfm_new_pc_pipeline)
ROUGHNESS_TABLES = {"parent": "master_roughness_parent", "ca": "master_roughness_as_per_ca",
                    "child_10m": "master_roughness_child_10m"}
RUTTING_TABLES = {"parent": "master_rutting_parent", "ca": "master_rutting_as_per_ca",
                  "child_10m": "master_rutting_child_10m"}
CRACKING_TABLES = {"ca": "master_cracking_as_per_ca", "child_10m": "master_cracking_10m"}
POTHOLE_TABLE = "master_pothole"
RAVELLING_TABLE = "master_ravelling"
# Silver tables that get defect_reference_id / current_status / final_status, and their
# udm_defect_reference.defect_collection (ris_kpi RisView.STATUS_TABLES)
STATUS_TABLES = {
    ROUGHNESS_TABLES["parent"]: "roughness_parent",
    RUTTING_TABLES["parent"]: "rutting_parent",
    RAVELLING_TABLE: "ravelling_parent",
    POTHOLE_TABLE: "pothole_child_ca",
    CRACKING_TABLES["ca"]: "cracking_child_ca",
}
# Silver column matched to udm_defect_reference.defect_id (ris_kpi RisView.STATUS_KEY)
STATUS_KEY = "display_defect_id"
# Silver tables read only for intermediate_10m_data
INTERMEDIATE_SILVER_TABLES = [ROUGHNESS_TABLES["ca"], ROUGHNESS_TABLES["child_10m"], RUTTING_TABLES["ca"],
                              RUTTING_TABLES["child_10m"], CRACKING_TABLES["child_10m"]]
SILVER_TABLES = list(STATUS_TABLES) + INTERMEDIATE_SILVER_TABLES
# Silver tables a run may lack completely (ris_kpi config.OPTIONAL_SILVER_TABLES): new-format surveys (deliverables
# D1-D4) have no ravelling, so with only such surveys there is no master_ravelling file at all -> an empty table
OPTIONAL_SILVER_TABLES = [RAVELLING_TABLE]
# Rows used for the CA values ('As per CA'); 10m rows with this threshold_name only are dropped ('As per IRC')
CA_THRESHOLD_NAME = "As per CA"
IRC_ONLY_THRESHOLD = "As per IRC"
# Pavement type spellings unified when the silver tables are read
PAVEMENT_TYPE_ALIASES = {"Cement concrete": "Cement Concrete"}

# Output table names (<OUTPUT_DIR>/<name>.csv)
INTERMEDIATE_NAME = "intermediate_10m_data"
RIS_VIEW_NAME = "ris_view_data"
RIS_TRENDS_NAME = "ris_trends_data"
CHANGED_UPCS_NAME = "changed_upcs"            # every UPC with a DB change, and whether it was updated
UPDATED_SURVEYS_NAME = "updated_surveys"      # upc + phase + cycle recalculated, with their silver folder
OUTPUT_TABLES = list(STATUS_TABLES) + [INTERMEDIATE_NAME, RIS_VIEW_NAME, RIS_TRENDS_NAME]


def load_env(path=ENV_FILE):
    """KEY=VALUE lines of the .env file into os.environ (variables already set win; # comments, quotes allowed).
    Returns True when the file exists."""
    path = Path(path)
    if not path.is_file():
        return False
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)
    return True


def to_bool(name, value):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "y"):
        return True
    if text in ("0", "false", "no", "n"):
        return False
    raise ValueError(f"{name} must be True or False, not {value!r}")


def _location(value):
    """S3 URIs stay text (no trailing '/'); anything else is a local folder, relative to the kpi_status_updation
    folder."""
    value = value.strip()
    if value.startswith("s3://"):
        return value.rstrip("/")
    path = Path(value)
    return path if path.is_absolute() else ROOT_DIR / path


@dataclass(frozen=True)
class Settings:
    ris_input_dir: object
    dic_root: object
    silver_root: object
    silver_subfolder: str
    output_dir: object
    drop_rows_without_ids: bool
    survey_id_fallback: bool

    source: str = "wfm"
    push: bool = False
    survey_id_placeholder: bool = False

    @classmethod
    def from_env(cls, env_file=ENV_FILE, source="wfm", **overrides):
        """Settings of one source from the environment / .env (overrides: field name -> value, e.g. for tests)."""
        load_env(env_file)
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r} (known: {list(SOURCES)})")
        get = lambda key: _env(key) or default_location(key)                     # noqa: E731
        gold_key, placeholder = SOURCES[source]
        gold = _location(get(gold_key))
        subfolder = os.getenv("STATUS_SILVER_SUBFOLDER", DEFAULTS["STATUS_SILVER_SUBFOLDER"])   # empty is allowed
        settings = dict(
            ris_input_dir=gold,
            dic_root=_location(get("STATUS_DIC_ROOT")),
            silver_root=_location(get("STATUS_SILVER_ROOT")),
            silver_subfolder=subfolder.strip().strip("/"),
            output_dir=_location(f"{str(gold).rstrip('/')}/{OUTPUT_SUBFOLDER}") if isinstance(gold, str)
            else gold / OUTPUT_SUBFOLDER,
            drop_rows_without_ids=to_bool("DROP_ROWS_WITHOUT_IDS", get("DROP_ROWS_WITHOUT_IDS")),
            survey_id_fallback=to_bool("SURVEY_ID_FALLBACK", get("SURVEY_ID_FALLBACK")),
            source=source,
            push=source in names("STATUS_PUSH"),
            survey_id_placeholder=placeholder,
        )
        settings.update(overrides)
        return cls(**settings)

    def describe(self):
        return " | ".join(f"{k.upper()}={v}" for k, v in self.__dict__.items())
