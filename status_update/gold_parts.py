"""The two ris_trends input steps of gold/src/ris_kpi/ris_trends/loaders.py without the file reading (the status update
has the silver tables in memory already)."""
import logging

from ris_kpi.config import RisTrends
from ris_kpi.ris_trends.loaders import filter_project_upcs

log = logging.getLogger(__name__)


def keep_pavement_types(df, name, keep_pavement_types=None):
    """Rows with a pavement_type the RIS trends use (RisTrends.KEEP_PAVEMENT_TYPES), as loaders.load_parent."""
    keep_types = keep_pavement_types or RisTrends.KEEP_PAVEMENT_TYPES
    keep = df["pavement_type"].isin(keep_types).fillna(False).astype(bool)
    if (~keep).any():
        dropped = df[~keep].assign(pavement_type=df.loc[~keep, "pavement_type"].fillna("(empty)"))
        log.warning("%s: %s rows dropped, pavement_type not in %s - these surveys get no ris_trends rows:\n%s",
                    name, f"{int((~keep).sum()):,}", keep_types,
                    dropped.groupby(["upc", "phase", "cycle", "pavement_type"], dropna=False).size()
                    .rename("rows").reset_index().to_string(index=False))
    kept = df[keep].reset_index(drop=True)
    log.info("%s: %s of %s rows kept (pavement_type in %s)", name, f"{len(kept):,}", f"{len(df):,}", keep_types)
    return kept


def trends_filter_project_upcs(tables, master_upc_project):
    """Rows of UPCs in master_upc_project only (loaders.filter_project_upcs, always on)."""
    return filter_project_upcs(tables, master_upc_project, True)
