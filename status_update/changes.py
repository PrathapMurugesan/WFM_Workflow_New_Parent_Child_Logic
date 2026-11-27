"""UPCs whose defect status may have changed between two daily DB pulls.

    udm_defect_reference (key id)   new id, removed id, or defect_status changed          -> its upc
    dlm_defect_case      (key id)   new case, removed case, or current_status /
                                    defect_reference_id changed                            -> its upc, else the upc of
                                                                                              its defect_reference_id
                                                                                              (udm_defect_reference)
The silver status comes from dlm_defect_case.current_status (through udm_defect_reference.id), so a case can change a
status while udm_defect_reference.defect_status stays the same: both tables are compared. A removed row counts too
(its silver rows lose their status). Values are compared as trimmed text (empty = null).
"""
import logging

import pandas as pd

from ris_kpi.common.keys import upc_slash

log = logging.getLogger(__name__)

UDM_COMPARED = ["defect_status"]
CASE_COMPARED = ["current_status", "defect_reference_id"]
COUNT_COLUMNS = ["udm_new", "udm_status_changed", "udm_removed", "case_new", "case_changed", "case_removed"]


def _text(series):
    return series.astype("string").str.strip().fillna("")


def _unique_ids(df, label):
    ids = _text(df["id"])
    df = df[(ids != "").to_numpy()].assign(id=ids[ids != ""])
    dups = df["id"].duplicated(keep="last")
    if dups.any():
        log.warning("%s: %s rows share an id — the last one kept", label, int(dups.sum()))
    return df[~dups.to_numpy()].set_index("id")


def compare(latest, previous, compared, label):
    """One row per changed id: id, change ('new' / 'removed' / 'changed'), side ('latest' / 'previous': where its
    other columns are read from)."""
    new, old = _unique_ids(latest, f"{label} (latest)"), _unique_ids(previous, f"{label} (previous)")
    added = new.index.difference(old.index)
    removed = old.index.difference(new.index)
    both = new.index.intersection(old.index)
    differs = pd.Series(False, index=both)
    for col in compared:
        differs |= (_text(new.loc[both, col]) != _text(old.loc[both, col])).to_numpy()
    changed = both[differs.to_numpy()]
    log.info("%s: %s new, %s removed, %s with a changed %s (of %s / %s ids)", label, f"{len(added):,}",
             f"{len(removed):,}", f"{len(changed):,}", " / ".join(compared), f"{len(new):,}", f"{len(old):,}")
    return pd.concat([pd.DataFrame({"id": added, "change": "new", "side": "latest"}),
                      pd.DataFrame({"id": removed, "change": "removed", "side": "previous"}),
                      pd.DataFrame({"id": changed, "change": "changed", "side": "latest"})], ignore_index=True)


def _by_id(table, column):
    """column of table indexed by the trimmed id (last row kept per id)."""
    t = table.assign(_id=_text(table["id"])).drop_duplicates("_id", keep="last")
    return _text(t[column]).set_axis(t["_id"].to_numpy())


def _lookup(changes, latest, previous, column):
    """For each changed id: column of that id in the latest table (new / changed) or the previous one (removed)."""
    out = pd.Series(pd.NA, index=changes.index, dtype="string")
    for side, table in (("latest", latest), ("previous", previous)):
        rows = (changes["side"] == side).to_numpy()
        out[rows] = changes.loc[rows, "id"].map(_by_id(table, column)).to_numpy()
    return out.replace("", pd.NA)


def changed_upcs(udm_latest, udm_previous, case_latest, case_previous):
    """One row per UPC with a change: upc + the counts of COUNT_COLUMNS. Changes whose UPC cannot be found are
    logged."""
    udm = compare(udm_latest, udm_previous, UDM_COMPARED, "udm_defect_reference")
    udm["upc"] = upc_slash(_lookup(udm, udm_latest, udm_previous, "upc"))
    udm["column"] = "udm_" + udm["change"].replace({"changed": "status_changed"})

    case = compare(case_latest, case_previous, CASE_COMPARED, "dlm_defect_case")
    case["upc"] = _lookup(case, case_latest, case_previous, "upc")
    no_upc = case["upc"].isna()
    if no_upc.any():  # the case's own upc is empty: the upc of its defect reference (latest, else previous pull)
        ref_id = _lookup(case[no_upc], case_latest, case_previous, "defect_reference_id")
        ref_upc = _by_id(udm_latest, "upc").combine_first(_by_id(udm_previous, "upc"))
        case.loc[no_upc, "upc"] = ref_id.map(ref_upc).to_numpy()
    case["upc"] = upc_slash(case["upc"].astype("string").replace("", pd.NA))
    case["column"] = "case_" + case["change"]

    changes = pd.concat([udm, case], ignore_index=True)
    lost = changes["upc"].isna()
    if lost.any():
        log.warning("%s changed row(s) without a upc (skipped):\n%s", int(lost.sum()),
                    changes.loc[lost, ["column", "id"]].head(20).to_string(index=False))
    changes = changes[~lost.to_numpy()]
    if changes.empty:
        return pd.DataFrame(columns=["upc"] + COUNT_COLUMNS)
    counts = (changes.groupby(["upc", "column"]).size().unstack(fill_value=0)
              .reindex(columns=COUNT_COLUMNS, fill_value=0).reset_index())
    counts.columns.name = None
    log.info("UPCs with a DB change: %s\n%s", len(counts), counts.to_string(index=False))
    return counts

