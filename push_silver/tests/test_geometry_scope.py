"""geometry_child has no UPC column: this survey's rows are found by the parent_id pattern."""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dhms_push.survey import scope_sql  # noqa: E402


def pattern(phase, cycle):
    sql, params = scope_sql("geometry_child", ["id", "parent_id"],
                            {"upc": "N/01002/01001/GJ", "phase": phase, "cycle": cycle})
    assert sql == '"parent_id"::text ~ :scope_value'
    return re.compile(params["scope_value"])


def test_matches_every_form_of_this_survey():
    p = pattern("1", "1")
    for pid in ["N_01002_01001_GJ_P1C1_geometry_parent",       # current silver
                "N_01002_01001_GJ_P1_C1_geometry_parent",      # earlier silver
                "N_01002_01001_GJ_geometry_parent"]:           # before the phase / cycle prefix
        assert p.match(pid), pid


def test_other_cycle_phase_and_upc_untouched():
    p = pattern("1", "1")
    for pid in ["N_01002_01001_GJ_P1_C2_geometry_parent", "N_01002_01001_GJ_P1C2_geometry_parent",
                "N_01002_01001_GJ_P2_C1_geometry_parent", "N_01002_01001_GJ_P11_C1_geometry_parent",
                "N_01002_01002_GJ_P1_C1_geometry_parent"]:
        assert not p.match(pid), pid
    assert pattern(1.0, "2.0").match("N_01002_01001_GJ_P1_C2_geometry_parent")   # 1.0 / '2.0' read as 1 / 2
