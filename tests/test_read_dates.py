"""silver read_dates: delivery typos like '202-07-14' become empty dates, not a crash."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "silver"))

from common.project import read_dates  # noqa: E402


def test_impossible_year_is_empty_not_a_crash():
    out = read_dates(["202-07-14", "14-07-2025", "2025-07-14", "", None, "garbage", "3025-01-01"])
    assert str(out.dtype) == "datetime64[ns]"
    assert out.iloc[1] == pd.Timestamp("2025-07-14") and out.iloc[2] == pd.Timestamp("2025-07-14")
    assert out.iloc[[0, 3, 4, 5, 6]].isna().all()


def test_normal_dates_unchanged():
    out = read_dates(["08-01-2026", "08/01/2026 10:30", "2026-01-10 00:00:00"])
    assert out.tolist() == [pd.Timestamp("2026-01-08"), pd.Timestamp("2026-01-08 10:30"), pd.Timestamp("2026-01-10")]
