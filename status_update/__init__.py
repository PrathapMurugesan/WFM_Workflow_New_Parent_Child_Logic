"""KPI status update: surveys whose defect status changed in the DB since the previous DB pull get their gold tables
rebuilt (see README.md). Uses the gold code of this repository (gold/src/ris_kpi)."""
import sys
from pathlib import Path

_GOLD_SRC = str(Path(__file__).resolve().parents[1] / "gold" / "src")
if _GOLD_SRC not in sys.path:
    sys.path.insert(0, _GOLD_SRC)
