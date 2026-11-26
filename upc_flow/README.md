# UPC flow

Runs the pipeline for a **list of UPCs** straight from the **NSV downloads** (`RAW_S3_ROOT` of `.env`), not from WFM:
**bronze → silver → images → gold**. Nothing is written to the database (no push_silver / push_gold, every call
`--no-write`).

| Layer | Where it goes |
|---|---|
| Bronze, Silver | the usual folders (`Bronze/`, `Silver/` of `.env`) - same as every run |
| Gold | its own folder: `<S3_GOLD_OUTPUT_DIR>/Latest_v2/<table>.csv` (+ `year=/month=/day=/` history in it). `Gold/Latest`, which push_gold reads, is never touched |
| survey_id | from `survey_data_map` as always; a survey without one gets the placeholder `<UPC>_P<phase>_C<cycle>_<hash>` (same on every run, so a re-run overwrites its rows) |

## The list

CSV with a `upc` column; `phase`, `cycle`, `zone` optional (empty = every one found in the downloads). Or a text
file with one UPC per line, or `--upc` on the command line. See `upcs.example.csv`.

```
upc,phase,cycle,zone
N/01002/01001/GJ,1,1,
N/01002/01001/GJ,1,2,
N/02001/12001/GJ,,,
```

**The NSV tracker sheet works as it is** (`.xlsx` or `.csv`): columns `Zone`, `Phase` (`Phase 2`), `Cycle`
(`Cycle 1`) and the UPC from the first column whose name has `upc` and holds a UPC in that row (`UPC code`, then
`UPC as per contract`). Rows without a valid UPC are skipped. `--sheet` picks the Excel sheet, `--zone A` keeps one
zone, `--limit 1` runs only the first survey found (a test).

## Commands (EC2, in tmux)

```bash
cd /root/dhms/dhms-nsv-pipeline
.venv/bin/python -m upc_flow --upcs my_upcs.csv --dry-run      # which surveys are in the downloads
.venv/bin/python -m upc_flow --upcs my_upcs.csv                # run them
.venv/bin/python -m upc_flow --upc N/01002/01001/GJ             # one UPC, every phase / cycle found
.venv/bin/python -m upc_flow --upcs my_upcs.csv --skip images   # without the image upload
.venv/bin/python -m upc_flow --upcs tracker.xlsx --zone A --limit 1   # tracker sheet, first survey only
.venv/bin/python -m upc_flow --upcs my_upcs.csv --gold-dir s3://dhms-new-stgae/DHMS/NSV/Gold/Latest_v3
```

Per survey each stage is its own `run.py --scheduled --no-write --only <stage>`; a warning of bronze / silver /
images (data quality) is noted and the next stage runs, a failure stops that survey only. Gold runs once at the end
for every survey whose silver is done. Result: `logs/upc_flow/<time>/results.csv` (status, where it stopped, the
passed warnings, the run log folders) + `surveys.csv`.

Settings (`.env` or environment): `RAW_S3_ROOT` (downloads), `UPC_FLOW_GOLD_DIR` (gold folder, default
`<S3_GOLD_OUTPUT_DIR>/Latest_v2`).

Tests: `python -m pytest -q upc_flow/tests`
