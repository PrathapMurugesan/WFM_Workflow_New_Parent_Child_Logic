# KPI status update

After a survey is processed and pushed, its defect cases keep changing in DHMS: they get closed or reopened. The
gold run only updates the counts of rows that already exist, so it can't add a "closed" row or remove an "open" row
that no longer applies. This job runs every day: it finds the surveys whose defect status changed since the previous
DB pull and rebuilds their gold tables from silver, using the gold code of this repository (`gold/src/ris_kpi`).

It was brought in from `RIS_status_update` (`soumyadip-SSAI/RIS_status_update`). The logic is unchanged, but it uses
our gold modules instead of its own copy, so the two can't drift apart.

## Where it fits

```
00:30 IST  wfm_flow.trigger (cron)   WFM surveys: bronze -> silver -> gold (Gold/Latest) -> push_silver, push_gold
           upc_flow (by hand)        NSV downloads: bronze -> silver -> gold (Gold/Latest_v2), never pushed
daily      DB export                 DIC_Staging/year=/month=/day=/ (udm_defect_reference, dlm_defect_case, ...)
02:00 IST  python -m status_update   both sources, below (waits for the trigger if it is still running)
```

## What it does, per source

| Source | Gold it checks | Output (overwritten every run) | DB |
|---|---|---|---|
| `wfm` | `Gold/Latest` (WFM surveys) | `Gold/Latest/status_update/` | pushed, then written back into `Gold/Latest` |
| `nsv` | `Gold/Latest_v2` (NSV downloads) | `Gold/Latest_v2/status_update/` | never pushed (survey_id placeholders on, as upc_flow) |

1. **DB pulls.** It takes the newest `DIC_Staging/year=/month=/day=` folder that has both `udm_defect_reference`
   and `dlm_defect_case`, and the newest earlier folder that has both.
2. **Changed UPCs.** A UPC counts as changed when either table has a new row, a removed row, or a changed status
   (`defect_status`; `current_status` or `defect_reference_id`).
3. **Surveys.** It keeps the upc + phase + cycle combinations in the source's `ris_view_data.csv` /
   `ris_trends_data.csv` whose UPC changed, and finds their silver under
   `Silver/Zone=*/Phase=/Cycle=/UPC=/dlm_kpi_new/`.
4. **Rebuild.** For those surveys it rebuilds the 5 silver tables with status, `intermediate_10m_data`,
   `ris_view_data` and `ris_trends_data`, plus the reports `changed_upcs.csv` and `updated_surveys.csv`.
5. **DB push (`wfm` only, `push.py`).** Both tables go in one transaction:
   - **Matching rows** (same key as `ris-kpi sync-db-ids`) keep their DB `id` and `created_at` and are UPDATEd.
   - **New rows** are INSERTed.
   - **Rows no longer produced** are DELETEd. These are DB rows of the same surveys (same upc_id + survey_id +
     survey_source_id) that no rebuilt row matches, for example the old "open" row or a duplicate.
   - **Checks first:** push_gold's type, NOT NULL and foreign-key checks run before anything is written. Any problem
     means nothing is written.
6. **Write-back (`wfm` only).** In `Gold/Latest/` and the newest dated copy, the rows of the pushed surveys are
   replaced by the pushed rows, which carry their DB ids. The next gold run and push_gold then start from what the DB
   has.

Within each source, everything is calculated before the first file is written. A failed source writes nothing and
doesn't stop the other source.

## Run

```bash
python -m status_update --dry-run              # calculated + checked against the DB, nothing written
python -m status_update                        # both sources; wfm pushed
python -m status_update --source wfm           # one source
python -m status_update --no-push              # S3 output only
python -m status_update --latest-date 2026-11-26 --previous-date 2026-11-24   # a missed day
python -m status_update --db dhms_staging      # database of the push (default DB_NAME)
bash status_update/install_cron.sh --daily-ist 02:00                          # cron (see the script header)
python -m pytest -q status_update/tests
```

The exit code is 0 when done, including when nothing changed, and 1 when a source failed.

## Settings (repository `.env`)

| Key | Default | |
|---|---|---|
| `STATUS_SOURCES` | `wfm,nsv` | sources that run |
| `STATUS_PUSH` | `wfm` | sources whose result goes to the DB (empty = none) |
| `STATUS_WFM_GOLD_DIR` | `<S3_GOLD_OUTPUT_DIR>/Latest` | gold of the WFM surveys |
| `STATUS_NSV_GOLD_DIR` | `UPC_FLOW_GOLD_DIR`, else `<S3_GOLD_OUTPUT_DIR>/Latest_v2` | gold of the NSV downloads surveys |
| `STATUS_DIC_ROOT` | `S3_DB_TABLES_DIR` without `/Latest` | DB export with the dated pulls |
| `STATUS_SILVER_ROOT` | `S3_SILVER_INPUT_DIR` | silver |
| `STATUS_SILVER_SUBFOLDER` | `dlm_kpi_new` | folder inside the UPC folder |
| `DROP_ROWS_WITHOUT_IDS`, `SURVEY_ID_FALLBACK` | as the gold run | |

## Notes

- **Missed days.** The job compares the two newest pulls only. To catch up on a skipped day, run it with
  `--latest-date` / `--previous-date`.
- **The dated DB export must run daily.** Without a new `year=/month=/day=` folder there is nothing new to compare.
- **ris_trends has no status column.** Its rows change only when the silver data changed.
