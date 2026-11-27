"""KPI status update for every source (see README.md): python -m status_update

    python -m status_update                          both sources (STATUS_SOURCES): compare the two newest DB pulls,
                                                     rebuild the changed surveys, write <gold>/status_update/, push wfm
    python -m status_update --dry-run                everything calculated and checked against the DB, nothing written
    python -m status_update --source wfm             one source only (wfm = Gold/Latest, nsv = Gold/Latest_v2)
    python -m status_update --no-push                S3 only, no DB push / no gold write-back
    python -m status_update --latest-date 2026-11-26 --previous-date 2026-11-24     compare these two pulls
    python -m status_update --db dhms_staging        the database of the push (default DB_NAME of .env)

Per source: everything is calculated before the first file is written (a failed source writes nothing); a failed
source does not stop the other one. Exit code 0 = every source done (also when nothing changed), 1 = a source failed.
"""
import argparse
import logging
import sys
import time
from datetime import date, datetime

from status_update import config

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
log = logging.getLogger("status_update")


def run_source(source, args, run_time):
    """One source: pipeline (S3 output), then for a pushed source the DB push + gold write-back. Returns a summary."""
    from status_update import pipeline, push
    settings = config.Settings.from_env(source=source)
    pushing = settings.push and not args.no_push
    log.info("%s\nSOURCE %s: gold %s -> %s | DB push: %s\n%s", "=" * 100, source, settings.ris_input_dir,
             settings.output_dir, "yes" if pushing else "no", "=" * 100)
    result = pipeline.run(settings, args.latest_date, args.previous_date, dry_run=args.dry_run, run_time=run_time)
    tables = result["tables"]
    summary = {"source": source, "changed_upcs": len(result["changed"]),
               "surveys": int((result["surveys"]["result"] == "recalculated").sum()) if len(result["surveys"]) else 0,
               "pushed": "-"}
    if pushing and tables:
        pushed = push.push(tables, dry_run=args.dry_run, db_name=args.db)
        if pushed and not args.dry_run:
            push.write_back(settings.ris_input_dir, pushed)
        summary["pushed"] = ("checked (dry run)" if args.dry_run else "yes") if pushed else "nothing"
    return summary


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog="\n".join(__doc__.split("\n")[1:]))
    p.add_argument("--dry-run", action="store_true", help="calculate and check everything, write nothing")
    p.add_argument("--source", action="append", choices=list(config.SOURCES), help="only this source (repeatable)")
    p.add_argument("--no-push", action="store_true", help="no DB push / gold write-back (S3 output only)")
    p.add_argument("--latest-date", type=date.fromisoformat, metavar="YYYY-MM-DD", help="DB pull used as the latest")
    p.add_argument("--previous-date", type=date.fromisoformat, metavar="YYYY-MM-DD", help="DB pull compared with")
    p.add_argument("--db", help="database of the push (default DB_NAME of .env)")
    args = p.parse_args(argv)

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    if not config.ENV_FILE.is_file():
        log.warning("No %s - defaults / environment variables used", config.ENV_FILE)
    config.load_env()
    sources = args.source or config.names("STATUS_SOURCES")
    run_time, start, summaries, failed = datetime.now(), time.time(), [], []
    for source in sources:
        try:
            summaries.append(run_source(source, args, run_time))
        except Exception:                                        # noqa: BLE001
            log.exception("SOURCE %s FAILED - nothing of it written to the DB", source)
            failed.append(source)
            summaries.append({"source": source, "changed_upcs": "-", "surveys": "-", "pushed": "FAILED"})
    log.info("%s\nKPI status update %s in %.0fs%s", "=" * 100, "FAILED" if failed else "done", time.time() - start,
             " (DRY RUN)" if args.dry_run else "")
    for s in summaries:
        log.info("  %-4s changed UPCs: %-5s surveys rebuilt: %-5s DB push: %s", s["source"], s["changed_upcs"],
                 s["surveys"], s["pushed"])
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
