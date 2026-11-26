"""UPC list -> bronze -> silver -> images -> gold, from the NSV downloads. Never writes to the database.

    python -m upc_flow --upcs upc_flow/upcs.csv            # the UPCs in the list (phase / cycle / zone optional)
    python -m upc_flow --upc N/01002/01001/GJ --upc ...     # or on the command line
    python -m upc_flow --upcs upcs.csv --dry-run            # only show the surveys found in the downloads
    python -m upc_flow --upcs upcs.csv --skip images        # without the image upload

1. Surveys: every Zone / Phase / Cycle folder of the downloads (RAW_S3_ROOT of .env, or --raw-root) that holds the
   UPC, kept when the list gives phase / cycle / zone and they match.
2. Per survey: run.py --scheduled --no-write --only <stage> for bronze, silver, images (same folders as always:
   Bronze/ and Silver/ of .env). A warning (data quality) is noted and the next stage runs; a failure stops that
   survey (the others go on).
3. Gold once for every survey whose silver is done, written to its own folder (UPC_FLOW_GOLD_DIR, default
   <S3_GOLD_OUTPUT_DIR>/Latest_v2): <dir>/<table>.csv + <dir>/year=/month=/day=/ history. Gold/Latest - what
   push_gold reads - is never touched. A survey without a survey_schedule_id gets the survey_id placeholder
   <UPC>_P<phase>_C<cycle>_<hash> (same on every run, so a re-run overwrites it).
push_silver / push_gold are never run. Result: logs/upc_flow/<time>/results.csv (+ surveys.csv, the run logs).
"""
import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_PY = Path(os.getenv("UPC_FLOW_RUN_PY") or ROOT / "run.py")     # tests point this to a fake run.py
SURVEY_STAGES = ["bronze", "silver", "images"]
ZONE = re.compile(r"^zone[\s_\-]*([A-Za-z])$", re.IGNORECASE)      # same folder rules as bronze/run_download.py
PHASE = re.compile(r"^phase[\s_\-]*(\d+)$", re.IGNORECASE)
CYCLE = re.compile(r"^cycle[\s_\-]*(\d+)$", re.IGNORECASE)


def log(text=""):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}" if text else "", flush=True)


def upc_key(text):
    """'N/01002/01001/GJ', 'N_01002_01001_GJ', 'n-01002-01001-gj' -> 'N/01002/01001/GJ'"""
    return re.sub(r"[/_\-\s]+", "/", str(text or "").strip()).upper()


def number(x):
    return re.sub(r"\.0$", "", str(x or "").strip())


# --- the list -------------------------------------------------------------------------------------------------

def read_list(path=None, upcs=()):
    """[{upc, phase, cycle, zone}] from a CSV / text file (column upc; phase, cycle, zone optional - empty = any)
    and / or --upc values. A file without a header: one UPC per line."""
    rows = []
    if path:
        text = Path(path).read_text(encoding="utf-8-sig")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if lines and "upc" in lines[0].lower():
            delim = "\t" if "\t" in lines[0] else ","
            for r in csv.DictReader(lines, delimiter=delim):
                r = {str(k).strip().lower(): (v or "").strip() for k, v in r.items() if k}
                if r.get("upc"):
                    rows.append({"upc": upc_key(r["upc"]), "phase": number(r.get("phase")),
                                 "cycle": number(r.get("cycle")), "zone": (r.get("zone") or "").upper()})
        else:
            rows += [{"upc": upc_key(ln.split(",")[0]), "phase": "", "cycle": "", "zone": ""} for ln in lines]
    rows += [{"upc": upc_key(u), "phase": "", "cycle": "", "zone": ""} for u in upcs or []]
    out = []
    for r in rows:
        if r not in out:
            out.append(r)
    return out


# --- the downloads --------------------------------------------------------------------------------------------

def _dirs(root):
    """Folder names directly under root (S3 'prefix/' or local path)."""
    if str(root).startswith("s3://"):
        import boto3
        bucket, _, prefix = str(root)[5:].partition("/")
        prefix = prefix if prefix.endswith("/") or not prefix else prefix + "/"
        names = []
        for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix,
                                                                                  Delimiter="/"):
            names += [p["Prefix"][len(prefix):].rstrip("/") for p in page.get("CommonPrefixes", [])]
        return sorted(n for n in names if n)
    p = Path(root)
    return sorted(x.name for x in p.iterdir() if x.is_dir()) if p.is_dir() else []


def _join(root, name):
    return f"{str(root).rstrip('/')}/{name}" if str(root).startswith("s3://") else str(Path(root) / name)


def find_surveys(raw_root, wanted):
    """(surveys [{zone, phase, cycle, upc, upc_folder}], not found [list rows]) - every Zone / Phase / Cycle folder
    of the downloads holding a listed UPC, filtered by the row's phase / cycle / zone when given."""
    by_upc = {}
    for r in wanted:
        by_upc.setdefault(r["upc"], []).append(r)
    found, hit = [], set()
    for z in (d for d in _dirs(raw_root) if ZONE.match(d)):
        zp = _join(raw_root, z)
        for ph in (d for d in _dirs(zp) if PHASE.match(d)):
            pp = _join(zp, ph)
            for cy in (d for d in _dirs(pp) if CYCLE.match(d)):
                for folder in _dirs(_join(pp, cy)):
                    upc = upc_key(folder)
                    if upc not in by_upc:
                        continue
                    s = {"zone": ZONE.match(z).group(1).upper(), "phase": PHASE.match(ph).group(1),
                         "cycle": CYCLE.match(cy).group(1), "upc": upc, "upc_folder": _join(_join(pp, cy), folder)}
                    for r in by_upc[upc]:
                        if all(not r[k] or r[k] == s[k] for k in ("phase", "cycle", "zone")):
                            hit.add(tuple(r.values()))
                            if not any(all(x[k] == s[k] for k in ("zone", "phase", "cycle", "upc")) for x in found):
                                found.append(s)
    missing = [r for r in wanted if tuple(r.values()) not in hit]
    return found, missing


# --- running run.py -------------------------------------------------------------------------------------------

def _run_log(log_dir, before):
    """(newest new logs/run_* folder, the reason run.py stopped or None)."""
    new = sorted(p for p in Path(log_dir).glob("run_*") if p.is_dir() and p.name not in before)
    if not new:
        return None, None
    folder, reason = new[-1], None
    f = folder / "pipeline.log"
    if f.is_file():
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            if "STOPPED:" in line:
                reason = line.split("STOPPED:", 1)[1].strip()
    return folder, reason


def run_py(args, env, log_dir):
    """(exit code, log folder, reason) of one run.py call (always --scheduled --no-write)."""
    cmd = [sys.executable, str(RUN_PY), "--scheduled", "--no-write"] + args
    before = {p.name for p in Path(log_dir).glob("run_*")} if Path(log_dir).is_dir() else set()
    log("  RUN  " + " ".join(cmd[1:]))
    code = subprocess.call(cmd, cwd=str(ROOT), env=env)
    folder, why = _run_log(log_dir, before)
    return code, folder, why


def gold_env(base_env, gold_dir):
    return {**base_env, "S3_GOLD_OUTPUT_DIR": gold_dir, "RIS_KPI_LATEST_FOLDER": "", "SURVEY_ID_PLACEHOLDER": "True"}


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m upc_flow", description=__doc__.split("\n\n")[0])
    p.add_argument("--upcs", help="CSV / text file: column upc (+ phase, cycle, zone optional), or one UPC per line")
    p.add_argument("--upc", action="append", default=[], help="one UPC (repeatable)")
    p.add_argument("--raw-root", help="NSV downloads root (default RAW_S3_ROOT of .env)")
    p.add_argument("--gold-dir", help="gold output folder (default UPC_FLOW_GOLD_DIR, else <S3_GOLD_OUTPUT_DIR>/Latest_v2)")
    p.add_argument("--skip", action="append", default=[], choices=SURVEY_STAGES + ["gold"], help="leave out a stage")
    p.add_argument("--continue-on-warning", default="bronze,silver,images",
                   help="stages whose warning is noted and passed (default bronze,silver,images)")
    p.add_argument("--dry-run", action="store_true", help="only list the surveys found in the downloads")
    o = p.parse_args(argv)

    sys.path.insert(0, str(ROOT))
    from pipeline.settings import load_settings
    s = load_settings()
    raw_root = (o.raw_root or s.get("RAW_S3_ROOT")).strip()
    gold_dir = (o.gold_dir or s.get("UPC_FLOW_GOLD_DIR") or
                f"{(s.get('S3_GOLD_OUTPUT_DIR') or 's3://dhms-new-stgae/DHMS/NSV/Gold').rstrip('/')}/Latest_v2")
    log_dir = s.path("LOG_DIR", "logs")
    wanted = read_list(o.upcs, o.upc)
    if not wanted:
        p.error("no UPC given (--upcs FILE or --upc)")
    if not raw_root:
        p.error("no downloads root: RAW_S3_ROOT is empty in .env (or give --raw-root)")

    log(f"UPC flow | {len(wanted)} list row(s) | downloads {raw_root} | gold -> {gold_dir} | no database writes")
    surveys, missing = find_surveys(raw_root, wanted)
    for sv in surveys:
        log(f"  found  {sv['upc']}  zone {sv['zone']}  phase {sv['phase']}  cycle {sv['cycle']}   {sv['upc_folder']}")
    for r in missing:
        log(f"  NOT FOUND in the downloads: {r['upc']} phase {r['phase'] or 'any'} cycle {r['cycle'] or 'any'} "
            f"zone {r['zone'] or 'any'}")
    work = log_dir / "upc_flow" / datetime.now().strftime("%Y%m%d_%H%M%S")
    work.mkdir(parents=True, exist_ok=True)
    with open(work / "surveys.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, ["zone", "phase", "cycle", "upc"])
        w.writeheader()
        w.writerows({k: sv[k] for k in ("zone", "phase", "cycle", "upc")} for sv in surveys)
    if o.dry_run or not surveys:
        log(f"{'dry run - ' if o.dry_run else ''}{len(surveys)} survey(s) found, list: {work / 'surveys.csv'}")
        return 0 if surveys else 1

    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    if o.raw_root:
        env["RAW_S3_ROOT"] = raw_root
    go_on = {x.strip() for x in o.continue_on_warning.split(",") if x.strip()}
    stages = [x for x in SURVEY_STAGES if x not in o.skip]
    results = []
    for n, sv in enumerate(surveys, 1):
        log(f"\n######## {n}/{len(surveys)}  {sv['upc']} zone {sv['zone']} phase {sv['phase']} cycle {sv['cycle']}")
        r = {**{k: sv[k] for k in ("zone", "phase", "cycle", "upc")}, "status": "ok", "stopped_at": "",
             "warnings": [], "logs": []}
        for stage in stages:
            code, folder, why = run_py(["--only", stage, "--zone", sv["zone"], "--phase", sv["phase"],
                                        "--cycle", sv["cycle"], "--upc", sv["upc"]], env, log_dir)
            r["logs"] += [str(folder)] if folder else []
            if code == 0:
                continue
            if code == 1 and why and "warning (exit" in why and stage in go_on:
                r["warnings"].append(why)
                log(f"  WARNING passed ({stage}): {why}")
                continue
            r["status"], r["stopped_at"] = "failed", why or f"{stage}: exit code {code}"
            log(f"  FAILED at {stage}: {r['stopped_at']} - next survey")
            break
        if r["status"] == "ok" and r["warnings"]:
            r["status"] = "warnings"
        results.append(r)

    ready = [r for r in results if r["status"] in ("ok", "warnings")]
    if "gold" not in o.skip and ready:
        gold_list = work / "gold_surveys.csv"
        with open(gold_list, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, ["zone", "phase", "cycle", "upc"])
            w.writeheader()
            w.writerows({k: r[k] for k in ("zone", "phase", "cycle", "upc")} for r in ready)
        log(f"\n######## gold for {len(ready)} survey(s) -> {gold_dir} (Gold/Latest not touched)")
        code, folder, why = run_py(["--only", "gold", "--surveys", str(gold_list)], gold_env(env, gold_dir), log_dir)
        for r in ready:
            r["logs"] += [str(folder)] if folder else []
            if code != 0:
                r["status"], r["stopped_at"] = "failed", f"gold: {why or f'exit code {code}'}"
    with open(work / "results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, ["zone", "phase", "cycle", "upc", "status", "stopped_at", "warnings", "logs"])
        w.writeheader()
        for r in results:
            w.writerow({**r, "warnings": " | ".join(r["warnings"]), "logs": " ".join(r["logs"])})

    log("\n######## result")
    for r in results:
        log(f"  {r['status']:9s} {r['upc']} zone {r['zone']} phase {r['phase']} cycle {r['cycle']}"
            f"{'  - ' + r['stopped_at'] if r['stopped_at'] else ''}")
    for r in missing:
        log(f"  not found  {r['upc']} phase {r['phase'] or 'any'} cycle {r['cycle'] or 'any'}")
    log(f"gold: {gold_dir}   |   results: {work / 'results.csv'}")
    return 0 if all(r["status"] in ("ok", "warnings") for r in results) and not missing else 1


if __name__ == "__main__":
    sys.exit(main())
