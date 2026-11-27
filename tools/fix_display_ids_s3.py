"""One-off repair: give every survey in S3 its own display_defect_id numbers (no number shared by two surveys of one date).

Silver numbered the defects of a survey D/<ddmmyy>/<N|D|R|O>/<00001> with one counter per date, but each EC2 run saw
only its own survey, so two surveys of the same date got the same numbers (e.g. N/08009/04002/GJ and N/08009/05001/GJ
both D/060426/N/00001 ...). New surveys continue after the ids in S3 (silver/common/display_ids.py); this tool fixes
the ids already written.

Per id folder (dlm_kpi_new = the hierarchy tables pushed to the DB, dlm_kpi = the older set; each has its own counter):
  1. Every survey's top-level numbers per date, from <silver root>/Zone=/Phase=/Cycle=/UPC=/<folder>/*.parquet.
  2. Per date, surveys in the order they were written to S3 (oldest first): a survey whose numbers do not overlap the
     ones before it keeps them; an overlapping one is shifted to continue after the highest number before it
     (order and gaps kept: n -> n + offset). The child ids follow their parent ('.../00015/0003' -> '.../00244/0003').
  3. The new ids are written to every file that holds them:
       silver   <survey>/<folder>/<table>.parquet and .csv            (display_defect_id)
       images   <survey>/**/image_uploads.csv                          (display_defect_id; so images are not uploaded again)
       gold     <gold dir>/<status table>.csv of every --gold-dir and its newest dated copy (rows of that upc + phase +
                cycle; dlm_kpi_new numbers)
Dry run by default: the plan and the full old -> new list are written to logs/manual/display_id_fix_<time>/, nothing
in S3 changes. --apply: every changed file is first copied to <silver root>/_backup_display_ids/<time>/<same key>
(gold files too), then overwritten.

NOT changed: the database (master_* tables, udm_defect_reference / dlm_defect_case) - the DHMS references keep the old
numbers; a renumbered survey's DB rows get the new ids only when its push_silver runs again.

    python tools/fix_display_ids_s3.py                                   # dry run, every survey in Silver
    python tools/fix_display_ids_s3.py --upc N/08009/05001/GJ            # only plan changes that touch this UPC
    python tools/fix_display_ids_s3.py --apply                           # back up + write
"""
import argparse
import io
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ("dlm_kpi_new", "dlm_kpi")
GOLD_FOLDER = "dlm_kpi_new"                      # gold reads the hierarchy tables
STATUS_TABLES = ["master_roughness_parent", "master_rutting_parent", "master_ravelling", "master_pothole",
                 "master_cracking_as_per_ca"]
ID = re.compile(r"^D/(\d{6})/([NDRO])/(\d+)((?:/\d+)*)$")
SURVEY = re.compile(r"^(?P<survey>(?:.*/)?Zone=[^/]+/Phase=(?P<phase>[^/]+)/Cycle=(?P<cycle>[^/]+)/UPC=(?P<upc>[^/]+))/")
COLUMN = "display_defect_id"
UPLOADS = "image_uploads.csv"


# --- storage: S3 URI or local folder (tests) ----------------------------------------------------------------------
def _s3():
    import boto3
    return boto3.client("s3")


def list_files(root):
    """[(location, modified)] of every file under root."""
    if str(root).startswith("s3://"):
        bucket, _, prefix = str(root)[5:].partition("/")
        prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        return [(f"s3://{bucket}/{o['Key']}", o["LastModified"].timestamp())
                for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    return [(str(p), p.stat().st_mtime) for p in Path(root).rglob("*") if p.is_file()]


def read_bytes(location):
    if str(location).startswith("s3://"):
        bucket, _, key = str(location)[5:].partition("/")
        return _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    return Path(location).read_bytes()


def write_bytes(location, data):
    if str(location).startswith("s3://"):
        bucket, _, key = str(location)[5:].partition("/")
        _s3().put_object(Bucket=bucket, Key=key, Body=data)
    else:
        Path(location).parent.mkdir(parents=True, exist_ok=True)
        Path(location).write_bytes(data)


def rel(location, root):
    return str(location).replace("\\", "/")[len(str(root).replace("\\", "/").rstrip("/")) + 1:]


# --- plan (pure) --------------------------------------------------------------------------------------------------
def top_numbers(ids):
    """{date: set of top-level numbers} of display ids."""
    out = defaultdict(set)
    for v in ids:
        m = ID.match(str(v).strip()) if v is not None else None
        if m:
            out[m.group(1)].add(int(m.group(3)))
    return out


def plan(surveys):
    """surveys: {survey: (written_at, [display ids])} -> {survey: {date: offset}} for the surveys to shift."""
    by_date = defaultdict(list)
    for survey, (written, ids) in surveys.items():
        for date, nums in top_numbers(ids).items():
            by_date[date].append((written, survey, nums))
    shifts = defaultdict(dict)
    for date, items in by_date.items():
        highest = 0
        for _, survey, nums in sorted(items, key=lambda x: (x[0], x[1])):
            if min(nums) > highest:
                highest = max(nums)
                continue
            offset = highest - min(nums) + 1
            shifts[survey][date] = offset
            highest = max(nums) + offset
    return dict(shifts)


def new_id(value, offsets):
    """The display id with its top number shifted by offsets[date] (unchanged when not shifted / not an id)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    m = ID.match(str(value).strip())
    if not m or m.group(1) not in offsets:
        return value
    num = int(m.group(3)) + offsets[m.group(1)]
    return f"D/{m.group(1)}/{m.group(2)}/{num:0{max(5, len(m.group(3)))}d}{m.group(4)}"


# --- file rewrites ------------------------------------------------------------------------------------------------
def rewrite_parquet(data, fn):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pq.read_table(io.BytesIO(data))
    if COLUMN not in table.column_names:
        return None, 0
    old = table.column(COLUMN).to_pylist()
    new = [fn(v) for v in old]
    changed = sum(a != b for a, b in zip(old, new))
    if not changed:
        return None, 0
    i = table.column_names.index(COLUMN)
    table = table.set_column(i, COLUMN, pa.array(new, type=table.schema.field(i).type))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue(), changed


def rewrite_csv(data, fn):
    text = data.decode("utf-8-sig")
    df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
    if COLUMN not in df.columns:
        return None, 0
    new = df[COLUMN].map(lambda v: fn(v) if v != "" else v)
    changed = int((new != df[COLUMN]).sum())
    if not changed:
        return None, 0
    df[COLUMN] = new
    bom = data.startswith(b"\xef\xbb\xbf")
    return df.to_csv(index=False).encode("utf-8-sig" if bom else "utf-8"), changed


def rewrite_gold_csv(data, shifts_by_survey):
    """Gold status table (all surveys stacked): ids shifted on the rows of the shifted surveys (upc + phase + cycle)."""
    df = pd.read_csv(io.StringIO(data.decode("utf-8-sig")), dtype=str, keep_default_na=False)
    if COLUMN not in df.columns or not {"upc", "phase", "cycle"} <= set(df.columns):
        return None, 0
    key = (df["upc"].str.replace("/", "_").str.strip().str.upper() + "|"
           + df["phase"].str.replace(r"\.0$", "", regex=True).str.strip() + "|"
           + df["cycle"].str.replace(r"\.0$", "", regex=True).str.strip())
    new = df[COLUMN].copy()
    for k, offsets in shifts_by_survey.items():
        rows = key == k
        if rows.any():
            new[rows] = df.loc[rows, COLUMN].map(lambda v: new_id(v, offsets) if v != "" else v)
    changed = int((new != df[COLUMN]).sum())
    if not changed:
        return None, 0
    df[COLUMN] = new
    return df.to_csv(index=False).encode("utf-8"), changed


def survey_key(upc, phase, cycle):
    return "|".join([upc.replace("-", "_").replace("/", "_").upper(), re.sub(r"\.0$", "", phase),
                     re.sub(r"\.0$", "", cycle)])


def gold_targets(gold_dirs, files_of):
    """<gold dir>/<status table>.csv and, for a .../Latest dir, the newest dated copy of each next to it."""
    out = []
    for gdir in gold_dirs:
        gdir = str(gdir).rstrip("/\\")
        g = gdir.replace("\\", "/")
        latest = g.endswith("/Latest")
        parent = gdir.rsplit("/", 1)[0] if gdir.startswith("s3://") else str(Path(gdir).parent)
        files = [f for f, _ in files_of(parent if latest else gdir)]
        for t in STATUS_TABLES:
            direct = [f for f in files if f.replace("\\", "/") == f"{g}/{t}.csv"]
            dated = sorted((f for f in files if re.search(r"year=\d{4}/month=\d{1,2}/day=\d{1,2}/" + t + r"\.csv$",
                                                           f.replace("\\", "/"))),
                           key=lambda f: [int(x) for x in re.findall(r"(?:year|month|day)=(\d+)", f)])
            out += direct + (dated[-1:] if latest else [])
    return list(dict.fromkeys(out))


# --- main ---------------------------------------------------------------------------------------------------------
def env_value(key, default=""):
    if os.environ.get(key):
        return os.environ[key].strip()
    env = ROOT / ".env"
    if env.is_file():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith(key + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'") or default
    return default


def main(argv=None):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    gold = env_value("S3_GOLD_OUTPUT_DIR", "s3://dhms-new-stgae/DHMS/NSV/Gold").rstrip("/")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog="\n".join(__doc__.split("\n")[1:]))
    p.add_argument("--silver-root", default=env_value("S3_SILVER_INPUT_DIR", "s3://dhms-new-stgae/DHMS/NSV/Silver"))
    p.add_argument("--gold-dir", action="append", help=f"gold folder with the status tables (default {gold}/Latest "
                                                       f"and {gold}/Latest_v2)")
    p.add_argument("--upc", action="append", default=[], help="only apply shifts of surveys of this UPC")
    p.add_argument("--apply", action="store_true", help="back up and write (default: dry run)")
    p.add_argument("--out", default=str(ROOT / "logs" / "manual" / f"display_id_fix_{datetime.now():%Y%m%d_%H%M%S}"))
    o = p.parse_args(argv)
    silver = o.silver_root.rstrip("/\\")
    gold_dirs = o.gold_dir or [f"{gold}/Latest", f"{gold}/Latest_v2"]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{silver}/_backup_display_ids/{stamp}" if silver.startswith("s3://") else str(Path(silver) / "_backup_display_ids" / stamp)
    out = Path(o.out)
    out.mkdir(parents=True, exist_ok=True)
    only = {survey_key(u, "", "").split("|")[0] for u in o.upc}
    print(f"{'APPLY' if o.apply else 'DRY RUN'} | silver {silver} | gold {gold_dirs}\n")

    files = [(f, t) for f, t in list_files(silver) if "/_backup_display_ids/" not in f.replace("\\", "/")]
    print(f"{len(files):,} files in silver")
    plans, rows, mapping, gold_shifts = {}, [], [], {}
    for folder in FOLDERS:
        per_survey = defaultdict(list)
        for f, t in files:
            r = rel(f, silver)
            m = SURVEY.match(r)
            if m and f"/{folder}/" in "/" + r and r.endswith(".parquet"):
                per_survey[m.group("survey")].append((f, t))

        def read_ids(item):
            survey, fl = item
            ids = []
            import pyarrow.parquet as pq
            for f, _ in fl:
                try:
                    ids += [v for v in pq.read_table(io.BytesIO(read_bytes(f)), columns=[COLUMN]).column(0).to_pylist()
                            if v is not None]
                except (KeyError, ValueError):
                    pass
            return survey, (min(t for _, t in fl), ids)

        with ThreadPoolExecutor(max_workers=16) as pool:
            surveys = dict(pool.map(read_ids, per_survey.items()))
        shifts = plan(surveys)
        if only:
            shifts = {s: v for s, v in shifts.items() if SURVEY.match(s + "/").group("upc").upper() in only}
        plans[folder] = (per_survey, shifts)
        print(f"\n{folder}: {len(surveys)} surveys, {len(shifts)} to renumber")
        for s, offsets in sorted(shifts.items()):
            for date, off in sorted(offsets.items()):
                nums = sorted(top_numbers(surveys[s][1])[date])
                print(f"  {s}  date {date}: {nums[0]:05d}-{nums[-1]:05d} -> {nums[0] + off:05d}-{nums[-1] + off:05d}")
                rows.append({"folder": folder, "survey": s, "date": date, "old_from": nums[0], "old_to": nums[-1],
                             "new_from": nums[0] + off, "new_to": nums[-1] + off, "offset": off})
            for v in sorted(set(surveys[s][1])):
                nv = new_id(v, offsets)
                if nv != v:
                    mapping.append({"folder": folder, "survey": s, "old_id": v, "new_id": nv})
            if folder == GOLD_FOLDER:
                m = SURVEY.match(s + "/")
                gold_shifts[survey_key(m.group("upc"), m.group("phase"), m.group("cycle"))] = offsets
    pd.DataFrame(rows, columns=["folder", "survey", "date", "old_from", "old_to", "new_from", "new_to", "offset"])         .to_csv(out / "renumbered_surveys.csv", index=False)
    pd.DataFrame(mapping, columns=["folder", "survey", "old_id", "new_id"]).to_csv(out / "id_mapping.csv", index=False)
    print(f"\nplan: {out / 'renumbered_surveys.csv'}\n      {out / 'id_mapping.csv'} ({len(mapping):,} ids)")

    # files to change
    jobs = []
    for folder, (per_survey, shifts) in plans.items():
        for s, offsets in shifts.items():
            fn = lambda v, off=offsets: new_id(v, off)                      # noqa: E731
            for f, _ in files:
                r = rel(f, silver)
                if not r.startswith(s + "/"):
                    continue
                if f"/{folder}/" in "/" + r and (r.endswith(".parquet") or r.endswith(".csv")):
                    jobs.append((f, "parquet" if r.endswith(".parquet") else "csv", fn))
                elif folder == GOLD_FOLDER and r.endswith("/" + UPLOADS):
                    jobs.append((f, "csv", fn))
    targets = gold_targets(gold_dirs, list_files) if gold_shifts else []

    changed = []
    for f, kind, fn in jobs:
        data = read_bytes(f)
        new, n = (rewrite_parquet if kind == "parquet" else rewrite_csv)(data, fn)
        if new is not None:
            changed.append((f, data, new, n))
    for f in targets:
        data = read_bytes(f)
        new, n = rewrite_gold_csv(data, gold_shifts)
        if new is not None:
            changed.append((f, data, new, n))
    print(f"\nfiles that change: {len(changed)}")
    for f, _, _, n in changed:
        print(f"  {n:7,} ids  {f}")
    pd.DataFrame([(f, n) for f, _, _, n in changed], columns=["file", "ids_changed"]).to_csv(out / "files.csv",
                                                                                           index=False)
    if not o.apply:
        print("\nDRY RUN - nothing written. Check the plan, then run again with --apply")
        return 0
    for f, old, new, _ in changed:
        key = f.split("://", 1)[1].split("/", 1)[1] if f.startswith("s3://") else rel(f, Path(silver).parent)
        write_bytes(f"{backup}/{key}", old)
    for f, _, new, _ in changed:
        write_bytes(f, new)
    write_bytes(f"{backup}/id_mapping.csv", (out / "id_mapping.csv").read_bytes())
    print(f"\nAPPLIED: {len(changed)} files written; originals in {backup}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
