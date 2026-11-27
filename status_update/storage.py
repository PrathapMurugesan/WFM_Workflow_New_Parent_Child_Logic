"""Reading, listing and writing files on S3 or on the local disk (a location is an S3 URI or a local path).

S3 access is boto3 with its usual credentials (the EC2 instance role, or `aws configure` locally). Files are read
through a temporary local copy and written through a temporary file, both deleted right away.
Needed on S3: s3:ListBucket + s3:GetObject on the inputs, s3:PutObject + s3:DeleteObject on OUTPUT_DIR.
"""
import io
import logging
import tempfile
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path, PurePosixPath

import pandas as pd

log = logging.getLogger(__name__)

# Error codes S3 gives for a missing key: 404, or 403 when the role may read objects but not list the bucket
MISSING_CODES = {"404", "NoSuchKey", "403", "AccessDenied"}


# --- S3 ---------------------------------------------------------------------------------------------------------

def is_s3(location):
    return str(location).startswith("s3://")


def split_s3_uri(uri):
    """'s3://bucket/a/b.csv' -> ('bucket', 'a/b.csv')."""
    if not is_s3(uri):
        raise ValueError(f"Not an S3 URI: {uri}")
    bucket, _, key = str(uri)[len("s3://"):].partition("/")
    if not bucket:
        raise ValueError(f"S3 URI needs a bucket: {uri}")
    return bucket, key


@lru_cache(maxsize=None)
def client():
    import boto3  # imported here so local runs never need it
    return boto3.client("s3")


def _error_code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _prefix(key):
    return key.rstrip("/") + "/" if key else ""


# --- locations --------------------------------------------------------------------------------------------------

def join(directory, *names):
    """<directory>/<name>/... as an S3 URI or a local Path, like directory."""
    if is_s3(directory):
        return "/".join([str(directory).rstrip("/")] + [n.strip("/") for n in names if n])
    return Path(directory).joinpath(*[n for n in names if n])


def name_of(location):
    return PurePosixPath(str(location)).name if is_s3(location) else Path(location).name


def suffix_of(location):
    return PurePosixPath(name_of(location)).suffix.lower()


def exists(location):
    if not is_s3(location):
        return Path(location).exists()
    bucket, key = split_s3_uri(location)
    try:
        client().head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        if _error_code(e) in MISSING_CODES:
            return False
        raise


def list_files(directory):
    """Every file under directory (any depth), sorted; [] when there is none."""
    if is_s3(directory):
        bucket, key = split_s3_uri(directory)
        pages = client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=_prefix(key))
        return sorted(f"s3://{bucket}/{obj['Key']}" for page in pages for obj in page.get("Contents", [])
                      if not obj["Key"].endswith("/"))
    base = Path(directory)
    return sorted(p for p in base.rglob("*") if p.is_file()) if base.is_dir() else []


def list_dirs(directory):
    """Names of the folders directly inside directory, sorted."""
    if is_s3(directory):
        bucket, key = split_s3_uri(directory)
        prefix = _prefix(key)
        pages = client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
        return sorted(p["Prefix"][len(prefix):].rstrip("/") for page in pages for p in page.get("CommonPrefixes", []))
    base = Path(directory)
    return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []


@contextmanager
def local_file(location):
    """A local path to read the location from (a temporary copy for S3, deleted afterwards)."""
    if not is_s3(location):
        if not Path(location).exists():
            raise FileNotFoundError(f"File not found: {location}")
        yield Path(location)
        return
    bucket, key = split_s3_uri(location)
    with tempfile.TemporaryDirectory(prefix="kpi_status_") as tmp:
        path = Path(tmp) / PurePosixPath(key).name
        try:
            client().download_file(bucket, key, str(path))
        except Exception as e:
            if _error_code(e) in MISSING_CODES:
                raise FileNotFoundError(f"{location} not found (or no read access)") from e
            raise
        yield path


# --- reading ----------------------------------------------------------------------------------------------------

def read_text_table(location, columns=None):
    """A .csv / .parquet / .xlsx file with every column as text and empty cells as null (columns: only these)."""
    suffix = suffix_of(location)
    with local_file(location) as path:
        if suffix == ".parquet":
            df = pd.read_parquet(path, columns=columns)
            return df.astype("string").astype(object).where(df.notna(), None)
        if suffix == ".csv":
            return pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False, usecols=columns)
        if suffix == ".xlsx":
            return pd.read_excel(path, dtype=str, usecols=columns)
    raise ValueError(f"Unsupported file type '{suffix}' for {location}")


def as_text(df):
    """df as it reads back from its CSV (every column text, empty = null), as the gold steps read their inputs."""
    return pd.read_csv(io.StringIO(df.to_csv(index=False)), dtype=str, low_memory=False)


# --- writing ----------------------------------------------------------------------------------------------------

def write_csv(df, directory, name):
    """<directory>/<name>.csv (overwritten); returns its location."""
    location = join(directory, f"{name}.csv")
    if is_s3(location):
        bucket, key = split_s3_uri(location)
        with tempfile.TemporaryDirectory(prefix="kpi_status_") as tmp:
            path = Path(tmp) / f"{name}.csv"
            df.to_csv(path, index=False)
            client().upload_file(str(path), bucket, key)
    else:
        Path(directory).mkdir(parents=True, exist_ok=True)
        df.to_csv(location, index=False)
    log.info("Saved %s — %s rows", location, f"{len(df):,}")
    return location


def delete(location):
    """Delete one file (no error when it is not there)."""
    if is_s3(location):
        bucket, key = split_s3_uri(location)
        client().delete_object(Bucket=bucket, Key=key)
    elif Path(location).exists():
        Path(location).unlink()
