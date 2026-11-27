"""Shared fixtures: an in-memory S3 (no AWS access needed) and a small synthetic survey (silver + DB tables)."""
import io
import sys
from pathlib import Path

import pandas as pd
import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from status_update import storage  # noqa: E402


class FakeS3:
    def __init__(self):
        self.objects = {}  # "bucket/key" -> bytes

    # --- test helpers ---
    def put(self, uri, data):
        bucket, key = storage.split_s3_uri(uri)
        self.objects[f"{bucket}/{key}"] = data if isinstance(data, bytes) else data.encode()

    def put_df(self, uri, df):
        if uri.endswith(".parquet"):
            buf = io.BytesIO()
            df.astype("string").to_parquet(buf, index=False)
            self.put(uri, buf.getvalue())
        else:
            self.put(uri, df.to_csv(index=False))

    def read_df(self, uri):
        bucket, key = storage.split_s3_uri(uri)
        return pd.read_csv(io.BytesIO(self.objects[f"{bucket}/{key}"]), dtype=str)

    def has(self, uri):
        bucket, key = storage.split_s3_uri(uri)
        return f"{bucket}/{key}" in self.objects

    # --- boto3 client API used by storage.py ---
    def _data(self, bucket, key, op):
        try:
            return self.objects[f"{bucket}/{key}"]
        except KeyError:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, op) from None

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self._data(Bucket, Key, "HeadObject"))}

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self._data(bucket, key, "GetObject"))

    def upload_file(self, path, bucket, key):
        self.objects[f"{bucket}/{key}"] = Path(path).read_bytes()

    def delete_object(self, Bucket, Key):
        self.objects.pop(f"{Bucket}/{Key}", None)

    def get_paginator(self, op):
        assert op == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix, Delimiter=None):
                keys = sorted(k.split("/", 1)[1] for k in fake.objects if k.startswith(f"{Bucket}/{Prefix}"))
                if Delimiter:
                    prefixes = sorted({Prefix + k[len(Prefix):].split(Delimiter, 1)[0] + Delimiter
                                       for k in keys if Delimiter in k[len(Prefix):]})
                    return [{"CommonPrefixes": [{"Prefix": p} for p in prefixes]}]
                half = len(keys) // 2   # two pages, like a real paginator on a large prefix
                return [{"Contents": [{"Key": k} for k in part]} if part else {} for part in (keys[:half], keys[half:])]
        return Paginator()


@pytest.fixture
def fake_s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(storage, "client", lambda: fake)
    return fake


# --- a synthetic survey: one lane of 40 m, roughness parent breached, one pothole, one crack ---------------------

def survey_rows(survey, **cols):
    n = len(next(iter(cols.values())))
    return pd.DataFrame({**{k: [v] * n for k, v in survey.items()}, **cols})


def silver_tables(upc="N/9", phase="1", cycle="1", zone="C"):
    s = {"upc": upc, "zone": zone, "phase": phase, "cycle": cycle, "source_system": "NSV", "detected_on": "2026-01-05"}
    parent = survey_rows(s, id=["1"], parent_id=["P1"], display_defect_id=["D1"], breach_flag=["1"],
                         pavement_type=["Asphalt"], threshold_name=["As per CA"], metric_value=["3000"],
                         threshold_value=["2500"], pci=["55"], lane=["L1"], start_chainage=["0"], end_chainage=["40"])
    ca = survey_rows(s, id=["C1", "C2"], parent_id=["P1", "P1"], lane=["L1", "L1"], start_chainage=["0", "20"],
                     end_chainage=["20", "40"], breach_flag=["1", "0"], pavement_type=["Asphalt"] * 2,
                     threshold_name=["As per CA"] * 2)
    child = survey_rows(s, parent_id=["P1"] * 4, lane=["L1"] * 4, start_chainage=["0", "10", "20", "30"],
                        end_chainage=["10", "20", "30", "40"], breach_flag=["0"] * 4, pavement_type=["Asphalt"] * 4,
                        threshold_name=["As per CA"] * 4)
    crack_ca = survey_rows(s, id=["K1"], display_defect_id=["K-D1"], lane=["L1"], start_chainage=["0"],
                           end_chainage=["40"], breach_flag=["1"], pavement_type=["Asphalt"],
                           threshold_name=["As per CA"])
    point = survey_rows(s, id=["1"], display_defect_id=["PH1"], breach_flag=["1"], pavement_type=["Asphalt"])
    return {
        "master_roughness_parent": parent, "master_rutting_parent": parent.assign(display_defect_id=[None],
                                                                                    breach_flag=["0"]),
        "master_roughness_as_per_ca": ca, "master_rutting_as_per_ca": ca, "master_roughness_child_10m": child,
        "master_rutting_child_10m": child, "master_cracking_as_per_ca": crack_ca,
        "master_cracking_10m": child.drop(columns="parent_id"), "master_pothole": point,
        "master_ravelling": point.assign(display_defect_id=[None], breach_flag=["0"]),
    }


def references(upc="N/9"):
    return pd.DataFrame({"id": ["r1", "r2", "r3"], "defect_collection": ["roughness_parent", "pothole_child_ca",
                                                                          "cracking_child_ca"],
                         "defect_id": ["D1", "PH1", "K-D1"], "upc": [upc] * 3, "defect_status": ["IN_PROGRESS"] * 3})


def cases(statuses, upc="N/9"):
    return pd.DataFrame({"id": ["c1", "c2", "c3"], "defect_reference_id": ["r1", "r2", "r3"], "upc": [upc] * 3,
                         "current_status": statuses, "defect_type": ["x"] * 3, "record_level": ["x"] * 3})


def master_tables(upc="N/9", phase="1", cycle="1"):
    return {
        "master_upc_project": pd.DataFrame({"id": ["u9"], "upc": [upc], "length_km": ["1"], "lane_length_km": ["4"]}),
        "master_survey_system": pd.DataFrame({"id": ["s-nsv"], "system_code": ["NSV"]}),
        "survey_data_map": pd.DataFrame({"survey_schedule_id": ["sv9"], "upc": [upc], "phase": [phase],
                                         "cycle": [cycle]}),
        "master_defect_type": pd.DataFrame({"id": ["dt-pot", "dt-crack", "dt-rough"], "survey_system": ["NSV"] * 3,
                                            "asset_type": ["Flexible Pavement", "Flexible Pavement", "Pavement (NSV)"],
                                            "defect_name": ["Pothole", "Cracking", "Roughness"]}),
        "wfm_survey_schedule": pd.DataFrame({"id": ["sv9"], "upc": [upc], "current_status": ["Survey Final Approved"]}),
    }
