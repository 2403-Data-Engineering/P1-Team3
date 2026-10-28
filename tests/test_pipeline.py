from __future__ import annotations

import copy
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from contracts.transaction_event import TransactionEventError, events_from_csv, txn_id_for_csv_row
from pipeline import glue_job
from pipeline.local import read_events, run_local, snapshot_bytes
from pipeline.package import build_package
from pipeline.reconcile import aggregates, compare, read_facts
from pipeline.sample import sample_csv
from pipeline.transform import TYPE_KEYS, event_to_fact, money, transform_events

FIXTURE = Path(__file__).parent / "fixtures" / "paysim_replay_sample.csv"
LABELED = Path(__file__).parent / "fixtures" / "paysim_labeled_sample.csv"
STAMP = datetime(2026, 10, 26, tzinfo=timezone.utc)


@pytest.fixture
def events():
    return events_from_csv(FIXTURE, ingested_at=STAMP, source_file="paysim_clean.csv")


def test_sample_reproducible_and_identity_matches_original(events, tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    assert sample_csv(FIXTURE, a, limit=3, ingested_at=STAMP, source_file="paysim_clean.csv") == 3
    sample_csv(FIXTURE, b, limit=3, ingested_at=STAMP, source_file="paysim_clean.csv")
    assert a.read_bytes() == b.read_bytes()
    assert list(read_events([a])) == events[:3]
    with pytest.raises(FileExistsError):
        sample_csv(FIXTURE, a, limit=3, ingested_at=STAMP)
    assert a.read_bytes() == b.read_bytes()


@pytest.mark.parametrize("limit", [0, -1, 100001])
def test_sample_limit(limit, tmp_path):
    with pytest.raises(ValueError):
        sample_csv(FIXTURE, tmp_path / "out.jsonl", ingested_at=STAMP, limit=limit)


def test_sample_refuses_labels_and_cleans_invalid_output(tmp_path):
    output = tmp_path / "sample.jsonl"
    with pytest.raises(ValueError, match="unlabeled"):
        sample_csv(LABELED, output, ingested_at=STAMP)
    assert not output.exists()
    malformed = tmp_path / "bad.csv"
    malformed.write_text(FIXTURE.read_text().replace("181.00", "oops", 1))
    with pytest.raises(TransactionEventError):
        sample_csv(malformed, output, ingested_at=STAMP)
    assert not output.exists()


def test_shared_transform_preserves_grain_nulls_and_dimensions(events):
    tables = transform_events(events)
    facts = tables["fact_transactions"]
    assert len(facts) == len({r["txn_id"] for r in facts}) == 4
    assert facts[0]["txn_id"] == min(e["txn_id"] for e in events)
    for row in facts:
        assert row["txn_id"] == txn_id_for_csv_row(row, source_file=row["source_file"], row_number=row["row_number"])
        assert row["date_key"] == row["source_step"] == row["step"]
        assert row["type_key"] == TYPE_KEYS[row["type"]]
        assert isinstance(row["amount"], Decimal)
        assert "isFraud" not in row and "isFlaggedFraud" not in row
        if row["nameDest"].startswith("M"):
            assert row["oldbalanceDest"] is row["newbalanceDest"] is None
    assert len(tables["dim_account"]) == 5
    assert tables["dim_date"][0]["datetime"] == "2026-03-01T00:00:00"
    assert tables["dim_date"][1]["hour"] == 1
    assert len(tables["dim_transaction_type"]) == 5
    assert transform_events(events[-1:])["dim_transaction_type"] == tables["dim_transaction_type"]


def test_replay_persists_same_snapshot_without_duplicate_facts(tmp_path):
    source = tmp_path / "sample.jsonl"
    sample_csv(FIXTURE, source, ingested_at=STAMP)
    output = tmp_path / "curated"
    first = run_local([source], output)
    first_bytes = (output / "tables.json").read_bytes()
    second = run_local([source, source], output)
    assert (output / "tables.json").read_bytes() == first_bytes
    assert len(read_facts(output / "tables.json")) == 4
    assert compare(first["fact_transactions"], second["fact_transactions"]) == []
    # Duplicate-looking original CSV rows stay separate (they have row-specific IDs).
    assert sum(r["type"] == "PAYMENT" for r in second["fact_transactions"]) == 2


@pytest.mark.parametrize("mutation", ["identity", "step", "labels", "amount"])
def test_bad_events_rejected(events, mutation):
    row = copy.deepcopy(events[0])
    if mutation == "identity":
        row["txn_id"] = row["event_id"] = "0" * 64
    elif mutation == "step":
        row["source_step"] += 1
    elif mutation == "labels":
        row["payload"]["isFraud"] = 1
    else:
        row["payload"]["amount"] = -1
    with pytest.raises(ValueError):
        event_to_fact(row)


def test_replay_timestamp_ignored_and_input_bound_enforced(events):
    replay = copy.deepcopy(events)
    for row in replay:
        row["ingested_at"] = "2026-10-27T00:00:00Z"
    assert snapshot_bytes(transform_events(events)) == snapshot_bytes(transform_events(replay + events))
    with pytest.raises(ValueError, match="bounded"):
        transform_events(events, max_records=3)


def test_decimal_precision_and_reconciliation(events):
    facts = transform_events(events)["fact_transactions"]
    groups = aggregates(facts)
    assert groups[(1, "PAYMENT")] == {"count": 2, "amount_sum": Decimal("362")}
    assert groups[(1, "TRANSFER")]["amount_sum"] == Decimal("10000.50")
    wrong = copy.deepcopy(facts)
    wrong[0]["amount"] += Decimal("0.01")
    assert len(compare(facts, wrong)) == 1
    assert compare(facts, facts[:-1])
    assert compare(facts, [])
    with pytest.raises(ValueError, match="duplicate"):
        compare(facts, facts + facts[:1])
    assert aggregates([{"txn_id": "a", "step": 1, "type": "PAYMENT", "amount": "0.1"},
                       {"txn_id": "b", "step": 1, "type": "PAYMENT", "amount": "0.2"}])[(1, "PAYMENT")]["amount_sum"] == Decimal("0.3")
    assert money(None) is None
    with pytest.raises(ValueError):
        money("0.00000000001")
    with pytest.raises(ValueError):
        money("10000000000000000000000000000")


def test_package_schema_loads_from_zip_without_repo_on_path(tmp_path):
    archive = build_package(tmp_path / "pipeline.zip")
    code = "import sys; sys.path.insert(0, sys.argv[1]); from contracts.transaction_event import load_event_schema; assert load_event_schema()['title'] == 'PaySim transaction event v1'"
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run([sys.executable, "-c", code, str(archive)], cwd=tmp_path,
                            env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


class Body(io.BytesIO):
    def iter_lines(self):
        return iter(self.getvalue().splitlines())


class FakeS3:
    def __init__(self, events, uris=None):
        self.objects = {
            "manifests/sample.json": json.dumps({"events": uris or ["s3://raw/events/sample.jsonl"]}).encode(),
            "events/sample.jsonl": ("\n".join(json.dumps(e) for e in events)).encode(),
        }
        self.requests = []

    def get_object(self, Bucket, Key):
        assert Bucket == "raw"
        self.requests.append(Key)
        return {"Body": Body(self.objects[Key]), "ContentLength": len(self.objects[Key])}


class FakeGlue:
    class exceptions:
        class AlreadyExistsException(Exception):
            pass

    def __init__(self):
        self.tables = {}

    def create_table(self, DatabaseName, TableInput):
        assert DatabaseName == "paysim"
        if TableInput["Name"] in self.tables:
            raise self.exceptions.AlreadyExistsException()
        self.tables[TableInput["Name"]] = TableInput

    def update_table(self, DatabaseName, TableInput, SkipArchive):
        assert DatabaseName == "paysim" and SkipArchive
        self.tables[TableInput["Name"]] = TableInput


def test_glue_orchestration_uses_same_transform_and_overwrites_replay(events, monkeypatch):
    from pipeline import transform
    storage = {}
    class Frame:
        def __init__(self, rows):
            self.rows = rows
            self.write = self
        def mode(self, mode):
            assert mode == "overwrite"
            return self
        def parquet(self, path):
            storage[path] = self.rows
    class Spark:
        def createDataFrame(self, rows, schema):
            assert schema
            return Frame(rows)
    monkeypatch.setattr(transform, "spark_schema", lambda name: list(transform.TABLE_COLUMNS[name]))
    s3, glue = FakeS3(events), FakeGlue()
    args = dict(manifest_uri="s3://raw/manifests/sample.json", raw_bucket="raw", output_root="s3://curated/batch", database="paysim")
    first = glue_job.run_batch(s3, glue, Spark(), **args)
    second = glue_job.run_batch(s3, glue, Spark(), **args)
    assert first == second
    assert first["fact_count"] == 4
    assert len(storage) == len(glue.tables) == 4
    fact_path = next(path for path in storage if path.endswith("/fact_transactions"))
    assert storage[fact_path] == transform_events(events)["fact_transactions"]
    assert compare(storage[fact_path], transform_events(events)["fact_transactions"]) == []
    assert glue.tables["fact_transactions"]["StorageDescriptor"]["Location"] == fact_path + "/"
    assert {c["Name"]: c["Type"] for c in glue_job.catalog_columns("fact_transactions")}["amount"] == "decimal(38,10)"


def test_glue_fails_before_catalog_publish_on_invalid_input(events):
    s3, glue = FakeS3(events, ["s3://foreign/events/sample.jsonl"]), FakeGlue()
    with pytest.raises(ValueError, match="configured raw bucket"):
        glue_job.run_batch(s3, glue, None, manifest_uri="s3://raw/manifests/sample.json", raw_bucket="raw",
                           output_root="s3://curated/batch", database="paysim")
    assert not glue.tables


def test_glue_write_failure_does_not_publish_catalog(events, monkeypatch):
    glue = FakeGlue()
    def fail_write(*args):
        raise OSError("fixture write failure")
    monkeypatch.setattr(glue_job, "write_parquet", fail_write)
    with pytest.raises(OSError, match="fixture write failure"):
        glue_job.run_batch(FakeS3(events), glue, None,
                           manifest_uri="s3://raw/manifests/sample.json", raw_bucket="raw",
                           output_root="s3://curated/batch", database="paysim")
    assert not glue.tables


def test_reconcile_cli_reports_csv_amount_mismatch(events, tmp_path):
    import csv
    facts = transform_events(events)["fact_transactions"]
    paths = [tmp_path / "expected.csv", tmp_path / "actual.csv"]
    for index, path in enumerate(paths):
        rows = copy.deepcopy(facts)
        if index:
            rows[0]["amount"] += Decimal("0.01")
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["txn_id", "step", "type", "amount"], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    result = subprocess.run([sys.executable, "-m", "pipeline.reconcile", *map(str, paths)],
                            capture_output=True, text=True)
    assert result.returncode == 1
    assert json.loads(result.stdout)["matches"] is False
    assert len(json.loads(result.stdout)["differences"]) == 1


def test_glue_raw_replay_and_manifest_limits(events, monkeypatch):
    s3 = FakeS3(events, ["s3://raw/events/sample.jsonl"] * 2)
    assert len(transform_events(glue_job.read_manifest_events(s3, "s3://raw/manifests/sample.json", "raw"))["fact_transactions"]) == 4
    monkeypatch.setattr(glue_job, "MAX_RAW_BYTES", 1)
    with pytest.raises(ValueError, match="128 MiB"):
        list(glue_job.read_manifest_events(s3, "s3://raw/manifests/sample.json", "raw"))
