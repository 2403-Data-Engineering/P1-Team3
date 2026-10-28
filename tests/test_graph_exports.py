from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from batching_python_driver import normalize_account, transaction_to_fact
from contracts.transaction_event import events_from_csv
from database_creation_script import rows_from_ops_csv
from detection.exports import (
    build_export, join_curated, main, read_export, tables_from_parquet, write_export,
)
from pipeline.local import snapshot_bytes
from pipeline.reconcile import compare
from pipeline.transform import transform_events

FIXTURE = Path(__file__).parent / "fixtures" / "paysim_replay_sample.csv"
STAMP = datetime(2026, 10, 26, tzinfo=timezone.utc)


@pytest.fixture
def source():
    records = rows_from_ops_csv(FIXTURE, source_file="paysim_clean.csv")
    facts = [transaction_to_fact(dict(row, transfer_cashout_flag=1 if row["type"] in {"TRANSFER", "CASH_OUT"} else None)) for row in records]
    ids = sorted({row[field] for row in records for field in ("nameOrig", "nameDest")})
    return dict(fact_transactions=facts,
                dim_account=[normalize_account(dict(account_id=aid, community_id=7, risk_score=10,
                                                    fan_out_flag=1, dense_community_flag=1)) for aid in ids])


@pytest.fixture
def curated():
    return transform_events(events_from_csv(FIXTURE, source_file="paysim_clean.csv", ingested_at=STAMP))


def bundle(source, run="run-1", stamp="2026-10-26T10:00:00Z"):
    return build_export(source, detection_run_id=run, detected_at=stamp, window_start_step=1, window_end_step=2)


def test_exported_signals_match_t05_fixture_and_curated_grain(source, curated):
    exported = bundle(source)
    joined = join_curated(curated, [exported], detection_run_id="run-1")
    expected_accounts = {row["account_id"]: row for row in source["dim_account"]}
    expected_facts = {row["txn_id"]: row for row in source["fact_transactions"]}
    for row in joined["dim_account"]:
        original = expected_accounts[row["account_id"]]
        assert all(row[key] == value for key, value in original.items())
        assert row["detection_run_id"] == "run-1" and row["graph_signal_present"]
    for row in joined["fact_transactions"]:
        assert row["transfer_cashout_flag"] == expected_facts[row["txn_id"]]["transfer_cashout_flag"]
        assert row["detected_at"] == "2026-10-26T10:00:00Z"
    assert compare(source["fact_transactions"], joined["fact_transactions"]) == []
    assert len(joined["fact_transactions"]) == len(curated["fact_transactions"]) == 4
    ring = exported["rings"][0]
    assert ring["ring_id"] == "7" and ring["account_count"] == 5
    assert ring["transaction_count"] == 4
    assert ring["internal_amount"] == Decimal("20363.00")
    assert ring["transfer_cashout_amount"] == Decimal("20001.00")
    assert ring["detection_run_id"] == "run-1"
    assert "graph_run" not in curated  # Joins don't mutate their inputs.


def test_two_runs_same_account_require_explicit_selection_and_keep_history(source, curated, tmp_path):
    first = bundle(source)
    later = copy.deepcopy(source)
    later["dim_account"][0]["risk_score"] = 35
    later["fact_transactions"][0]["transfer_cashout_flag"] = 1
    second = bundle(later, "run-2", "2026-10-26T12:00:00Z")
    paths = [write_export(item, tmp_path) for item in (first, second)]
    loaded = [read_export(path) for path in paths]
    left = join_curated(curated, loaded, detection_run_id="run-1")
    right = join_curated(curated, loaded, detection_run_id="run-2")
    aid = source["dim_account"][0]["account_id"]
    assert next(row for row in left["dim_account"] if row["account_id"] == aid)["risk_score"] == 10
    assert next(row for row in right["dim_account"] if row["account_id"] == aid)["risk_score"] == 35
    assert all(row["detection_run_id"] == "run-2" for row in right["fact_transactions"])
    assert len(left["dim_account"]) == len(right["dim_account"]) == 5
    assert len(left["fact_transactions"]) == len(right["fact_transactions"]) == 4
    assert left["graph_rings"][0]["detection_run_id"] != right["graph_rings"][0]["detection_run_id"]
    assert first["accounts"][0]["risk_score"] == 10
    with pytest.raises(FileExistsError):
        write_export(first, tmp_path)


def test_unmatched_left_joins_are_null_and_never_preserve_stale_signals(source, curated):
    exported = bundle(source)
    target = copy.deepcopy(curated)
    target["dim_account"].append(dict(account_id="unseen", risk_score=99))
    new_fact = dict(target["fact_transactions"][0], txn_id="f" * 64, transfer_cashout_flag=1)
    target["fact_transactions"].append(new_fact)
    joined = join_curated(target, [exported], detection_run_id="run-1")
    account, fact = joined["dim_account"][-1], joined["fact_transactions"][-1]
    assert account["risk_score"] is None and account["detection_run_id"] is None
    assert fact["transfer_cashout_flag"] is None and fact["detected_at"] is None
    assert account["graph_signal_present"] is fact["graph_signal_present"] is False
    assert len(joined["fact_transactions"]) == 5


@pytest.mark.parametrize("case", ["duplicate_account", "duplicate_txn", "missing_account", "unscored", "outside_window"])
def test_bad_source_snapshot_rejected(source, case):
    if case == "duplicate_account":
        source["dim_account"].append(source["dim_account"][0])
    elif case == "duplicate_txn":
        source["fact_transactions"].append(source["fact_transactions"][0])
    elif case == "missing_account":
        source["dim_account"].pop()
    elif case == "unscored":
        source["dim_account"][0]["risk_score"] = None
    else:
        with pytest.raises(ValueError, match="outside"):
            build_export(source, detection_run_id="run", detected_at="2026-10-26T10:00:00Z", window_start_step=2, window_end_step=2)
        return
    with pytest.raises(ValueError):
        bundle(source)


@pytest.mark.parametrize("run,stamp,start,end", [
    ("../escape", "2026-10-26T10:00:00Z", 1, 2),
    ("run", "2026-10-26T10:00:00", 1, 2),
    ("run", "2026-10-26T10:00:00Z", 3, 2),
])
def test_invalid_run_metadata_refused(source, run, stamp, start, end):
    with pytest.raises(ValueError):
        build_export(source, detection_run_id=run, detected_at=stamp, window_start_step=start, window_end_step=end)


def test_timezone_normalized_and_export_integrity_checked(source, tmp_path):
    data = bundle(source, stamp="2026-10-26T06:00:00-04:00")
    assert data["metadata"]["detected_at"] == "2026-10-26T10:00:00Z"
    path = write_export(data, tmp_path)
    assert read_export(path) == data
    target = path / "accounts" / "part-00000.jsonl"
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="checksum"):
        read_export(path)


def test_mixed_runs_and_duplicate_bundles_never_join(source, curated):
    data = bundle(source)
    with pytest.raises(ValueError, match="duplicate detection run"):
        join_curated(curated, [data, data], detection_run_id="run-1")
    with pytest.raises(ValueError, match="unavailable"):
        join_curated(curated, [data], detection_run_id="missing")
    data["accounts"][0]["detection_run_id"] = "other-run"
    with pytest.raises(ValueError, match="mixed detection"):
        join_curated(curated, [data], detection_run_id="run-1")


def test_local_parquet_adapter_requires_completion_marker_and_matches_t05(source, tmp_path):
    class Row(dict):
        def asDict(self):
            return dict(self)
    class Spark:
        def __init__(self):
            self.read = self
        def parquet(self, path):
            class Frame:
                def toLocalIterator(self):
                    return iter(Row(row) for row in source[Path(path).name])
            return Frame()
    marker = tmp_path / "_EXPORT_COMPLETE.json"
    marker.write_text(json.dumps({name: len(rows) for name, rows in source.items()}))
    assert tables_from_parquet(Spark(), tmp_path) == source
    (tmp_path / "_EXPORT_INCOMPLETE").touch()
    with pytest.raises(ValueError, match="incomplete"):
        tables_from_parquet(Spark(), tmp_path)


def test_offline_join_cli_writes_a_new_file(source, curated, tmp_path):
    directory = write_export(bundle(source), tmp_path)
    input_path = tmp_path / "curated.json"
    input_path.write_bytes(snapshot_bytes(curated))
    output = tmp_path / "joined.json"
    args = ["join", str(input_path), str(directory), "--detection-run-id", "run-1", "--output", str(output)]
    main(args)
    result = json.loads(output.read_bytes())
    assert result["graph_run"]["detection_run_id"] == "run-1"
    assert len(result["fact_transactions"]) == 4
    with pytest.raises(FileExistsError):
        main(args)
