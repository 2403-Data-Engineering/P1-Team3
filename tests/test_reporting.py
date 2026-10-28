from __future__ import annotations

import copy
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from batching_python_driver import normalize_account, transaction_to_fact
from contracts.transaction_event import events_from_csv
from database_creation_script import rows_from_ops_csv
from detection.exports import build_export, write_export
from download_data import split_ops_and_eval
from pipeline.transform import transform_events
from reporting.registry import write_registry

ROOT = Path(__file__).resolve().parent.parent
SQL = ROOT / "reporting" / "sql"
FIXTURE = ROOT / "tests" / "fixtures" / "paysim_replay_sample.csv"
LABELED = FIXTURE.with_name("paysim_labeled_sample.csv")


def insert_rows(db, name, rows):
    columns = {row[1] for row in db.execute(f'PRAGMA table_info("{name}")')}
    for row in rows:
        selected = {key: str(value) if isinstance(value, Decimal) else value for key, value in row.items() if key in columns}
        names = list(selected)
        db.execute(f'INSERT INTO "{name}" ({",".join(names)}) VALUES ({",".join("?" for _ in names)})', list(selected.values()))


def row(db, query):
    return dict(db.execute(query).fetchone())


@pytest.fixture
def db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    # This adapter supplies the semantics of the sole Athena-specific function.
    db.create_function("from_iso8601_timestamp", 1, lambda value:
                       datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat(timespec="microseconds"))
    db.executescript("""
      CREATE TABLE fact_transactions (txn_id TEXT, step INTEGER, type_key INTEGER, type TEXT,
        amount NUMERIC, nameOrig TEXT, nameDest TEXT, date_key INTEGER);
      CREATE TABLE dim_account (account_id TEXT);
      CREATE TABLE dim_date (date_key INTEGER, date TEXT);
      CREATE TABLE dim_transaction_type (type_key INTEGER, type_name TEXT);
      CREATE TABLE graph_runs (detection_run_id TEXT, detected_at TEXT, window_start_step INTEGER,
        window_end_step INTEGER, window_kind TEXT, status TEXT);
      CREATE TABLE graph_accounts (account_id TEXT, detection_run_id TEXT, detected_at TEXT,
        window_start_step INTEGER, window_end_step INTEGER, window_kind TEXT,
        community_id INTEGER, risk_score INTEGER, fan_out_flag INTEGER, fan_in_flag INTEGER,
        drain_behavior_flag INTEGER, transfer_cashout_flag INTEGER, dense_community_flag INTEGER,
        in_cycle_flag INTEGER, guilt_by_association_flag INTEGER);
      CREATE TABLE graph_transactions (txn_id TEXT, transfer_cashout_flag INTEGER, detection_run_id TEXT,
        detected_at TEXT, window_start_step INTEGER, window_end_step INTEGER, window_kind TEXT);
      CREATE TABLE graph_rings (ring_id TEXT, community_id INTEGER, account_count INTEGER,
        transaction_count INTEGER, internal_amount TEXT, dense_community_flag INTEGER,
        detection_run_id TEXT, detected_at TEXT, window_start_step INTEGER, window_end_step INTEGER, window_kind TEXT);
      CREATE TABLE eval_labels (txn_id TEXT, isFraud INTEGER, isFlaggedFraud INTEGER);
    """)
    events = events_from_csv(FIXTURE, source_file="paysim_clean.csv", ingested_at=datetime(2026, 10, 26, tzinfo=timezone.utc))
    tables = transform_events(events)
    for name, rows in tables.items():
        insert_rows(db, name, rows)
    facts = [transaction_to_fact(dict(record, transfer_cashout_flag=None)) for record in rows_from_ops_csv(FIXTURE, source_file="paysim_clean.csv")]
    accounts = [normalize_account(dict(account_id=account["account_id"], community_id=7, risk_score=0,
                                       dense_community_flag=1)) for account in tables["dim_account"]]
    old = build_export(dict(dim_account=accounts, fact_transactions=facts), detection_run_id="old-run",
                       detected_at="2026-10-26T10:00:00Z", window_start_step=1, window_end_step=2)
    newer = copy.deepcopy(old)
    # A fractional-second newer run tests real timestamp sorting, not string order.
    for rows in [newer["accounts"], newer["transactions"], newer["rings"], [newer["metadata"]]]:
        for item in rows:
            item["detection_run_id"] = "new-run"
            item["detected_at"] = "2026-10-26T10:00:00.500000Z"
    for item in newer["accounts"]:
        item["risk_score"] = 40 if item["account_id"] == "C3333333333" else 0
    for item in newer["transactions"]:
        item["transfer_cashout_flag"] = 1 if item["txn_id"] == events[2]["txn_id"] else None
    for bundle in (old, newer):
        insert_rows(db, "graph_runs", [dict(bundle["metadata"], status="complete")])
        for target, source in (("graph_accounts", "accounts"), ("graph_transactions", "transactions"), ("graph_rings", "rings")):
            insert_rows(db, target, bundle[source])
    _, labels = split_ops_and_eval(pd.read_csv(LABELED), ops_source_file="paysim_clean.csv")
    insert_rows(db, "eval_labels", labels.to_dict("records"))
    for path in sorted(SQL.glob("*.sql")):
        db.executescript(path.read_text(encoding="utf-8"))
    yield db
    db.close()


def test_every_visual_view_executes_and_latest_run_does_not_multiply_rows(db):
    names = [entry[0] for entry in db.execute("SELECT name FROM sqlite_master WHERE type='view'")]
    for name in names:
        db.execute(f'SELECT * FROM "{name}"').fetchall()
    kpi = row(db, "SELECT * FROM rpt_kpis")
    assert kpi["detection_run_id"] == "new-run"
    assert kpi["total_transactions"] == 4 and kpi["total_accounts"] == 5
    assert kpi["scored_accounts"] == 5 and kpi["assessed_transactions"] == 4
    assert kpi["flagged_accounts"] == 1 and kpi["rings_detected"] == 1
    assert kpi["total_flagged_volume"] == pytest.approx(20001)
    assert row(db, "SELECT * FROM rpt_data_quality")["selected_run_count"] == 1
    assert all(value == 0 for key, value in row(db, "SELECT * FROM rpt_data_quality").items() if key != "selected_run_count")


def test_visual_results_match_fixture_grains_and_totals(db):
    histogram = [dict(item) for item in db.execute("SELECT * FROM rpt_risk_distribution ORDER BY bucket_start")]
    assert [(r["bucket_start"], r["account_count"]) for r in histogram] == [(0, 4), (40, 1)]
    time = [dict(item) for item in db.execute("SELECT * FROM rpt_flagged_volume_over_time ORDER BY step")]
    assert [(r["step"], r["flagged_volume"]) for r in time] == [(1, 10000.5), (2, 10000.5)]
    assert time[0]["simulated_date"] == "2026-03-01"
    assert row(db, "SELECT * FROM rpt_top_10_accounts WHERE risk_rank=1")["account_id"] == "C3333333333"
    ring = row(db, "SELECT * FROM rpt_ring_volume")
    assert ring["internal_volume"] == pytest.approx(20363)
    assert ring["transaction_count"] == 4
    assert ring["full_graph_internal_volume"] == pytest.approx(20363)
    types = {item["type_name"]: dict(item) for item in db.execute("SELECT * FROM rpt_transaction_type_comparison")}
    assert len(types) == 5 and types["PAYMENT"]["model_flag_rate"] == 0
    assert types["TRANSFER"]["model_flag_rate"] == 1
    assert types["DEBIT"]["transaction_count"] == 0 and types["DEBIT"]["model_flag_rate"] is None


def test_chosen_run_overrides_latest_and_run_history_is_distinguishable(db):
    db.execute("DROP VIEW rpt_settings")
    db.execute("CREATE VIEW rpt_settings AS SELECT 'old-run' AS chosen_detection_run_id, 30 AS risk_threshold")
    kpi = row(db, "SELECT * FROM rpt_kpis")
    assert kpi["detection_run_id"] == "old-run"
    assert kpi["flagged_accounts"] == 0 and kpi["total_flagged_volume"] == 0
    assert kpi["total_transactions"] == 4


def test_unknown_chosen_run_does_not_fall_back_to_latest(db):
    db.execute("DROP VIEW rpt_settings")
    db.execute("CREATE VIEW rpt_settings AS SELECT 'absent-run' AS chosen_detection_run_id, 30 AS risk_threshold")
    kpi = row(db, "SELECT * FROM rpt_kpis")
    assert kpi["detection_run_id"] is None and kpi["rings_detected"] is None
    assert kpi["total_transactions"] == 4 and kpi["scored_accounts"] == 0


def test_threshold_changes_model_metrics_only(db):
    before = [tuple(item) for item in db.execute("SELECT * FROM rpt_ground_truth_fraud_by_type ORDER BY type_key")]
    db.execute("DROP VIEW rpt_settings")
    db.execute("CREATE VIEW rpt_settings AS SELECT CAST(NULL AS VARCHAR) AS chosen_detection_run_id, 50 AS risk_threshold")
    kpi = row(db, "SELECT * FROM rpt_kpis")
    assert kpi["flagged_accounts"] == 0
    assert kpi["total_flagged_volume"] == pytest.approx(10000.5)  # Relationship flag still fires.
    assert [tuple(item) for item in db.execute("SELECT * FROM rpt_ground_truth_fraud_by_type ORDER BY type_key")] == before


def test_ground_truth_separate_from_flags_with_label_coverage(db):
    rates = {item["type_name"]: dict(item) for item in db.execute("SELECT * FROM rpt_ground_truth_fraud_by_type")}
    assert rates["PAYMENT"]["ground_truth_fraud_rate"] == 0.5
    assert rates["PAYMENT"]["label_coverage"] == 1
    assert rates["TRANSFER"]["ground_truth_fraud_rate"] == 1
    assert rates["DEBIT"]["ground_truth_fraud_rate"] is None
    before = row(db, "SELECT * FROM rpt_kpis")
    db.execute("UPDATE eval_labels SET isFraud=0")
    assert row(db, "SELECT * FROM rpt_kpis") == before
    assert row(db, "SELECT * FROM rpt_ground_truth_fraud_by_type WHERE type_name='TRANSFER'")["ground_truth_fraud_rate"] == 0
    for path in SQL.glob("*.sql"):
        if path.name != "90_evaluation_only.sql":
            assert "eval_labels" not in path.read_text()


def test_partial_labels_do_not_treat_missing_labels_as_legitimate(db):
    db.execute("DELETE FROM eval_labels WHERE isFraud=0")
    payment = row(db, "SELECT * FROM rpt_ground_truth_fraud_by_type WHERE type_name='PAYMENT'")
    assert payment["labeled_transactions"] == 1 and payment["unlabeled_transactions"] == 1
    assert payment["ground_truth_fraud_rate"] == 1 and payment["label_coverage"] == 0.5
    db.execute("INSERT INTO eval_labels SELECT * FROM eval_labels")
    payment = row(db, "SELECT * FROM rpt_ground_truth_fraud_by_type WHERE type_name='PAYMENT'")
    assert payment["labeled_transactions"] == 0 and payment["ground_truth_fraud_rate"] is None


def test_missing_graph_signals_are_unknown_not_zero_scores(db):
    db.execute("DELETE FROM graph_runs")
    kpi = row(db, "SELECT * FROM rpt_kpis")
    assert kpi["total_transactions"] == 4 and kpi["total_accounts"] == 5
    assert kpi["scored_accounts"] == 0 and kpi["flagged_accounts"] is None
    assert kpi["total_flagged_volume"] is None and kpi["assessed_transactions"] == 0
    assert kpi["rings_detected"] is None
    assert row(db, "SELECT * FROM rpt_data_quality")["unassessed_transactions"] == 4


def test_duplicate_signal_keys_flag_quality_without_fact_multiplication(db):
    db.execute("INSERT INTO graph_accounts SELECT * FROM graph_accounts WHERE detection_run_id='new-run'")
    assert row(db, "SELECT * FROM rpt_kpis")["total_transactions"] == 4
    assert row(db, "SELECT * FROM rpt_data_quality")["duplicate_signal_account_keys"] == 5


def test_registry_prepares_only_verified_local_runs_and_refuses_duplicates(tmp_path):
    records = rows_from_ops_csv(FIXTURE, source_file="paysim_clean.csv")
    facts = [transaction_to_fact(record) for record in records]
    ids = sorted({record[field] for record in records for field in ("nameOrig", "nameDest")})
    bundle = build_export(dict(dim_account=[normalize_account(dict(account_id=aid, risk_score=0)) for aid in ids],
                               fact_transactions=facts), detection_run_id="sample", detected_at="2026-10-26T00:00:00Z",
                          window_start_step=1, window_end_step=2)
    path = write_export(bundle, tmp_path)
    out = tmp_path / "runs.jsonl"
    rows = write_registry([path], out)
    assert json.loads(out.read_text())["status"] == "complete" and len(rows) == 1
    with pytest.raises(FileExistsError):
        write_registry([path], out)
    with pytest.raises(ValueError, match="duplicate run"):
        write_registry([path, path], tmp_path / "duplicate.jsonl")
