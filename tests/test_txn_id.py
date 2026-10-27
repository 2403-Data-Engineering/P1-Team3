from __future__ import annotations

import csv
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from contracts.transaction_event import (
    GRAPH_SCORE_PROPERTY,
    SCHEMA_VERSION,
    TransactionEventError,
    build_transaction_event,
    compute_txn_id,
    events_from_csv,
    idempotency_key,
    payload_digest,
    validate_transaction_event,
)

FIXTURE = Path(__file__).parent / "fixtures" / "paysim_replay_sample.csv"
INGESTED_AT = datetime(2026, 10, 26, 23, 0, 0, tzinfo=timezone.utc)

CUSTOMER_TRANSFER = {
    "step": 1,
    "type": "TRANSFER",
    "amount": "10000.50",
    "nameOrig": "C2222222222",
    "oldbalanceOrg": "15000.00",
    "newbalanceOrig": "4999.50",
    "nameDest": "C3333333333",
    "oldbalanceDest": "200.00",
    "newbalanceDest": "10200.50",
}

MERCHANT_PAYMENT = {
    "step": 1,
    "type": "PAYMENT",
    "amount": "181.00",
    "nameOrig": "C1234567890",
    "oldbalanceOrg": "181.00",
    "newbalanceOrig": "0.00",
    "nameDest": "M111111111",
    "oldbalanceDest": "0",
    "newbalanceDest": "0",
}


def _txn_kwargs(row: dict, source_file: str, row_number: int) -> dict:
    dest_old = row["oldbalanceDest"]
    dest_new = row["newbalanceDest"]
    return {
        "step": int(row["step"]),
        "type": row["type"],
        "amount": Decimal(str(row["amount"])),
        "name_orig": row["nameOrig"],
        "old_balance_org": Decimal(str(row["oldbalanceOrg"])),
        "new_balance_orig": Decimal(str(row["newbalanceOrig"])),
        "name_dest": row["nameDest"],
        "old_balance_dest": None if dest_old in ("", None) else Decimal(str(dest_old)),
        "new_balance_dest": None if dest_new in ("", None) else Decimal(str(dest_new)),
        "source_file": source_file,
        "row_number": row_number,
    }


def test_txn_id_is_deterministic():
    first = compute_txn_id(**_txn_kwargs(CUSTOMER_TRANSFER, "paysim_clean.csv", 3))
    second = compute_txn_id(**_txn_kwargs(CUSTOMER_TRANSFER, "paysim_clean.csv", 3))
    assert first == second
    assert len(first) == 64


def test_replay_of_same_logical_row_keeps_txn_id():
    event_a = build_transaction_event(
        CUSTOMER_TRANSFER,
        source_file="paysim_clean.csv",
        row_number=3,
        ingested_at=INGESTED_AT,
    )
    later = datetime(2026, 10, 27, 1, 0, 0, tzinfo=timezone.utc)
    event_b = build_transaction_event(
        CUSTOMER_TRANSFER,
        source_file="paysim_clean.csv",
        row_number=3,
        ingested_at=later,
    )
    assert event_a["txn_id"] == event_b["txn_id"]
    assert event_a["event_id"] == event_a["txn_id"]
    assert event_a["ingested_at"] != event_b["ingested_at"]


def test_duplicate_looking_rows_get_distinct_txn_ids():
    events = events_from_csv(FIXTURE, ingested_at=INGESTED_AT)
    first, second = events[0], events[1]
    assert first["payload"] == second["payload"]
    assert first["txn_id"] != second["txn_id"]
    assert first["row_number"] == 1
    assert second["row_number"] == 2


def test_source_path_is_not_part_of_txn_id():
    left = compute_txn_id(**_txn_kwargs(CUSTOMER_TRANSFER, "paysim_clean.csv", 3))
    right = compute_txn_id(
        **_txn_kwargs(CUSTOMER_TRANSFER, r"C:\data\paysim_clean.csv", 3)
    )
    assert left == right


def test_merchant_dest_balances_are_null_not_zero():
    event = build_transaction_event(
        MERCHANT_PAYMENT,
        source_file="paysim_replay_sample.csv",
        row_number=1,
        ingested_at=INGESTED_AT,
    )
    assert event["payload"]["oldbalanceDest"] is None
    assert event["payload"]["newbalanceDest"] is None
    assert event["payload"]["oldbalanceDest"] != 0
    assert event["payload"]["newbalanceDest"] != 0


def test_customer_dest_balances_are_preserved():
    event = build_transaction_event(
        CUSTOMER_TRANSFER,
        source_file="paysim_replay_sample.csv",
        row_number=3,
        ingested_at=INGESTED_AT,
    )
    assert event["payload"]["oldbalanceDest"] == pytest.approx(200.0)
    assert event["payload"]["newbalanceDest"] == pytest.approx(10200.50)


def test_empty_csv_cells_become_null_then_merchant_stays_null():
    row = dict(MERCHANT_PAYMENT)
    row["oldbalanceDest"] = ""
    row["newbalanceDest"] = ""
    event = build_transaction_event(
        row,
        source_file="paysim_replay_sample.csv",
        row_number=1,
        ingested_at=INGESTED_AT,
    )
    assert event["payload"]["oldbalanceDest"] is None
    assert event["payload"]["newbalanceDest"] is None


def test_schema_rejects_missing_name_orig():
    row = dict(CUSTOMER_TRANSFER)
    row["nameOrig"] = ""
    with pytest.raises(TransactionEventError, match="nameOrig"):
        build_transaction_event(
            row,
            source_file="paysim_replay_sample.csv",
            row_number=1,
            ingested_at=INGESTED_AT,
        )


def test_schema_rejects_unknown_type():
    row = dict(CUSTOMER_TRANSFER)
    row["type"] = "WIRE"
    with pytest.raises(TransactionEventError, match="type"):
        build_transaction_event(
            row,
            source_file="paysim_replay_sample.csv",
            row_number=1,
            ingested_at=INGESTED_AT,
        )


def test_schema_rejects_fraud_labels_on_operational_event():
    row = dict(CUSTOMER_TRANSFER)
    row["isFraud"] = "1"
    with pytest.raises(TransactionEventError, match="isFraud"):
        build_transaction_event(
            row,
            source_file="paysim_replay_sample.csv",
            row_number=1,
            ingested_at=INGESTED_AT,
        )


def test_schema_rejects_malformed_amount():
    row = dict(CUSTOMER_TRANSFER)
    row["amount"] = "not-a-number"
    with pytest.raises(TransactionEventError, match="amount"):
        build_transaction_event(
            row,
            source_file="paysim_replay_sample.csv",
            row_number=1,
            ingested_at=INGESTED_AT,
        )


def test_validate_rejects_extra_properties_and_label_fields():
    event = build_transaction_event(
        CUSTOMER_TRANSFER,
        source_file="paysim_replay_sample.csv",
        row_number=3,
        ingested_at=INGESTED_AT,
    )
    tainted = deepcopy(event)
    tainted["isFraud"] = 1
    with pytest.raises(TransactionEventError, match="schema validation"):
        validate_transaction_event(tainted)


def test_csv_resort_does_not_collapse_duplicate_looking_rows(tmp_path: Path):
    events = events_from_csv(FIXTURE, ingested_at=INGESTED_AT)
    duplicate_ids = {events[0]["txn_id"], events[1]["txn_id"]}
    assert len(duplicate_ids) == 2

    with FIXTURE.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    resorted = tmp_path / "resorted.csv"
    with resorted.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reversed(rows))

    resorted_events = events_from_csv(
        resorted,
        ingested_at=INGESTED_AT,
        source_file="paysim_replay_sample.csv",
    )
    resorted_dupes = [
        event
        for event in resorted_events
        if event["payload"]["nameDest"] == "M111111111"
    ]
    assert len(resorted_dupes) == 2
    assert resorted_dupes[0]["txn_id"] != resorted_dupes[1]["txn_id"]


def test_payload_digest_is_stable_when_a_unique_row_moves():
    digest_at_row_3 = payload_digest(
        step=1,
        type="TRANSFER",
        amount=Decimal("10000.50"),
        name_orig="C2222222222",
        old_balance_org=Decimal("15000.00"),
        new_balance_orig=Decimal("4999.50"),
        name_dest="C3333333333",
        old_balance_dest=Decimal("200.00"),
        new_balance_dest=Decimal("10200.50"),
    )
    moved = compute_txn_id(**_txn_kwargs(CUSTOMER_TRANSFER, "paysim_replay_sample.csv", 1))
    unmoved = compute_txn_id(
        **_txn_kwargs(CUSTOMER_TRANSFER, "paysim_replay_sample.csv", 3)
    )
    assert moved != unmoved
    assert digest_at_row_3 == payload_digest(
        step=1,
        type="TRANSFER",
        amount=Decimal("10000.50"),
        name_orig="C2222222222",
        old_balance_org=Decimal("15000.00"),
        new_balance_orig=Decimal("4999.50"),
        name_dest="C3333333333",
        old_balance_dest=Decimal("200.00"),
        new_balance_dest=Decimal("10200.50"),
    )


def test_idempotency_key_is_txn_id():
    event = build_transaction_event(
        CUSTOMER_TRANSFER,
        source_file="paysim_replay_sample.csv",
        row_number=3,
        ingested_at=INGESTED_AT,
    )
    assert idempotency_key(event) == event["txn_id"]


def test_risk_score_name_is_locked():
    assert GRAPH_SCORE_PROPERTY == "risk_score"
    assert SCHEMA_VERSION == "v1"


def test_fixture_csv_round_trip_has_four_valid_events():
    events = events_from_csv(FIXTURE, ingested_at=INGESTED_AT)
    assert len(events) == 4
    assert events[0]["schema_version"] == "v1"
    assert events[2]["source_step"] == 1
    assert events[3]["source_step"] == 2
    ids = [event["txn_id"] for event in events]
    assert len(set(ids)) == 4
