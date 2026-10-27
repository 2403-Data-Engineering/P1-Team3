from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from contracts.transaction_event import TransactionEventError, events_from_csv
from download_data import (
    EVAL_COLUMNS,
    LABEL_COLUMNS,
    OPS_CSV_NAME,
    split_ops_and_eval,
    write_ops_and_eval,
)

LABELED_FIXTURE = Path(__file__).parent / "fixtures" / "paysim_labeled_sample.csv"
INGESTED_AT = datetime(2026, 10, 26, 23, 0, 0, tzinfo=timezone.utc)


def test_ops_output_has_no_label_columns(tmp_path: Path):
    labeled = pd.read_csv(LABELED_FIXTURE)
    ops_path = tmp_path / OPS_CSV_NAME
    eval_path = tmp_path / "eval" / "paysim_eval_labels.csv"
    write_ops_and_eval(labeled, ops_path=ops_path, eval_path=eval_path)

    ops = pd.read_csv(ops_path)
    for column in LABEL_COLUMNS:
        assert column not in ops.columns
    assert list(ops.columns) == [
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
    ]


def test_eval_sidecar_joins_ops_one_to_one_on_txn_id(tmp_path: Path):
    labeled = pd.read_csv(LABELED_FIXTURE)
    ops_path = tmp_path / OPS_CSV_NAME
    eval_path = tmp_path / "eval" / "paysim_eval_labels.csv"
    write_ops_and_eval(labeled, ops_path=ops_path, eval_path=eval_path)

    ops = pd.read_csv(ops_path)
    eval_df = pd.read_csv(eval_path)
    assert list(eval_df.columns) == list(EVAL_COLUMNS)
    assert len(eval_df) == len(ops)
    assert eval_df["txn_id"].is_unique
    assert set(eval_df["source_file"].unique()) == {OPS_CSV_NAME}

    events = events_from_csv(
        ops_path, ingested_at=INGESTED_AT, source_file=OPS_CSV_NAME
    )
    event_ids = [event["txn_id"] for event in events]
    assert event_ids == eval_df["txn_id"].tolist()

    merged = pd.DataFrame({"txn_id": event_ids}).merge(eval_df, on="txn_id", how="outer")
    assert len(merged) == len(ops)
    assert merged["isFraud"].notna().all()
    assert merged["isFlaggedFraud"].notna().all()


def test_duplicate_looking_rows_keep_distinct_labels():
    labeled = pd.read_csv(LABELED_FIXTURE)
    ops, eval_df = split_ops_and_eval(labeled, ops_source_file=OPS_CSV_NAME)
    payload_cols = [column for column in ops.columns if column not in LABEL_COLUMNS]
    assert ops.iloc[0][payload_cols].equals(ops.iloc[1][payload_cols])
    assert eval_df.loc[0, "txn_id"] != eval_df.loc[1, "txn_id"]
    assert int(eval_df.loc[0, "isFraud"]) == 0
    assert int(eval_df.loc[1, "isFraud"]) == 1


def test_split_requires_label_columns():
    labeled = pd.read_csv(LABELED_FIXTURE).drop(columns=["isFraud"])
    with pytest.raises(ValueError, match="isFraud"):
        split_ops_and_eval(labeled, ops_source_file=OPS_CSV_NAME)


def test_operational_event_path_still_rejects_labels(tmp_path: Path):
    labeled = pd.read_csv(LABELED_FIXTURE)
    labeled_path = tmp_path / "still_labeled.csv"
    labeled.to_csv(labeled_path, index=False)
    with pytest.raises(TransactionEventError, match="isFraud"):
        events_from_csv(labeled_path, ingested_at=INGESTED_AT)
