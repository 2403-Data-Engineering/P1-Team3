from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from database_creation_script import (
    ACCOUNT_ID_CONSTRAINT,
    ENV_PASSWORD,
    ENV_URI,
    ENV_USER,
    TRANSACTION_TXN_ID_CONSTRAINT,
    UPSERT_TRANSACTIONS_CYPHER,
    MissingNeo4jConfigError,
    load_ops_csv,
    load_rows_idempotent,
    neo4j_connect_args,
    rows_from_ops_csv,
)
from download_data import OPS_CSV_NAME, write_ops_and_eval
from contracts.transaction_event import events_from_csv

OPS_FIXTURE = Path(__file__).parent / "fixtures" / "paysim_replay_sample.csv"
LABELED_FIXTURE = Path(__file__).parent / "fixtures" / "paysim_labeled_sample.csv"


def test_double_load_keeps_same_account_and_transaction_counts():
    rows = rows_from_ops_csv(OPS_FIXTURE, source_file=OPS_CSV_NAME)
    accounts, transactions = load_rows_idempotent(rows)
    first = (len(accounts), len(transactions))
    load_rows_idempotent(rows, accounts=accounts, transactions=transactions)
    second = (len(accounts), len(transactions))
    assert first == second
    assert first[1] == 4
    assert first[0] == len({row["nameOrig"] for row in rows} | {row["nameDest"] for row in rows})


def test_upsert_cypher_merges_on_txn_id_and_does_not_create_bare_edges():
    cypher = " ".join(UPSERT_TRANSACTIONS_CYPHER.split())
    assert "MERGE (orig)-[t:TRANSACTION {txn_id: row.txn_id}]->(dest)" in cypher
    assert "CREATE (orig)-[:TRANSACTION" not in cypher
    assert "CREATE (a)-[:TRANSACTION" not in cypher


def test_uniqueness_constraints_cover_account_id_and_txn_id():
    assert "REQUIRE a.id IS UNIQUE" in ACCOUNT_ID_CONSTRAINT
    assert "REQUIRE r.txn_id IS UNIQUE" in TRANSACTION_TXN_ID_CONSTRAINT
    assert "TRANSACTION" in TRANSACTION_TXN_ID_CONSTRAINT
    cypher_file = Path("ingest/constraints.cypher").read_text(encoding="utf-8")
    assert "REQUIRE a.id IS UNIQUE" in cypher_file
    assert "REQUIRE r.txn_id IS UNIQUE" in cypher_file


def test_ingest_rows_match_eval_sidecar_txn_ids(tmp_path: Path):
    labeled = pd.read_csv(LABELED_FIXTURE)
    ops_path = tmp_path / OPS_CSV_NAME
    eval_path = tmp_path / "eval" / "paysim_eval_labels.csv"
    write_ops_and_eval(labeled, ops_path=ops_path, eval_path=eval_path)

    ingest_rows = rows_from_ops_csv(ops_path, source_file=OPS_CSV_NAME)
    eval_ids = pd.read_csv(eval_path)["txn_id"].tolist()
    assert [row["txn_id"] for row in ingest_rows] == eval_ids

    from datetime import datetime, timezone

    events = events_from_csv(
        ops_path,
        ingested_at=datetime(2026, 10, 26, tzinfo=timezone.utc),
        source_file=OPS_CSV_NAME,
    )
    assert [event["txn_id"] for event in events] == eval_ids


def test_refuses_labeled_csv():
    with pytest.raises(ValueError, match="labeled CSV"):
        rows_from_ops_csv(LABELED_FIXTURE, source_file=OPS_CSV_NAME)


def test_connect_args_require_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(ENV_URI, raising=False)
    monkeypatch.delenv(ENV_USER, raising=False)
    monkeypatch.delenv(ENV_PASSWORD, raising=False)
    with pytest.raises(MissingNeo4jConfigError, match=ENV_URI):
        neo4j_connect_args()


def test_connect_args_read_env_only(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_URI, "bolt://localhost:7687")
    monkeypatch.setenv(ENV_USER, "neo4j")
    monkeypatch.setenv(ENV_PASSWORD, "from-env-not-source")
    uri, user, password = neo4j_connect_args()
    assert uri == "bolt://localhost:7687"
    assert user == "neo4j"
    assert password == "from-env-not-source"


def test_source_has_no_hardcoded_neo4j_password():
    source = Path("database_creation_script.py").read_text(encoding="utf-8")
    assert "NEO4J_PASSWORD" not in source
    assert "require_env(ENV_PASSWORD)" in source
    assert "GraphDatabase.driver(uri, auth=(user, password))" in source


def test_fake_session_double_load_uses_merge_cypher():
    class FakeSession:
        def __init__(self) -> None:
            self.accounts: dict[str, dict] = {}
            self.transactions: dict[str, dict] = {}
            self.queries: list[str] = []

        def run(self, query: str, **params):
            self.queries.append(" ".join(query.split()))
            if "UNWIND" in query:
                load_rows_idempotent(
                    params["rows"],
                    accounts=self.accounts,
                    transactions=self.transactions,
                )
                return self
            if "count(a)" in query:
                return self
            return self

        def single(self):
            return {
                "accounts": len(self.accounts),
                "transactions": len(self.transactions),
            }

    session = FakeSession()
    first = load_ops_csv(session, OPS_FIXTURE, source_file=OPS_CSV_NAME)
    second = load_ops_csv(session, OPS_FIXTURE, source_file=OPS_CSV_NAME)
    assert first == second
    assert any("REQUIRE a.id IS UNIQUE" in query for query in session.queries)
    assert any("REQUIRE r.txn_id IS UNIQUE" in query for query in session.queries)
    assert any("MERGE (orig)-[t:TRANSACTION {txn_id: row.txn_id}]->(dest)" in query for query in session.queries)
