"""
Load the unlabeled operational PaySim CSV into Neo4j.

Accounts are MERGE'd on id. TRANSACTION relationships are MERGE'd on
txn_id so a second load does not create duplicate edges. Ground-truth
labels are not ingested.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping

from decimal import Decimal

from dotenv import load_dotenv

from contracts.transaction_event import apply_merchant_balance_nulls, txn_id_for_csv_row
from download_data import LABEL_COLUMNS, OPS_CSV_NAME

ENV_URI = "db_uri"
ENV_USER = "db_user"
ENV_PASSWORD = "db_password"

ACCOUNT_ID_CONSTRAINT = (
    "CREATE CONSTRAINT account_id IF NOT EXISTS "
    "FOR (a:Account) REQUIRE a.id IS UNIQUE"
)
TRANSACTION_TXN_ID_CONSTRAINT = (
    "CREATE CONSTRAINT transaction_txn_id IF NOT EXISTS "
    "FOR ()-[r:TRANSACTION]-() REQUIRE r.txn_id IS UNIQUE"
)

UPSERT_TRANSACTIONS_CYPHER = """
UNWIND $rows AS row
MERGE (orig:Account {id: row.nameOrig})
MERGE (dest:Account {id: row.nameDest})
MERGE (orig)-[t:TRANSACTION {txn_id: row.txn_id}]->(dest)
SET t.type = row.type,
    t.amount = row.amount,
    t.step = row.step,
    t.oldbalanceOrg = row.oldbalanceOrg,
    t.newbalanceOrig = row.newbalanceOrig,
    t.oldbalanceDest = row.oldbalanceDest,
    t.newbalanceDest = row.newbalanceDest,
    t.source_file = row.source_file,
    t.row_number = row.row_number
"""

COUNT_CYPHER = """
MATCH (a:Account)
WITH count(a) AS accounts
OPTIONAL MATCH ()-[t:TRANSACTION]->()
RETURN accounts, count(t) AS transactions
"""

BATCH_SIZE = 5000


class MissingNeo4jConfigError(RuntimeError):
    """Raised when Neo4j environment variables are missing."""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        raise MissingNeo4jConfigError(
            f"environment variable {name} is required for Neo4j ingest"
        )
    return str(value)


def neo4j_connect_args() -> tuple[str, str, str]:
    return require_env(ENV_URI), require_env(ENV_USER), require_env(ENV_PASSWORD)


def rows_from_ops_csv(
    csv_path: str | Path, *, source_file: str = OPS_CSV_NAME
) -> list[dict[str, Any]]:
    path = Path(csv_path)
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("operational CSV has no header")
        present_labels = [
            column for column in LABEL_COLUMNS if column in reader.fieldnames
        ]
        if present_labels:
            raise ValueError(
                "operational ingest refuses labeled CSV columns: "
                f"{present_labels}. Use {OPS_CSV_NAME}, not the eval sidecar."
            )
        for row_number, row in enumerate(reader, start=1):
            txn_id = txn_id_for_csv_row(
                row, source_file=source_file, row_number=row_number
            )
            old_dest, new_dest = apply_merchant_balance_nulls(
                row["nameDest"],
                _csv_decimal(row["oldbalanceDest"], allow_null=True),
                _csv_decimal(row["newbalanceDest"], allow_null=True),
            )
            rows.append(
                {
                    "txn_id": txn_id,
                    "source_file": source_file,
                    "row_number": row_number,
                    "nameOrig": row["nameOrig"],
                    "nameDest": row["nameDest"],
                    "type": row["type"],
                    "amount": float(_csv_decimal(row["amount"])),
                    "step": int(row["step"]),
                    "oldbalanceOrg": float(_csv_decimal(row["oldbalanceOrg"])),
                    "newbalanceOrig": float(_csv_decimal(row["newbalanceOrig"])),
                    "oldbalanceDest": None if old_dest is None else float(old_dest),
                    "newbalanceDest": None if new_dest is None else float(new_dest),
                }
            )
    return rows


def _csv_decimal(value: Any, *, allow_null: bool = False) -> Decimal | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        if allow_null:
            return None
        raise ValueError("numeric field is required")
    return Decimal(str(value))


def merge_into_graph(
    accounts: MutableMapping[str, dict[str, Any]],
    transactions: MutableMapping[str, dict[str, Any]],
    row: Mapping[str, Any],
) -> None:
    """In-memory MERGE: unique Account.id and unique TRANSACTION.txn_id."""
    orig = str(row["nameOrig"])
    dest = str(row["nameDest"])
    txn_id = str(row["txn_id"])
    accounts[orig] = {"id": orig}
    accounts[dest] = {"id": dest}
    transactions[txn_id] = dict(row)


def load_rows_idempotent(
    rows: Iterable[Mapping[str, Any]],
    *,
    accounts: MutableMapping[str, dict[str, Any]] | None = None,
    transactions: MutableMapping[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    account_store = accounts if accounts is not None else {}
    txn_store = transactions if transactions is not None else {}
    for row in rows:
        merge_into_graph(account_store, txn_store, row)
    return account_store, txn_store


def batched(rows: list[dict[str, Any]], size: int = BATCH_SIZE) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def ensure_constraints(session: Any) -> None:
    session.run(ACCOUNT_ID_CONSTRAINT)
    session.run(TRANSACTION_TXN_ID_CONSTRAINT)


def upsert_transactions(session: Any, rows: list[dict[str, Any]]) -> None:
    for batch in batched(rows):
        session.run(UPSERT_TRANSACTIONS_CYPHER, rows=batch)


def graph_counts(session: Any) -> tuple[int, int]:
    record = session.run(COUNT_CYPHER).single()
    if record is None:
        return 0, 0
    return int(record["accounts"]), int(record["transactions"])


def load_ops_csv(
    session: Any,
    csv_path: str | Path,
    *,
    source_file: str = OPS_CSV_NAME,
) -> tuple[int, int]:
    rows = rows_from_ops_csv(csv_path, source_file=source_file)
    ensure_constraints(session)
    upsert_transactions(session, rows)
    return graph_counts(session)


def main() -> None:
    from neo4j import GraphDatabase

    load_dotenv()
    uri, user, password = neo4j_connect_args()
    ops_path = Path.cwd() / OPS_CSV_NAME
    if not ops_path.exists():
        raise FileNotFoundError(
            f"{ops_path} not found. Run download_data.py first "
            "(unlabeled operational CSV only)."
        )

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            accounts, transactions = load_ops_csv(session, ops_path)
        print(
            f"Loaded {ops_path.name}: {accounts} accounts, "
            f"{transactions} transactions (idempotent on txn_id)."
        )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
