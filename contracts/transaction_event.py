"""v1 operational transaction event contract.

Idempotency
-----------
The idempotency key is ``txn_id``. The same ``txn_id`` MUST map to one fact
row and one graph relationship. Retries and CSV replay upsert / MERGE on
``txn_id``; they must not insert a second fact or ``CREATE`` a second edge.

``txn_id`` is SHA-256 (hex) over canonical PaySim fields plus the source CSV
basename and 1-based data ``row_number``. That distinguishes duplicate-looking
payloads in the same file. ``ingested_at`` is excluded so clock time cannot
split one logical row. Neo4j ``elementId`` is not a business key.

Merchant dest balances
----------------------
PaySim does not populate destination balances for merchant accounts
(``nameDest`` starting with ``M``). After parse, those fields are **null**,
never ``0``. Empty CSV cells are also null. Customer destination balances
are kept as numbers.

Operational vs evaluation
-------------------------
This schema rejects ``isFraud`` / ``isFlaggedFraud``. Ground-truth labels
belong on a sidecar dataset (T0.2), not the scoring path.

Graph score name
----------------
Account risk is stored as ``risk_score`` (Fraud-Reference formula). Do not
introduce ``fraud_score`` as a second name.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_VERSION = "v1"
SCHEMA_PATH = Path(__file__).with_name("transaction_event.v1.json")

TRANSACTION_TYPES = frozenset(
    {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
)
GRAPH_SCORE_PROPERTY = "risk_score"
MERCHANT_ID_PREFIX = "M"

PAYSIM_FIELD_ORDER = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
)

_CANONICAL_SEP = "\x1f"


class TransactionEventError(ValueError):
    """Raised when a row or event does not satisfy the v1 contract."""


def load_event_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def event_validator() -> Draft202012Validator:
    return Draft202012Validator(load_event_schema(), format_checker=FormatChecker())


def source_file_basename(source_file: str | Path) -> str:
    name = Path(source_file).name
    if not name or name in {".", ".."}:
        raise TransactionEventError("source_file basename is required")
    return name


def is_merchant_account(account_id: str) -> bool:
    return account_id.startswith(MERCHANT_ID_PREFIX)


def _canonical_number(value: Decimal | None) -> str:
    if value is None:
        return ""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text if text else "0"


def _as_decimal(value: Any, field: str, *, allow_null: bool) -> Decimal | None:
    if value is None:
        if allow_null:
            return None
        raise TransactionEventError(f"{field} is required")
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            if allow_null:
                return None
            raise TransactionEventError(f"{field} is required")
        value = stripped
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TransactionEventError(f"{field} is not numeric") from exc
    if not number.is_finite() or number < 0:
        raise TransactionEventError(f"{field} must be a finite number >= 0")
    return number


def _as_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or value is None or value == "":
        raise TransactionEventError(f"{field} must be an integer")
    try:
        if isinstance(value, int):
            number = value
        else:
            as_decimal = Decimal(str(value).strip())
            if as_decimal != as_decimal.to_integral_value():
                raise TransactionEventError(f"{field} must be an integer")
            number = int(as_decimal)
    except (InvalidOperation, ValueError) as exc:
        raise TransactionEventError(f"{field} must be an integer") from exc
    if number < 1:
        raise TransactionEventError(f"{field} must be >= 1")
    return number


def _as_text(value: Any, field: str) -> str:
    if value is None:
        raise TransactionEventError(f"{field} is required")
    text = str(value).strip()
    if not text:
        raise TransactionEventError(f"{field} is required")
    return text


def apply_merchant_balance_nulls(
    name_dest: str, old_balance_dest: Decimal | None, new_balance_dest: Decimal | None
) -> tuple[Decimal | None, Decimal | None]:
    if is_merchant_account(name_dest):
        return None, None
    return old_balance_dest, new_balance_dest


def payload_digest(
    *,
    step: int,
    type: str,
    amount: Decimal,
    name_orig: str,
    old_balance_org: Decimal,
    new_balance_orig: Decimal,
    name_dest: str,
    old_balance_dest: Decimal | None,
    new_balance_dest: Decimal | None,
) -> str:
    """SHA-256 of PaySim fields only (order-independent of CSV row position)."""
    parts = [
        str(step),
        type,
        _canonical_number(amount),
        name_orig,
        _canonical_number(old_balance_org),
        _canonical_number(new_balance_orig),
        name_dest,
        _canonical_number(old_balance_dest),
        _canonical_number(new_balance_dest),
    ]
    encoded = _CANONICAL_SEP.join(parts).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def txn_id_for_csv_row(
    row: Mapping[str, Any],
    *,
    source_file: str | Path,
    row_number: int,
) -> str:
    """Stable ``txn_id`` for a PaySim CSV row (labels must already be stripped)."""
    parsed = _parse_payload(row)
    return compute_txn_id(
        step=parsed["step"],
        type=parsed["type"],
        amount=parsed["amount"],
        name_orig=parsed["nameOrig"],
        old_balance_org=parsed["oldbalanceOrg"],
        new_balance_orig=parsed["newbalanceOrig"],
        name_dest=parsed["nameDest"],
        old_balance_dest=parsed["oldbalanceDest"],
        new_balance_dest=parsed["newbalanceDest"],
        source_file=source_file,
        row_number=row_number,
    )


def compute_txn_id(
    *,
    step: int,
    type: str,
    amount: Decimal,
    name_orig: str,
    old_balance_org: Decimal,
    new_balance_orig: Decimal,
    name_dest: str,
    old_balance_dest: Decimal | None,
    new_balance_dest: Decimal | None,
    source_file: str | Path,
    row_number: int,
) -> str:
    """Idempotency key: canonical payload + source basename + row number."""
    digest = payload_digest(
        step=step,
        type=type,
        amount=amount,
        name_orig=name_orig,
        old_balance_org=old_balance_org,
        new_balance_orig=new_balance_orig,
        name_dest=name_dest,
        old_balance_dest=old_balance_dest,
        new_balance_dest=new_balance_dest,
    )
    basename = source_file_basename(source_file)
    if row_number < 1:
        raise TransactionEventError("row_number must be >= 1")
    material = f"{digest}{_CANONICAL_SEP}{basename}{_CANONICAL_SEP}{row_number}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _decimal_to_json(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _parse_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    forbidden = {"isFraud", "isFlaggedFraud"}
    present = forbidden.intersection(row)
    if present:
        raise TransactionEventError(
            f"operational events must not include {sorted(present)}"
        )

    tx_type = _as_text(row.get("type"), "type")
    if tx_type not in TRANSACTION_TYPES:
        raise TransactionEventError(f"type is not a PaySim transaction type: {tx_type}")

    name_dest = _as_text(row.get("nameDest"), "nameDest")
    old_dest = _as_decimal(row.get("oldbalanceDest"), "oldbalanceDest", allow_null=True)
    new_dest = _as_decimal(row.get("newbalanceDest"), "newbalanceDest", allow_null=True)
    old_dest, new_dest = apply_merchant_balance_nulls(name_dest, old_dest, new_dest)

    return {
        "step": _as_int(row.get("step"), "step"),
        "type": tx_type,
        "amount": _as_decimal(row.get("amount"), "amount", allow_null=False),
        "nameOrig": _as_text(row.get("nameOrig"), "nameOrig"),
        "oldbalanceOrg": _as_decimal(
            row.get("oldbalanceOrg"), "oldbalanceOrg", allow_null=False
        ),
        "newbalanceOrig": _as_decimal(
            row.get("newbalanceOrig"), "newbalanceOrig", allow_null=False
        ),
        "nameDest": name_dest,
        "oldbalanceDest": old_dest,
        "newbalanceDest": new_dest,
    }


def build_transaction_event(
    row: Mapping[str, Any],
    *,
    source_file: str | Path,
    row_number: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    parsed = _parse_payload(row)
    if ingested_at.tzinfo is None:
        ingested_at = ingested_at.replace(tzinfo=timezone.utc)
    ingested_at = ingested_at.astimezone(timezone.utc)

    txn_id = compute_txn_id(
        step=parsed["step"],
        type=parsed["type"],
        amount=parsed["amount"],
        name_orig=parsed["nameOrig"],
        old_balance_org=parsed["oldbalanceOrg"],
        new_balance_orig=parsed["newbalanceOrig"],
        name_dest=parsed["nameDest"],
        old_balance_dest=parsed["oldbalanceDest"],
        new_balance_dest=parsed["newbalanceDest"],
        source_file=source_file,
        row_number=row_number,
    )
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": txn_id,
        "txn_id": txn_id,
        "source_file": source_file_basename(source_file),
        "row_number": row_number,
        "ingested_at": ingested_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_step": parsed["step"],
        "payload": {
            "step": parsed["step"],
            "type": parsed["type"],
            "amount": _decimal_to_json(parsed["amount"]),
            "nameOrig": parsed["nameOrig"],
            "oldbalanceOrg": _decimal_to_json(parsed["oldbalanceOrg"]),
            "newbalanceOrig": _decimal_to_json(parsed["newbalanceOrig"]),
            "nameDest": parsed["nameDest"],
            "oldbalanceDest": _decimal_to_json(parsed["oldbalanceDest"]),
            "newbalanceDest": _decimal_to_json(parsed["newbalanceDest"]),
        },
    }
    validate_transaction_event(event)
    return event


def validate_transaction_event(event: Mapping[str, Any]) -> None:
    errors = sorted(event_validator().iter_errors(event), key=lambda e: list(e.path))
    if errors:
        messages = "; ".join(err.message for err in errors)
        raise TransactionEventError(f"event failed schema validation: {messages}")
    if event.get("event_id") != event.get("txn_id"):
        raise TransactionEventError("event_id must equal txn_id for v1 replay")
    if event.get("schema_version") != SCHEMA_VERSION:
        raise TransactionEventError("unsupported schema_version")


def idempotency_key(event: Mapping[str, Any]) -> str:
    validate_transaction_event(event)
    return str(event["txn_id"])


def events_from_csv(
    csv_path: str | Path,
    *,
    ingested_at: datetime,
    source_file: str | Path | None = None,
) -> list[dict[str, Any]]:
    path = Path(csv_path)
    basename = source_file_basename(source_file or path)
    events: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise TransactionEventError("CSV has no header")
        for row_number, row in enumerate(reader, start=1):
            events.append(
                build_transaction_event(
                    row,
                    source_file=basename,
                    row_number=row_number,
                    ingested_at=ingested_at,
                )
            )
    return events
