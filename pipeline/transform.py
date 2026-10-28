"""Shared local/Glue transformations at the contract's transaction grain."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from typing import Iterable, Mapping, Any

from contracts.transaction_event import (
    PAYSIM_FIELD_ORDER, TRANSACTION_TYPES, TRANSACTION_TYPE_KEYS, build_transaction_event,
    txn_id_for_csv_row, validate_transaction_event,
)

# Frozen dictionary: never enumerate only the types present in one batch.
TYPE_KEYS = TRANSACTION_TYPE_KEYS
assert set(TYPE_KEYS) == TRANSACTION_TYPES
START_DATE = datetime(2026, 3, 1)  # Existing export's synthetic calendar, not event time.
MONEY_FIELDS = ("amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest")
FACT_COLUMNS = ("txn_id", "source_file", "row_number", "source_step", *PAYSIM_FIELD_ORDER, "type_key", "date_key")
TABLE_COLUMNS = {
    "fact_transactions": FACT_COLUMNS,
    "dim_account": ("account_id",),
    "dim_transaction_type": ("type_key", "type_name"),
    "dim_date": ("date_key", "datetime", "date", "hour", "day_of_week"),
}


def money(value: Any) -> Decimal | None:
    """Exact decimal(38,10); reject overflow/rounding rather than lose value."""
    if value is None:
        return None
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            number = Decimal(str(value))
            scaled = number.quantize(Decimal("0.0000000001"))
            if not number.is_finite() or number < 0 or number != scaled or number >= Decimal(10) ** 28:
                raise ValueError("money is outside decimal(38,10)")
            return scaled
    except InvalidOperation as exc:
        raise ValueError("invalid money value") from exc


def event_to_fact(event: Mapping[str, Any]) -> dict[str, Any]:
    validate_transaction_event(event)
    payload = event["payload"]
    # Schema validation alone does not verify identity or merchant null semantics.
    canonical = build_transaction_event(
        payload, source_file=event["source_file"], row_number=event["row_number"],
        ingested_at=datetime.fromisoformat(event["ingested_at"].replace("Z", "+00:00")),
    )
    if (event["txn_id"] != txn_id_for_csv_row(
            payload, source_file=event["source_file"], row_number=event["row_number"])
            or event["source_step"] != payload["step"]):
        raise ValueError("event identity or source_step disagrees with payload")
    fact = {field: canonical["payload"][field] for field in PAYSIM_FIELD_ORDER}
    for field in MONEY_FIELDS:
        fact[field] = money(fact[field])
    fact.update(txn_id=event["txn_id"], source_file=canonical["source_file"],
                row_number=event["row_number"], source_step=event["source_step"],
                type_key=TYPE_KEYS[fact["type"]], date_key=fact["step"])
    return fact


def date_dimension(steps: Iterable[int]) -> list[dict[str, Any]]:
    """Shared synthetic calendar for CSV/Glue facts and the graph export."""
    dates = []
    for step in sorted(set(steps)):
        dt = START_DATE + timedelta(hours=step - 1)
        dates.append(dict(date_key=step, datetime=dt.isoformat(), date=dt.date().isoformat(),
                          hour=dt.hour, day_of_week=("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")[dt.weekday()]))
    return dates


def transform_events(events: Iterable[Mapping[str, Any]], *, max_records: int = 200_000) -> dict[str, list[dict[str, Any]]]:
    """Complete bounded snapshot. Exact replay dedupes; conflicting IDs fail closed."""
    facts: dict[str, dict[str, Any]] = {}
    for index, event in enumerate(events, 1):
        if index > max_records:
            raise ValueError("raw event count exceeds bounded batch limit")
        fact = event_to_fact(event)
        key = fact["txn_id"]
        if key in facts and facts[key] != fact:
            raise ValueError("conflicting payload for txn_id")
        facts[key] = fact
    rows = sorted(facts.values(), key=lambda row: row["txn_id"])
    accounts = sorted({r[field] for r in rows for field in ("nameOrig", "nameDest")})
    dates = date_dimension(r["step"] for r in rows)
    return {
        "fact_transactions": rows,
        "dim_account": [{"account_id": aid} for aid in accounts],
        "dim_transaction_type": [{"type_key": key, "type_name": name} for name, key in TYPE_KEYS.items()],
        "dim_date": dates,
    }


def spark_schema(table: str):
    """Lazy import keeps the shared transform usable without Spark/Glue installed."""
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType
    integers = {"step", "source_step", "row_number", "date_key", "type_key", "hour"}
    return StructType([
        StructField(name, DateType() if name == "date" else DecimalType(38, 10) if name in MONEY_FIELDS else
                    LongType() if name in integers else StringType(),
                    name in {"oldbalanceDest", "newbalanceDest"})
        for name in TABLE_COLUMNS[table]
    ])


def write_parquet(spark, tables, output: str) -> None:
    """Same normalized records and explicit schemas for local Spark and Glue."""
    for name, rows in tables.items():
        if name == "dim_date":
            from datetime import date
            rows = [dict(row, date=date.fromisoformat(row["date"])) for row in rows]
        spark.createDataFrame(rows, schema=spark_schema(name)).write.mode("overwrite").parquet(
            f"{output.rstrip('/')}/{name}")
