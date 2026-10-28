from __future__ import annotations

import copy
import json
import sys
import types
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import batching_python_driver as export
from contracts.transaction_event import TRANSACTION_TYPE_KEYS, events_from_csv
from database_creation_script import MissingNeo4jConfigError, rows_from_ops_csv
from pipeline.reconcile import compare
from pipeline.transform import transform_events

FIXTURE = Path(__file__).parent / "fixtures" / "paysim_replay_sample.csv"


# Test the real schema-construction functions without a Spark install/JVM.
@pytest.fixture(autouse=True)
def schema_types(monkeypatch):
    module = types.ModuleType("pyspark.sql.types")
    for name in ("StringType", "LongType", "DoubleType", "DateType"):
        setattr(module, name, type(name, (), {}))
    class DecimalType:
        def __init__(self, precision, scale):
            self.precision, self.scale = precision, scale
    class StructField:
        def __init__(self, name, dataType, nullable):
            self.name, self.dataType, self.nullable = name, dataType, nullable
    class StructType:
        def __init__(self, fields):
            self.fields = fields
    module.DecimalType, module.StructField, module.StructType = DecimalType, StructField, StructType
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", types.ModuleType("pyspark.sql"))
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", module)


class Result(list):
    def single(self):
        return self[0] if self else None


class Reader:
    def __init__(self, rows=None):
        self.transactions = copy.deepcopy(rows if rows is not None else rows_from_ops_csv(FIXTURE, source_file="paysim_clean.csv"))
        for row in self.transactions:
            row.setdefault("transfer_cashout_flag", 1 if row["type"] in {"TRANSFER", "CASH_OUT"} else None)
        ids = sorted({row[key] for row in self.transactions for key in ("nameOrig", "nameDest")})
        self.accounts = [{"account_id": account, "risk_score": 35, "community_id": 2,
                          "outDegree": 2.0, "unusualFanIn": True,
                          "community_total_amount": 123.45, "fraud_score": 999} for account in ids]
        self.calls, self.closed, self.count_reads = [], False, 0
        self.count_override = None

    def session(self, **kwargs):
        assert kwargs == {"default_access_mode": "READ"}
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, query, **params):
        self.calls.append((query, params))
        if query == export.COUNTS_QUERY:
            self.count_reads += 1
            values = dict(accounts=len(self.accounts), account_ids=len({a["account_id"] for a in self.accounts if a["account_id"] is not None}),
                          transactions=len(self.transactions), txn_ids=len({r["txn_id"] for r in self.transactions if r.get("txn_id") is not None}),
                          missing_txn_ids=sum(r.get("txn_id") is None for r in self.transactions))
            if self.count_override:
                values.update(self.count_override(self.count_reads))
            return Result([values])
        if query == export.ACCOUNTS_QUERY:
            return Result(self.accounts)
        assert query == export.TRANSACTIONS_QUERY
        return Result(sorted([row for row in self.transactions if row["txn_id"] > params["last_txn_id"]],
                             key=lambda row: row["txn_id"])[:params["batch_size"]])

    def close(self):
        self.closed = True


class SparkSink:
    def __init__(self):
        self.tables, self.schemas, self.writes = {}, {}, []
        self.read = self
        self.stopped = False
        self.corrupt_count = None

    def createDataFrame(self, rows, schema):
        for row in rows:
            assert set(row) == {field.name for field in schema.fields}
            for field in schema.fields:
                value = row[field.name]
                if value is None:
                    assert field.nullable, field.name
                elif type(field.dataType).__name__ == "DecimalType":
                    assert isinstance(value, Decimal)
                elif type(field.dataType).__name__ == "DateType":
                    assert isinstance(value, date)
                elif type(field.dataType).__name__ == "LongType":
                    assert isinstance(value, int) and not isinstance(value, bool)
        sink = self
        class Frame:
            def __init__(self):
                self.write, self.write_mode = self, None
            def mode(self, mode):
                assert mode in {"errorifexists", "append"}
                self.write_mode = mode
                return self
            def option(self, key, value):
                assert (key, value) == ("compression", "snappy")
                return self
            def partitionBy(self, key):
                assert key == "date"
                return self
            def parquet(self, path):
                if self.write_mode == "errorifexists":
                    assert path not in sink.tables
                    sink.tables[path] = []
                sink.tables[path].extend(copy.deepcopy(rows))
                sink.schemas[path] = schema
                sink.writes.append((Path(path).name, self.write_mode))
        return Frame()

    def parquet(self, path):
        sink = self
        class Frame:
            def count(self):
                return len(sink.tables[path]) + int(Path(path).name == sink.corrupt_count)
        return Frame()

    def stop(self):
        self.stopped = True


def test_main_exports_all_four_tables_with_read_only_fake_reader(monkeypatch, tmp_path, capsys):
    reader, spark = Reader(), SparkSink()
    monkeypatch.setattr(export, "create_driver", lambda: reader)
    monkeypatch.setattr(export, "create_spark", lambda: spark)
    output = tmp_path / "new"
    counts = export.main(["--output", str(output), "--batch-size", "2"])
    assert counts == {"dim_account": 5, "fact_transactions": 4, "dim_transaction_type": 5, "dim_date": 2}
    assert reader.closed and spark.stopped
    assert {Path(path).name for path in spark.tables} == set(counts)
    assert not (output / "_EXPORT_INCOMPLETE").exists()
    assert json.loads((output / "_EXPORT_COMPLETE.json").read_text()) == counts
    assert json.loads(capsys.readouterr().out) == counts
    assert [mode for table, mode in spark.writes if table == "fact_transactions"] == ["errorifexists", "append"]
    facts = spark.tables[str(output / "fact_transactions")]
    assert len({r["txn_id"] for r in facts}) == 4
    assert sum(r["transfer_cashout_flag"] == 1 for r in facts) == 2
    assert "elementId" not in export.TRANSACTIONS_QUERY
    assert "t.txn_id > $last_txn_id" in export.TRANSACTIONS_QUERY
    assert reader.count_reads == 2


def test_fixture_matches_shared_t2_transform_and_reconciliation():
    reader = Reader()
    actual = [fact for batch in export.transaction_batches(reader, 1) for fact in batch]
    events = events_from_csv(FIXTURE, source_file="paysim_clean.csv", ingested_at=datetime(2026, 10, 26, tzinfo=timezone.utc))
    expected = transform_events(events)["fact_transactions"]
    assert compare(expected, actual) == []
    for row, canonical in zip(actual, expected):
        assert {key: row[key] for key in canonical} == canonical
        assert isinstance(row["date"], date)
        if row["nameDest"].startswith("M"):
            assert row["oldbalanceDest"] is row["newbalanceDest"] is None


def test_schemas_score_flags_nullability_and_decimal_types():
    fields = {field.name: field for field in export.schema_for("dim_account").fields}
    assert "risk_score" in fields and "fraud_score" not in fields
    assert "a.risk_score AS risk_score" in export.ACCOUNTS_QUERY
    assert "fraud_score" not in export.ACCOUNTS_QUERY
    assert not fields["account_id"].nullable
    assert fields["risk_score"].nullable
    fact_fields = {field.name: field for field in export.schema_for("fact_transactions").fields}
    assert fact_fields["txn_id"].nullable is False
    assert fact_fields["transfer_cashout_flag"].nullable is True
    assert fact_fields["newbalanceDest"].nullable is True
    assert fact_fields["amount"].nullable is False
    assert (fact_fields["amount"].dataType.precision, fact_fields["amount"].dataType.scale) == (38, 10)
    account = export.normalize_account(Reader().accounts[0])
    assert account["risk_score"] == 35
    assert account["unusualFanIn"] == 1
    assert account["fan_out_flag"] is None
    assert account["community_total_amount"] == Decimal("123.45")
    assert "fraud_score" not in account


def test_frozen_types_empty_graph_and_new_output(tmp_path):
    spark = SparkSink()
    output = tmp_path / "empty"
    counts = export.export_star_schema(Reader([]), spark, output)
    assert counts == {"dim_account": 0, "fact_transactions": 0, "dim_transaction_type": 5, "dim_date": 0}
    assert spark.tables[str(output / "dim_transaction_type")] == [
        {"type_key": key, "type_name": name} for name, key in TRANSACTION_TYPE_KEYS.items()]
    # No replacement allowed even when output contains no financial rows.
    with pytest.raises(FileExistsError):
        export.export_star_schema(Reader([]), SparkSink(), output)


def test_output_refused_before_connection(monkeypatch, tmp_path):
    monkeypatch.setattr(export, "create_driver", lambda: pytest.fail("must not connect"))
    with pytest.raises(FileExistsError):
        export.main(["--output", str(tmp_path)])


@pytest.mark.parametrize("missing", [None, "", "not-a-contract-id"])
def test_missing_or_bad_txn_id_refused(missing):
    record = Reader().transactions[0]
    record["txn_id"] = missing
    with pytest.raises(ValueError, match="contract txn_id"):
        export.transaction_to_fact(record)


def test_legacy_relationships_refused_before_writes(tmp_path):
    reader = Reader()
    reader.transactions[0]["txn_id"] = None
    with pytest.raises(ValueError, match="unique txn_id"):
        export.export_star_schema(reader, SparkSink(), tmp_path / "new")
    assert not (tmp_path / "new").exists()


def test_stored_id_not_regenerated_and_invalid_flags_fail():
    row = Reader().transactions[0]
    row["txn_id"] = "0" * 64
    with pytest.raises(ValueError, match="stored txn_id"):
        export.transaction_to_fact(row)
    row = Reader().transactions[0]
    row["transfer_cashout_flag"] = 3
    with pytest.raises(ValueError, match="null, 0 or 1"):
        export.transaction_to_fact(row)
    row = Reader().transactions[0]
    row["source_file"] = None
    with pytest.raises(ValueError, match="source_file"):
        export.transaction_to_fact(row)


def test_nonadvancing_pagination_detected():
    class BrokenReader(Reader):
        def run(self, query, **params):
            if query == export.TRANSACTIONS_QUERY:
                return Result([self.transactions[0], self.transactions[0]])
            return super().run(query, **params)
    with pytest.raises(ValueError, match="cursor"):
        list(export.transaction_batches(BrokenReader(), 2))


@pytest.mark.parametrize("failure", ["graph_change", "written_count", "missing_account"])
def test_incomplete_exports_never_get_success_marker(tmp_path, failure):
    output, reader, spark = tmp_path / "new", Reader(), SparkSink()
    if failure == "graph_change":
        reader.count_override = lambda number: {"transactions": 5, "txn_ids": 5} if number == 2 else {}
    elif failure == "written_count":
        spark.corrupt_count = "fact_transactions"
    else:
        reader.accounts.pop()
    with pytest.raises(ValueError):
        export.export_star_schema(reader, spark, output)
    assert (output / "_EXPORT_INCOMPLETE").exists()
    assert not (output / "_EXPORT_COMPLETE.json").exists()


def test_main_closes_reader_and_spark_on_failure(monkeypatch, tmp_path):
    reader, spark = Reader(), SparkSink()
    spark.corrupt_count = "fact_transactions"
    monkeypatch.setattr(export, "create_driver", lambda: reader)
    monkeypatch.setattr(export, "create_spark", lambda: spark)
    with pytest.raises(ValueError):
        export.main(["--output", str(tmp_path / "new")])
    assert reader.closed and spark.stopped


def test_environment_connection_is_mocked_and_remote_endpoint_refused(monkeypatch):
    import dotenv
    neo4j = types.ModuleType("neo4j")
    neo4j.GraphDatabase = types.SimpleNamespace(driver=None)
    monkeypatch.setitem(sys.modules, "neo4j", neo4j)
    from neo4j import GraphDatabase
    monkeypatch.setattr(dotenv, "load_dotenv", lambda: None)
    monkeypatch.setenv("db_uri", "bolt://localhost:7687")
    monkeypatch.setenv("db_user", "fixture-user")
    monkeypatch.setenv("db_password", "fixture-placeholder")
    captured = []
    monkeypatch.setattr(GraphDatabase, "driver", lambda uri, auth: captured.append((uri, auth)) or Reader([]))
    export.create_driver()
    assert captured == [("bolt://localhost:7687", ("fixture-user", "fixture-placeholder"))]
    monkeypatch.setenv("db_uri", "neo4j+s://remote.invalid")
    with pytest.raises(ValueError, match="local loopback"):
        export.create_driver()
    assert len(captured) == 1
    monkeypatch.delenv("db_uri")
    with pytest.raises(MissingNeo4jConfigError):
        export.create_driver()
