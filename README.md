# PaySim graph fraud detection — P1-Team3

Members: Silas Bucur, Seth Gleason, Sai Palepu.

This repository contains a fixture-tested, batch-first implementation toward the
hybrid AWS architecture in [PLAN.md](PLAN.md). It includes contracts and eval-label
separation, idempotent graph-ingest code, required detection code, a four-table
exporter, shared local/Glue transforms, versioned graph exports, reporting SQL,
and Terraform definitions.

## What's deployed vs. not

**Nothing was deployed for this repo-only delivery.** No AWS stack, hosted Neo4j,
live Athena views, scheduler, Power BI report or paid runtime was created. The
reported evidence is local fixture tests and Terraform schema validation. Live
Neo4j/GDS, actual Spark/Parquet execution, S3/Glue, native Athena/Hive SQL, BI
refresh and measured costs remain unverified. SQLite reporting tests are not
native Athena validation. No live teardown was necessary.

- [Verification checklist](docs/VERIFICATION_CHECKLIST.md): evidence and pending live checks.
- [Architecture](docs/ARCHITECTURE.md): delivered code vs. the unchanged target plan.
- [Implementation status](IMPLEMENTATION_PLAN.md) and [handoff](AGENT_HANDOFF.md).
- [Project requirements](P1-specifications.md) and [required signals](Fraud-Reference.md).

## Quick-start: local fixtures only

Use Python 3.13 (the tested runtime). In a new environment, optionally create
and activate a virtual environment, then install the project requirements:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest -q
```

Dependency installation can use the internet; it does not download PaySim.
Tests use the committed small fixtures and fake Neo4j/Spark/cloud interfaces.
They do not require credentials, a database server or a Spark JVM. The reporting
suite uses Python's built-in SQLite engine with a local timestamp-function
adapter. Full-suite results are recorded in AGENT_HANDOFF.md.

To exercise the data split, sample, shared transform, duplicate replay,
reconciliation and ZIP packaging locally, choose fresh artifact filenames:

```powershell
python -c "from pathlib import Path; import pandas as pd; from download_data import write_ops_and_eval; write_ops_and_eval(pd.read_csv('tests/fixtures/paysim_labeled_sample.csv'), ops_path=Path('artifacts/paysim_clean.csv'), eval_path=Path('artifacts/eval/paysim_eval_labels.csv'))"
python -m pipeline.sample artifacts/paysim_clean.csv artifacts/events.jsonl --source-file paysim_clean.csv --ingested-at 2026-10-26T00:00:00Z
python -m pipeline.local artifacts/events.jsonl --output artifacts/curated
python -m pipeline.local artifacts/events.jsonl artifacts/events.jsonl --output artifacts/replayed
python -m pipeline.reconcile artifacts/curated/tables.json artifacts/replayed/tables.json
python -m pipeline.package artifacts/paysim_pipeline.zip
```

These commands use four fixture rows, not the full dataset. Expected facts: 4,
amount sum: 20363.00, account rows: 5. Duplicate-looking source rows remain
separate transactions; replaying their IDs does not create new facts. The sampler
and packager refuse existing output files; local curated snapshots deliberately
replace their dedicated tables.json. Do not point them at unrelated output.
`artifacts/` is gitignored. No command in this offline quick-start opens Neo4j or AWS.

## Running each stage

| Stage | Local/fixture entrypoint | Runtime boundary |
|---|---|---|
| Contracts / eval split | Above split helper; `python -m pytest tests/test_txn_id.py tests/test_eval_sidecar.py -q` | `download_data.py` main optionally downloads PaySim from Kaggle and removes its cache; it was not run for this delivery |
| Graph ingest | `python -m pytest tests/test_neo4j_ingest.py -q` | Future `python database_creation_script.py` reads root paysim_clean.csv and writes local Neo4j using env credentials; describe/approve writes first |
| Required detection | `python -m pytest tests/test_detection.py -q` | Future `python -m detection.pipeline` writes graph properties and rebuilds its GDS projection; scheduled/windowed only, after approval |
| Four-table baseline | `python -m pytest tests/test_star_schema.py -q` | Future `python batching_python_driver.py --output artifacts/new-baseline` reads local Neo4j and writes Parquet; needs PySpark/JVM and a new output path |
| Shared batch / Glue | Offline quick-start; `python -m pytest tests/test_pipeline.py -q` | `pipeline/glue_job.py` is a Glue entrypoint, not a plain local command. No job/upload was run; see [pipeline instructions](pipeline/README.md) |
| Versioned graph export / joins | `python -m pytest tests/test_graph_exports.py -q` | Local completed graph JSON or Parquet input, with explicit run ID, completion timestamp and step bounds; commands below |
| Reporting | `python -m pytest tests/test_reporting.py -q` | SQL views, Hive DDL templates and a dashboard specification; no deployed views or report. See [reporting instructions](reporting/README.md) |
| Infrastructure | Existing initialized checkout: `terraform -chdir=infra validate` | Definitions only; no plan/apply/destroy is part of this quick-start. See [infra instructions](infra/README.md) for provider/backend prerequisites |

Future graph runtime credentials use `db_uri`, `db_user`, `db_password` only.
[.env.example](.env.example) contains names with empty values; configure a private
ignored `.env` or process environment locally. Never record real credentials in
source, examples, commands or logs. Ingest reads the CSV with the Python driver;
manual copying into Neo4j's import folder is no longer required. Do not load the
eval sidecar into Neo4j. The exporter accepts local loopback endpoints only.

PySpark remains a separately documented runtime prerequisite, not a dependency
installed by the tests. Its JVM/Python pairing and schema types are in
[STAR_SCHEMA.md](docs/STAR_SCHEMA.md). A real GDS installation/compatible server
is needed for detection; connected-component fixture behavior does not prove
Louvain correctness.

Once a completed local T0.5 Parquet export exists, the following are local-file
operations (the --parquet adapter starts local Spark, not a database reader):

```powershell
python -m detection.exports export artifacts/new-baseline --parquet --output artifacts/graph --detection-run-id batch-001 --detected-at 2026-10-26T10:00:00Z --window-start-step 1 --window-end-step 24
python -m detection.exports join artifacts/curated/tables.json artifacts/graph/graph_signals/detection_run_id=batch-001 --detection-run-id batch-001 --output artifacts/joined.json
python -m reporting.registry artifacts/graph/graph_signals/detection_run_id=batch-001 --output artifacts/runs.jsonl
```

Replace the example timestamp/window with the actual completed detection scope;
all graph facts must be within it. The examples are not claims that a baseline
or detection run exists. Immutable run directories and joined/registry output
files refuse overwrite. See [graph export contract](detection/EXPORTS.md).

## Graph and reporting model

Nodes are `:Account {id}` for both customers and merchants, sourced from nameOrig
and nameDest. **Every transaction relationship has the single type
`:TRANSACTION`**; the PaySim category is the `type` property:

```cypher
(:Account {id: 'sender'})-[:TRANSACTION {
  txn_id: '<contract SHA-256>', type: 'TRANSFER', step: 1
}]->(:Account {id: 'receiver'})
```

There are no separate PAYMENT/TRANSFER/CASH_OUT relationship types. step is a
simulated ordinal hour, not a real timestamp. MERGE and unique keys express the
replay-safe identity model; live constraint support still needs verification.

The star schema contains dim_account, dim_transaction_type, dim_date and
fact_transactions at txn_id grain. Required graph results use risk_score and
versioned account/transaction/ring exports. Reports select one completed run;
community IDs cannot be merged across runs. Evaluation fraud rates come only
from the separate label sidecar and never feed scoring.

[Dashboard specification](reporting/DASHBOARD_SPEC.md) maps every required visual
to a view, fields and chart type for Power BI Import or another BI tool. There
is no .pbix or deployed dashboard. Threshold 30 is provisional; choosing and
justifying a real threshold remains a live/data-review step.

## Next steps and cleanup

Complete the live gates in [the verification checklist](docs/VERIFICATION_CHECKLIST.md)
before describing the pipeline as deployed or reconciled end to end. Streaming
with Kinesis/Lambda and Redshift remain deferred stretches; the delivered batch
prototype uses Athena SQL design and local Neo4j code instead.

There is nothing deployed by this delivery to tear down. For any future approved
deployment, follow [the teardown runbook](docs/TEARDOWN.md); retain external state,
review data/query-result deletion and obtain explicit destruction approval.
Do not delete state files as a substitute for removing resources. No commits or
pushes are performed by these delivery sessions.