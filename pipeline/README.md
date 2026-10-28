# T2 batch code (not run in AWS)

The shared core is `transform.py`. It imports no Spark or AWS libraries until
the Parquet adapter is invoked. Local and Glue entrypoints call the same core;
they do not read Neo4j or run the old export's keyset pagination loop. Nothing
connects to a service merely by importing these modules.

## Sample and local fixture run

The sampling rule is **the first 100,000 data rows of the unlabeled operational
CSV in its original order**, or all rows if fewer exist. This is a bounded,
reproducible demo slice with temporal/ordering bias, not a random or stratified
evaluation sample. No full dataset was downloaded or sampled in this session.

The sampler writes contract-valid JSONL, preserving the original source basename
and original 1-based row numbers, rather than recomputing IDs against the sample
filename. Use the same ops basename as the eval sidecar and graph ingest. A
fixed ingestion timestamp makes sample bytes reproducible. Never pass a labeled
CSV; extra columns, including evaluation labels, are refused.

Example local commands using only the committed small fixture:

```powershell
python -m pipeline.sample tests/fixtures/paysim_replay_sample.csv sample-events.jsonl --source-file paysim_clean.csv --ingested-at 2026-10-26T00:00:00Z
python -m pipeline.local sample-events.jsonl --output local-curated
python -m pipeline.local sample-events.jsonl sample-events.jsonl --output local-curated
python -m pipeline.reconcile local-curated/tables.json local-curated/tables.json
```

Choose new output paths: sampling uses exclusive creation and refuses overwrite.
The local runner deliberately replaces `tables.json` atomically, so its folder
must be dedicated to this snapshot. Repeating input does not append facts.
Local JSON files serialize money as decimal strings. With an installed PySpark
and compatible JVM, `pipeline.local --parquet` also writes the same tables to
`<output>/parquet/` through the shared Spark adapter. Spark is not installed in
the session's Python environment; actual Spark/Parquet execution remains untested.
Existing project requirements suffice for the pure Python fixture path.

## Grain and types

- Reuse the v1 contract's parser, merchant-null policy, validation, and `txn_id`
  calculation. Verify the supplied ID against the payload/source coordinates and
  verify `source_step == payload.step`. Ingestion time is excluded from facts.
- Deduplicate on `txn_id`. Original duplicate-looking CSV rows remain distinct
  because their row numbers differ. Exact replay collapses; inconsistent IDs or
  payloads fail instead of silently replacing financial data.
- Monetary fields use `Decimal` and Parquet `decimal(38,10)`. This preserves up
  to ten decimal places from the v1 JSON event representation. Values needing
  rounding or more than 28 integer digits fail explicitly. The existing v1 event
  builder emits JSON float numbers; this task does not redesign that contract.
- Preserve the earlier export's PaySim fields and date/type/account dimensions.
  Integer columns use Spark long/Glue bigint. Account IDs and source fields are
  strings. Only destination balances are nullable. Merchant destination balances
  remain null. No ground-truth labels or fabricated graph scores are produced.
- The type dictionary now lives in `contracts.transaction_event.TRANSACTION_TYPE_KEYS`:
  CASH_IN=1, CASH_OUT=2, DEBIT=3, PAYMENT=4, TRANSFER=5. T0.5 should reuse it.
  `dim_transaction_type` always contains all five entries.
- `date_key` is the step. Reuse the old export's synthetic 2026-03-01 origin and
  `step - 1` ordinal hours; date/hour/weekdays are derived from that convention,
  not from ingestion time. Parquet date is a date; datetime is an ISO string.
- `dim_account` contains unique account IDs only. Graph signals and versioned
  joins are deferred to T3/T0.5; do not confuse these rows with verified graph
  exports. All four tables have explicit schemas, including empty tables.

## Glue artifact and snapshot protocol

`python -m pipeline.package <new-local-path>.zip` builds a dependency ZIP with
the shared `pipeline/`, `contracts/`, and contract JSON schema. Schema loading
uses package resources so it works from a ZIP. This command never uploads.
The Terraform job definition expects the following future raw-bucket objects:

- `artifacts/glue_job.py`: the entrypoint copied from `pipeline/glue_job.py`.
- `artifacts/paysim_pipeline.zip`: the matching dependency archive.
- `events/sample.jsonl`: the operational event sample (never the eval sidecar).
- `manifests/sample.json`: an authoritative complete input manifest such as
  `{"events":["s3://<raw-bucket>/events/sample.jsonl"]}`.

Uploading any of these is outside this code-only session. Raw objects should be
immutable and the manifest published after its inputs. The job reads only the
explicit manifest, rejects cross-bucket/out-of-prefix event references, and
limits input to 200,000 raw records, 128 MiB, and 1,000 objects. These bounds are
deliberate for the ~100K-row prototype; this is not a full-scale Spark ETL design.
Transforming the bounded snapshot on the driver ensures identical semantics to
the local path, then Spark writes explicit-schema Parquet.

All rows in a run are the **complete authoritative snapshot**, not a delta.
Output is `curated/batch/snapshots/<content-sha256>/<table>/`. The content hash
ignores input order, duplicates and ingestion timestamps. Replays overwrite
the same table paths; there is no append and Glue bookmarks are disabled.
Different input snapshots occupy different prefixes. Catalog table locations
point to one snapshot each: queries must use those tables, never recursively
scan every snapshot directory. This prevents old/new snapshots being counted
together. Historical snapshot storage must be removed only under a separately
approved retention/teardown process.

All four writes finish before catalog registration. Glue tables are created or
updated to explicit unpartitioned Parquet locations with no crawler. The raw
JSONL audit table is defined in Terraform and can contain replay copies; it is
not the deduplicated reporting fact. T2 does not run a reconciliation against
the missing T0.5 oracle or claim AWS registration succeeded.

Maximum job concurrency is one, retries are disabled, and the same job owns its
output. Replacement writes and the four catalog updates are not a multi-table
transaction. A failure during overwrite or catalog updates can leave incomplete
data or mixed table versions; rerun the same complete manifest and reconcile
before allowing reporting. Do not run the local Spark writer against live S3
prefixes, and do not launch another job against this output concurrently.

## Reconciliation

`python -m pipeline.reconcile <expected> <actual>` accepts local `tables.json`,
CSV facts, or a Parquet file/directory (the latter requires local PySpark/JVM).
It rejects duplicate `txn_id`s, compares counts and exact Decimal amount sums
for the union of `(step, type)` groups, prints differences, and exits 1 on any
mismatch. Supply a local export/downloaded copy when available; no S3 reads or
Neo4j queries are embedded in the reconciliation script. The future T0.5
baseline must contain `txn_id`, `step`, `type`, and `amount`.

## Terraform, IAM, cost, and verification boundary

`infra/glue.tf` defines Glue 5.0, two G.1X workers, a ten-minute timeout, no retries,
one concurrent run, the artifact/manifest paths, and the shared module ZIP.
Glue supplies Spark/boto3; `jsonschema==4.25.1` is an additional Python module.
Future job startup needs access to its package source; no package installation
in AWS was attempted. A run uses two DPUs; estimate compute as
`2 * billed_hours * regional_DPU_hour_price`, plus S3/catalog/logging charges.
A nominal ten-minute run is one-third DPU-hour. No actual run cost was measured,
and timeout/budget alerts are not hard spend caps.

T1's role already grants project-bucket reads and curated writes/deletes, project
catalog updates and root log writes. T2 adds writes only to precreated `/error`
and `/output` log groups, each with seven-day retention. No broad Glue or S3
managed policy is attached. The deployment/operator needs separate pass-role,
job-start, artifact-upload and raw-upload permissions; the job cannot upload raw
events. No credentials are stored in job arguments. No secrets service is needed
because this job does not connect to Neo4j.

Fixture tests exercise real shared transforms/local snapshot persistence and
fake S3/Glue/Spark orchestration, ZIP schema loading, sampling, replay, nulls,
bad records, exact aggregates and reconciliation mismatches. Terraform validate
checks the local provider schema. Neither these checks nor the status
`code-complete (fixture-tested; not run in AWS)` prove a deployed pipeline.
