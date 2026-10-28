# T3 versioned graph exports (code only)

`detection.exports` consumes **already computed** T0.5-style account and fact
tables. It does not invoke detection, open a database driver, upload to S3, or
schedule a service. The local export and join APIs are usable without Neo4j,
boto3 or PySpark installed. The optional local Parquet adapter requires Spark.

Each completed detection run needs an explicit immutable `detection_run_id`, a
timezone-aware `detected_at` (the detection completion time), and inclusive
`window_start_step`/`window_end_step`. Timestamps are normalized to UTC. Do not
generate a new completion time merely because an old snapshot is exported again.

The initial window policy is **full_graph_snapshot**: run T0.4 once per scheduled
batch or manually selected completed snapshot, covering exactly the graph built
from the declared step range. Export only after detection finishes, with graph
ingestion/detection paused during T0.5 reads. Never run GDS or score an event as
part of the per-transaction ingestion path. No scheduler is provisioned here.

The supplied facts must all be within the declared range; the exporter rejects
out-of-window rows rather than filtering an already scored graph. Account scores
must be present (zero is valid); it never substitutes zero for missing scores.
It cannot prove the operator ran detection on that exact topology: scope and
completion are attestations until live orchestration verifies them. For a future
rolling window, build/detect/export a separate scoped snapshot; filtering facts
after full-graph Louvain does not reproduce windowed communities. Community IDs
can change between runs and must always be interpreted with the run ID.

## Signal grains and layout

Every row carries `detection_run_id`, `detected_at`, window bounds and kind:

- `accounts`: one `account_id` per run, reusing T0.5 normalization and its required
  account signals/`risk_score`. Nullable graph properties remain null.
- `transactions`: one `txn_id` per run with the relationship
  `transfer_cashout_flag`. Reuses T0.5's contract identity validation; no graph
  internal IDs or evaluation labels appear.
- `rings`: one `ring_id` (string community_id) per run, with community_id, sorted
  account_ids, account_count, internal transaction_count, internal_amount,
  transfer_cashout_amount and dense_community_flag. Totals are computed once from
  source facts whose endpoints belong to the same community. Not every community
  is a flagged ring; consumers filter dense_community_flag explicitly. Aggregate
  amounts are exact Decimals, serialized as strings; they are not sums of the
  repeated account-level community totals.

S3-ready **local** directory layout, relative to the chosen root:

```text
graph_signals/detection_run_id=<run-id>/
  accounts/part-00000.jsonl
  transactions/part-00000.jsonl
  rings/part-00000.jsonl
  manifest.json
```

The JSONL partitions retain run metadata in their rows as well as their paths.
The manifest records schema version, window metadata, per-file SHA-256 and row
counts; it is written last as a completion marker. Run directories are created
exclusively and never overwritten. Failed/partial directories require inspection
and separate approval before deletion. Readers verify checksums/counts, fixed
relative paths, unique keys and consistent row metadata. Checksums detect file
changes, not malicious rewrites of both a manifest and its files.

The tree can later be copied under the curated bucket prefix without renaming.
No uploader, new Terraform resource, IAM expansion, or catalog deployment is
needed for this local-code deliverable. The existing T1/T2 scoped curated-bucket
permissions already cover future project object access; actual upload and table
registration need separately authorized work. JSON decimal strings need explicit
decimal casts when future reporting tables/views are defined.

## Explicit-run joins and local usage

`join_curated(curated, exports, detection_run_id=...)` selects exactly one run,
then left-joins account signals into `dim_account` on account_id and transaction
flags into facts on txn_id. Curated keys and run-specific signal keys must be
unique. Multiple bundles with the same run ID, mixed row versions, and unknown
run selection fail. There is no implicit latest-run or many-to-many history join.

Unmatched curated rows retain their grain and have null graph properties/run
metadata plus `graph_signal_present=false`; old signals are cleared instead of
being carried forward. Matched rows retain the selected run's timestamp/window.
The selected run's ring table and metadata are returned as `graph_rings` and
`graph_run`. Ring totals cover that entire detection snapshot, not an arbitrary
smaller report filter. Use account dimensions for sender and receiver joins;
do not join ring history on community_id alone.

Local JSON API/CLI input is a tables dictionary with `dim_account` and
`fact_transactions` as emitted by the T0.5 normalization. Curated join input is
the T2 `tables.json` format. Example future local commands:

```powershell
python -m detection.exports export graph-tables.json --output local-graph --detection-run-id batch-001 --detected-at 2026-10-26T10:00:00Z --window-start-step 1 --window-end-step 24
python -m detection.exports join curated-tables.json local-graph/graph_signals/detection_run_id=batch-001 --detection-run-id batch-001 --output joined-tables.json
```

`export --parquet <local-directory>` instead reads the account/fact Parquet
tables from a completed T0.5 output. It requires `_EXPORT_COMPLETE.json`, refuses
`_EXPORT_INCOMPLETE`, and checks row counts using an injected local Spark reader.
It never connects to Neo4j. JSON join output is exclusive: use a new filename.
This adapter materializes the sample in memory; it is not a full-scale graph
export system.

Fixture checks include two runs for the same account, explicit run selection,
no fact multiplication, exact correspondence to the T0.5-normalized sample,
ring amounts, unmatched rows, bad scope/version data, ZIP-independent imports,
local file integrity and fake Parquet reads. No real Parquet engine, Neo4j, AWS,
scheduled detection, or live graph/report agreement was exercised. Status:
**code-complete (fixture-tested; not run live)**.
