# T4 SQL reporting (not run in Athena)

Apply `sql/*.sql` in filename order **only after later authorization**, in the
T1 Glue database containing the T2 curated tables. These are standard Athena
engine 3 `CREATE VIEW` statements; rerunning creation requires reviewed drops or
replacement of existing view definitions. Nothing automatically executes SQL,
uploads data or configures a connector. See `DASHBOARD_SPEC.md` for chart fields,
flag/ground-truth definitions and Import-mode behavior.

Source contracts:

| Source | Producer / grain |
|---|---|
| `fact_transactions` | T2 canonical Parquet, unique txn_id; type_key, step/date_key, amount, nameOrig/nameDest |
| `dim_account`, `dim_date`, `dim_transaction_type` | T2 unique account/date/type keys; frozen contract type dictionary |
| `graph_accounts`, `graph_transactions`, `graph_rings` | T3 immutable JSONL, unique `(detection_run_id, account_id/txn_id/ring_id)` respectively |
| `graph_runs` | Flat registry of verified complete T3 runs, unique detection_run_id and canonical UTC detected_at/window metadata |
| `eval_labels` | T0.2 evaluation-only CSV sidecar, unique txn_id; isFraud ground truth; never an operational scoring input |

T3 JSONL contains decimal strings, so `graph_rings.internal_amount` is a string
at the external-table boundary and explicitly cast to decimal(38,10) for report
context. Curated money remains decimal(38,10). The current sample amounts fit
that reporting range; overflows fail rather than silently round or substitute.
Account/table identifiers are case-insensitive in these SQL definitions.

`ddl/sources.sql` and `ddl/register_run.sql` are **unexecuted Hive/Athena DDL
templates** with `{{...}}` placeholders. They define the missing graph/registry
and eval sources; the four curated tables are already defined by T2. Graph
signal tables use explicit run partitions with locations pointing at the
individual `accounts/`, `transactions/`, and `rings/` folders. Their unused table
root locations are intentionally empty. Do not run MSCK over the mixed
graph_signals/ tree or point all tables at that tree: it contains incompatible
schemas and manifests.

Future publishing sequence, not performed in this session:

1. Verify the completed local T3 exports using `read_export` (checksums, counts,
   schema and run metadata). Build a new flat registry locally, for example:
   `python -m reporting.registry <run-directory> [<other-run-directory>] --output <new-runs.jsonl>`.
   The helper refuses duplicate IDs and existing output paths; it never uploads.
2. Under separate approval, copy the verified signal directories to the curated
   bucket without changing paths. Register all three table partitions for each
   run with `ddl/register_run.sql` after substituting the validated run ID/bucket.
   Publish the new authoritative registry only after those objects/partitions
   are ready. Keep exactly one registry row per completed run; do not append the
   same registry repeatedly. The registry's table location contains only flat
   run JSONL, never full T3 manifests or mixed signal files.
3. Keep eval CSV in its own evaluation-only prefix/bucket and use the exact
   T0.2 column order in the DDL. Set future BI/operator IAM accordingly; do not
   grant evaluation access to scoring jobs or place labels in events/. No IAM
   principal or permissions change was made here. T1's existing broad raw-bucket
   read prefix is not an evaluation access control; choose a separate eval
   bucket or review narrower permissions before deploying a sidecar there.
4. Select a completed run/threshold, create views, run quality/reconciliation
   checks, and configure the BI Import refresh. Freeze the selected run and
   current curated snapshot during refresh. Ground-truth views are standalone
   and do not depend on graph/model views. Unknown/missing graph data is surfaced
   through nulls/coverage and cannot justify a fully reconciled report.

`infra/athena.tf` defines only a future Athena engine 3 workgroup: enforced
100 MiB per-query scan cutoff, expected result bucket owner, SSE-S3 output under
curated/athena-results/, metrics, and tags. It does not deploy these SQL files,
create a BI user, start queries, or grant IAM access. `force_destroy=false`
preserves workgroup deletion safeguards. Future teardown must also handle query
results under the approved bucket data-deletion process.

Local verification uses Python's built-in SQLite engine over the committed CSV
fixtures and actual T0.2/T0.3/T0.5/T2 normalization. **All 16 view definitions are
executed unchanged**, and each view is selected. SQLite is supplied a local
`from_iso8601_timestamp` adapter with normalized UTC ordering, including the
fractional-second edge case. This substitutes only Athena's timestamp parser;
it does not prove native Athena type/catalog semantics. SQLite NUMERIC uses
floating arithmetic for non-integral amounts, so small result checks use the
fixture's exact half-unit amounts/approximate comparisons, not a claim of
arbitrary-precision financial validation. Athena retains decimal types.

**Not locally executed:** the two Hive external-table/partition DDL files
(SQLite has no S3, SerDe, Glue catalog or Hive partition model). No live Athena
query, actual Parquet read, ODBC/BI connection, query result encryption/scan-limit
enforcement, or IAM access was tested. Terraform validate checks provider schema
only. Status: **code-complete (fixture-tested; not run in Athena)**.
