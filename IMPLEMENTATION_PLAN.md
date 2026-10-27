# Implementation plan: hybrid AWS stream-and-batch migration

Architecture source of truth: `PLAN.md`. Do not replace that design.

This file tracks small, testable tasks for separate agent sessions. Status values: `todo` | `in_progress` | `verified`.

**Rules for each session:** read `PLAN.md`, this file, and `AGENT_HANDOFF.md`. Implement only the assigned task ID. Do not apply Terraform, start paid AWS services, or push git unless the user explicitly asks. Do not copy secrets into docs or code comments.

**Local-first:** complete Phase 0 (T0.1–T0.6) before introducing AWS services (Phase 2+). T2.1 may *design* IaC in parallel after T0.1, but must not `apply` until the user approves spend.

---

## Phase 0 — Local foundations (before AWS)

### T0.1 — Event and data contracts

| Field | Value |
|---|---|
| **Status** | verified |
| **Objective** | Define a versioned transaction event containing PaySim fields, source step, ingestion timestamp, and a stable event/transaction ID. Define merchant-balance nulls and idempotency/dedup rules as code plus tests. |
| **Inspect** | `PLAN.md` (step 1), `download_data.py`, `Fraud-Reference.md`, `batching_python_driver.py` (schemas), `docs/MIGRATION_REVIEW.md` (C1, C3) |
| **Modify** | None required beyond new contract/test files unless a tiny shared module is added under `contracts/` |
| **New files (expected)** | `contracts/transaction_event.v1.json` (or equivalent Pydantic), `tests/test_txn_id.py`, tiny fixture CSV under `tests/fixtures/` |
| **Depends on** | None |
| **Approach** | Canonical fields + `source_file` + `row_number` hashed into `txn_id`; keep audit attributes. Schema version on every event. Tests for duplicate-looking rows, CSV re-sort, merchant nulls, malformed rejection. Do not use Neo4j `elementId` as a business key. |
| **Acceptance** | Deterministic IDs; same logical row → same `txn_id` on replay; merchant dest balances are null not zero; schema rejects malformed records; no AWS. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | Yes, with T0.6 |

### T0.2 — Evaluation-label sidecar

| Field | Value |
|---|---|
| **Status** | verified |
| **Objective** | Keep `isFraud` / `isFlaggedFraud` off the operational scoring path while retaining a joinable eval dataset for ground-truth reporting. |
| **Inspect / modify** | `download_data.py` |
| **New files (expected)** | Eval layout documentation; gitignored data path (e.g. `eval/`); tests that ops CSV has no label columns |
| **Depends on** | T0.1 (shared `txn_id` field names) |
| **Approach** | Write unlabeled ops CSV as today; write labeled eval file keyed by `txn_id`. Never load labels into Neo4j or the operational event stream. |
| **Acceptance** | Ops output has no label columns; eval joins 1:1 on `txn_id`; unit test covers both. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | After T0.1, yes with T0.3 (different files) |

### T0.3 — Idempotent local Neo4j load

| Field | Value |
|---|---|
| **Status** | verified |
| **Objective** | Replay-safe graph ingest: uniqueness constraints and MERGE on `txn_id` so retries do not create duplicate TRANSACTION relationships. |
| **Inspect / modify** | `database_creation_script.py` |
| **New files (expected)** | Tests for double-load counts; optional Cypher constraint script |
| **Depends on** | T0.1; T0.2 recommended so ingest uses the ops (unlabeled) file |
| **Approach** | Unique `Account.id`. Unique transaction identity via relationship uniqueness if the Neo4j version supports it, otherwise a `:Transaction` node with unique `txn_id`. Credentials only from environment variables. |
| **Acceptance** | Load twice → same node and relationship counts. Env-only auth. No hardcoded secrets. |
| **AWS needed?** | No |
| **Cost?** | No (local Neo4j only) |
| **Parallel-safe?** | Not with T0.4/T0.5 on the same database |

### T0.4 — Required detection completeness

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Make required Fraud-Reference signals runnable and consistent: Louvain `community_id`, SET `fan_out_flag`, existing Cypher flags, write `risk_score` (name locked in T0.1 contract). |
| **Inspect** | `cypher_queries.md`, `Fraud-Reference.md` |
| **Modify** | `cypher_queries.md` and/or a new `detection/` runner |
| **New files (expected)** | Ordered Cypher or Python detection runner; fixture-graph tests |
| **Depends on** | T0.3 |
| **Approach** | Script what is today manual Browser work. Stretch signals (node similarity, PageRank, betweenness, ring-to-ring) stay out of scope. Align property names with export schema. |
| **Acceptance** | Fixture graph produces expected flags; `risk_score` present; fan-out flag actually SET; Louvain precedes dense-community queries. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | No with T0.5 on the same database |

### T0.5 — Restore full Parquet star-schema baseline

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Re-enable the full local export (`dim_account`, `dim_transaction_type`, `dim_date`, `fact_transactions`) as the migration oracle. Include `txn_id`. Move credentials to env. |
| **Inspect / modify** | `batching_python_driver.py`, `requirements.txt` |
| **New files (expected)** | Optional star-schema note under `docs/`; export tests on a small graph |
| **Depends on** | T0.1; T0.4 (flags/score on the graph) |
| **Approach** | Restore commented full `main()`; add `txn_id` to fact grain; export relationship flags needed for reporting; add PySpark to deps or document the JVM requirement. |
| **Acceptance** | All four tables written; row counts match Neo4j; facts have stable `txn_id`; no hardcoded Neo4j endpoints or passwords in source. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | After T0.4 only |

### T0.6 — Session continuity docs

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Keep `IMPLEMENTATION_PLAN.md` and `AGENT_HANDOFF.md` accurate after each verified task (this file already exists; later sessions update status). |
| **Inspect / modify** | `IMPLEMENTATION_PLAN.md`, `AGENT_HANDOFF.md` |
| **Depends on** | This review (done). Ongoing after every task. |
| **Approach** | After each task: set status, record tests, set next task ID. Do not rewrite `PLAN.md`. |
| **Acceptance** | Handoff always names exactly one next task. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | Yes with T0.1 |

---

## Phase 1 — PLAN step 1 (contracts)

T0.1 *is* PLAN step 1. Optional follow-up:

### T1.1 — Warehouse type lock

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Lock Glue/Redshift column types (decimal vs double, `step` vs timestamps, nullability) in the same contract package. |
| **Inspect** | `contracts/` from T0.1, `batching_python_driver.py` Spark schemas |
| **New files (expected)** | Warehouse/Glue schema companion in `contracts/` |
| **Depends on** | T0.1 |
| **Acceptance** | Single documented type mapping used by later Glue and Redshift tasks. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | Yes with remaining T0 work |

---

## Phase 2 — PLAN step 2: AWS foundation

### T2.1 — IaC skeleton, IAM, secrets, logs, budget, teardown

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Scoped IAM, secrets management, encryption, logging, cost controls, and destroy path. Document env-specific config. Do not `apply` until the user approves. |
| **Inspect** | `PLAN.md` step 2, `docs/MIGRATION_REVIEW.md` |
| **New files (expected)** | `infra/` Terraform or CDK; `.env.example` with **names only** (no secret values) |
| **Depends on** | T0.1 |
| **Acceptance** | Plan/validate succeeds locally; README/infra notes say how to tear down; no credentials in git. |
| **AWS needed?** | Design yes; apply only when user approves |
| **Cost?** | Apply = yes; plan-only = no |
| **Parallel-safe?** | After T0.1, yes with T0.3–T0.5 if apply is skipped |

### T2.2 — Neo4j deployment choice (Aura vs AWS-hosted) + GDS

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Decision record: GDS availability, Glue/Lambda connectivity, security, cost. Do not provision until chosen. |
| **Inspect** | `PLAN.md` decisions, `cypher_queries.md` (GDS usage) |
| **New files (expected)** | Short decision note (e.g. in `docs/` or handoff) |
| **Depends on** | None strictly; before T3.4 / T5.1 |
| **Acceptance** | Written choice with GDS yes/no and network path. |
| **AWS needed?** | Research only unless a trial is started |
| **Cost?** | Only if a trial cluster is created |
| **Parallel-safe?** | Yes with T2.1 |

---

## Phase 3 — PLAN step 3: streaming path

### T3.1 — Local producer using the event contract

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Replay PaySim CSV into versioned JSON events (file or moto), no live AWS required. |
| **Inspect** | T0.1 contracts, `download_data.py` |
| **New files (expected)** | `ingest/replay_producer.py` (or similar), tests |
| **Depends on** | T0.1 |
| **Acceptance** | Fixture CSV → valid events with stable IDs; second run same IDs. |
| **AWS needed?** | No |
| **Cost?** | No |
| **Parallel-safe?** | Yes with T2.x (no apply) |

### T3.2 — Kinesis + raw S3

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Publish replayed events to one Kinesis stream; persist raw events to S3 for audit/replay. |
| **Inspect** | `PLAN.md` step 3, `infra/` |
| **New files (expected)** | Producer config, Firehose or consumer-to-S3, IAM |
| **Depends on** | T2.1 (applied), T3.1 |
| **Acceptance** | Replay produces Kinesis records and raw S3 objects with `txn_id`. |
| **AWS needed?** | Yes |
| **Cost?** | Yes |
| **Parallel-safe?** | No with other tasks that mutate the same stream/bucket without coordination |

### T3.3 — Lambda validation, dedup, simple alerts (batched)

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Bounded validation, idempotent dedup, transaction-local rules, routing of alert candidates. Use Kinesis batch windows. Share the T0.1 transform library. |
| **Inspect** | `PLAN.md` step 3, contracts |
| **New files (expected)** | Lambda handler, unit tests with sample records |
| **Depends on** | T3.2 |
| **Acceptance** | Duplicate events do not create duplicate downstream facts; malformed records handled; no per-record GDS. Document alert latency and retries. |
| **AWS needed?** | Yes |
| **Cost?** | Yes |
| **Parallel-safe?** | After T3.2; not with competing stream consumers unless documented |

### T3.4 — Batched Neo4j graph-writer consumer

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Idempotent batched writes of new events into Neo4j. Reserve GDS for scheduled detection. |
| **Inspect** | `database_creation_script.py` (after T0.3), T2.2 decision |
| **Depends on** | T0.3, T2.2, T3.2 |
| **Acceptance** | Replay twice → no duplicate relationships; writer uses MERGE on `txn_id`. |
| **AWS needed?** | Yes (plus Neo4j) |
| **Cost?** | Yes |
| **Parallel-safe?** | Not with T5.1 on the same graph without locking a window |

---

## Phase 4 — PLAN step 4: Glue batch and curated data

### T4.1 — Glue Data Catalog and S3 prefixes

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Catalog raw and curated datasets; define `raw/` and `curated/` prefixes. |
| **Depends on** | T2.1 applied |
| **Acceptance** | Tables/databases exist in Glue Catalog; paths documented. |
| **AWS needed?** | Yes |
| **Cost?** | Yes (catalog + S3) |
| **Parallel-safe?** | Yes with T3.1 local producer |

### T4.2 — Glue batch: S3 → canonical grain and dimensions

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Historical load and scheduled refresh to the same canonical transaction grain and dims (facts, accounts, types, dates) **without** requiring graph flags yet. |
| **Inspect** | `batching_python_driver.py` schemas (reuse types, not Neo4j keyset pagination) |
| **Depends on** | T0.1, T1.1 recommended, T4.1 |
| **Acceptance** | Curated Parquet matches contract; counts reconcilable to source CSV by `txn_id`. |
| **AWS needed?** | Yes |
| **Cost?** | Yes (Glue DPU) |
| **Parallel-safe?** | Not with T4.3 writing the same curated prefix |

### T4.3 — Glue streaming or micro-batch (only if needed)

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Heavier stream transforms if T3 cannot land the curated grain. Same library as T4.2. |
| **Depends on** | T4.2 (shared transforms), T3.2 |
| **Acceptance** | Documented choice; if skipped, mark verified as “not needed” with evidence. |
| **AWS needed?** | Yes if implemented |
| **Cost?** | Yes if implemented |
| **Parallel-safe?** | Coordinate with T4.2 outputs |

### T4.4 — Batch vs stream reconciliation job

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Compare Glue batch output and streaming output at `txn_id` grain (counts, amount sums by `step` and `type`). |
| **Depends on** | T4.2; T3.3 and/or T4.3 |
| **Acceptance** | Job reports agreement or a bounded, explained diff. |
| **AWS needed?** | Yes |
| **Cost?** | Yes |
| **Parallel-safe?** | After producers are stable |

---

## Phase 5 — PLAN step 5: Neo4j detection connected to reporting

### T5.1 — Scheduled cloud detection

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Port T0.4 Cypher/GDS to the chosen Neo4j; run on a schedule or incremental window, not per Kinesis record. |
| **Inspect** | `cypher_queries.md`, `detection/` from T0.4 |
| **Depends on** | T0.4, T2.2, T3.4 (graph has data) |
| **Acceptance** | Detection run is versioned (`detection_run_id`, `detected_at`). |
| **AWS needed?** | Yes (scheduler) + Neo4j |
| **Cost?** | Yes |
| **Parallel-safe?** | Not overlapping another GDS write on the same DB |

### T5.2 — Export graph features and join to curated tables

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Export account/transaction/ring results to S3 and join to curated tables on `account_id` / `txn_id`. |
| **Inspect** | `batching_python_driver.py` after T0.5 |
| **Depends on** | T5.1, T4.2 |
| **Acceptance** | Reports can distinguish a new detection run from a prior one; edge flags (e.g. transfer-cashout) available where required. |
| **AWS needed?** | Yes |
| **Cost?** | Yes |
| **Parallel-safe?** | After T5.1 completes |

### T5.3 — Scope: required signals only

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Confirm stretch signals are deferred; required dashboard fields are populated. |
| **Depends on** | T5.2 |
| **Acceptance** | Checklist vs `Fraud-Reference.md` primary signals and `P1-specifications.md` visuals. |
| **AWS needed?** | No additional |
| **Cost?** | No additional |
| **Parallel-safe?** | Yes |

---

## Phase 6 — PLAN step 6: Redshift and Power BI

### T6.1 — Load curated data into Redshift

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | COPY or Spectrum from curated Parquet; dist/sort keys documented. Athena may be compared in prototype; Redshift remains the reporting target. |
| **Depends on** | T4.2, T5.2 (for graph fields on reports), T1.1 |
| **Acceptance** | Totals match curated S3 for transaction count and amount. Pause/delete procedure documented. |
| **AWS needed?** | Yes |
| **Cost?** | Yes — high if left running |
| **Parallel-safe?** | After curated tables are stable |

### T6.2 — Power BI Import mode and required visuals

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | Connect Power BI (Import, scheduled refresh). Recreate KPIs, risk-score distribution, flagged volume over time, transaction-type comparison, riskiest accounts, ring volume. DirectQuery only if freshness justifies cost. Ground-truth fraud-rate uses eval sidecar only. |
| **Inspect** | `P1-specifications.md` |
| **Depends on** | T6.1, T0.2 for ground-truth rate |
| **Acceptance** | Dashboard answers the spec questions; refresh succeeds. |
| **AWS needed?** | Yes (Redshift reachability) |
| **Cost?** | Indirect (Redshift on) |
| **Parallel-safe?** | After T6.1 |

---

## Phase 7 — PLAN step 7: migrate and verify

### T7.1 — Incremental verification and teardown

| Field | Value |
|---|---|
| **Status** | todo |
| **Objective** | End-to-end checks from `PLAN.md` Verification, then resource teardown. |
| **Inspect** | Local Parquet baseline (T0.5), curated S3, Neo4j, Redshift |
| **Depends on** | T3.2, T4.4, T5.2, T6.2 |
| **Approach** | (1) CSV replay → valid Kinesis + raw S3 with stable IDs. (2) Replay twice; no duplicate facts or graph rels. (3) Batch vs stream agreement. (4) Graph joins. (5) Redshift vs S3 vs local baseline. (6) Power BI. (7) IAM, logs, failure recovery, teardown. |
| **Acceptance** | Checklist signed off in `AGENT_HANDOFF.md`; paid resources stopped or destroyed per user instruction. |
| **AWS needed?** | Yes |
| **Cost?** | Yes until teardown |
| **Parallel-safe?** | No — final gate |

---

## Dependency graph (summary)

```
T0.1 ─┬─ T0.2 ─ T0.3 ─ T0.4 ─ T0.5
      ├─ T0.6 (ongoing)
      ├─ T1.1
      ├─ T2.1 ─ T3.2 ─┬─ T3.3
      ├─ T3.1 ────────┘      └─ T3.4
      └─ T2.2 ──────────────────┘
T4.1 (after T2.1 apply) ─ T4.2 ─┬─ T4.3 (optional) ─ T4.4
T0.4 + T2.2 + T3.4 ─ T5.1 ─ T5.2 ─ T5.3
T4.2 + T5.2 + T1.1 ─ T6.1 ─ T6.2
T4.4 + T5.2 + T6.2 ─ T7.1
```

## Out of scope (from PLAN.md)

Replacing Neo4j, multi-region DR, ML model training, stretch GDS signals until required signals are done.
