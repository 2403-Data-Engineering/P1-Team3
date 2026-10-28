# Implementation plan: PaySim fraud pipeline migration (thin-slice edition)

Architecture target: `PLAN.md` (do not modify). Evidence and reasoning: `docs/MIGRATION_REVIEW.md` (open only the correction IDs a task cites, e.g. C2, C8). Current state: `AGENT_HANDOFF.md`.

Status values: `todo` | `in_progress` | `verified`. Code-complete work awaiting live verification stays `in_progress`.

## Rules for every session

- Read `AGENT_HANDOFF.md` and **only the assigned task's section** of this file. Don't read the whole repo; open only the files the task lists.
- Implement only the assigned task ID. Stop as soon as its acceptance criteria pass. No extra refactors or summaries.
- Check git status against the handoff before starting. If the handoff and the repo disagree in a way that affects the task, stop and say so.
- Never `terraform apply`/`destroy`, start paid AWS services, commit, or push unless the task's Permissions line explicitly allows it.
- Never put credentials in files, logs, or docs.
- Update `AGENT_HANDOFF.md` at the end: what changed, tests run, issues, next task ID.

## Scope decisions (locked; change only on purpose)

| Decision | Choice | Reason |
|---|---|---|
| Dataset for cloud work | Sample of ~100K PaySim rows, taken by a fixed rule so it is reproducible. Full 6.3M rows is out of scope. | Cost and agent-credit budget |
| Reporting target (first pass) | Athena over curated Parquet in S3. Redshift is stretch task S2. | Pay-per-query, near zero cost at this size |
| Neo4j | Runs locally. T1 writes a short Aura vs AWS-hosted decision note but provisions nothing. | Avoids hosting cost |
| Streaming (Kinesis + Lambda) | Stretch task S1, run briefly on the sample, then torn down. The batch path ships first. | Batch already proves the contract, curated grain, and reporting |
| Score name | `risk_score` (locked in T0.1) | |
| Ground-truth fraud rate | From the eval sidecar only (locked in T0.2) | |

`PLAN.md` remains the target architecture. This plan builds its batch half and the reporting layer first, and adds streaming as a stretch. The final write-up (T5) should say so plainly.

## Mapping from the earlier task list

| Earlier IDs | Now |
|---|---|
| T0.1, T0.2 | unchanged (verified) |
| T0.3, T0.4, T0.5 | unchanged; T1.1 (type lock) merged into T0.5 |
| T0.6 | dropped (the handoff is updated every session) |
| T2.1, T2.2, T4.1 | T1 |
| T4.2, T4.4 (batch part) | T2 |
| T5.1, T5.2, T5.3 | T3 |
| T6.1 (Athena), T6.2 | T4 |
| T7.1 | T5 |
| T3.1-T3.3, T4.3, T4.4 (stream part) | S1 (stretch) |
| T3.4 | dropped; the batch loader from T0.3 serves the demo |
| T6.1 (Redshift) | S2 (stretch) |

---

## Phase 0: Local foundations

T0.3 live check, T0.4 live run, T0.5 live export, and live T3 are deferred until Neo4j is available: no local Neo4j environment variables and no unlabeled ops CSV were configured. Repo-only T5 is complete; no next task is authorized. T0.5/T2/T3/T4 remain fixture-tested code only; live checks, T1 plan and provisioning remain pending before any future cloud execution.

### T0.1 - Event and data contracts
**Status:** verified

### T0.2 - Evaluation-label sidecar
**Status:** verified

### T0.3 - Idempotent local Neo4j load
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (mock-tested); live Neo4j verification pending |
| **Objective** | Replay-safe graph ingest. Loading the ops CSV twice gives identical node and relationship counts. |
| **Files** | `database_creation_script.py`; reuse `txn_id` from `contracts/` |
| **Approach** | Unique constraint on `Account.id`. Transaction identity by relationship uniqueness if the local Neo4j version supports it, otherwise a `:Transaction` node with a unique `txn_id`. MERGE, not CREATE. Credentials from environment only. Ingest the unlabeled CSV, never the eval sidecar. Document which identity model was chosen and why. |
| **Acceptance** | Double load leaves counts unchanged; constraints exist; no hardcoded secrets; test covers the double load. |
| **AWS / cost** | No / No |
| **Permissions** | Local Neo4j read/write via environment credentials. Confirm the instance is local and describe any write before running it. Don't delete data without asking. |

### T0.4 - Required detection signals
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (mock-tested); live Neo4j verification pending |
| **Objective** | Make the required Fraud-Reference signals runnable as a script instead of manual Browser steps. |
| **Files** | `cypher_queries.md`, `Fraud-Reference.md`; new `detection/` runner |
| **Approach** | Louvain writes `community_id` before the dense-community queries. SET `fan_out_flag` from the p99 threshold. Compute and write `risk_score` per `Fraud-Reference.md`. Unify property names (`unusualFanIn` etc.). Drain logic must ignore merchant destination balances (C8). Stretch signals (similarity, PageRank, betweenness) stay out of scope. |
| **Acceptance** | A small fixture graph produces the expected flags and `risk_score`; `fan_out_flag` is actually set; Louvain runs before community queries; property names match the export schema. |
| **AWS / cost** | No / No |
| **Permissions** | Local Neo4j read/write with GDS via environment credentials. Prefer the fixture graph first. Describe writes to the main graph before running them. |

### T0.5 - Local star-schema baseline (the oracle)
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (fixture-tested; not run against Neo4j) |
| **Objective** | Restore the full local export (`dim_account`, `dim_transaction_type`, `dim_date`, `fact_transactions`) so later cloud output has something to be compared against. Includes the type lock formerly in T1.1. |
| **Files** | `batching_python_driver.py`, `requirements.txt` |
| **Approach** | Re-enable the commented full `main()`. Add `txn_id` to the fact grain. Export relationship flags (e.g. `transfer_cashout_flag`). Read `risk_score`, not `fraud_score`. Freeze `type_key` as a fixed dictionary in the contract (C10). Document the column type mapping (decimal vs double, `step` handling, nullability). Replace the hardcoded Neo4j connection with environment variables (C11). Add pyspark to requirements or document the JVM requirement. |
| **Acceptance** | All four tables written; row counts match Neo4j; facts carry `txn_id`; `type_key` is stable across runs; no hardcoded endpoints or passwords in source. |
| **AWS / cost** | No / No |
| **Permissions** | Read-only on local Neo4j. Writing Parquet to a local output folder is allowed, but don't overwrite existing output without asking. |

---

## Phase 1: AWS foundation (design first; apply only with approval)

### T1 - Terraform skeleton, buckets, catalog, budget, decision note
| Field | Value |
|---|---|
| **Status** | in_progress — validate passed; AWS plan pending runtime account ID and budget alert recipient |
| **Objective** | Minimum AWS foundation for the batch path, with cost controls and a destroy path. |
| **Files (new)** | `infra/` (Terraform), `.env.example` with names only, a short Neo4j decision note in `docs/` |
| **Approach** | Raw and curated S3 buckets with encryption and public-access block; a Glue database; a scoped IAM role for Glue; an AWS Budget alert at a low threshold; state backend settings with `use_lockfile`; tags on everything. Decision note: Aura vs AWS-hosted Neo4j (GDS availability, network path from cloud jobs, cost), with a recommendation but no provisioning. Document how to tear everything down. |
| **Acceptance** | `terraform validate` and `terraform plan` succeed; plan output lists only expected resources; teardown steps documented; no credentials in git. |
| **AWS / cost** | Design yes; apply only with approval. Low cost when applied (S3, catalog, IAM). |
| **Permissions** | Write Terraform and run `validate` and `plan` only. No `apply` or `destroy` without my explicit go-ahead in that session, after a budget alert exists. |

---

## Phase 2: Cloud batch path

### T2 - Sample to S3 to curated Parquet (Glue batch)
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (fixture-tested; not run in AWS) |
| **Objective** | Take the reproducible ~100K-row sample through the contract into raw S3, then produce curated Parquet at the canonical `txn_id` grain plus dimensions. Reconcile against the T0.5 local baseline. |
| **Files** | `contracts/`, T0.5 schemas and transforms (reuse types and transforms, not the Neo4j keyset-pagination loop); new `pipeline/` and a Glue job script |
| **Approach** | One shared transform module used by both local runs and the Glue job. Raw events land in S3 with stable IDs. Glue batch job (smallest worker config) writes curated Parquet and registers tables. Reconciliation script compares counts and amount sums by `step` and `type` between local baseline and curated output. Duplicate-replay test: run twice, expect no duplicate facts. |
| **Acceptance** | Curated tables match the contract; counts and sums reconcile with the local baseline; replaying the same input creates no duplicates; Glue job cost on the sample is noted. |
| **AWS / cost** | Yes / Yes (Glue DPU-hours; keep runs few and small) |
| **Permissions** | Apply or run only resources defined in T1, in my dev account. Before any Glue run, list what will run and its approximate cost, then wait for my yes. |

---

## Phase 3: Graph results into reporting

### T3 - Versioned graph export joined to curated data
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (fixture-tested; not run live) |
| **Objective** | Export graph-derived account, transaction, and ring signals with a detection run version, land them in S3, and join them to curated tables. |
| **Files** | `detection/` from T0.4, `batching_python_driver.py` from T0.5 |
| **Approach** | Every export carries `detection_run_id` and `detected_at`. Account-level signals join on `account_id`, transaction-level flags on `txn_id`. Detection runs on a schedule or window (never per event); document the window, since incremental Louvain communities can differ from full-graph ones. Required signals only. |
| **Acceptance** | Reports can tell one detection run from another; edge flags (e.g. transfer-cashout) are available where the dashboard needs them; the joined result matches the local export for the sample. |
| **AWS / cost** | S3 writes only / negligible |
| **Permissions** | Read from local Neo4j; write to the S3 buckets from T1. |

---

## Phase 4: Reporting

### T4 - Athena views and dashboard
| Field | Value |
|---|---|
| **Status** | in_progress — code-complete (fixture-tested; not run in Athena) |
| **Objective** | Answer the project's dashboard questions from Athena over curated and graph data, then connect Power BI (or a simpler dashboard if Power BI is not available). |
| **Files** | `P1-specifications.md` (visual requirements) |
| **Approach** | Athena views for KPIs, risk-score distribution, flagged volume over time, transaction-type comparison, riskiest accounts, and ring volume. Ground-truth fraud rate joins the eval sidecar only and never feeds scoring. Power BI in Import mode with scheduled refresh; DirectQuery only if freshness justifies the cost. |
| **Acceptance** | Each required visual has a working query or report page; numbers match the curated tables; ground-truth rate is clearly separate from model flags. |
| **AWS / cost** | Yes / low (Athena scans on a small sample) |
| **Permissions** | Create Athena views and workgroup settings from T1's Terraform. Keep a scan-size limit on the workgroup. |

---

## Phase 5: Verify, tear down, document

### T5 - Final verification, teardown, and architecture write-up
| Field | Value |
|---|---|
| **Status** | verified — complete (repo-only delivery; no live verification or teardown performed) |
| **Objective** | Finalize the repo-only delivery: fixture verification evidence, accurate README/architecture, names-only env configuration, ignore coverage and a future teardown runbook. |
| **Approach** | User-approved repo-only adaptation: document what fixtures prove and what remains unverified. State explicitly that SQLite tests do not validate Athena Trino/Presto syntax/functions or Hive DDL. Update README and architecture mapping without modifying PLAN.md. Verify env/ignore/runbook coverage and rerun all tests. Nothing was deployed, so no live check or teardown is performed. |
| **Acceptance** | Repo-only checklist signed off in AGENT_HANDOFF.md; 96 local tests pass; README/docs match current code and state live limits; names-only env example and secret/state ignore checks pass. No deployed-resource or account-wide teardown claim. |
| **AWS / cost** | No / No (repo-only adaptation) |
| **Permissions** | Local files only. No AWS, Neo4j, paid services, commits or pushes. Future live teardown requires explicit approval in that session. |

---

## Stretch tasks (only after T5, if credits and interest remain)

### S1 - Streaming slice (Kinesis, Lambda, reconciliation)
Local replay producer first (no AWS), then a 1-shard Kinesis stream and a batched Lambda that validates and de-duplicates by `txn_id` and lands raw events in S3. Reconcile stream output against the batch output at `txn_id` grain by `step` and `type`. Run on the sample for a short window, then delete the stream. Needs explicit cost approval before any resource is created.

### S2 - Redshift load
COPY or Spectrum from curated Parquet, with documented distribution and sort keys, and a pause/delete procedure. Compare against the Athena numbers. Needs explicit cost approval.

---

## Out of scope

Replacing Neo4j, multi-region DR, ML model training, stretch GDS signals (node similarity, PageRank, betweenness), full 6.3M-row cloud runs.
