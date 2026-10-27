# Migration review: local PaySim pipeline vs PLAN.md

Read-only review (2026-10-26). No application code was changed in the review session. `PLAN.md` remains the proposed architecture.

This document is the evidence pack for `IMPLEMENTATION_PLAN.md` and `AGENT_HANDOFF.md`.

---

## 1. Existing architecture

The git tree is **ten tracked files at repo root** (plus this `docs/` note). There is no `src/`, `tests/`, `terraform/`, Docker, notebooks, or `.pbix` in version control. CSVs and `Output_Parquet/` are gitignored (`.gitignore`).

### How the local pipeline actually runs

There is no Makefile or orchestrator. Operators follow `README.md` plus three `if __name__ == "__main__"` modules. Detection is not automated in Python.

1. **`download_data.py`** — `kagglehub.dataset_download("mtalaltariq/paysim-data")`, read the first `*.csv`, **drop `isFraud` and `isFlaggedFraud`**, write `paysim_clean.csv` in the working directory, delete the Kaggle cache tree.
2. **Manual copy** of that CSV into the Neo4j **import** folder.
3. **`database_creation_script.py`** — Neo4j driver from env (`db_uri`, `db_user`, `db_password` via `load_dotenv()`). Creates an index `FOR (a:Account) ON (a.id)` (not a uniqueness constraint). `LOAD CSV WITH HEADERS FROM 'File:///paysim_clean.csv'` in `IN TRANSACTIONS OF 5000 ROWS`. **`MERGE` account nodes** from `nameOrig` / `nameDest`. **`CREATE` directed `:TRANSACTION` relationships** with properties `type`, `amount`, `step`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`. No transaction ID.
4. **`cypher_queries.md`** — run by hand in Neo4j Browser: GDS project `accountGraph`, degree (out/in), drain pattern, TRANSFER→CASH_OUT, community stats (expects `community_id` already present), guilt-by-association, optional node similarity (documented as unused last run). Flags written onto nodes and some relationships.
5. **`batching_python_driver.py`** — PySpark star-schema export designed for `dim_account`, `dim_transaction_type`, `dim_date`, intermediate `_fact_transactions_raw`, then `fact_transactions` partitioned by a synthetic calendar date (`START_DATE = 2026-03-01`, PaySim `step` as hours). **Active `main()` only rebuilds `dim_account`.** The full pipeline `main()` is commented out. Transaction reads use keyset pagination on `elementId(t)`, then **drop the cursor** before Parquet write.
6. **Power BI** — required by `P1-specifications.md` (Import mode, `.pbix`); artifact is not in the repository.

```
Kaggle PaySim
    → download_data.py
    → paysim_clean.csv (labels removed)
    → manual Neo4j import folder
    → database_creation_script.py
    → (:Account)-[:TRANSACTION {type,...}]->(:Account)
    → cypher_queries.md (manual GDS/Cypher)
    → node/rel flags
    → batching_python_driver.py
    → Output_Parquet/dim_account  (facts/other dims not written by current main)
    → Power BI (external)
```

### Graph model (code, not README)

- **Nodes:** `:Account {id}` derived from sender/receiver strings.
- **Edges:** single type `:TRANSACTION` with a `type` property (`PAYMENT`, `TRANSFER`, `CASH_OUT`, etc.). README incorrectly describes separate edge types / `[:TRANSFER]`.
- **Detection extras:** `:CommunityStats` in Cypher docs; GDS in-memory graph `accountGraph`.

### What to preserve

| Asset | Why |
|---|---|
| `download_data.py` | Canonical unlabeled operational extract |
| `database_creation_script.py` | Account MERGE + batched LOAD CSV pattern (must become idempotent) |
| `cypher_queries.md` | Team’s actual detection queries |
| `batching_python_driver.py` | Star-schema intent, Spark types, keyset pagination design |
| `Fraud-Reference.md` | Required signals and `risk_score` formula |
| `P1-specifications.md` | Dashboard and phase deliverables |
| `requirements.txt` | Local Python pins (extend later; do not discard) |
| `PLAN.md` | Target hybrid architecture |

### What is leftover or planning-only

`PLAN.md` is not executed today. Commented full export `main()`, README edge-type narrative, a comment referring to `extract_to_parquet.py` (file not in repo), and Power BI as an external artifact.

---

## 2. Repository audit (paths and evidence)

### Tracked files (at review)

- `README.md`
- `PLAN.md`
- `P1-specifications.md`
- `Fraud-Reference.md`
- `cypher_queries.md`
- `batching_python_driver.py`
- `database_creation_script.py`
- `download_data.py`
- `requirements.txt`
- `.gitignore`

Remote (from review): `https://github.com/2403-Data-Engineering/P1-Team3.git`, branch `main`. Commit themes: initial setup, scripts, README. AWS appears only in `PLAN.md`.

### Components

| Component | Path | Status | Evidence |
|---|---|---|---|
| Data prep | `download_data.py` | Complete | Explicit `df.drop(columns=["isFraud", "isFlaggedFraud"])`; writes `Path.cwd() / "paysim_clean.csv"` |
| Graph ingest | `database_creation_script.py` | Incomplete / unsafe to replay | `MERGE` accounts, `CREATE` edges; index not uniqueness; module docstring still describes download; unused `pandas` import |
| Detection | `cypher_queries.md` | Incomplete / unverified | See gaps below |
| Signal catalog | `Fraud-Reference.md` | Complete as spec | `risk_score` formula; Louvain prescribed for rings |
| Export | `batching_python_driver.py` | Partial | Commented full `main()` ~lines 337–389; live `main()` ~392–416 dim_account only |
| Specs | `P1-specifications.md` | Spec vs code drift | Pandas export vs PySpark; star schema “document before export” has no standalone schema doc |
| Ops docs | `README.md` | Stale | `database_creation.py` vs `database_creation_script.py`; `specifications.md` vs `P1-specifications.md` |
| Deps | `requirements.txt` | Incomplete for export | neo4j, pandas, kagglehub, python-dotenv; **no pyspark**, pytest, boto3 |
| Tests | — | Missing | No `test_*.py`, `tests/`, or CI (`.gitignore` mentions `.pytest_cache/` only) |
| CI | — | Missing | No `.github/` |
| IaC / Docker | — | Missing | No Terraform, CDK, CloudFormation, compose |
| AWS code | — | Missing | No boto3, Glue jobs, Lambda, Kinesis, Redshift COPY |
| Config | `.env` gitignored | No example | Ingest: env vars. Export: hardcoded local URI/user/password in source (**do not copy values into docs**) |
| Reporting | `.pbix` | External | Required by spec; not in git |

### Detection gaps (`cypher_queries.md`)

- GDS `degree.write` NATURAL writes `outDegree` and returns p99 as `fanOutp99` but **never SETs `fan_out_flag`**. Later node-similarity query filters `a.fan_out_flag = 1`.
- Fan-in flags with hardcoded `inDegree > 14` → `unusualFanIn`.
- Dense community **assumes `a.community_id` exists**. `Fraud-Reference.md` says `gds.louvain.write`; **no Louvain query in `cypher_queries.md`**.
- **No Cypher writes `risk_score` or `fraud_score`.** Export `ACCOUNTS_QUERY` / `ACCOUNT_SCHEMA` expect `fraud_score`. Reference formula uses `risk_score`.
- Property inconsistency: `UnusualFanIn` vs `unusualFanIn`; similarity follow-up uses `{flagged: false}` vs `flagged = 1`.
- TRANSFER→CASH_OUT SETs flags on `t1`, `t2`, and account `b`. Fact export **does not** pull edge flags.

### Export schema (code)

Path: `batching_python_driver.py`

**`dim_account`:** `account_id`, `community_id`, `community_total_amount`, `community_transaction_count`, `drain_behavior_flag`, `dense_community_flag`, `unusualFanIn`, `transfer_cashout_flag`, `guilt_by_association_flag`, `fraud_score`.

**Fact batch (cursor dropped):** `nameOrig`, `nameDest`, `step`, `type`, `amount`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`.

**`dim_transaction_type`:** `type_key`, `type_name` (assigned by sorted unique types seen during stream — not a stable warehouse key across runs unless types are frozen).

**`dim_date`:** `date_key` = step, synthetic datetime/date/hour/dow from `START_DATE`.

### Transaction identity, duplicates, labels

- Cleaned CSV has **no transaction ID** (PaySim grain is a row: step, type, amounts, balances, names).
- Neo4j rels are identified only by internal `elementId` at export time — **not stored at ingest**, **not stable across DB rebuilds**.
- Re-ingest **duplicates edges**.
- Labels are absent from graph and Parquet → **low leakage** into scoring; **cannot** compute ground-truth fraud rate from ops data (`P1-specifications.md` “Fraud rate by transaction type”).

### Dependencies and runtime

- Python: `requirements.txt` (neo4j==6.1.0, pandas, kagglehub git pin, python-dotenv, …).
- Export also needs a JVM/Spark install (unpinned).
- Neo4j server version and GDS plugin are **not pinned** in repo; GDS is assumed by Cypher docs.

---

## 3. Repository vs PLAN.md

PLAN recommendation (keep): **one Kinesis stream + Glue batch**, both landing a **consistent S3 dataset**; **Redshift** for Power BI; **Neo4j** for graph detection on **batches/windows**, not per event. Initial source is **CSV replay** with a versioned event format.

### Steps that can be implemented as written

- Versioned events with PaySim fields, source step, ingest timestamp, stable ID.
- Raw S3 + Glue catalog + curated Parquet.
- Lambda for bounded validation/dedup/simple alerts; heavier work in Glue.
- Separate batched graph-writer; GDS scheduled.
- Star schema: transaction facts, accounts, types, dates, graph signals joined when available.
- Incremental verify: replay, retries, malformed records, missing balances, batch/stream agreement, Power BI refresh.

### Steps that need adjustment to this codebase

| PLAN assumption | Repo reality | Adjustment |
|---|---|---|
| Local export is a verification oracle | Only `dim_account` is produced by live `main()` | Restore full export (T0.5) before using it as Glue/Redshift truth |
| Graph load is a migration reference | `CREATE` is not idempotent | MERGE on `txn_id` + uniqueness (T0.3) |
| Join graph results to curated tables | No business txn key; edge flags not in facts | Put `txn_id` on events, graph, and facts |
| `cypher_queries.md` is complete detection | Missing Louvain, score, fan-out flag | Close locally (T0.4) then port |
| Labels handled via eval dataset | `download_data.py` discards labels | Unlabeled ops + labeled sidecar (T0.2) |
| Glue/Lambda use env config | Export hardcodes local Neo4j connection | Secrets/env only; never ship local endpoints |
| Spark/Glue compatible | Local PySpark loop is Neo4j keyset pagination | Reuse **schemas and transforms**, not that read loop inside Glue |

### Missing implementation steps (not spelled out in PLAN)

- Canonical event schema as **shared code** (producer, Lambda, Glue).
- Neo4j uniqueness constraints.
- `detection_run_id` / `detected_at` on exported flags.
- One transform library for batch and stream.
- Redshift load method (COPY vs Spectrum) and dist/sort keys.
- Local pytest as first CI.
- Cost guardrails (Kinesis, Glue DPU, Redshift pause, Neo4j).
- Confirm GDS on the chosen Neo4j product.

### Compatibility, correctness, cost, security

- **IDs / duplicates / idempotency:** see C1–C2. `elementId` must not be the fact PK or Kinesis partition key.
- **Label leakage:** ops path is clean; eval join must stay off scoring (C3).
- **Neo4j updates:** streaming writer upserts edges; GDS is whole-graph or windowed batch (C5). Incremental Louvain ≠ same communities as full-graph Louvain — document the window.
- **Spark/Glue:** Glue jobs cannot call `127.0.0.1` Neo4j; graph export is a separate path from CSV→S3 curated facts.
- **Redshift:** needs a real `txn_id` grain and consistent types (T1.1).
- **Batch vs stream:** same casts, null rules, `txn_id`; reconcile counts and sums by `step` and `type` (T4.4).
- **Cost:** 6.3M Kinesis puts + Lambda + Glue + Redshift idle time (C6). Do not start paid services in early tasks.
- **Security:** env/secrets manager; `.env` stays gitignored; never paste credentials into markdown.

---

## 4. Plan corrections

Do **not** replace hybrid stream-and-batch. Record these as constraints when implementing PLAN steps.

### C1 — Stable transaction IDs

File + row number is necessary but not sufficient if the CSV is re-sorted. Use `txn_id = hash(canonical PaySim fields + source_file + row_number)` and keep `source_file` / `row_number` as audit attributes. Do not use Neo4j `elementId` as a business key.

### C2 — Duplicate graph edges on replay

Current ingest `CREATE`s a relationship per CSV row every run. Graph writer and historical load must **MERGE** on `txn_id` with a uniqueness constraint (relationship uniqueness if the server supports it; otherwise a `:Transaction` node).

### C3 — Label stripping vs fraud-rate visuals

Keep labels **out** of Kinesis and Neo4j scoring. Land an evaluation-only dataset keyed by `txn_id` for ground-truth “fraud rate by type”. Operational flags remain model/graph flags.

### C4 — Incomplete local Parquet baseline

Commented-out full export cannot be the migration oracle. Restore dim/fact tables and `txn_id` locally (T0.5) before comparing Glue/Redshift.

### C5 — GDS is not a per-event API

Degree/Louvain/similarity need a projected graph. Schedule detection; streaming only upserts topology. Do not invoke GDS once per Kinesis record (PLAN already says this; make it a hard task boundary for T3.4 vs T5.1).

### C6 — Lambda at PaySim scale

Avoid one invocation per event as the design center. Use Kinesis **batch** windows and the **same** transform module as Glue.

### Additional corrections (same review)

- **C7 Score naming:** lock `risk_score` vs `fraud_score` in the T0.1 contract (reference formula is `risk_score`; export currently reads `fraud_score`).
- **C8 Merchant balances:** empty dest balances on `M*` accounts must be **null**, not `0`, so drain logic does not treat merchants as emptied accounts (`Fraud-Reference.md` notes).
- **C9 Detection completeness:** implement Louvain, SET `fan_out_flag`, write the locked score property, export relationship `transfer_cashout_flag` before cloud reporting joins.
- **C10 `type_key` stability:** assigning `type_key` as `enumerate(sorted(seen_types))` during a stream is not a durable warehouse key unless the type dictionary is frozen in the contract.
- **C11 Credentials:** remove hardcoded local Neo4j auth from export before any cloud job copy-paste; use env/secrets only. Do not record secret values in this document.

---

## 5. Implementation phasing

Preserve PLAN phase structure. **Phase 0 is local-only** and should finish before paid AWS.

| Phase | PLAN step | Tasks | AWS / cost |
|---|---|---|---|
| 0 Local foundations | (prerequisite) | T0.1–T0.6 | No |
| 1 Contracts | Step 1 | T0.1, optional T1.1 | No |
| 2 Foundation | Step 2 | T2.1–T2.2 | Apply = cost |
| 3 Streaming | Step 3 | T3.1 (local) then T3.2–T3.4 | T3.2+ cost |
| 4 Glue curated | Step 4 | T4.1–T4.4 | Yes |
| 5 Graph → reports | Step 5 | T5.1–T5.3 | Yes |
| 6 Redshift / PBI | Step 6 | T6.1–T6.2 | Yes (Redshift especially) |
| 7 Verify / teardown | Step 7 | T7.1 | Yes until teardown |

**First implementation task:** T0.1 (event contract, stable ID, nulls, idempotency tests). No Neo4j writes, no AWS.

Task-level files, dependencies, acceptance, AWS, cost, and parallel-safety: `IMPLEMENTATION_PLAN.md`.

---

## 6. Unresolved decisions and risks

| Decision / risk | Why it blocks or costs |
|---|---|
| Neo4j Aura vs self-hosted on AWS, GDS SKU | T2.2, T3.4, T5.1 |
| Rel uniqueness vs `:Transaction` nodes | Depends on Neo4j version (T0.3) |
| Glue Streaming vs batch-only for “stream transforms” | T4.3; batch-first is cheaper for CSV replay |
| Redshift provisioned vs Serverless vs Athena prototype | PLAN prefers Redshift; cost profile differs |
| Full 6.3M-row PaySim in cloud prototype? | Kinesis + Glue + Redshift bill |
| Ownership of `.pbix` | T6.2 / T7.1 |
| Local Neo4j + GDS versions | Cypher portability |
| `risk_score` vs `fraud_score` | Lock in T0.1 |
| Fraud-rate visual = flags vs ground truth | T0.2 policy |
| Incremental vs full-graph Louvain | Community IDs will drift if windows differ |

**Out of scope (PLAN):** replacing Neo4j, multi-region DR, ML training.

---

## 7. Session continuity

- Keep `PLAN.md` as architecture.
- Track work in `IMPLEMENTATION_PLAN.md`.
- Each session updates `AGENT_HANDOFF.md` (completed task, tests, next ID).
- Do not duplicate architecture into more markdown files unless a decision record is required (T2.2).
