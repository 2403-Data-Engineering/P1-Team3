# Agent handoff

Use this file at the start and end of every implementation session. Architecture: `PLAN.md`. Task tracker: `IMPLEMENTATION_PLAN.md`. Review evidence: `docs/MIGRATION_REVIEW.md`.

## Session status

| Field | Value |
|---|---|
| Last completed task | **T0.3 — Idempotent local Neo4j load** (verified) |
| Last session | MERGE ingest on `txn_id`, uniqueness constraints, env-only Neo4j auth, double-load tests |
| **Next task** | **T0.4 — Required detection completeness** |
| Overall status | T0.1, T0.2, and T0.3 verified. All other task IDs remain `todo`. |

## Next session prompt (copy)

Read `PLAN.md`, `IMPLEMENTATION_PLAN.md`, `AGENT_HANDOFF.md`, and `docs/MIGRATION_REVIEW.md`. Implement **only T0.4**. Align property names with the contract (`risk_score`). Do not start T0.5 Parquet export unless the user expands scope. Do not apply Terraform, start paid AWS services, commit, or push unless the user asks. Do not copy secrets into files.

## Tests run

```
python -m pytest tests/test_txn_id.py tests/test_eval_sidecar.py tests/test_neo4j_ingest.py -v
```

31 passed (Python 3.13.14, pytest 8.4.2). T0.3: double-load keeps the same account and transaction counts; Cypher MERGEs on `txn_id` (no bare CREATE edges); unique `Account.id` and `TRANSACTION.txn_id`; ingest `txn_id`s match the eval sidecar; labeled CSV refused; connect args from env only. No live Neo4j process was required (in-memory MERGE + fake session). No AWS.

## What T0.3 added

- `database_creation_script.py` — reads unlabeled ops CSV; computes `txn_id` via the v1 contract; `CREATE CONSTRAINT` unique `Account.id` and unique `TRANSACTION.txn_id`; `MERGE` accounts and `MERGE (orig)-[t:TRANSACTION {txn_id}]->(dest)`; refuses label columns; credentials from `db_uri` / `db_user` / `db_password` only (loaded in `main()` via dotenv).
- `ingest/constraints.cypher` — same constraints for Browser/manual apply.
- `tests/test_neo4j_ingest.py`

**Graph identity (locked):** keep `:TRANSACTION` relationships (so existing Cypher patterns stay valid). Unique `txn_id` on the relationship requires **Neo4j 5.18+**. Do not introduce `:Transaction` nodes unless a deployment cannot create relationship uniqueness constraints.

## What is true about the repo right now

- Local pipeline: `download_data.py` → `paysim_clean.csv` **and** `eval/paysim_eval_labels.csv` → `database_creation_script.py` (idempotent MERGE on `txn_id`) → manual `cypher_queries.md` → `batching_python_driver.py` (active `main()` writes **only** `dim_account`; facts still lack `txn_id`).
- No AWS/IaC/Docker/CI.
- Full Parquet star-schema `main()` in `batching_python_driver.py` is still commented out.
- Export script still uses hardcoded local Neo4j connection settings (do not quote or copy secret values). Ingest uses env vars. No `.env.example`.
- `pyspark` is still imported in the export script but not listed in `requirements.txt`.

## Known issues (from audit)

1. **Stable transaction ID not on export.** Ingest now stores `txn_id`; Parquet facts still drop Neo4j `elementId` and have no `txn_id`. **C1 remaining: T0.5.**
2. ~~Non-idempotent ingest.~~ **Closed in T0.3 (C2).**
3. ~~Fraud labels stripped with no eval path.~~ **Closed in T0.2 (C3).** Keep labels out of Neo4j and the operational stream.
4. **Local Parquet baseline incomplete.** **C4; T0.5.**
5. **Detection gaps.** No Louvain; `fan_out_flag` never SET; no Cypher for `risk_score`; property typos; edge flags not exported. **T0.4 (next), T0.5.** Use `risk_score`, not `fraud_score`.
6. **Docs drift.** README script/spec names and edge-type narrative vs code.
7. **P1 spec vs code.** Pandas vs PySpark export; no standalone star-schema doc.
8. **Lambda-at-full-scale cost.** **C6; T3.3 / T4.2.** No per-event GDS (**C5; T3.4 / T5.1**).
9. ~~No uniqueness constraints on `Account.id`.~~ **Closed in T0.3.**
10. **Power BI `.pbix` not in git.**
11. Relationship property uniqueness needs Neo4j 5.18+. Older servers will fail `ensure_constraints`.

## Unresolved decisions (do not silently pick)

- Neo4j Aura vs AWS-hosted, and GDS availability (before T3.4 / T5.1)
- ~~Relationship uniqueness vs `:Transaction` nodes~~ **Locked in T0.3:** unique `TRANSACTION.txn_id` on the relationship (Neo4j 5.18+)
- Glue Streaming vs batch-only for stream transforms (T4.3)
- Redshift provisioned vs Serverless vs Athena-only prototype (T6.1)
- Whether full 6.3M-row PaySim is in cloud prototype scope (cost)
- ~~`risk_score` vs `fraud_score`~~ **Locked in T0.1: `risk_score`**
- ~~Power BI fraud rate = model flags vs ground truth~~ **Locked in T0.2:** ground truth from eval sidecar; operational flagged KPIs from graph scores

## Do not

- Modify `PLAN.md` architecture unless the user asks to record a correction.
- `terraform apply` / `destroy`, start Kinesis/Glue/Redshift, or leave paid Neo4j running without explicit approval.
- Commit unless asked.
- Paste passwords, tokens, or `.env` values into markdown or source.
- Load `eval/paysim_eval_labels.csv` into Neo4j.

## After T0.4 (checklist for the next agent)

- [ ] Mark T0.4 `verified` in `IMPLEMENTATION_PLAN.md` only if acceptance criteria pass (Louvain before dense community, `fan_out_flag` SET, `risk_score` present, fixture-graph tests).
- [ ] Update this file: last completed, tests, leftover issues, **next task = T0.5**.
- [ ] Do not start T0.5 Parquet export unless the user expands scope.
