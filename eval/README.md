# Evaluation-label sidecar

Operational PaySim processing uses unlabeled `paysim_clean.csv` (repo root, gitignored). Ground-truth labels live only in this directory.

| File | Role | Git |
|---|---|---|
| `../paysim_clean.csv` | Operational extract: PaySim columns **without** `isFraud` / `isFlaggedFraud` | gitignored (`*.csv`) |
| `paysim_eval_labels.csv` | Evaluation sidecar: `txn_id`, `source_file`, `row_number`, `isFraud`, `isFlaggedFraud` | gitignored (`eval/*.csv`) |

`download_data.py` writes both. `txn_id` is computed with the v1 contract using **`source_file = paysim_clean.csv`** and the **1-based operational row number**, so replay of the ops CSV matches the sidecar.

## Policy

- **Do not** load `isFraud` or `isFlaggedFraud` into Neo4j, Kinesis, or operational events.
- **Ground-truth** “fraud rate by transaction type” (and similar eval metrics) **join this sidecar on `txn_id`**.
- **Operational / dashboard “flagged” KPIs** use graph detection (`risk_score` and signal flags), not these labels.
- Join grain is **1:1** `txn_id`. Duplicate-looking PaySim rows stay distinct via `row_number` inside `txn_id`.
