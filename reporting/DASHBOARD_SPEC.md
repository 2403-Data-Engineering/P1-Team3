# T4 dashboard specification (tool independent, code only)

This document specifies a report; no `.pbix`, live Athena query or deployed
dashboard was created. Use Power BI **Import mode** or any BI tool that can
consume tabular query results. All view definitions are under `reporting/sql/`.

| Visual | View | Fields / aggregation | Chart type |
|---|---|---|---|
| KPI summary | `rpt_kpis` | `total_transactions`, `total_accounts`, `flagged_accounts`, `rings_detected`, `total_flagged_volume`; display each single returned value, do not sum across refreshes | Five cards, plus coverage/status cards |
| Risk distribution | `rpt_risk_distribution` | x=`bucket_start`–`bucket_end`, y=`account_count`; numeric bucket order; threshold annotation from `risk_threshold` | Column histogram (fixed ten-point bins) |
| Flagged volume over time | `rpt_flagged_volume_over_time` | x=`step` (or `simulated_date`), y=`flagged_volume`; optionally tooltip `transaction_count`, `flagged_transactions`, `assessed_transactions` | Line or area chart |
| Transaction-type comparison (operational) | `rpt_transaction_type_comparison` | x=`type_name`, y=`model_flag_rate` formatted as percentage; tooltips `transaction_count`, `assessed_transactions`, `flagged_transactions`, `flagged_volume` | Column chart, explicitly titled **Model flag rate by type** |
| Fraud rate by transaction type (required ground truth) | `rpt_ground_truth_fraud_by_type` | x=`type_name`, y=`ground_truth_fraud_rate` as percentage; mandatory coverage annotation `label_coverage`, `labeled_transactions`, `unlabeled_transactions` | Separate column chart titled **Evaluation-only ground-truth fraud rate** |
| Top 10 riskiest accounts | `rpt_top_10_accounts` | category=`account_id`, value=`risk_score`, order=`risk_rank`; tooltips `community_id`, required signal flags, threshold | Horizontal bars or conditional-format table |
| Rings moving the most money | `rpt_ring_volume` | category=`ring_id`, value=`internal_volume`; tooltips `transaction_count`, `graph_member_count`, `full_graph_internal_volume`; sort volume descending | Bar chart; optional member-count/volume scatter |
| Run / coverage / quality banner | `rpt_selected_run`, `rpt_kpis`, `rpt_data_quality` | run ID, detected timestamp, step bounds/kind, threshold; scored/total accounts, assessed/total transactions, quality counters | Text banner and status table |

Every graph-derived view exposes `detection_run_id` and `detected_at`. Display
these in page subtitles/tooltips; never merge ring IDs across runs. The newest
**complete** run is selected by actual ISO timestamp, breaking ties by run ID.
Set `chosen_detection_run_id` in `00_settings.sql` to pin a completed run; an
unknown chosen run returns no graph selection rather than silently using latest.
Use pinned selection for a BI refresh so separate queries cannot see different
latest runs while new exports arrive. Keep registry/curated input fixed for that
refresh; enable refresh only after data-quality checks pass.

The initial `risk_threshold=30` is a **provisional configurable value**, not a
claim of calibration on four fixture transactions. Use the histogram and a
separately justified population review to choose the final threshold. Change
the SQL settings view and refresh all operational views together. The threshold
is applied to existing risk scores; it does not change T0.4 scoring. Use the
same threshold consistently in cards, time series and operational type rates.

Definitions are explicit:

- An account is high risk when its selected-run score is at least the threshold.
  Missing scores remain unknown, never zero. The histogram excludes unknowns;
  the banner must show their count.
- A transaction is model-flagged if its relationship transfer-cashout flag is 1
  **or** either endpoint is high risk. Count its amount once even if several
  conditions fire. A known negative needs both endpoint scores and a matched
  transaction signal; otherwise its status is unknown. Model-rate denominators
  are assessed transactions, not all transactions. Show coverage alongside rates.
- Flagged volume sums amounts of these model-flagged facts. It is neither
  ground-truth fraud volume nor a forecast. Missing graph runs produce null
  graph metrics and a missing-run status, while total transactions/accounts
  remain available. Do not format null as “zero fraud.”
- A detected ring is a selected-run community with `dense_community_flag=1` and
  at least one member present in the curated account dimension. Internal volume
  is recomputed from **current curated facts** with both endpoints in that
  community. `full_graph_internal_volume` is context from the full T3 snapshot;
  it may differ for a smaller curated sample and must not replace the primary
  volume. Inter-community transfers do not contribute to internal ring volume.
- Ground truth is `isFraud` from the evaluation sidecar, joined by `txn_id`.
  `isFlaggedFraud` is not substituted as truth. Rate = fraud-labeled facts /
  labeled facts. Missing, invalid or duplicate labels are excluded from this
  denominator and lower coverage. No labels feed operational views or scoring.
- PaySim step/date are **simulated** ordinal time using the shared synthetic
  2026-03-01 origin, not real transaction or ingestion timestamps.

For Import mode, import the aggregate visual views as independent tables; each
has its own grain. Do not create fact-to-fact relationships among aggregates,
sum rates, or sum KPI cards across unrelated chart selections. Type/day filters
on preaggregated views are local to those visuals unless corresponding filters
are applied to all detail datasets before aggregation. A dynamic slicer-aware
semantic model can instead import `rpt_accounts` and `rpt_transactions` and
rebuild measures with the exact rules above; join accounts twice for sender and
receiver roles. Preserve txn_id uniqueness and selected-run consistency.

Quality gate: `selected_run_count=1`; all duplicate-key, missing-key,
mismatched-metadata, missing-transaction-signal and unscored/unassessed counters
must be zero for a fully reconciled operational report. Do not publish a partial
graph population as fully assessed. Evaluation-only charts may show partial
label coverage but must display that coverage and explain it. Reconcile the
time/type sums to KPI volume and ring totals to the appropriately scoped facts.

No refresh schedule or connector was installed here. A future authorized Import
refresh should follow batch completion and verified graph export publication;
DirectQuery is deferred. The query workgroup has a 100 MiB scan limit and SSE-S3
result output, but scan limits/budget alerts are not hard account-wide cost caps.
