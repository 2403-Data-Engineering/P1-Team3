CREATE VIEW rpt_data_quality AS
SELECT
  (SELECT COUNT(*) FROM (SELECT txn_id FROM fact_transactions GROUP BY txn_id HAVING COUNT(*) > 1) x) AS duplicate_fact_keys,
  (SELECT COUNT(*) FROM (SELECT account_id FROM dim_account GROUP BY account_id HAVING COUNT(*) > 1) x) AS duplicate_account_keys,
  (SELECT COUNT(*) FROM (SELECT detection_run_id FROM graph_runs GROUP BY detection_run_id HAVING COUNT(*) > 1) x) AS duplicate_run_keys,
  (SELECT COUNT(*) FROM (SELECT g.account_id FROM graph_accounts g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id GROUP BY g.account_id HAVING COUNT(*) > 1) x) AS duplicate_signal_account_keys,
  (SELECT COUNT(*) FROM (SELECT g.txn_id FROM graph_transactions g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id GROUP BY g.txn_id HAVING COUNT(*) > 1) x) AS duplicate_signal_transaction_keys,
  (SELECT COUNT(*) FROM (SELECT g.ring_id FROM graph_rings g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id GROUP BY g.ring_id HAVING COUNT(*) > 1) x) AS duplicate_signal_ring_keys,
  (SELECT COUNT(*) FROM rpt_accounts WHERE risk_score IS NULL) AS unscored_accounts,
  (SELECT COUNT(*) FROM rpt_transactions WHERE is_model_flagged IS NULL) AS unassessed_transactions,
  (SELECT COUNT(*) FROM fact_transactions f LEFT JOIN rpt_transaction_signals t ON f.txn_id = t.txn_id WHERE t.txn_id IS NULL) AS missing_transaction_signals,
  (SELECT COUNT(*) FROM (
     SELECT g.detected_at, g.window_start_step, g.window_end_step, g.window_kind
       FROM graph_accounts g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id
     UNION ALL
     SELECT g.detected_at, g.window_start_step, g.window_end_step, g.window_kind
       FROM graph_transactions g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id
     UNION ALL
     SELECT g.detected_at, g.window_start_step, g.window_end_step, g.window_kind
       FROM graph_rings g JOIN rpt_selected_run r ON g.detection_run_id = r.detection_run_id
   ) g CROSS JOIN rpt_selected_run r
   WHERE CASE WHEN g.detected_at = r.detected_at AND g.window_start_step = r.window_start_step
                    AND g.window_end_step = r.window_end_step AND g.window_kind = r.window_kind
              THEN 0 ELSE 1 END = 1) AS mismatched_signal_metadata,
  (SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_date d ON f.date_key = d.date_key WHERE d.date_key IS NULL) AS missing_date_keys,
  (SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_transaction_type d ON f.type_key = d.type_key WHERE d.type_key IS NULL) AS missing_type_keys,
  (SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_account a ON f.nameOrig = a.account_id LEFT JOIN dim_account b ON f.nameDest = b.account_id WHERE a.account_id IS NULL OR b.account_id IS NULL) AS missing_account_keys,
  (SELECT COUNT(*) FROM rpt_selected_run) AS selected_run_count;
