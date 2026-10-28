CREATE VIEW rpt_ring_volume AS
WITH internal AS (
    SELECT origin_community_id AS community_id, COUNT(*) AS transaction_count,
           SUM(amount) AS internal_volume
    FROM rpt_transactions
    WHERE origin_community_id = destination_community_id
    GROUP BY origin_community_id
)
SELECT g.detection_run_id, g.detected_at, g.ring_id, g.community_id,
       g.account_count AS graph_member_count,
       COALESCE(i.transaction_count, 0) AS transaction_count,
       COALESCE(i.internal_volume, CAST(0 AS DECIMAL(38,10))) AS internal_volume,
       CAST(g.internal_amount AS DECIMAL(38,10)) AS full_graph_internal_volume
FROM rpt_ring_signals g LEFT JOIN internal i ON g.community_id = i.community_id
WHERE g.dense_community_flag = 1
  AND EXISTS (SELECT 1 FROM rpt_accounts a WHERE a.community_id = g.community_id);

CREATE VIEW rpt_kpis AS
SELECT r.detection_run_id, r.detected_at, p.risk_threshold,
       (SELECT COUNT(*) FROM rpt_transactions) AS total_transactions,
       (SELECT COUNT(*) FROM rpt_accounts) AS total_accounts,
       (SELECT COUNT(risk_score) FROM rpt_accounts) AS scored_accounts,
       (SELECT SUM(is_high_risk) FROM rpt_accounts) AS flagged_accounts,
       CASE WHEN r.detection_run_id IS NULL THEN NULL
            ELSE (SELECT COUNT(*) FROM rpt_ring_volume) END AS rings_detected,
       (SELECT COUNT(is_model_flagged) FROM rpt_transactions) AS assessed_transactions,
       (SELECT SUM(CASE WHEN is_model_flagged = 1 THEN amount
                        WHEN is_model_flagged = 0 THEN CAST(0 AS DECIMAL(38,10))
                        ELSE NULL END) FROM rpt_transactions) AS total_flagged_volume
FROM rpt_settings p LEFT JOIN rpt_selected_run r ON 1 = 1;

CREATE VIEW rpt_risk_distribution AS
SELECT detection_run_id, detected_at, risk_threshold,
       CAST(risk_score / 10 AS BIGINT) * 10 AS bucket_start,
       CAST(risk_score / 10 AS BIGINT) * 10 + 9 AS bucket_end,
       COUNT(*) AS account_count
FROM rpt_accounts WHERE risk_score IS NOT NULL
GROUP BY detection_run_id, detected_at, risk_threshold, CAST(risk_score / 10 AS BIGINT);

CREATE VIEW rpt_flagged_volume_over_time AS
SELECT detection_run_id, detected_at, risk_threshold, step, simulated_date,
       COUNT(*) AS transaction_count, COUNT(is_model_flagged) AS assessed_transactions,
       SUM(CASE WHEN is_model_flagged = 1 THEN 1 ELSE 0 END) AS flagged_transactions,
       SUM(CASE WHEN is_model_flagged = 1 THEN amount
                WHEN is_model_flagged = 0 THEN CAST(0 AS DECIMAL(38,10)) ELSE NULL END) AS flagged_volume
FROM rpt_transactions
GROUP BY detection_run_id, detected_at, risk_threshold, step, simulated_date;

CREATE VIEW rpt_transaction_type_comparison AS
SELECT r.detection_run_id, r.detected_at, p.risk_threshold,
       d.type_key, d.type_name, COUNT(t.txn_id) AS transaction_count,
       COUNT(t.is_model_flagged) AS assessed_transactions,
       SUM(CASE WHEN t.is_model_flagged = 1 THEN 1 ELSE 0 END) AS flagged_transactions,
       SUM(CASE WHEN t.is_model_flagged = 1 THEN t.amount
                WHEN t.is_model_flagged = 0 THEN CAST(0 AS DECIMAL(38,10)) ELSE NULL END) AS flagged_volume,
       CAST(SUM(CASE WHEN t.is_model_flagged = 1 THEN 1 ELSE 0 END) AS DOUBLE)
         / NULLIF(COUNT(t.is_model_flagged), 0) AS model_flag_rate
FROM dim_transaction_type d CROSS JOIN rpt_settings p
LEFT JOIN rpt_selected_run r ON 1 = 1
LEFT JOIN rpt_transactions t ON t.type_key = d.type_key
GROUP BY r.detection_run_id, r.detected_at, p.risk_threshold, d.type_key, d.type_name;

CREATE VIEW rpt_riskiest_accounts AS
SELECT a.*, ROW_NUMBER() OVER (ORDER BY risk_score DESC, account_id) AS risk_rank
FROM rpt_accounts a WHERE risk_score IS NOT NULL;

CREATE VIEW rpt_top_10_accounts AS
SELECT * FROM rpt_riskiest_accounts WHERE risk_rank <= 10;
