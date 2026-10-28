CREATE VIEW rpt_accounts AS
SELECT a.account_id, r.detection_run_id, r.detected_at,
       p.risk_threshold, g.community_id, g.risk_score,
       g.fan_out_flag, g.fan_in_flag, g.drain_behavior_flag,
       g.transfer_cashout_flag, g.dense_community_flag, g.in_cycle_flag,
       g.guilt_by_association_flag,
       CASE WHEN g.risk_score IS NULL THEN NULL
            WHEN g.risk_score >= p.risk_threshold THEN 1 ELSE 0 END AS is_high_risk
FROM dim_account a CROSS JOIN rpt_settings p
LEFT JOIN rpt_selected_run r ON 1 = 1
LEFT JOIN rpt_account_signals g ON a.account_id = g.account_id;

CREATE VIEW rpt_transactions AS
SELECT f.txn_id, f.step, f.type_key, f.type, f.amount,
       f.nameOrig, f.nameDest, d.date AS simulated_date,
       r.detection_run_id, r.detected_at, p.risk_threshold,
       o.community_id AS origin_community_id, z.community_id AS destination_community_id,
       o.risk_score AS origin_risk_score, z.risk_score AS destination_risk_score,
       t.transfer_cashout_flag,
       CASE WHEN t.transfer_cashout_flag = 1 OR o.is_high_risk = 1 OR z.is_high_risk = 1 THEN 1
            WHEN t.txn_id IS NOT NULL AND o.risk_score IS NOT NULL AND z.risk_score IS NOT NULL THEN 0
            ELSE NULL END AS is_model_flagged
FROM fact_transactions f CROSS JOIN rpt_settings p
LEFT JOIN rpt_selected_run r ON 1 = 1
LEFT JOIN dim_date d ON f.date_key = d.date_key
LEFT JOIN rpt_accounts o ON f.nameOrig = o.account_id
LEFT JOIN rpt_accounts z ON f.nameDest = z.account_id
LEFT JOIN rpt_transaction_signals t ON f.txn_id = t.txn_id;
