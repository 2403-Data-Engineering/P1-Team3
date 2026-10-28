-- Evaluation only. No operational/model view depends on this view or table.
-- Invalid/duplicate labels are excluded and reduce reported coverage.
CREATE VIEW rpt_ground_truth_fraud_by_type AS
WITH labels AS (
    SELECT txn_id, MAX(isFraud) AS is_fraud
    FROM eval_labels GROUP BY txn_id
    HAVING COUNT(*) = 1 AND MIN(isFraud) IN (0, 1)
)
SELECT d.type_key, d.type_name, COUNT(f.txn_id) AS transaction_count,
       COUNT(l.txn_id) AS labeled_transactions,
       COUNT(f.txn_id) - COUNT(l.txn_id) AS unlabeled_transactions,
       SUM(CASE WHEN l.is_fraud = 1 THEN 1 ELSE 0 END) AS ground_truth_fraud_transactions,
       CAST(SUM(CASE WHEN l.is_fraud = 1 THEN 1 ELSE 0 END) AS DOUBLE)
         / NULLIF(COUNT(l.txn_id), 0) AS ground_truth_fraud_rate,
       CAST(COUNT(l.txn_id) AS DOUBLE) / NULLIF(COUNT(f.txn_id), 0) AS label_coverage
FROM dim_transaction_type d LEFT JOIN fact_transactions f ON f.type_key = d.type_key
LEFT JOIN labels l ON f.txn_id = l.txn_id
GROUP BY d.type_key, d.type_name;
