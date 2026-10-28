CREATE VIEW rpt_selected_run AS
WITH ranked AS (
    SELECT r.*, ROW_NUMBER() OVER (
        ORDER BY from_iso8601_timestamp(r.detected_at) DESC, r.detection_run_id DESC
    ) AS run_rank
    FROM graph_runs r CROSS JOIN rpt_settings p
    WHERE r.status = 'complete'
      AND (p.chosen_detection_run_id IS NULL
           OR r.detection_run_id = p.chosen_detection_run_id)
)
SELECT detection_run_id, detected_at, window_start_step, window_end_step, window_kind
FROM ranked WHERE run_rank = 1;

-- Reject ambiguous duplicate keys rather than multiplying reporting facts.
-- rpt_data_quality must pass before a report is published.
CREATE VIEW rpt_account_signals AS
WITH scoped AS (
    SELECT g.* FROM graph_accounts g JOIN rpt_selected_run r
      ON g.detection_run_id = r.detection_run_id AND g.detected_at = r.detected_at
     AND g.window_start_step = r.window_start_step AND g.window_end_step = r.window_end_step
     AND g.window_kind = r.window_kind
), valid_keys AS (
    SELECT account_id FROM scoped GROUP BY account_id HAVING COUNT(*) = 1
)
SELECT s.* FROM scoped s JOIN valid_keys k ON s.account_id = k.account_id;

CREATE VIEW rpt_transaction_signals AS
WITH scoped AS (
    SELECT g.* FROM graph_transactions g JOIN rpt_selected_run r
      ON g.detection_run_id = r.detection_run_id AND g.detected_at = r.detected_at
     AND g.window_start_step = r.window_start_step AND g.window_end_step = r.window_end_step
     AND g.window_kind = r.window_kind
), valid_keys AS (
    SELECT txn_id FROM scoped GROUP BY txn_id HAVING COUNT(*) = 1
)
SELECT s.* FROM scoped s JOIN valid_keys k ON s.txn_id = k.txn_id;

CREATE VIEW rpt_ring_signals AS
WITH scoped AS (
    SELECT g.* FROM graph_rings g JOIN rpt_selected_run r
      ON g.detection_run_id = r.detection_run_id AND g.detected_at = r.detected_at
     AND g.window_start_step = r.window_start_step AND g.window_end_step = r.window_end_step
     AND g.window_kind = r.window_kind
), valid_keys AS (
    SELECT ring_id FROM scoped GROUP BY ring_id HAVING COUNT(*) = 1
)
SELECT s.* FROM scoped s JOIN valid_keys k ON s.ring_id = k.ring_id;
