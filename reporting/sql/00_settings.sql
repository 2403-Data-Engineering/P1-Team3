-- Set chosen_detection_run_id to a completed run ID for a pinned BI refresh.
-- NULL means latest completed run. Threshold 30 is provisional, not calibrated.
CREATE VIEW rpt_settings AS
SELECT CAST(NULL AS VARCHAR) AS chosen_detection_run_id,
       CAST(30 AS BIGINT) AS risk_threshold;
