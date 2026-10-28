-- Athena/Hive DDL TEMPLATE ONLY. Replace {{...}} locally after authorization.
-- Execute in the T1 Glue database. T2 already registers the four curated tables.
-- Register partitions ONLY after read_export verifies a completed T3 manifest.
-- Separate empty table roots prevent recursive scans mixing manifests/signals.
CREATE EXTERNAL TABLE IF NOT EXISTS graph_runs (
  detection_run_id STRING, detected_at STRING, window_start_step BIGINT,
  window_end_step BIGINT, window_kind STRING, status STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE LOCATION 's3://{{curated_bucket}}/reporting-input/runs/';

CREATE EXTERNAL TABLE IF NOT EXISTS graph_accounts (
  account_id STRING, detected_at STRING, window_start_step BIGINT,
  window_end_step BIGINT, window_kind STRING, community_id BIGINT,
  risk_score BIGINT, fan_out_flag BIGINT, fan_in_flag BIGINT,
  unusualFanIn BIGINT, drain_behavior_flag BIGINT, dense_community_flag BIGINT,
  transfer_cashout_flag BIGINT, in_cycle_flag BIGINT,
  guilt_by_association_flag BIGINT
)
PARTITIONED BY (detection_run_id STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE LOCATION 's3://{{curated_bucket}}/reporting-input/empty-accounts/';

CREATE EXTERNAL TABLE IF NOT EXISTS graph_transactions (
  txn_id STRING, transfer_cashout_flag BIGINT, detected_at STRING,
  window_start_step BIGINT, window_end_step BIGINT, window_kind STRING
)
PARTITIONED BY (detection_run_id STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE LOCATION 's3://{{curated_bucket}}/reporting-input/empty-transactions/';

CREATE EXTERNAL TABLE IF NOT EXISTS graph_rings (
  ring_id STRING, community_id BIGINT, account_count BIGINT,
  transaction_count BIGINT, internal_amount STRING,
  transfer_cashout_amount STRING, dense_community_flag BIGINT,
  detected_at STRING, window_start_step BIGINT, window_end_step BIGINT,
  window_kind STRING
)
PARTITIONED BY (detection_run_id STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE LOCATION 's3://{{curated_bucket}}/reporting-input/empty-rings/';

-- Evaluation sidecar only. Keep it outside the operational events/ prefix.
-- Column order is T0.2 EVAL_COLUMNS. No model/scoring view references this table.
CREATE EXTERNAL TABLE IF NOT EXISTS eval_labels (
  txn_id STRING, source_file STRING, row_number BIGINT,
  isFraud INT, isFlaggedFraud INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',', 'quoteChar'='"')
STORED AS TEXTFILE LOCATION 's3://{{evaluation_bucket}}/{{evaluation_prefix}}/'
TBLPROPERTIES ('skip.header.line.count'='1');
