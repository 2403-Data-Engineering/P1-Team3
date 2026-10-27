# Plan: Hybrid AWS Stream-and-Batch Migration

## Recommendation

Use a hybrid architecture: one Kinesis stream for incoming events, plus Glue batch processing for historical loads, reconciliation, and scheduled reporting. Both paths should produce a consistent dataset in S3; Redshift can serve the curated reporting model to Power BI. Keep Neo4j for graph-based fraud detection, rather than trying to run graph analytics on every incoming event.

## Architecture

- **Ingest:** Replay the PaySim CSV into Kinesis initially; later, a live producer can use the same versioned event format.
- **Store:** Preserve the original events in S3, then use Glue to catalog and transform them into curated Parquet tables.
- **Stream processing:** Use Lambda for bounded validation, deduplication, routing, and simple low-latency alerts. Use Glue Streaming or micro-batches for heavier stream transformations.
- **Graph detection:** Keep Neo4j for graph analysis. Process transaction events in batches and run GDS/Cypher detection on scheduled or incremental windows. Don't invoke graph analytics once per event.
- **Reporting:** Load the curated star schema into Redshift for Power BI. Compare Athena during the prototype if useful, but Redshift is the selected reporting target.
- **Batch processing:** Use Glue batch jobs for historical backfill, replay, reconciliation, and scheduled refresh. This is the "one batch" complement to the streaming path.

## Steps

1. **Define the event and data contracts**
   - Specify a versioned transaction event containing the PaySim fields, source step, an ingestion timestamp, and a stable event ID.
   - For CSV replay, derive a stable ID that distinguishes duplicate-looking rows, such as one based on the source file and row number.
   - Define how missing merchant balance values are represented.
   - Define idempotency and deduplication rules so retries and replay do not create duplicate facts or graph relationships.

2. **Establish the AWS foundation**
   - Set up scoped IAM roles, secrets management, encryption, network access, logging, and resource teardown/cost controls.
   - Compare Neo4j Aura and AWS-hosted Neo4j for connectivity from Glue/Lambda, GDS availability, security, and cost.
   - Document environment-specific configuration rather than carrying local credentials or endpoints into cloud jobs.

3. **Build the streaming path**
   - Publish replayed PaySim events to Kinesis, with a future live producer using the same contract.
   - Persist raw events to S3 for recovery, replay, and audit.
   - Use Lambda for validation, deduplication, simple transaction-local rules, and routing alert candidates.
   - Add a separate batched graph-writer consumer if new events must reach Neo4j continuously. Use idempotent batched writes; reserve graph-wide detection for scheduled or incremental analysis.
   - Choose and document the required alert latency and retry behavior.

4. **Build the Glue batch and curated-data path**
   - Use Glue batch jobs to load historical data from S3, transform it, and support scheduled refreshes and reconciliation.
   - Use Glue Streaming or a controlled micro-batch consumer for heavier stream transformations if needed.
   - Make the batch and streaming transformations converge on the same canonical transaction grain, dimensions, and fraud fields.
   - Catalog the raw and curated data with Glue Data Catalog.
   - Preserve the current star-schema intent: transaction facts, accounts, transaction types, and dates, with graph-derived account/ring signals joined where available.

5. **Connect Neo4j detection to reporting**
   - Retain the project's Cypher/GDS detection logic as the graph-analysis layer.
   - Define how graph-derived account, transaction, and ring results are exported and joined to the curated tables.
   - Keep graph-derived fields versioned or timestamped so reports can distinguish a new detection run from a prior one.
   - Limit scope initially to the existing required signals; treat stretch signals as later additions.

6. **Serve reports from Redshift**
   - Load curated data into Redshift and establish the Power BI connection.
   - Start with Power BI Import mode and scheduled refresh; evaluate DirectQuery only if freshness requirements justify its performance and cost.
   - Recreate the required KPIs, risk-score distribution, flagged volume over time, transaction-type comparison, riskiest accounts, and ring-volume views.

7. **Migrate and verify incrementally**
   - Use the existing local export as a reference, then replace local file paths and manual steps with S3/Glue-based workflows.
   - Test replay, retries, duplicate events, malformed records, missing balance values, and batch/stream agreement.
   - Compare transaction counts and key aggregates between local output, curated S3 data, Neo4j, and Redshift.
   - Test Power BI refresh and confirm dashboards still answer the project questions.

## Relevant files

- `database_creation_script.py`: current CSV-to-Neo4j ingestion; reference for graph loading and idempotent migration.
- `batching_python_driver.py`: current keyset-paginated Neo4j export and star-schema/Parquet logic; migration reference, though its active `main()` currently rebuilds only `dim_account`.
- `cypher_queries.md`: existing graph-detection queries to preserve or adapt.
- `Fraud-Reference.md`: required signals, scoring, and detection guidance.
- `P1-specifications.md`: dashboard and project deliverables.
- `download_data.py`: current input preparation; review how fraud labels are handled before designing evaluation reporting.
- `README.md`: current setup and graph model documentation to update for the cloud workflow.

## Verification

1. Confirm a CSV replay produces valid Kinesis events and raw S3 records with stable IDs.
2. Replay the same events twice; verify deduplication prevents duplicate facts and graph relationships.
3. Compare Glue batch output and streaming output for the same events at the agreed transaction grain.
4. Validate graph detection outputs and their joins to account, transaction, and ring reporting data.
5. Check Redshift totals and Power BI visuals against the curated S3 data.
6. Verify IAM/network access, failure recovery, logs, and resource teardown.

## Decisions and caveats

- The plan targets both a learning demo and a credible production evolution, with a moderate budget.
- The initial stream source is CSV replay, with a compatible path for a future live producer.
- PaySim `step` is simulated ordinal time, not a real timestamp. Preserve it separately from event and ingestion timestamps.
- The current input preparation removes `isFraud` and `isFlaggedFraud`. As a result, fraud-rate visuals and ground-truth evaluation cannot be computed from the cleaned data alone. Recommendation: retain labels in a separate evaluation-only dataset, not in the operational scoring path.
- Keep Neo4j; compare Aura and AWS-hosted deployment options before choosing one.
- Out of scope for the first migration: replacing Neo4j, multi-region disaster recovery, and ML model training.
