# Project 1: Graph-Based Fraud Detection

## Description

Build a fraud detection system on the PaySim synthetic financial dataset. Model the transaction network as a graph in Neo4j, use Cypher and graph algorithms to surface fraudulent accounts, transactions, and rings, then export the results into a star schema and present them through a PowerBI dashboard.

See the companion document *Fraud Signals Reference* for the specific patterns to detect.

---

## Project Phases

### 1. Ingestion
Load the PaySim dataset into Neo4j. Design the graph model — accounts as nodes, transactions as relationships — and import the data so that the resulting graph supports the detection work in the next phase.

### 2. Detection
Run Cypher queries and GDS algorithms against the graph to flag fraudulent accounts, transactions, and rings. Write findings back onto the graph as node and relationship properties (risk scores, ring ids, flags, community assignments). See *Fraud Signals Reference* for the required and stretch signals.

### 3. Modeling
Design the star schema for the reporting layer. Define the fact table and its grain, the dimensions needed to answer the dashboard questions, and the keys connecting them. Document the schema before moving to export.

### 4. Export
Write one Cypher query per star schema table to pull the data out of Neo4j in the correct shape. Use Python (with the Neo4j driver and pandas) to execute each query and write the results to Parquet files — one Parquet file per table.

### 5. Reporting
Load the Parquet files into PowerBI Desktop (Import mode) and build a dashboard that answers the required questions. Save as a `.pbix` file.

Use the risk score distribution visualization in the dashboard to determine a defensible threshold for flagging an account as high-risk. Look for a natural break in the distribution — typically a valley between the cluster of legitimate low-scoring accounts and the tail of higher-scoring suspicious ones. The chosen threshold will be justified during the presentation.

Prepare a presentation as described in the *Presentation* section below.

---

## Required Visualizations

The PowerBI dashboard must include a visualization for each of the following. The suggested chart types are standard choices — we can pick alternatives if they can be justified.

### KPI summary cards
A row of single-value tiles at the top of the dashboard showing headline numbers: total transactions, total accounts, flagged accounts, rings detected, and total flagged volume. Use PowerBI's Card or Multi-row Card visuals. Gives the viewer immediate scale before they dig into the details.

### Risk score distribution
Histogram of account risk scores across the population. Demonstrates whether the scoring approach produces meaningful separation between likely-fraudulent and likely-legitimate accounts, and helps justify where the "flagged" threshold is set.

### Total flagged volume over time
Line chart or area chart with `step` (or derived day) on the x-axis and summed flagged transaction amount on the y-axis. A line chart emphasizes trend; an area chart emphasizes magnitude.

### Fraud rate by transaction type
Bar chart (or column chart) with transaction type on one axis and fraud rate (as a percentage) on the other. A 100% stacked bar showing fraud vs. legitimate per type is an alternative that conveys both volume and rate.

### Top 10 riskiest accounts
Horizontal bar chart ranking accounts by risk score, with account id as the label and score as the bar length. A table with conditional formatting on the risk score column is a reasonable alternative for dense detail.

### Which rings moved the most money
Bar chart with ring id (community id) on one axis and total internal transaction volume on the other. For added context, a treemap can show relative ring sizes at a glance, or a scatter plot can show ring size (member count) vs. total volume to surface small-but-active rings.

---

## Presentation

Prepare a PowerPoint (or equivalent) deck walking the audience through the project from start to finish. The deck should tell the story of the work, not just display the artifacts. Sections to cover:

### Project journey
Brief overview of the approach taken from ingestion through reporting — what decisions were made at each phase and why. This frames the rest of the presentation.

### Detection findings
What the detection phase surfaced. Which signals were implemented, what each one revealed about the data, and how they were combined into the risk score. Describe the scoring approach and justify the weighting.

### Star schema
The data model designed for the reporting layer. Show the fact table and dimensions, explain the grain of the fact table, and describe how the schema was derived from the dashboard's questions.

### Dashboard walkthrough
Live walkthrough (or screenshots) of the PowerBI dashboard. Answer each of the required dashboard questions using the visualizations built. Point out anything unexpected the data revealed.

### Case studies
Two or three specific flagged accounts or rings, walked through in detail. Connect the signals that fired to the actual behavior in the data. Include Neo4j Browser graph visualizations where they help tell the story — a ring of accounts with money flowing in a cycle is far more compelling than a bar chart.