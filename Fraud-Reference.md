# Fraud Signals Reference

A catalog of the fraud patterns to look for in the PaySim dataset, with the tools and algorithms used to detect each. This is a reference document — the project's real focus is on the data engineering pipeline, not the analytical theory behind each signal.

Each signal is tagged with:
- **Level:** account, transaction, or ring
- **Pass:** first-pass (run directly on the raw graph) or second-pass (depends on prior flagging)

---

## Dataset Overview

PaySim is a synthetic dataset generated from a mobile money service operating in an African country. It simulates 30 days of activity, producing roughly 6.3 million transaction records across five transaction types.

### Columns

- **`step`** — Unit of simulated time. One step = one hour. Values run from 1 to 744 (30 days × 24 hours). This is an ordinal counter, not a real timestamp — there are no dates, minutes, or weekdays. Any calendar structure in the dashboard (day, week) is derived from `step` (e.g., `day = step / 24`).
- **`type`** — The transaction type. One of five values:
  - `CASH_IN` — depositing cash into the mobile money account
  - `CASH_OUT` — withdrawing cash from the mobile money account
  - `DEBIT` — sending money from the mobile money service to a bank account
  - `PAYMENT` — paying a merchant for goods or services
  - `TRANSFER` — sending money to another customer
- **`amount`** — Transaction amount in local currency.
- **`nameOrig`** — Identifier of the account initiating the transaction (the sender). Customer accounts start with `C`.
- **`oldbalanceOrg`** — The sender's account balance *before* the transaction.
- **`newbalanceOrig`** — The sender's account balance *after* the transaction.
- **`nameDest`** — Identifier of the recipient account. Customer accounts start with `C`, merchant accounts start with `M`.
- **`oldbalanceDest`** — The recipient's account balance *before* the transaction. Not populated for merchant recipients (accounts starting with `M`).
- **`newbalanceDest`** — The recipient's account balance *after* the transaction. Not populated for merchant recipients.

### Notes on the data

- The merchant accounts (`M...`) do not have balance information recorded. This matters for drain-behavior detection and any signal relying on recipient balance changes.
- There is no geography, demographic, or device data — the graph is purely accounts and money flows.
- The dataset is highly imbalanced: the vast majority of transactions are legitimate, and fraud occurs almost exclusively in `TRANSFER` and `CASH_OUT` types.

---

## Primary Signals (Required)

### Unusual fan-out
- **Level:** account
- **Pass:** first
- **What it is:** One account sending to many destinations in a short window. Classic mule hub.
- **How to find it:** Use `gds.degree.write` with `orientation: 'NATURAL'` to compute out-degree for every account and write it back as a node property. Then filter for accounts whose out-degree sits well above the population (e.g., top 1% or more than 3 standard deviations above the mean). For the "short window" aspect, group transactions by `step` in a Cypher query and count distinct destinations per source account per step — flag accounts with unusually high distinct-destination counts in a single step.

### Unusual fan-in
- **Level:** account
- **Pass:** first
- **What it is:** One account receiving from many sources. Collection/consolidation point.
- **How to find it:** Same as fan-out but reversed: use `gds.degree.write` with `orientation: 'REVERSE'` to compute in-degree per account. Flag accounts in the top tail of the distribution. Can also be approached with plain Cypher: `MATCH (a)-[t:TRANSACTION]->(b) WITH b, count(DISTINCT a) AS sources WHERE sources > threshold RETURN b`.

### Drain behavior
- **Level:** account
- **Pass:** first
- **What it is:** Account receives a large sum, then empties out shortly after. Balance goes from high to ~zero fast.
- **How to find it:** No GDS algorithm for this — pure Cypher. PaySim transactions carry `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, and `newbalanceDest` fields. Match transaction pairs on the same account ordered by `step` and flag cases where an account's balance jumps up significantly (large incoming transaction) then drops to near zero within a small number of steps. Rough pattern: `MATCH (a)-[t1:TRANSACTION]->(b), (b)-[t2:TRANSACTION]->(c) WHERE t2.step - t1.step <= N AND t2.newbalanceOrig < (t1.amount * 0.1) RETURN b`.

### Large transfers followed immediately by cash-out
- **Level:** transaction
- **Pass:** first
- **What it is:** The PaySim "signature" fraud pattern. Money comes in as TRANSFER, leaves as CASH_OUT within a step or two.
- **How to find it:** Cypher pattern match. Match a TRANSFER edge into an account and a CASH_OUT edge out of the same account where the CASH_OUT `step` is within 1–2 of the TRANSFER `step` and the amounts are similar. Example: `MATCH (a)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b)-[t2:TRANSACTION {type: 'CASH_OUT'}]->(c) WHERE t2.step - t1.step <= 2 AND abs(t1.amount - t2.amount) / t1.amount < 0.1 RETURN t1, t2`. Flag both transactions in the pair.

### Dense community with high internal volume
- **Level:** ring
- **Pass:** first
- **What it is:** A cluster of accounts moving lots of money among its own members.
- **How to find it:** Use `gds.louvain.write` to assign every account a `community_id` property. Then aggregate with Cypher: for each community, compute total transaction volume *within* the community (both endpoints share the same community_id) and total volume *leaving* the community. A high internal-to-external ratio combined with small community size (3–15 accounts) is the ring signature. Flag communities that cross a threshold on both metrics.

### Cycles
- **Level:** ring
- **Pass:** first
- **What it is:** Money returns to its origin through intermediaries.
- **How to find it:** Cypher variable-length path patterns. To find cycles of length 3–5: `MATCH path = (a:Account)-[:TRANSACTION*3..5]->(a) RETURN path`. Filter by time ordering (each hop should have a `step` greater than the previous) and by minimum amount to avoid surfacing trivial loops. GDS also offers `gds.alpha.allShortestPaths` but for cycle-specific work, Cypher's path matching is simpler. Warning: cycle queries can be expensive — cap the path length.

### Guilt by association
- **Level:** account
- **Pass:** second
- **Prerequisites:** At least one first-pass signal must have flagged a set of accounts.
- **What it is:** Accounts that transact frequently with flagged accounts but haven't been flagged themselves. Propagates risk one hop out from known bad actors.
- **How to find it:** Two options. (1) Simple Cypher: match unflagged accounts connected to N or more flagged accounts: `MATCH (a:Account {flagged: false})-[:TRANSACTION]-(b:Account {flagged: true}) WITH a, count(DISTINCT b) AS bad_neighbors WHERE bad_neighbors >= 2 RETURN a`. (2) Use `gds.labelPropagation.write` seeded with the flagged accounts as initial labels — the algorithm spreads the "flagged" label to closely-connected neighbors.

### Node similarity among flagged accounts
- **Level:** account
- **Pass:** second
- **Prerequisites:** A set of already-flagged accounts to use as the comparison set.
- **What it is:** Unflagged accounts that behave structurally like flagged ones (similar neighborhoods, similar transaction patterns). Catches mules that didn't trigger the simpler rules.
- **How to find it:** Use `gds.nodeSimilarity.write` to compute Jaccard similarity between accounts based on their shared neighbors. For each flagged account, look at its top-K most similar unflagged accounts and flag those above a similarity threshold. The algorithm writes similarity scores as a new relationship type between similar nodes, which can then be queried with Cypher.

---

## Stretch Signals (Optional)

### High PageRank among suspicious neighbors
- **Level:** account
- **Pass:** second
- **Prerequisites:** A set of already-flagged accounts; PageRank must be restricted to the flagged subgraph (running on the full graph just surfaces banks/merchants).
- **What it is:** Important node in a sketchy neighborhood — likely a ringleader or consolidation hub.
- **How to find it:** Project a subgraph containing only flagged accounts using `gds.graph.project.cypher` with filters like `MATCH (a:Account) WHERE a.flagged = true RETURN id(a) AS id`. Then run `gds.pageRank.write` over that projection. Top-scoring nodes in the filtered graph are ringleaders. Do NOT run PageRank on the full graph — the result will be dominated by high-volume legitimate accounts.

### Betweenness centrality on flagged subgraph
- **Level:** account
- **Pass:** second
- **Prerequisites:** A set of already-flagged accounts forming the subgraph.
- **What it is:** "Bridge" accounts sitting between rings. Money launderers use these to connect otherwise-disconnected fraud clusters. Good for finding cross-ring coordinators.
- **How to find it:** Project a subgraph of flagged accounts, then run `gds.betweenness.write` over the projection. Top-scoring nodes sit on the most shortest paths between other flagged accounts — these are the bridges. Note: betweenness is O(V·E), so restricting to the flagged subgraph isn't just about accuracy, it's about runtime.

### Ring-to-ring money flow
- **Level:** ring
- **Pass:** second
- **Prerequisites:** Louvain must have already assigned `community_id`s, and some communities must already be marked suspicious.
- **What it is:** Transaction volume flowing *between* suspicious communities. Suggests coordinated ring networks rather than isolated rings.
- **How to find it:** Pure Cypher aggregation on top of the Louvain output. Example: `MATCH (a:Account)-[t:TRANSACTION]->(b:Account) WHERE a.community_id <> b.community_id AND a.suspicious_ring = true AND b.suspicious_ring = true RETURN a.community_id, b.community_id, sum(t.amount) AS volume, count(t) AS tx_count ORDER BY volume DESC`. Pairs of communities with high cross-flow are candidate coordinated networks.

---

## Risk Score

Each account gets a single numeric risk score on a 0–100 scale, computed as a weighted sum of the primary signals it triggered. Each signal contributes its weight only if it fired for that account (flag value 0 or 1).

### Formula

```
risk_score =
    (10 * fan_out_flag) +
    (10 * fan_in_flag) +
    (15 * drain_flag) +
    (15 * transfer_cashout_flag) +
    (20 * in_suspicious_ring_flag) +
    (10 * in_cycle_flag) +
    (10 * guilt_by_association_flag) +
    (10 * similar_to_flagged_flag)
```

Maximum possible score: 100. An account with no flags scores 0.

### Weight rationale

- **In suspicious ring (20)** — highest weight. Membership in a detected fraud ring is the strongest single indicator, since it combines structural evidence (community detection) with internal volume (aggregation).
- **Drain behavior (15) and TRANSFER → CASH_OUT (15)** — the two behavioral signatures most specific to PaySim-style fraud.
- **Fan-out (10), fan-in (10), cycles (10), node similarity (10), guilt by association (10)** — supporting signals. Each adds meaningful evidence but is noisier or more indirect than the top three. Equal weighting reflects that they're roughly comparable in reliability.

### Applying the score

- Write `risk_score` back onto each account node as a property during the detection phase.
- The score should be available on each account when the data is exported for the reporting layer.
- The dashboard's "Top 10 riskiest accounts" visualization ranks by this value.
- The "flagged" threshold is determined during the reporting phase, not hard-coded. Use the risk score distribution histogram in the dashboard to identify a natural break — typically a valley between a large cluster of low-scoring legitimate accounts and a smaller tail of higher-scoring suspicious ones. The threshold chosen should be defensible from what the distribution shows.

### Notes

- The score is additive and explainable — we can always point at an account and list exactly which signals fired and how many points each contributed.
- Stretch signals (PageRank, betweenness, ring-to-ring flow) are not included in this formula. If we implement them, we should either leave the score unchanged or propose an extension and justify it.