// Ordered required-signal pipeline. Stretch GDS (node similarity, PageRank,
// betweenness) is out of scope. community_id comes from Louvain BEFORE dense
// community aggregation. Score property is risk_score.

// STEP reset
MATCH (s:CommunityStats) DETACH DELETE s;
MATCH (a:Account)
SET a.fan_out_flag = null,
    a.fan_in_flag = null,
    a.unusualFanIn = null,
    a.drain_behavior_flag = null,
    a.transfer_cashout_flag = null,
    a.dense_community_flag = null,
    a.in_cycle_flag = null,
    a.guilt_by_association_flag = null,
    a.similar_to_flagged_flag = null,
    a.community_id = null,
    a.community_transaction_count = null,
    a.community_total_amount = null,
    a.outDegree = null,
    a.inDegree = null,
    a.risk_score = 0;
MATCH ()-[t:TRANSACTION]->()
SET t.transfer_cashout_flag = null;

// STEP project
CALL gds.graph.drop('accountGraph', false) YIELD graphName;
CALL gds.graph.project(
  'accountGraph',
  'Account',
  { TRANSACTION: { orientation: 'UNDIRECTED' } }
)
YIELD graphName
RETURN graphName;

// STEP degree_out
CALL gds.degree.write('accountGraph', { writeProperty: 'outDegree', orientation: 'NATURAL' })
YIELD nodePropertiesWritten
RETURN nodePropertiesWritten;

// STEP fan_out_flag
MATCH (a:Account)
WHERE a.outDegree IS NOT NULL
WITH percentileCont(a.outDegree, 0.99) AS p99
MATCH (n:Account)
WHERE n.outDegree >= p99 AND n.outDegree >= 2
SET n.fan_out_flag = 1;

// STEP degree_in
CALL gds.degree.write('accountGraph', { writeProperty: 'inDegree', orientation: 'REVERSE' })
YIELD nodePropertiesWritten
RETURN nodePropertiesWritten;

// STEP fan_in_flag
MATCH (a:Account)
WHERE a.inDegree IS NOT NULL
WITH percentileCont(a.inDegree, 0.99) AS p99
MATCH (n:Account)
WHERE n.inDegree >= p99 AND n.inDegree >= 2
SET n.fan_in_flag = 1, n.unusualFanIn = 1;

// STEP drain
MATCH (a)-[t1:TRANSACTION]->(b)-[t2:TRANSACTION]->(c)
WHERE t2.step - t1.step <= 3
  AND t2.newbalanceOrig < 50
SET b.drain_behavior_flag = 1;

// STEP transfer_cashout
MATCH (a:Account)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b:Account)-[t2:TRANSACTION {type: 'CASH_OUT'}]->(c:Account)
WHERE t2.step - t1.step <= 2
  AND t1.amount > 0
  AND abs(t1.amount - t2.amount) / t1.amount < 0.1
SET t1.transfer_cashout_flag = 1,
    t2.transfer_cashout_flag = 1,
    b.transfer_cashout_flag = 1;

// STEP louvain
CALL gds.louvain.write('accountGraph', { writeProperty: 'community_id' })
YIELD communityCount
RETURN communityCount;

// STEP dense_community
CREATE INDEX community_stats_id IF NOT EXISTS FOR (c:CommunityStats) ON (c.community_id);
CREATE INDEX account_community_id IF NOT EXISTS FOR (a:Account) ON (a.community_id);
MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
WHERE a.community_id = b.community_id
WITH a.community_id AS community, count(t) AS tc, sum(t.amount) AS ta
MERGE (c:CommunityStats {community_id: community})
SET c.transaction_count = tc, c.total_amount = ta;
MATCH (a:Account)
WITH a.community_id AS community, count(a) AS size
MATCH (c:CommunityStats {community_id: community})
SET c.size = size;
MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
WHERE a.community_id <> b.community_id
WITH a.community_id AS community, sum(t.amount) AS leaving
MATCH (c:CommunityStats {community_id: community})
SET c.out_amount = leaving;
MATCH (acc:Account)
MATCH (c:CommunityStats {community_id: acc.community_id})
SET acc.community_transaction_count = c.transaction_count,
    acc.community_total_amount = c.total_amount;
MATCH (c:CommunityStats)
WHERE c.size >= 3 AND c.size <= 15
  AND c.transaction_count >= c.size
  AND coalesce(c.out_amount, 0) < coalesce(c.total_amount, 0) * 0.1
MATCH (a:Account {community_id: c.community_id})
SET a.dense_community_flag = 1;

// STEP cycles
MATCH (a:Account)-[t1:TRANSACTION]->(b:Account)-[t2:TRANSACTION]->(c:Account)-[t3:TRANSACTION]->(a)
WHERE b <> a AND c <> a AND b <> c
  AND t2.step >= t1.step AND t3.step >= t2.step
SET a.in_cycle_flag = 1, b.in_cycle_flag = 1, c.in_cycle_flag = 1;

// STEP guilt
MATCH (a:Account)-[:TRANSACTION]-(b:Account)
WHERE a.dense_community_flag IS NULL
  AND a.drain_behavior_flag IS NULL
  AND a.unusualFanIn IS NULL
  AND a.fan_out_flag IS NULL
  AND a.transfer_cashout_flag IS NULL
  AND (
    b.dense_community_flag = 1 OR b.drain_behavior_flag = 1
    OR b.unusualFanIn = 1 OR b.fan_out_flag = 1
    OR b.transfer_cashout_flag = 1
  )
WITH a, count(DISTINCT b) AS bad_neighbors
WHERE bad_neighbors >= 2
SET a.guilt_by_association_flag = 1;

// STEP risk_score
MATCH (a:Account)
SET a.risk_score =
    (10 * coalesce(a.fan_out_flag, 0)) +
    (10 * coalesce(a.fan_in_flag, 0)) +
    (15 * coalesce(a.drain_behavior_flag, 0)) +
    (15 * coalesce(a.transfer_cashout_flag, 0)) +
    (20 * coalesce(a.dense_community_flag, 0)) +
    (10 * coalesce(a.in_cycle_flag, 0)) +
    (10 * coalesce(a.guilt_by_association_flag, 0)) +
    (10 * coalesce(a.similar_to_flagged_flag, 0));
