# Team 3's Cypher Queries

## Unusual Fan-out
<pre>
CALL gds.graph.project(
'accountGraph',
'Account',
  {
    TRANSACTION:   {orientation: 'NATURAL'}  
  }
)
YIELD graphName
</pre>
<pre>
CALL gds.degree.write(
  'accountGraph',
  {
    writeProperty: 'outDegree',
    orientation: 'NATURAL'
  }
)
YIELD
  nodePropertiesWritten,
  centralityDistribution
RETURN
  centralityDistribution.p99 as fanOutp99
</pre>
## Unusual Fan-in
<pre>
CALL gds.degree.write(
  'accountGraph',
  {
    writeProperty: 'inDegree',
    orientation: 'REVERSE'
  }
)
YIELD
  nodePropertiesWritten,
  centralityDistribution
RETURN
  centralityDistribution.p99 as fanInp99
</pre>
<pre>
MATCH (n:Account)
SET (CASE WHEN n.inDegree > 14 THEN n END).unusualFanIn = 1
RETURN n.inDegree, n.unusualFanIn
ORDER BY n.inDegree DESC
LIMIT 500;
</pre>
## Drain Behavior
<pre>
MATCH (a)-[t1:TRANSACTION]->(b)-[t2:TRANSACTION]->(c)
WHERE t2.step - t1.step <= 3
  AND t2.newbalanceOrig < 50
SET b.drain_behavior_flag = 1
</pre>
## Large Transfers followed immediately by cash-out
<pre>
MATCH (a:Account)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b:Account) -[t2:TRANSACTION {type: 'CASH_OUT'}]->(c:Account) WHERE t2.step - t1.step <= 2 AND t1.amount > 0 AND abs(t1.amount - t2.amount) / t1.amount < 0.1 
SET t1.transfer_cashout_flag = 1, t2.transfer_cashout_flag = 1, b.transfer_cashout_flag = 1;
</pre>
## Dense community with high internal volume
<pre>
CREATE INDEX community_stats_id IF NOT EXISTS FOR (c:CommunityStats) ON (c.community_id);
CREATE INDEX account_community_id IF NOT EXISTS FOR (a:Account) ON (a.community_id);
</pre>
<pre>
MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
WHERE a.community_id = b.community_id
WITH a.community_id AS community, count(t) AS tc, sum(t.amount) AS ta
WHERE tc > 1
CALL (community, tc, ta) {
  MERGE (c:CommunityStats {community_id: community})
  SET c.transaction_count = tc, c.total_amount = ta
} IN TRANSACTIONS OF 10000 ROWS;
</pre>
<pre>
MATCH (acc:Account)
CALL (acc){
  MATCH (c:CommunityStats {community_id: acc.community_id})
  SET acc.community_transaction_count = c.transaction_count,
      acc.community_total_amount = c.total_amount
} IN TRANSACTIONS OF 10000 ROWS;
</pre>
<pre>
MATCH (c:CommunityStats)
CALL (c){
  MATCH (a:Account{community_id:c.community_id})-[t:TRANSACTION{type:"CASH_OUT"}]->(d)
WITH sum(t.amount) AS out_amount, count(a) AS size
SET c.out_amount = out_amount, c.size = size
}
MATCH (c:CommunityStats)
CALL (c){
MATCH (a:Account{community_id:c.community_id})
WHERE a.community_total_amount * 0.1 > c.out_amount AND c.size >= 3 AND c.size <= 15
SET a.dense_community_flag = 1
}
</pre>

## Guilt by association
<pre>
MATCH (a:Account)-[:TRANSACTION]-(b:Account)
WHERE a.dense_community_flag IS NULL AND a.drain_behavior_flag IS NULL AND
a.unusualFanIn IS NULL AND
a.transfer_cashout_flag IS NULL AND
(b.dense_community_flag = 1 OR b.drain_behavior_flag = 1 OR b.unusualFanIn = 1 OR b.transfer_cashout_flag = 1)
WITH a, count(DISTINCT b) AS bad_neighbors
WHERE bad_neighbors >= 2
SET a.guilt_by_association_flag = 1
</pre>

## Node similarity among flagged accounts
<pre>
MATCH (a:Account)
WHERE a.fan_out_flag = 1 OR 
    a.UnusualFanIn = 1 OR
    a.drain_behavior_flag = 1 OR
    a.transfer_cashout_flag = 1 OR
    a.dense_community_flag = 1 OR
    a.guilt_by_association_flag = 1 
SET a.flagged = 1
</pre>
<pre>
CALL gds.graph.project( 'accountGraph', 'Account', { TRANSACTION: {orientation: ‘NATURAL’} } ) YIELD graphName
</pre>
<pre>
CALL gds.nodeSimilarity.write('accountGraph',
{
topK: 10,
similarityCutoff: 0.4,
degreeCutoff: 2,
writeRelationshipType: 'SIMILAR_TO',
writeProperty: 'score' })
YIELD nodesCompared, relationshipsWritten;
</pre>
The previous query didn't create any new relationships indicating that there were any similar accounts fitting those conditions. If there were, we would have ran the following query
<pre>
MATCH (a:Account {flagged: false})-[s:SIMILAR_TO]->(b:Account {flagged: true})
SET a.similar_to_flagged_flag=1
</pre>