// Uniqueness for idempotent PaySim ingest (T0.3).
// TRANSACTION stays an Account-to-Account relationship so existing GDS/Cypher
// patterns keep working. Identity is txn_id on the relationship, not Neo4j elementId.

CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS UNIQUE;

CREATE CONSTRAINT transaction_txn_id IF NOT EXISTS
FOR ()-[r:TRANSACTION]-() REQUIRE r.txn_id IS UNIQUE;
