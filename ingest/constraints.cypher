// Unique Account.id and unique TRANSACTION.txn_id (Neo4j 5.18+ relationship
// property uniqueness). Applied by database_creation_script.ensure_constraints.
CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS UNIQUE;

CREATE CONSTRAINT transaction_txn_id IF NOT EXISTS
FOR ()-[r:TRANSACTION]-() REQUIRE r.txn_id IS UNIQUE;
