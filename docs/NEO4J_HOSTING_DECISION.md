# Neo4j hosting decision — T1, 2026-10-26

Keep Neo4j local for the sample demo, as locked in the implementation plan.
Nothing is provisioned. Live ingest/detection verification remains deferred
until local credentials and an unlabeled ops CSV are configured. Cloud batch
jobs should exchange versioned graph exports through S3 rather than attempt
to reach a workstation's localhost.

| Option | GDS and compatibility | Network/security | Cost and operation |
|---|---|---|---|
| Neo4j Aura | Current documentation supports a Graph Analytics plugin on AuraDB Professional, Neo4j 5+, with at least 4 GB memory. Confirm the chosen tier/region and required procedures before purchase; do not assume every Aura tier has GDS. | Managed TLS endpoint; cloud jobs need a permitted route and credentials from a secrets service. Private networking availability depends on the service/tier. | Managed service reduces patch/backup work, but minimum instance capacity and analytics resource use must fit the budget. |
| Self-managed Neo4j on AWS | Install a compatible GDS plugin and pin server/plugin versions. Verify relationship uniqueness, edition/license requirements, and all required detection procedures. | Private subnet, restricted Bolt access from an approved job security group, TLS, secrets, backups and patching are the operator's responsibility. No public Bolt exposure. | EC2, persistent disks, backups and possibly network charges accrue; stopping compute does not remove storage charges. GDS memory requirements drive sizing. |

If a later requirement forces cloud graph hosting, evaluate Aura first for
operational simplicity, but choose it only after a small compatibility test and
a current quote. Use self-managed hosting when plugin/version control or private
network requirements justify its operational burden. This is a conditional future
recommendation, not a change to the local-demo decision. No exact cost estimate
is reliable until region, memory, runtime and licensing are selected.

Live checks still required: server version and relationship uniqueness support,
`SHOW CONSTRAINTS`, ingest replay counts, GDS availability, and the entire ordered
detection run. The mock fixture uses connected components as an approximation
for Louvain; it does not establish live Louvain equivalence. Analytics must run
in scheduled batches/windows, never once per transaction.

Sources consulted for this design:

- [Neo4j GDS installation](https://neo4j.com/docs/graph-data-science/current/installation/): GDS is a database plugin for self-managed deployments.
- [Aura alternative deployments](https://neo4j.com/docs/aura/graph-analytics/alternative-deployments/): current plugin requirements; plugin compute shares database resources and may affect transactional queries.
- [Aura graph analytics](https://neo4j.com/docs/aura/graph-analytics/): managed analytics deployment choices. Check current product terms before any hosting decision.
