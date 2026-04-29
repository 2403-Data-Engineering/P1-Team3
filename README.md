# P1-Team3

- Members: Silas Bucur, Seth Gleason, Sai Palepu

- Neo4J setup:
  - Run the download_data.py script
  - Create a new instance of neo4j
  - Add the paysim_clean.csv file to the import folder of the newly created instance.
  - Run the database_creation.py script to initialize the DB.


Node : Accounts
    Represents both customers and merchants
    From:
        nameOrig (sender)
        nameDest (receiver)

Edges : Transactions
    Payment, Transfer, Cash_out, Debit
    Holds all the other data

Each row = one relationship (edge)

(:Account)-[:TRANSFER {step: 1}]->(:Account)
 means:
“this transaction happened in hour 1 of the simulation”