"""
Download the PaySim dataset, strip the fraud-flag columns, write a clean
copy to the working directory, and delete the cached download.
"""
from neo4j import GraphDatabase
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import os



load_dotenv()



def main():

    driver = GraphDatabase.driver(
        str(os.getenv("db_uri")),
        auth=(str(os.getenv("db_user")), str(os.getenv("db_password"))))



    with driver.session() as session:
# importing with neo4j doesn't do MERGE
# One is a source one is destination
# I have less nodes
        session.run ("CREATE INDEX FOR (a:Account) ON (a.id);")

        session.run("\
            LOAD CSV WITH HEADERS FROM 'File:///paysim_clean.csv' AS row\
            CALL {\
            WITH row\
            MERGE (a:Account {id: row.nameOrig})\
            MERGE (b:Account {id: row.nameDest})\
            CREATE (a)-[:TRANSACTION {\
                type: row.type,\
                amount: toFloat(row.amount),\
                step: toInteger(row.step),\
                oldbalanceOrg: toFloat(row.oldbalanceOrg),\
                newbalanceOrig: toFloat(row.newbalanceOrig),\
                oldbalanceDest: toFloat(row.oldbalanceDest),\
                newbalanceDest: toFloat(row.newbalanceDest)\
            }]->(b)\
            } IN TRANSACTIONS OF 5000 ROWS\
            "
        )

# The data in the CSV is all transactions, none of those rows are about accounts. 
# We derive the accounts from the source and destination IDs in the transactions. 
# If an account is part of multiple transactions, 
# that's going to generate duplicate nodes. We need to MERGE them.


    driver.close()
 
if __name__ == "__main__":
    main()