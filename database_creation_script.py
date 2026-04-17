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




    driver.close()
 
if __name__ == "__main__":
    main()