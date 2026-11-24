from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(URI, auth=(USER, PASS))

with driver.session(database="org1000") as session:
    result = session.run("MATCH (n:SalesProfile) RETURN n.owner_user_id AS owner LIMIT 10")
    for record in result:
        print(record)

driver.close()
