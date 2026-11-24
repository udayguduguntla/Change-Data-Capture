from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
driver = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"), auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")))

with driver.session(database="org1000") as session:
    result = session.run("MATCH (n:SalesProfile) WHERE n.owner_user_id IS NOT NULL RETURN n.id, n.owner_user_id LIMIT 5")
    for record in result:
        val = record["n.owner_user_id"]
        print(f"ID: {record['n.id']}, Value: {repr(val)}, Type: {type(val)}")

driver.close()
