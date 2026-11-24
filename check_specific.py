from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
driver = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"), auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")))

with driver.session(database="org1000") as session:
    result = session.run("MATCH (n:SalesProfile {id: 'cp4f307f9233e84f9fb7cce2684abe4001'}) RETURN n.owner_user_id")
    for record in result:
        print(f"Record: {record}")
driver.close()
