from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
driver = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"), auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")))

with driver.session(database="org1000") as session:
    result = session.run("MATCH (n:SalesProfile{owner_user_id : 1000}) RETURN n LIMIT 2")
    records = list(result)
    print(f"Found {len(records)} records")
    for r in records:
        print(r['n'])
driver.close()
