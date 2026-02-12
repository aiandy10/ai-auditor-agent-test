from fastapi import FastAPI
from pydantic import BaseModel
from neo4j import GraphDatabase
import boto3
import csv

app = FastAPI(title="AI Auditor Agent Test")

# --- AWS S3 Setup ---
BUCKET_NAME = "ai-auditor-test-bucket"
FILE_KEY = "ipbill.csv"
s3 = boto3.client("s3")

# --- Neo4j Setup ---
NEO4J_URI = "neo4j+s://bolt://44.201.65.132"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "truth-hulls-rust"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- Request Models ---
class QueryRequest(BaseModel):
    question: str

# --- Helper: Ingest CSV from S3 into Neo4j ---
def ingest_csv_to_neo4j():
    response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    lines = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(lines)

    with driver.session() as session:
        for row in reader:
            patient_id = row.get("patient_id")
            doctor = row.get("doctor")
            item = row.get("item")
            category = row.get("category")

            session.run("""
                MERGE (p:Patient {id: $patient_id})
                MERGE (d:Doctor {name: $doctor})
                MERGE (i:Item {name: $item})
                MERGE (c:Category {name: $category})
                MERGE (p)-[:CONSULTED]->(d)
                MERGE (p)-[:CONSUMED]->(i)
                MERGE (i)-[:BELONGS_TO]->(c)
            """, patient_id=patient_id, doctor=doctor, item=item, category=category)

    print("CSV ingested into Neo4j successfully!")

# --- NL2Cypher Stub ---
def nl2cypher_query(question: str) -> str:
    q = question.lower()
    if "insulin" in q:
        return "MATCH (p:Patient)-[:CONSUMED]->(i:Item {name:'Insulin'}) RETURN p.id, i.name"
    elif "doctor" in q:
        return "MATCH (p:Patient)-[:CONSULTED]->(d:Doctor) RETURN p.id, d.name"
    else:
        return "MATCH (n) RETURN n LIMIT 10"

# --- Startup Hook ---
@app.on_event("startup")
def startup_event():
    """Automatically ingest CSV when server starts."""
    ingest_csv_to_neo4j()

# --- Endpoints ---
@app.post("/query")
def run_query(req: QueryRequest):
    """Run natural language query via NL2Cypher and return results."""
    cypher = nl2cypher_query(req.question)
    with driver.session() as session:
        result = session.run(cypher)
        records = [dict(r) for r in result]
    return {"cypher": cypher, "results": records}