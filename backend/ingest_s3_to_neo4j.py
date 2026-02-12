import boto3
import csv
from neo4j import GraphDatabase

# --- AWS S3 Setup ---
BUCKET_NAME = "ai-auditor-test-bucket"   
FILE_KEY = "ipbill.csv"                   

s3 = boto3.client("s3")

# --- Neo4j Setup ---
NEO4J_URI = "neo4j+s://bolt://44.201.65.132"   
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "truth-hulls-rust"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- Helper Function ---
def upload_csv_to_neo4j():
    # Download file from S3
    response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    lines = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(lines)

    with driver.session() as session:
        for row in reader:
            # Example schema: Patient, Doctor, Item, Category
            patient_id = row.get("patient_id")
            doctor = row.get("doctor")
            item = row.get("item")
            category = row.get("category")

            # Create nodes and relationships
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

if __name__ == "__main__":
    upload_csv_to_neo4j()