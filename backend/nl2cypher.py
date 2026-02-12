def nl2cypher_query(question: str) -> str:
    """
    Convert natural language question into Cypher query.
    For now, return a simple stub. Replace with your real NL2Cypher logic.
    """
    if "insulin" in question.lower():
        return "MATCH (p:Patient)-[:CONSUMED]->(i:Item {name:'Insulin'}) RETURN p.id, i.name"
    return "MATCH (n) RETURN n LIMIT 5"