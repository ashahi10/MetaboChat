from neo4j_connection import Neo4JConnection

if __name__ == "__main__":
    conn = Neo4JConnection()
    print("Connected to Neo4J!")
    conn.close()
