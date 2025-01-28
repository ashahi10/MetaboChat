import sqlite3
import os

DB_PATH = os.path.abspath("../data/metabolites.db")
print(f"Using database path: {DB_PATH}")



def query_by_name(name):
    """
    Query the database for metabolites by name.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT Name, Short_Description, Diseases FROM metabolites WHERE Name LIKE ? LIMIT 10;"
    cursor.execute(query, (f"%{name}%",))
    results = cursor.fetchall()
    conn.close()
    return results

def query_by_disease(disease):
    """
    Query the database for metabolites associated with a disease.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT Name, Short_Description FROM metabolites WHERE Diseases LIKE ? LIMIT 10;"
    cursor.execute(query, (f"%{disease}%",))
    results = cursor.fetchall()
    conn.close()
    return results
#yo

def query_by_pathway(pathway):
    """
    Query the database for metabolites involved in a pathway.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT Name, Short_Description FROM metabolites WHERE Pathways LIKE ? LIMIT 10;"
    cursor.execute(query, (f"%{pathway}%",))
    results = cursor.fetchall()
    conn.close()
    return results

if __name__ == "__main__":
    # Example queries to test the functionality
    print("Query by Name: 'histidine'")
    results = query_by_name("histidine")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}, Diseases: {row[2]}")

    print("\nQuery by Disease: 'cancer'")
    results = query_by_disease("cancer")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}")

    print("\nQuery by Pathway: 'glycolysis'")
    results = query_by_pathway("glycolysis")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}")
