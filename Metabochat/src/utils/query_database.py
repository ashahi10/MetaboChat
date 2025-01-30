import sqlite3
import os

class DatabaseHandler:
    """
    A class to handle all database-related operations for the metabolites database.
    """

    def __init__(self, db_path=None):
        """
        Initialize the DatabaseHandler with a database path.
        """
        self.db_path = db_path or os.path.abspath("../data/metabolites.db")
        print(f"Using database path: {self.db_path}")
        self.initialize_full_text_search()

    def initialize_full_text_search(self):
        """
        Initialize a full-text search (FTS) virtual table for the description column.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if the FTS table exists, and create it if not
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metabolites_fts';")
        if not cursor.fetchone():
            print("Creating FTS table for full-text search...")
            cursor.execute("""
                CREATE VIRTUAL TABLE metabolites_fts USING fts5(
                    name, 
                    description, 
                    pathways, 
                    diseases, 
                    content='metabolites',
                    content_rowid='id'
                );
            """)
            cursor.execute("""
                INSERT INTO metabolites_fts (rowid, name, description, pathways, diseases)
                SELECT id, name, description, pathways, diseases FROM metabolites;
            """)
            conn.commit()
            print("FTS table created successfully.")
        conn.close()

    def query_by_name(self, name):
        """
        Query the database for metabolites by name.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = "SELECT Name, Short_Description, Diseases FROM metabolites WHERE Name LIKE ? LIMIT 10;"
        cursor.execute(query, (f"%{name}%",))
        results = cursor.fetchall()
        conn.close()
        return results

    def query_by_disease(self, disease):
        """
        Query the database for metabolites associated with a disease.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = "SELECT Name, Short_Description FROM metabolites WHERE Diseases LIKE ? LIMIT 10;"
        cursor.execute(query, (f"%{disease}%",))
        results = cursor.fetchall()
        conn.close()
        return results

    def query_by_pathway(self, pathway):
        """
        Query the database for metabolites involved in a pathway.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = "SELECT Name, Short_Description FROM metabolites WHERE Pathways LIKE ? LIMIT 10;"
        cursor.execute(query, (f"%{pathway}%",))
        results = cursor.fetchall()
        conn.close()
        return results

    def full_text_search(self, term, limit=10):
        """
        Perform a full-text search on the description column using FTS.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = f"""
            SELECT name, description, diseases, pathways 
            FROM metabolites_fts 
            WHERE metabolites_fts MATCH ? 
            LIMIT ?;
        """
        cursor.execute(query, (term, limit))
        results = cursor.fetchall()
        conn.close()
        return results

    def query_advanced(self, search_term, column=None):
        """
        Perform a broader query based on a search term, with optional filtering by column.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if column:
            query = f"SELECT Name, Short_Description, {column} FROM metabolites WHERE {column} LIKE ? LIMIT 10;"
            cursor.execute(query, (f"%{search_term}%",))
        else:
            query = f"""
                SELECT Name, Short_Description, Diseases, Pathways 
                FROM metabolites 
                WHERE Name LIKE ? OR Short_Description LIKE ? OR Diseases LIKE ? OR Pathways LIKE ? 
                LIMIT 10;
            """
            cursor.execute(query, (f"%{search_term}%", f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

        results = cursor.fetchall()
        conn.close()
        return results


if __name__ == "__main__":
    # Initialize the database handler
    db_handler = DatabaseHandler()

    print("\n--- Query by Name: 'histidine' ---")
    results = db_handler.query_by_name("histidine")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}, Diseases: {row[2]}")

    print("\n--- Query by Disease: 'cancer' ---")
    results = db_handler.query_by_disease("cancer")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}")

    print("\n--- Query by Pathway: 'glycolysis' ---")
    results = db_handler.query_by_pathway("glycolysis")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}")

    print("\n--- Full-Text Search: 'oxidative stress' ---")
    results = db_handler.full_text_search("oxidative stress")
    for row in results:
        print(f"Name: {row[0]}, Description: {row[1]}, Diseases: {row[2]}, Pathways: {row[3]}")

    print("\n--- Advanced Query: 'diabetes' across all fields ---")
    results = db_handler.query_advanced("diabetes")
    for row in results:
        print(f"Name: {row[0]}, Short Description: {row[1]}, Diseases: {row[2]}, Pathways: {row[3]}")
