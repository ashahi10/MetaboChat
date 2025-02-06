import unittest
from query_database import query_by_name, query_by_disease, query_by_pathway
import os
import sqlite3

DB_PATH = os.path.abspath("../data/metabolites.db")
print(f"Using database path in tests: {DB_PATH}")

class TestQueryDatabase(unittest.TestCase):

    def setUp(self):
        """
        Add test data to the database if needed.
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metabolites (Name, Short_Description, Diseases, Pathways)
            VALUES ('Test Metabolite', 'Test Description', 'Test Disease', 'glycolysis');
        """)
        conn.commit()
        conn.close()

    def tearDown(self):
        """
        Remove test data from the database.
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM metabolites WHERE Name = 'Test Metabolite';")
        conn.commit()
        conn.close()

    def test_database_connection(self):
        """
        Test if the database file exists and can be connected to.
        """
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.close()
        except sqlite3.Error as e:
            self.fail(f"Database connection failed: {e}")

    def test_query_by_name_valid(self):
        """
        Test query_by_name with a valid metabolite name.
        """
        results = query_by_name("histidine")
        self.assertGreater(len(results), 0, "Expected at least one result for valid name 'histidine'.")
        for row in results:
            self.assertIn("histidine", row[0].lower(), "Expected 'histidine' to be part of the name.")

    def test_query_by_name_invalid(self):
        """
        Test query_by_name with an invalid metabolite name.
        """
        results = query_by_name("invalid_name")
        self.assertEqual(len(results), 0, "Expected no results for invalid name 'invalid_name'.")

    def test_query_by_disease_valid(self):
        """
        Test query_by_disease with a valid disease name.
        """
        results = query_by_disease("cancer")
        self.assertGreater(len(results), 0, "Expected at least one result for valid disease 'cancer'.")
        for row in results:
            self.assertIn("cancer", row[2].lower(), "Expected 'cancer' to appear in the diseases field.")

    def test_query_by_disease_invalid(self):
        """
        Test query_by_disease with an invalid disease name.
        """
        results = query_by_disease("invalid_disease")
        self.assertEqual(len(results), 0, "Expected no results for invalid disease 'invalid_disease'.")

    def test_query_by_pathway_valid(self):
        """
        Test query_by_pathway with a valid pathway name.
        """
        results = query_by_pathway("glycolysis")
        self.assertGreater(len(results), 0, "Expected at least one result for valid pathway 'glycolysis'.")

    def test_query_by_pathway_invalid(self):
        """
        Test query_by_pathway with an invalid pathway name.
        """
        results = query_by_pathway("invalid_pathway")
        self.assertEqual(len(results), 0, "Expected no results for invalid pathway 'invalid_pathway'.")

    def test_query_edge_cases(self):
        """
        Test edge cases like empty strings and None as inputs.
        """
        self.assertEqual(len(query_by_name("")), 0, "Expected empty result for empty string in query_by_name.")
        self.assertEqual(len(query_by_name(None)), 0, "Expected empty result for None in query_by_name.")
        self.assertEqual(len(query_by_disease("")), 0, "Expected empty result for empty string in query_by_disease.")
        self.assertEqual(len(query_by_pathway(None)), 0, "Expected empty result for None in query_by_pathway.")

if __name__ == "__main__":
    unittest.main()
