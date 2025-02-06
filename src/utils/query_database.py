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
        Initialize an FTS5 virtual table for enhanced search capabilities.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Create the FTS table if it does not exist.
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='metabolites_fts';"
            )
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

    def query_by_name(self, name):
        """
        Optimized query for exact and fuzzy name matching.
        1. Try an exact (case-insensitive) match.
        2. Run an FTS search with BM25 ranking.
        3. Fallback to a LIKE search.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 1. Exact match (case-insensitive)
            query = """
                SELECT Name, Short_Description, Diseases 
                FROM metabolites 
                WHERE lower(Name) = lower(?)
                LIMIT 1;
            """
            cursor.execute(query, (name,))
            exact_match = cursor.fetchall()
            if exact_match:
                return exact_match

            # 2. FTS search using BM25 ranking for fuzzy matching.
            query = """
                SELECT name, description, diseases 
                FROM metabolites_fts 
                WHERE name MATCH ? 
                ORDER BY bm25(metabolites_fts)
                LIMIT 5;
            """
            cursor.execute(query, (name,))
            fts_results = cursor.fetchall()
            if fts_results:
                return fts_results

            # 3. Fallback: Use LIKE search.
            query = """
                SELECT Name, Short_Description, Diseases 
                FROM metabolites 
                WHERE Name LIKE ? 
                LIMIT 5;
            """
            cursor.execute(query, (f"%{name}%",))
            fallback_results = cursor.fetchall()
            return fallback_results

    def query_by_disease(self, disease):
        """
        Query for metabolites related to a specific disease.
        The FTS query uses custom BM25 weighting so that the diseases column
        (the 4th column) is given a lower weight.
        
        Then we post-process the results using a ratio score:
            score = (number of occurrences of the search term) / (total number of disease entries)
        This approach is general and will work for any disease (or similar field) query.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Use a custom BM25 weighting: assign weight 0.5 to the diseases column.
            # Sanitize the disease term by replacing single quotes with two single quotes.
            sanitized_disease = disease.replace("'", "''")
            # Wrap the sanitized term in double quotes for FTS MATCH.
            query = f"""
                SELECT name, description, diseases 
                FROM metabolites_fts 
                WHERE diseases MATCH '"{sanitized_disease}"'
                ORDER BY bm25(metabolites_fts, 1.0, 1.0, 1.0, 0.5)
                LIMIT 10;
            """
            # Do not pass a parameter tuple since the search term is already injected.
            cursor.execute(query)
            results = cursor.fetchall()

            # Define a helper function to compute the ratio score.
            def score_row(row, term):
                field = row[2]  # diseases field
                term_lower = term.lower()
                # Count occurrences of the search term.
                count = field.lower().count(term_lower)
                # Assume diseases are separated by commas; count them.
                entries = [entry.strip() for entry in field.split(',') if entry.strip()]
                total = len(entries) if entries else 1
                # Return the ratio; a higher ratio means stronger association.
                return count / total

            # Filter out results that do not mention the term at all.
            filtered = [row for row in results if row[2] and disease.lower() in row[2].lower()]
            if filtered:
                # Re-sort the filtered results by the ratio score.
                sorted_filtered = sorted(filtered, key=lambda row: score_row(row, disease), reverse=True)
                return sorted_filtered[:5]

            # Fallback using a LIKE query on the diseases column.
            query = """
                SELECT Name, Short_Description, Diseases 
                FROM metabolites 
                WHERE lower(Diseases) LIKE '%' || lower(?) || '%'
                LIMIT 5;
            """
            cursor.execute(query, (disease,))
            fallback_results = cursor.fetchall()
            return fallback_results

    def query_by_pathway(self, pathway):
        """
        Query for metabolites in a specific pathway.
        Uses an FTS query with a wildcard for partial matching and a fallback LIKE search.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # FTS search with a wildcard appended.
            search_term = pathway if pathway.endswith('*') else pathway + '*'
            query = """
                SELECT name, description 
                FROM metabolites_fts 
                WHERE pathways MATCH ? 
                ORDER BY bm25(metabolites_fts)
                LIMIT 5;
            """
            cursor.execute(query, (search_term,))
            results = cursor.fetchall()
            if results:
                return results

            # Fallback: Use a LIKE search on the pathways field.
            query = """
                SELECT Name, Short_Description 
                FROM metabolites 
                WHERE lower(pathways) LIKE '%' || lower(?) || '%'
                LIMIT 5;
            """
            cursor.execute(query, (pathway,))
            like_results = cursor.fetchall()
            return like_results

    def full_text_search(self, term, limit=5):
        """
        Perform a full-text search across all indexed fields using BM25 ranking.
        If no FTS results are found, fallback to a LIKE search.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = """
                SELECT name, description, diseases, pathways 
                FROM metabolites_fts 
                WHERE metabolites_fts MATCH ? 
                ORDER BY bm25(metabolites_fts, 1.0, 0.5, 1.0, 1.0)
                LIMIT ?;
            """
            cursor.execute(query, (term, limit))
            results = cursor.fetchall()
            if results:
                return results

            # Fallback: Use a LIKE search across all fields.
            query = """
                SELECT name, description, diseases, pathways 
                FROM metabolites 
                WHERE lower(name) LIKE '%' || lower(?) || '%'
                   OR lower(description) LIKE '%' || lower(?) || '%'
                   OR lower(diseases) LIKE '%' || lower(?) || '%'
                   OR lower(pathways) LIKE '%' || lower(?) || '%'
                LIMIT ?;
            """
            cursor.execute(query, (term, term, term, term, limit))
            fallback_results = cursor.fetchall()
            return fallback_results

    def query_advanced(self, search_term, column=None):
        """
        Perform a broader search.
        1. If a column is specified, first attempt a LIKE search.
           If the column is 'diseases', apply custom BM25 weighting and the ratio scoring.
        2. Otherwise, process the multi-term query:
             - Split tokens and join with AND so that all tokens are required.
             - Run an FTS query with BM25 ranking.
        3. Fallback to a broader LIKE search if no FTS result is found.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            if column:
                # First, attempt a LIKE search.
                query = f"""
                    SELECT Name, Short_Description, {column} 
                    FROM metabolites 
                    WHERE lower({column}) LIKE '%' || lower(?) || '%'
                    LIMIT 5;
                """
                cursor.execute(query, (search_term,))
                results = cursor.fetchall()
                if results:
                    return results

                # Fallback: Use an FTS search on that column.
                if column.lower() == "diseases":
                    # Sanitize the search term for FTS MATCH.
                    sanitized_term = search_term.replace("'", "''")
                    query = f"""
                        SELECT name, description, diseases, pathways 
                        FROM metabolites_fts 
                        WHERE {column} MATCH '"{sanitized_term}"'
                        ORDER BY bm25(metabolites_fts, 1.0, 1.0, 1.0, 0.5)
                        LIMIT 10;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()

                    def score_row(row, term):
                        field = row[2]
                        term_lower = term.lower()
                        count = field.lower().count(term_lower)
                        entries = [entry.strip() for entry in field.split(',') if entry.strip()]
                        total = len(entries) if entries else 1
                        return count / total

                    filtered = [row for row in results if row[2] and search_term.lower() in row[2].lower()]
                    if filtered:
                        sorted_filtered = sorted(filtered, key=lambda row: score_row(row, search_term), reverse=True)
                        return sorted_filtered[:5]
                    return results
                else:
                    query = f"""
                        SELECT name, description, diseases, pathways 
                        FROM metabolites_fts 
                        WHERE {column} MATCH ? 
                        ORDER BY bm25(metabolites_fts)
                        LIMIT 5;
                    """
                    cursor.execute(query, (search_term,))
                    results = cursor.fetchall()
                    return results
            else:
                # For multi-term queries, split tokens and join with AND.
                if " and " in search_term.lower():
                    tokens = [token.strip() for token in search_term.lower().split(" and ")]
                else:
                    tokens = search_term.split()
                fts_query = " AND ".join(tokens)
                query = """
                    SELECT name, description, diseases, pathways 
                    FROM metabolites_fts 
                    WHERE metabolites_fts MATCH ? 
                    ORDER BY bm25(metabolites_fts, 1.0, 0.5, 1.0, 1.0)
                    LIMIT 5;
                """
                cursor.execute(query, (fts_query,))
                results = cursor.fetchall()
                if results:
                    return results

                # Fallback: broader LIKE search over all fields.
                query = """
                    SELECT name, description, diseases, pathways 
                    FROM metabolites 
                    WHERE lower(name) LIKE '%' || lower(?) || '%'
                       OR lower(description) LIKE '%' || lower(?) || '%'
                       OR lower(diseases) LIKE '%' || lower(?) || '%'
                       OR lower(pathways) LIKE '%' || lower(?) || '%'
                    LIMIT 5;
                """
                cursor.execute(query, (search_term, search_term, search_term, search_term))
                fallback_results = cursor.fetchall()
                return fallback_results


if __name__ == "__main__":
    db_handler = DatabaseHandler()

    print("\n--- Test: Query by Name ---")
    test_names = ["histidine", "glucose", "serotonin", "random_nonexistent"]
    for name in test_names:
        print(f"\n🔍 Searching for: {name}")
        results = db_handler.query_by_name(name)
        if results:
            for row in results:
                print(f"Name: {row[0]}, Short Description: {row[1]}, Diseases: {row[2]}")
        else:
            print(f"No results found for '{name}'.")

    print("\n--- Test: Query by Disease ---")
    test_diseases = ["cancer", "diabetes", "Alzheimer's", "nonexistent_disease"]
    for disease in test_diseases:
        print(f"\n🔍 Searching for disease: {disease}")
        results = db_handler.query_by_disease(disease)
        if results:
            for row in results:
                print(f"Name: {row[0]}, Description: {row[1]}, Diseases: {row[2]}")
        else:
            print(f"No results found for '{disease}'.")

    print("\n--- Test: Query by Pathway ---")
    test_pathways = ["glycolysis", "Krebs cycle", "lipid metabolism", "random_pathway"]
    for pathway in test_pathways:
        print(f"\n🔍 Searching for pathway: {pathway}")
        results = db_handler.query_by_pathway(pathway)
        if results:
            for row in results:
                print(f"Name: {row[0]}, Description: {row[1]}")
        else:
            print(f"No results found for '{pathway}'.")

    print("\n--- Test: Full-Text Search ---")
    test_terms = ["oxidative stress", "neurodegeneration", "energy metabolism", "random_text"]
    for term in test_terms:
        print(f"\n🔍 Searching for: {term}")
        results = db_handler.full_text_search(term)
        if results:
            for row in results:
                print(f"Name: {row[0]}, Description: {row[1]}, Diseases: {row[2]}, Pathways: {row[3]}")
        else:
            print(f"No results found for '{term}'.")

    print("\n--- Test: Advanced Query ---")
    test_advanced_queries = ["diabetes and energy metabolism", "oxidative stress and aging"]
    for query in test_advanced_queries:
        print(f"\n🔍 Running advanced query: {query}")
        results = db_handler.query_advanced(query)
        if results:
            for row in results:
                print(f"Name: {row[0]}, Description: {row[1]}, Diseases: {row[2]}, Pathways: {row[3]}")
        else:
            print(f"No results found for '{query}'.")
