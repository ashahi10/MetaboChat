#!/usr/bin/env python3

import psycopg2

class PostgresDBHandler:
    """
    A class for fast, accurate queries of your HMDB-based Postgres schema,
    using weighted full-text search plus direct join queries.
    """

    def __init__(self, dbname="metabolites_pg", user="postgres",
                 password="your_password", host="localhost", port="5432"):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def _connect(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    ######################################################
    # refresh_doc_column
    ######################################################
    def refresh_doc_column(self):
        """
        Rebuild or refresh the weighted 'doc' tsvector column in 'metabolites'
        by merging:
          - name => weight 'A'
          - synonyms => 'C'
          - diseases => 'C'
          - pathways => 'D'
          - biospecimen_locations => 'B'
        Then re-run to keep updated if data changes.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # 1) name (A) + synonyms (C) + biospecimen_locations (B)
            cur.execute("""
                UPDATE metabolites
                   SET doc =
                     setweight(to_tsvector('english', COALESCE(name,'')), 'A') ||
                     setweight(to_tsvector('english', COALESCE(biospecimen_locations::text,'')), 'B') ||
                     setweight(to_tsvector('english', COALESCE(synonyms::text,'')), 'C')
            """)

            # 2) diseases => 'C' (Fixed to use disease_metabolites)
            cur.execute("""
                WITH disease_texts AS (
                  SELECT dm.metabolite_id,
                         string_agg(d.disease_name, ' ') AS disease_str
                    FROM disease_metabolites dm
                    JOIN diseases d ON dm.disease_id = d.id
                   GROUP BY dm.metabolite_id
                )
                UPDATE metabolites m
                   SET doc = m.doc || setweight(to_tsvector('english', COALESCE(d.disease_str,'')), 'C')
                  FROM disease_texts d
                 WHERE d.metabolite_id = m.id
            """)

            # 3) pathways => 'D'
            cur.execute("""
                WITH pathway_texts AS (
                    SELECT mp.metabolite_id,
                            string_agg(p.pathway_name, ' ') AS path_str
                        FROM metabolite_pathways mp
                        JOIN pathways p ON mp.pathway_id = p.id
                    GROUP BY mp.metabolite_id
                    )
                    UPDATE metabolites m
                    SET doc = m.doc || setweight(to_tsvector('english', COALESCE(p.path_str,'')), 'D')
                    FROM pathway_texts p
                    WHERE p.metabolite_id = m.id
            """)

            conn.commit()
            print("Refreshed 'doc' column with weighting: name=A, biospecimen_locations=B, synonyms/diseases=C, pathways=D.")

    ############################################
    # FULL-TEXT SEARCH with Weighted Fields
    ############################################
    def full_text_search(self, term, limit=5):
        """
        Weighted FTS on the 'doc' column.
        If no hits, fallback to partial ILIKE on name/synonyms/biospecimen_locations.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # Weighted search using ts_rank_cd
            query_fts = """
                SELECT id, hmdb_id, name,
                        ts_rank_cd(doc, websearch_to_tsquery('english', %s)) AS rank
                    FROM metabolites
                    WHERE doc @@ websearch_to_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT %s
            """
            cur.execute(query_fts, (term, term, limit))
            rows = cur.fetchall()
            if rows:
                return rows

            # Fallback partial match
            query_fallback = """
                SELECT id, hmdb_id, name
                  FROM metabolites
                 WHERE name ILIKE %s
                    OR synonyms::text ILIKE %s
                    OR biospecimen_locations::text ILIKE %s
                 LIMIT %s
            """
            cur.execute(query_fallback, (f"%{term}%", f"%{term}%", f"%{term}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Name (Exact -> partial fallback)
    ############################################
    def query_by_name(self, name, limit=5):
        with self._connect() as conn:
            cur = conn.cursor()
            # exact
            cur.execute("""
                SELECT id, hmdb_id, name, chemical_formula, molecular_weight_avg, smiles
                  FROM metabolites
                 WHERE lower(name) = lower(%s)
                 LIMIT %s
            """, (name, limit))
            rows = cur.fetchall()
            if rows:
                return rows

            # partial
            cur.execute("""
                SELECT id, hmdb_id, name, chemical_formula, molecular_weight_avg, smiles
                  FROM metabolites
                 WHERE name ILIKE %s
                    OR synonyms::text ILIKE %s
                 LIMIT %s
            """, (f"%{name}%", f"%{name}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Disease (Exact -> partial with synonyms)
    ############################################
    def query_by_disease(self, disease, limit=5):
        with self._connect() as conn:
            cur = conn.cursor()
            # exact (Fixed to use disease_metabolites)
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, d.disease_name
                  FROM disease_metabolites dm
                  JOIN diseases d ON dm.disease_id = d.id
                  JOIN metabolites m ON dm.metabolite_id = m.id
                 WHERE lower(d.disease_name) = lower(%s)
                 LIMIT %s
            """, (disease, limit))
            exact = cur.fetchall()
            if exact:
                return exact

            # partial with potential synonym support (Note: assumes disease_synonyms table exists; adjust if not)
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, d.disease_name
                  FROM disease_metabolites dm
                  JOIN diseases d ON dm.disease_id = d.id
                  JOIN metabolites m ON dm.metabolite_id = m.id
                 WHERE d.disease_name ILIKE %s
                    OR EXISTS (
                        SELECT 1 FROM disease_synonyms ds
                        WHERE ds.disease_name = d.disease_name
                        AND ds.synonym ILIKE %s
                    )
                 LIMIT %s
            """, (f"%{disease}%", f"%{disease}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Pathway (Exact -> partial with synonyms)
    ############################################
    def query_by_pathway(self, pathway, limit=5):
        with self._connect() as conn:
            cur = conn.cursor()
            # exact (Fixed to use metabolite_pathways)
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, p.pathway_name
                  FROM metabolite_pathways mp
                  JOIN pathways p ON mp.pathway_id = p.id
                  JOIN metabolites m ON mp.metabolite_id = m.id
                 WHERE lower(p.pathway_name) = lower(%s)
                 LIMIT %s
            """, (pathway, limit))
            rows = cur.fetchall()
            if rows:
                return rows

            # partial with potential synonym support (Note: assumes pathway_synonyms table exists; adjust if not)
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, p.pathway_name
                  FROM metabolite_pathways mp
                  JOIN pathways p ON mp.pathway_id = p.id
                  JOIN metabolites m ON mp.metabolite_id = m.id
                 WHERE p.pathway_name ILIKE %s
                    OR EXISTS (
                        SELECT 1 FROM pathway_synonyms ps
                        WHERE ps.pathway_name = p.pathway_name
                        AND ps.synonym ILIKE %s
                    )
                 LIMIT %s
            """, (f"%{pathway}%", f"%{pathway}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Biofluid (New Method)
    ############################################
    def query_by_biofluid(self, biofluid, limit=5):
        """
        Query metabolites detected in a specific biofluid (e.g., CSF, urine).
        Uses JSONB @> for optimized search.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, hmdb_id, name, biospecimen_locations
                 FROM metabolites
                WHERE biospecimen_locations @> to_jsonb(ARRAY[%s]::text[])
                LIMIT %s
            """, (biofluid, limit))  # Removed % wildcard, expects exact biofluid name
            return cur.fetchall()

    ############################################
    # Predicted Properties
    ############################################
    def query_predicted_properties(self, hmdb_id):
        """
        Return predicted props (logP, pKa, etc.) for HMDB ID
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
            row = cur.fetchone()
            if not row:
                return []
            mid = row[0]

            cur.execute("""
                SELECT property_kind, property_value, property_source
                  FROM predicted_properties
                 WHERE metabolite_id = %s
            """, (mid,))
            return cur.fetchall()

    ############################################
    # Concentrations
    ############################################
    def query_concentrations(self, hmdb_id, ctype='normal'):
        with self._connect() as conn:
            cur = conn.cursor()
            # Fixed to join with metabolites using hmdb_id
            cur.execute("""
                SELECT c.concentration_type, c.biofluid_type, c.concentration_value,
                       c.subject_age, c.subject_sex, c.subject_condition
                  FROM concentrations c
                  JOIN metabolites m ON c.metabolite_id = m.id
                 WHERE m.hmdb_id = %s
                   AND c.concentration_type = %s
            """, (hmdb_id, ctype))  # Use hmdb_id directly in query
            return cur.fetchall()

    ############################################
    # Proteins
    ############################################
    def query_proteins(self, hmdb_id):
        with self._connect() as conn:
            cur = conn.cursor()
            # Fixed to use protein_metabolites and join with hmdb_id
            cur.execute("""
                SELECT p.uniprot_id, p.protein_name, p.gene_name
                  FROM protein_metabolites pm
                  JOIN proteins p ON pm.protein_id = p.id
                  JOIN metabolites m ON pm.metabolite_id = m.id
                 WHERE m.hmdb_id = %s
            """, (hmdb_id,))  # Use hmdb_id directly in query
            return cur.fetchall()

    def query_by_hmdb_id(self, hmdb_id):
        """
        Fetch the single metabolite record that matches exactly the given HMDB ID.
        Return a single row or empty if not found.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, hmdb_id, name, chemical_formula, molecular_weight_avg, smiles
                  FROM metabolites
                 WHERE hmdb_id = %s
                 LIMIT 1
            """, (hmdb_id,))
            row = cur.fetchone()
            return row  # Either (id, hmdb_id, name, formula, mol_weight, smiles) or None

#######################################
# Demo Testing
#######################################
if __name__ == "__main__":
    db = PostgresDBHandler(password="your_password")  # adjust as needed

    print("If you haven't done so, make sure you've run the lines to add doc column, GIN index, and then call `db.refresh_doc_column()` once ingestion is done.\n")

    # db.refresh_doc_column()  # Uncomment if you want to forcibly rebuild doc

    print("\n=== Weighted FTS Test ===")
    for t in ["glucose", "serotonin", "oxidative stress", "nonexisting"]:
        hits = db.full_text_search(t, limit=5)
        print(f"Term '{t}' => {len(hits)} hits")
        for h in hits:
            print("   ", h)

    print("\n=== By Name ===")
    for name in ["histidine", "glucose", "random_unknown"]:
        recs = db.query_by_name(name, limit=5)
        print(f"\nName = {name} => {len(recs)} results")
        for r in recs:
            print("  ", r)

    print("\n=== By Disease ===")
    for dis in ["diabetes", "alzheimers", "randomDisease"]:
        recs = db.query_by_disease(dis, limit=5)
        print(f"\nDisease = {dis} => {len(recs)} results")
        for r in recs:
            print("  ", r)

    print("\n=== By Pathway ===")
    for pw in ["glycolysis", "lipid metabolism", "unknownPathway"]:
        recs = db.query_by_pathway(pw, limit=5)
        print(f"\nPathway = {pw} => {len(recs)} results")
        for r in recs:
            print("  ", r)

    print("\n=== By Biofluid ===")
    for bio in ["CSF", "urine", "serum"]:
        recs = db.query_by_biofluid(bio, limit=5)
        print(f"\nBiofluid = {bio} => {len(recs)} results")
        for r in recs:
            print("  ", r)

    print("\n=== Check Predicted Properties ===")
    props = db.query_predicted_properties("HMDB0000001")
    print("Props for HMDB0000001 =>", len(props))
    for p in props[:3]:
        print("  ", p)

    print("\n=== Check Concentrations ===")
    cvals = db.query_concentrations("HMDB0000001", ctype="normal")
    print("Normal concs for HMDB0000001 =>", len(cvals))
    for c in cvals[:3]:
        print("  ", c)

    print("\n=== Check Proteins ===")
    prots = db.query_proteins("HMDB0000001")
    print("Proteins =>", len(prots))
    for pr in prots[:3]:
        print("  ", pr)