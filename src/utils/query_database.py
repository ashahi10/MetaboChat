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

    ############################################
    # FULL-TEXT SEARCH with Weighted Fields
    ############################################
    def full_text_search(self, term, limit=5):
        """
        Search the weighted 'doc' column in 'metabolites'.
        - We use plainto_tsquery(...) so it handles multi-word input gracefully.
        - We rank results via ts_rank_cd, which respects the weighting we assigned.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            query = """
                SELECT id, hmdb_id, name,
                       ts_rank_cd(doc, plainto_tsquery('english', %s)) AS rank
                  FROM metabolites
                 WHERE doc @@ plainto_tsquery('english', %s)
                 ORDER BY rank DESC
                 LIMIT %s;
            """
            cur.execute(query, (term, term, limit))
            return cur.fetchall()

    ############################################
    # Query by exact/partial Name
    ############################################
    def query_by_name(self, name, limit=5):
        """
        1) Exact match on 'name' (case-insensitive).
        2) If none found, partial match on name or synonyms.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # 1) exact
            cur.execute("""
                SELECT id, hmdb_id, name, chemical_formula
                  FROM metabolites
                 WHERE lower(name) = lower(%s)
                 LIMIT %s
            """, (name, limit))
            rows = cur.fetchall()
            if rows:
                return rows

            # 2) partial
            cur.execute("""
                SELECT id, hmdb_id, name, chemical_formula
                  FROM metabolites
                 WHERE name ILIKE %s
                    OR synonyms::text ILIKE %s
                 LIMIT %s
            """, (f"%{name}%", f"%{name}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Disease
    ############################################
    def query_by_disease(self, disease, limit=5):
        """
        Join 'diseases' => 'metabolites'.
        1) exact match
        2) partial fallback
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # exact
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, d.disease_name
                  FROM diseases d
                  JOIN metabolites m ON d.metabolite_id = m.id
                 WHERE lower(d.disease_name) = lower(%s)
                 LIMIT %s
            """, (disease, limit))
            rows = cur.fetchall()
            if rows:
                return rows

            # partial
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, d.disease_name
                  FROM diseases d
                  JOIN metabolites m ON d.metabolite_id = m.id
                 WHERE d.disease_name ILIKE %s
                 LIMIT %s
            """, (f"%{disease}%", limit))
            return cur.fetchall()

    ############################################
    # Query by Pathway
    ############################################
    def query_by_pathway(self, pathway, limit=5):
        """
        Join 'pathways' => 'metabolites'.
        Exact + partial fallback.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # exact
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, p.pathway_name
                  FROM pathways p
                  JOIN metabolites m ON p.metabolite_id = m.id
                 WHERE lower(p.pathway_name) = lower(%s)
                 LIMIT %s
            """, (pathway, limit))
            exact = cur.fetchall()
            if exact:
                return exact

            # partial
            cur.execute("""
                SELECT m.id, m.hmdb_id, m.name, p.pathway_name
                  FROM pathways p
                  JOIN metabolites m ON p.metabolite_id = m.id
                 WHERE p.pathway_name ILIKE %s
                 LIMIT %s
            """, (f"%{pathway}%", limit))
            return cur.fetchall()

    ############################################
    # Predicted Properties
    ############################################
    def query_predicted_properties(self, hmdb_id):
        """
        Return predicted properties (logP, pKa, etc.) for a given metabolite.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            # get metabolite ID
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
        """
        Return normal/abnormal concentrations for HMDB ID.
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
            row = cur.fetchone()
            if not row:
                return []
            mid = row[0]

            cur.execute("""
                SELECT concentration_type, biofluid_type, concentration_value,
                       subject_age, subject_sex, subject_condition
                  FROM concentrations
                 WHERE metabolite_id = %s
                   AND concentration_type = %s
            """, (mid, ctype))
            return cur.fetchall()

    ############################################
    # Proteins
    ############################################
    def query_proteins(self, hmdb_id):
        """
        Return proteins that link to the given metabolite (by HMDB ID).
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
            row = cur.fetchone()
            if not row:
                return []
            mid = row[0]

            cur.execute("""
                SELECT uniprot_id, protein_name, gene_name
                  FROM proteins
                 WHERE metabolite_id = %s
            """, (mid,))
            return cur.fetchall()

#######################################
# Simple Testing
#######################################
if __name__ == "__main__":
    db = PostgresDBHandler(
        password="your_password"  # Adjust as needed
    )

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
