import os
import json
import logging
import time
import xml.etree.ElementTree as ET
from typing import List, Optional
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import psycopg2

#########################################
# 1) CONFIG & LOGGING
#########################################
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection details
DB_NAME = "metabolites_pg"
DB_USER = "postgres"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Paths to the HMDB-style XML files
DATA_FILES = [
    "./data/hmdb_metabolites.xml",
    "./data/feces_metabolites.xml",
    "./data/hmdb_proteins.xml",
    "./data/saliva_metabolites.xml",
    "./data/serum_metabolites.xml",
    "./data/sweat_metabolites.xml",
    "./data/urine_metabolites.xml"
]

#########################################
# 2) DB CONNECTION & TABLE CREATION
#########################################
def connect_db():
    """Returns a new psycopg2 connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_tables():
    """Creates all necessary tables for metabolites, pathways, diseases, and related mappings."""
    conn = connect_db()
    cur = conn.cursor()

    # Metabolites table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS metabolites (
            id SERIAL PRIMARY KEY,
            hmdb_id TEXT UNIQUE NOT NULL,
            name TEXT,
            chemical_formula TEXT,
            synonyms JSONB,
            status TEXT,
            molecular_weight_avg REAL,
            molecular_weight_monoisotopic REAL,
            iupac_name TEXT,
            smiles TEXT,
            inchi TEXT,
            inchikey TEXT,
            taxonomy_kingdom TEXT,
            taxonomy_superclass TEXT,
            taxonomy_class TEXT,
            taxonomy_subclass TEXT,
            taxonomy_direct_parent TEXT,
            taxonomy_alternative_parents JSONB,
            cellular_locations JSONB,
            biospecimen_locations JSONB,
            tissue_locations JSONB,
            creation_date TIMESTAMP NULL,
            update_date TIMESTAMP NULL,
            version TEXT
        );
    ''')

    # Pathways table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pathways (
            id SERIAL PRIMARY KEY,
            pathway_name TEXT UNIQUE NOT NULL,
            kegg_id TEXT,
            smpdb_id TEXT
        );
    ''')

    # Diseases table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS diseases (
            id SERIAL PRIMARY KEY,
            disease_name TEXT UNIQUE NOT NULL,
            "references" TEXT
        );
    ''')

    # Proteins table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS proteins (
            id SERIAL PRIMARY KEY,
            uniprot_id TEXT UNIQUE NOT NULL,
            protein_name TEXT,
            gene_name TEXT
        );
    ''')

    # Many-to-Many Relationship Tables
    cur.execute('''
        CREATE TABLE IF NOT EXISTS metabolite_pathways (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            pathway_id INT REFERENCES pathways(id) ON DELETE CASCADE,
            UNIQUE (metabolite_id, pathway_id)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS disease_metabolites (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            disease_id INT REFERENCES diseases(id) ON DELETE CASCADE,
            UNIQUE (metabolite_id, disease_id)
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS protein_metabolites (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            protein_id INT REFERENCES proteins(id) ON DELETE CASCADE,
            UNIQUE (metabolite_id, protein_id)
        );
    ''')

    conn.commit()
    conn.close()
    logger.info("Tables created or verified successfully.")

#########################################
# 3) PARSING & INSERTING DATA
#########################################
def safe_text(element: Optional[ET.Element]) -> Optional[str]:
    """Returns stripped text from an XML element or None if missing."""
    return element.text.strip() if element is not None and element.text else None

def extract_list_values(parent: ET.Element, child_tag: str, ns: str) -> List[str]:
    """Returns all text values from <child_tag> under 'parent' as a list."""
    return [safe_text(c) for c in parent.findall(f"{ns}{child_tag}") if safe_text(c)]

def parse_hmdb_xml(xml_file: str, conn):
    """Parses the HMDB XML file and inserts relevant data into PostgreSQL."""
    if not os.path.exists(xml_file):
        logger.warning(f"File not found: {xml_file}")
        return

    cursor = conn.cursor()
    ns = "{http://www.hmdb.ca}"
    context = ET.iterparse(xml_file, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    for event, elem in context:
        try:
            if event == "end" and elem.tag == f"{ns}metabolite":
                # Parse basic metabolite info
                hmdb_id = safe_text(elem.find(f"{ns}accession"))
                name = safe_text(elem.find(f"{ns}name"))
                formula = safe_text(elem.find(f"{ns}chemical_formula"))
                smiles = safe_text(elem.find(f"{ns}smiles"))
                inchi = safe_text(elem.find(f"{ns}inchi"))
                inchikey = safe_text(elem.find(f"{ns}inchikey"))
                bio_locs = extract_list_values(elem, "biospecimen_location", ns)
                # Extract locations
                bio_root = elem.find(f"{ns}biological_properties/{ns}biospecimen_locations")
                bio_locs = [safe_text(b) for b in bio_root.findall(f"{ns}biospecimen")] if bio_root is not None else []

                cell_root = elem.find(f"{ns}biological_properties/{ns}cellular_locations")
                cell_locs = [safe_text(c) for c in cell_root.findall(f"{ns}cellular")] if cell_root is not None else []

                tissue_root = elem.find(f"{ns}biological_properties/{ns}tissue_locations")
                tissue_locs = [safe_text(t) for t in tissue_root.findall(f"{ns}tissue")] if tissue_root is not None else []

                # Insert metabolite and get ID
                # cursor.execute("""
                #     INSERT INTO metabolites (hmdb_id, name, chemical_formula, smiles, inchi, inchikey, biospecimen_locations)
                #     VALUES (%s, %s, %s, %s, %s, %s, %s)
                #     ON CONFLICT (hmdb_id) DO NOTHING
                #     RETURNING id
                # """, (hmdb_id, name, formula, smiles, inchi, inchikey, json.dumps(bio_locs)))

                # result = cursor.fetchone()
                # if result is None:
                #     cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
                #     result = cursor.fetchone()

                # if result is None:
                #     logger.error(f"Metabolite {hmdb_id} not found/inserted")
                #     continue

                # metabolite_id = result[0]

                # Insert metabolite with all location fields
                cursor.execute("""
                    INSERT INTO metabolites (
                        hmdb_id, name, chemical_formula, smiles, inchi, inchikey,
                        biospecimen_locations, cellular_locations, tissue_locations
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hmdb_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        chemical_formula = EXCLUDED.chemical_formula,
                        smiles = EXCLUDED.smiles,
                        inchi = EXCLUDED.inchi,
                        inchikey = EXCLUDED.inchikey,
                        biospecimen_locations = EXCLUDED.biospecimen_locations,
                        cellular_locations = EXCLUDED.cellular_locations,
                        tissue_locations = EXCLUDED.tissue_locations
                    RETURNING id
                """, (
                    hmdb_id, name, formula, smiles, inchi, inchikey,
                    json.dumps(bio_locs), json.dumps(cell_locs), json.dumps(tissue_locs)
                ))

                result = cursor.fetchone()
                if result is None:
                    cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
                    result = cursor.fetchone()

                metabolite_id = result[0] if result else None

                # if bio_locs:
                #     cursor.execute("""
                #         UPDATE metabolites 
                #         SET biospecimen_locations = to_jsonb(%s) 
                #         WHERE id = %s
                #     """, (json.dumps(bio_locs), metabolite_id))

                # ✅ Pathway Insertion & Linking
                pwy_root = elem.find(f"{ns}biological_properties/{ns}pathways")
                if pwy_root is not None:
                    for pwy in pwy_root.findall(f"{ns}pathway"):
                        pathway_name = safe_text(pwy.find(f"{ns}name"))
                        kegg_id = safe_text(pwy.find(f"{ns}kegg_map_id"))
                        smpdb_id = safe_text(pwy.find(f"{ns}smpdb_id"))

                        if pathway_name:
                            cursor.execute("""
                                INSERT INTO pathways (pathway_name, kegg_id, smpdb_id)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (pathway_name, kegg_id, smpdb_id) DO NOTHING
                                RETURNING id
                            """, (pathway_name, kegg_id, smpdb_id))

                            pathway_id = cursor.fetchone()
                            if pathway_id is None:
                                cursor.execute("SELECT id FROM pathways WHERE pathway_name = %s", (pathway_name,))
                                pathway_id = cursor.fetchone()

                            if pathway_id:
                                cursor.execute("""
                                    INSERT INTO metabolite_pathways (metabolite_id, pathway_id)
                                    VALUES (%s, %s)
                                    ON CONFLICT (metabolite_id, pathway_id) DO NOTHING
                                """, (metabolite_id, pathway_id[0]))

                # ✅ Disease Insertion & Linking
                dis_root = elem.find(f"{ns}diseases")
                if dis_root is not None:
                    for disease_el in dis_root.findall(f"{ns}disease"):
                        disease_name = safe_text(disease_el.find(f"{ns}name"))
                        references = safe_text(disease_el.find(f"{ns}references"))

                        if disease_name:
                            cursor.execute("""
                                INSERT INTO diseases (disease_name, "references")
                                VALUES (%s, %s)
                                ON CONFLICT (disease_name, "references") DO NOTHING
                                RETURNING id
                            """, (disease_name, references))

                            disease_id = cursor.fetchone()
                            if disease_id is None:
                                cursor.execute("SELECT id FROM diseases WHERE disease_name = %s", (disease_name,))
                                disease_id = cursor.fetchone()

                            if disease_id:
                                cursor.execute("""
                                    INSERT INTO disease_metabolites (metabolite_id, disease_id)
                                    VALUES (%s, %s)
                                    ON CONFLICT (metabolite_id, disease_id) DO NOTHING
                                """, (metabolite_id, disease_id[0]))

                # ✅ Protein Insertion & Linking
                # Improved Protein Insertion & Linking
                prot_root = elem.find(f"{ns}protein_associations")
                if prot_root is not None:
                    for prot in prot_root.findall(f"{ns}protein"):
                        uniprot_id = safe_text(prot.find(f"{ns}uniprot_id"))
                        protein_name = safe_text(prot.find(f"{ns}name"))
                        gene_name = safe_text(prot.find(f"{ns}gene_name"))

                        if uniprot_id:
                            # Insert protein
                            cursor.execute("""
                                INSERT INTO proteins (uniprot_id, protein_name, gene_name)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (uniprot_id) DO NOTHING
                                RETURNING id
                            """, (uniprot_id, protein_name, gene_name))

                            protein_id = cursor.fetchone()
                            if protein_id is None:
                                cursor.execute("SELECT id FROM proteins WHERE uniprot_id = %s", (uniprot_id,))
                                protein_id = cursor.fetchone()

                            if protein_id:
                                # Link the current metabolite to this protein
                                cursor.execute("""
                                    INSERT INTO protein_metabolites (metabolite_id, protein_id)
                                    VALUES (%s, %s)
                                    ON CONFLICT (metabolite_id, protein_id) DO NOTHING
                                """, (metabolite_id, protein_id[0]))
                

                elem.clear()

        except Exception as e:
            logger.error(f"Error processing element: {str(e)}")
            conn.rollback()

    conn.commit()
    cursor.close()
    logger.info(f"Processed {xml_file}")



#########################################
# 4) MAIN EXECUTION
#########################################
if __name__ == "__main__":
    logger.info("Creating tables (if needed)...")
    create_tables()

    conn = connect_db()
    try:
        with ThreadPoolExecutor(max_workers=min(4, len(DATA_FILES))) as executor:
            executor.map(lambda fp: parse_hmdb_xml(fp, conn), DATA_FILES)

        # Refresh text search index after inserting data
        cursor = conn.cursor()
        cursor.execute("UPDATE metabolites SET doc = to_tsvector('english', name || ' ' || biospecimen_locations)")
        conn.commit()
        cursor.close()

    finally:
        conn.close()
        logger.info("All XML files processed successfully!")