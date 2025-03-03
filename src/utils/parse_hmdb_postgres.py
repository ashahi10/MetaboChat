import os
import psycopg2
import xml.etree.ElementTree as ET
import json

# PostgreSQL Connection Details
DB_NAME = "metabolites_pg"  # The database you created
DB_USER = "postgres"        # The default superuser in PostgreSQL
DB_PASSWORD = "ashahi1"     # If you haven't set a password, leave it empty
DB_HOST = "localhost"       # Since PostgreSQL is running locally
DB_PORT = "5432"            # Default PostgreSQL port

# Connect to PostgreSQL
def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Create tables if they do not exist
def create_tables():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metabolites (
            id SERIAL PRIMARY KEY,
            hmdb_id TEXT UNIQUE,
            name TEXT,
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
            cellular_locations JSONB,
            biospecimen_locations JSONB,
            tissue_locations JSONB
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pathways (
            id SERIAL PRIMARY KEY,
            metabolite_id INTEGER REFERENCES metabolites(id),
            pathway_name TEXT,
            kegg_id TEXT,
            smpdb_id TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS diseases (
            id SERIAL PRIMARY KEY,
            metabolite_id INTEGER REFERENCES metabolites(id),
            disease_name TEXT,
            "references" TEXT
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proteins (
            id SERIAL PRIMARY KEY,
            metabolite_id INTEGER REFERENCES metabolites(id),
            protein_name TEXT,
            gene_name TEXT,
            uniprot_id TEXT
        );
    ''')

    conn.commit()
    conn.close()

# Incrementally parse HMDB XML and insert into PostgreSQL
def parse_hmdb_metabolites_incremental(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = connect_db()
    cursor = conn.cursor()

    # Use iterparse to process XML efficiently
    context = ET.iterparse(file_path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    count = 0
    for event, elem in context:
        if event == "end" and elem.tag == "{http://www.hmdb.ca}metabolite":
            # Extract fields with proper None checks
            hmdb_id = elem.find("{http://www.hmdb.ca}accession")
            hmdb_id = hmdb_id.text if hmdb_id is not None else None
            
            name = elem.find("{http://www.hmdb.ca}name")
            name = name.text if name is not None else None
            
            status = elem.find("{http://www.hmdb.ca}status")
            status = status.text if status is not None else None

            molecular_weight_avg = elem.find("{http://www.hmdb.ca}average_molecular_weight")
            molecular_weight_avg = float(molecular_weight_avg.text) if molecular_weight_avg is not None and molecular_weight_avg.text else None
            
            molecular_weight_mono = elem.find("{http://www.hmdb.ca}monoisotopic_molecular_weight")
            molecular_weight_mono = float(molecular_weight_mono.text) if molecular_weight_mono is not None and molecular_weight_mono.text else None

            iupac_name = elem.find("{http://www.hmdb.ca}iupac_name")
            iupac_name = iupac_name.text if iupac_name is not None else None
            
            smiles = elem.find("{http://www.hmdb.ca}smiles")
            smiles = smiles.text if smiles is not None else None
            
            inchi = elem.find("{http://www.hmdb.ca}inchi")
            inchi = inchi.text if inchi is not None else None
            
            inchikey = elem.find("{http://www.hmdb.ca}inchikey")
            inchikey = inchikey.text if inchikey is not None else None

            # Extract JSON fields
            synonyms = [syn.text for syn in elem.findall("{http://www.hmdb.ca}synonyms/{http://www.hmdb.ca}synonym") if syn.text]
            
            taxonomy = elem.find("{http://www.hmdb.ca}taxonomy")
            if taxonomy is not None:
                taxonomy_kingdom = taxonomy.find("{http://www.hmdb.ca}kingdom")
                taxonomy_kingdom = taxonomy_kingdom.text if taxonomy_kingdom is not None else None
                
                taxonomy_superclass = taxonomy.find("{http://www.hmdb.ca}superclass")
                taxonomy_superclass = taxonomy_superclass.text if taxonomy_superclass is not None else None
                
                taxonomy_class = taxonomy.find("{http://www.hmdb.ca}class")
                taxonomy_class = taxonomy_class.text if taxonomy_class is not None else None
                
                taxonomy_subclass = taxonomy.find("{http://www.hmdb.ca}subclass")
                taxonomy_subclass = taxonomy_subclass.text if taxonomy_subclass is not None else None
            else:
                taxonomy_kingdom = taxonomy_superclass = taxonomy_class = taxonomy_subclass = None

            cellular_locations = [loc.text for loc in elem.findall("{http://www.hmdb.ca}cellular_locations/{http://www.hmdb.ca}cellular_location") if loc.text]
            biospecimen_locations = [loc.text for loc in elem.findall("{http://www.hmdb.ca}biospecimen_locations/{http://www.hmdb.ca}biospecimen_location") if loc.text]
            tissue_locations = [loc.text for loc in elem.findall("{http://www.hmdb.ca}tissue_locations/{http://www.hmdb.ca}tissue_location") if loc.text]

            # Insert metabolite data
            cursor.execute('''
                INSERT INTO metabolites 
                (hmdb_id, name, synonyms, status, molecular_weight_avg, molecular_weight_monoisotopic, 
                iupac_name, smiles, inchi, inchikey, taxonomy_kingdom, taxonomy_superclass, 
                taxonomy_class, taxonomy_subclass, cellular_locations, biospecimen_locations, tissue_locations) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (hmdb_id, name, json.dumps(synonyms), status, molecular_weight_avg, molecular_weight_mono, 
                  iupac_name, smiles, inchi, inchikey, taxonomy_kingdom, taxonomy_superclass, 
                  taxonomy_class, taxonomy_subclass, json.dumps(cellular_locations), json.dumps(biospecimen_locations), json.dumps(tissue_locations)))

            count += 1

            # Clear memory
            root.clear()

    conn.commit()
    print(f"Parsed and stored {count} metabolites in PostgreSQL database.")
    conn.close()

if __name__ == "__main__":
    file_path = "./data/hmdb_metabolites.xml"

    print("Setting up PostgreSQL tables...")
    create_tables()
    
    print("Parsing HMDB Metabolites and storing in PostgreSQL...")
    parse_hmdb_metabolites_incremental(file_path)
    print("Data successfully stored in PostgreSQL.")