import os
import sqlite3
import xml.etree.ElementTree as ET

def parse_hmdb_metabolites_incremental(file_path, db_file):
    """
    Incrementally parse the HMDB metabolites XML file and store relevant data in an SQLite database.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table to store metabolites
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metabolites (
            id INTEGER PRIMARY KEY,
            name TEXT,
            chemical_formula TEXT,
            description TEXT,
            pathways TEXT,
            diseases TEXT
        )
    ''')
    conn.commit()

    # Use iterparse to parse the XML file incrementally
    context = ET.iterparse(file_path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    count = 0
    for event, elem in context:
        if event == "end" and elem.tag == "{http://www.hmdb.ca}metabolite":
            # Extract relevant data
            name = elem.find("{http://www.hmdb.ca}name").text if elem.find("{http://www.hmdb.ca}name") is not None else None
            chemical_formula = elem.find("{http://www.hmdb.ca}chemical_formula").text if elem.find("{http://www.hmdb.ca}chemical_formula") is not None else None
            description = elem.find("{http://www.hmdb.ca}description").text if elem.find("{http://www.hmdb.ca}description") is not None else None
            pathways = ", ".join(
                pathway.find("{http://www.hmdb.ca}name").text
                for pathway in elem.findall("{http://www.hmdb.ca}pathways/{http://www.hmdb.ca}pathway")
                if pathway.find("{http://www.hmdb.ca}name") is not None
            )
            diseases = ", ".join(
                disease.find("{http://www.hmdb.ca}name").text
                for disease in elem.findall("{http://www.hmdb.ca}diseases/{http://www.hmdb.ca}disease")
                if disease.find("{http://www.hmdb.ca}name") is not None
            )

            # Insert data into SQLite
            cursor.execute('''
                INSERT INTO metabolites (name, chemical_formula, description, pathways, diseases)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, chemical_formula, description, pathways, diseases))
            conn.commit()

            count += 1

            # Clear processed element to free memory
            root.clear()

    print(f"Parsed and stored {count} metabolites in the database.")
    conn.close()

if __name__ == "__main__":
    file_path = "./data/hmdb_metabolites.xml"
    db_file = "./data/metabolites.db"

    print("Parsing HMDB Metabolites incrementally and storing in SQLite...")
    parse_hmdb_metabolites_incremental(file_path, db_file)
    print(f"Data successfully stored in {db_file}")
