import os
import json
import xml.etree.ElementTree as ET
import psycopg2

# Database connection (adjust credentials as needed)
conn = psycopg2.connect(
    dbname="metabolites_pg",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# List of XML files to process (corrected from tuple to list, removed duplicate)
xml_files = [
    "./data/hmdb_metabolites.xml",
    "./data/feces_metabolites.xml",
    "./data/hmdb_proteins.xml",
    "./data/saliva_metabolites.xml",
    "./data/serum_metabolites.xml",
    "./data/sweat_metabolites.xml",
    "./data/urine_metabolites.xml",
    "./data/csf_metabolites.xml",
]

# Namespace for HMDB XML
ns = "{http://www.hmdb.ca}"

# Helper function to safely extract text from an XML element
def safe_text(element):
    return element.text.strip() if element is not None and element.text is not None else None

# Process each XML file
for xml_file in xml_files:
    if not os.path.exists(xml_file):
        print(f"File not found: {xml_file}")
        continue
    print(f"Processing {xml_file}...")
    
    # Parse the XML file incrementally to handle large files efficiently
    context = ET.iterparse(xml_file, events=("start", "end"))
    count = 0  # Track the number of metabolites processed
    
    for event, elem in context:
        if event == "end" and elem.tag == f"{ns}metabolite":
            # Extract the hmdb_id from the <accession> tag
            hmdb_id_elem = elem.find(f"{ns}accession")
            hmdb_id = safe_text(hmdb_id_elem)
            if hmdb_id is None:
                print("Skipping metabolite without accession.")
                continue  # Skip if no hmdb_id is found
            
            # Extract biospecimen locations from the nested structure
            bio_root = elem.find(f"{ns}biological_properties/{ns}biospecimen_locations")
            bio_locs = []
            if bio_root is not None:
                bio_locs = [safe_text(b) for b in bio_root.findall(f"{ns}biospecimen") if safe_text(b)]
            
            # Update the database with the biospecimen locations
            cursor.execute("""
                UPDATE metabolites
                SET biospecimen_locations = %s
                WHERE hmdb_id = %s
            """, (json.dumps(bio_locs), hmdb_id))
            
            # Check if the update affected any rows
            if cursor.rowcount == 0:
                print(f"No rows updated for {hmdb_id}. Check if it exists in the database.")
            
            # Clear the element to free memory
            elem.clear()
            count += 1
            
            # Print progress every 1000 metabolites
            if count % 1000 == 0:
                print(f"Processed {count} metabolites in {xml_file}")
    
    print(f"Finished processing {xml_file}. Total metabolites updated: {count}")

# Commit the changes to the database and close connections
conn.commit()
cursor.close()
conn.close()
print("All biospecimen locations updated successfully.")