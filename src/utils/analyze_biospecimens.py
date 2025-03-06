import os
import logging
import xml.etree.ElementTree as ET
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Database connection details
DB_NAME = "metabolites_pg"
DB_USER = "postgres"
DB_PASSWORD = "your_password"  # Replace with your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

# XML files to process (focus on hmdb_proteins.xml first)
DATA_FILES = [
    "./data/hmdb_proteins.xml",  # Primary file for protein definitions
    # Add others if needed: "./data/hmdb_metabolites.xml", etc.
]

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def safe_text(element):
    return element.text.strip() if element is not None and element.text else None

def get_unlinked_proteins(cursor):
    """Retrieve unlinked proteins' uniprot_ids from the database."""
    cursor.execute("""
        SELECT p.uniprot_id
        FROM proteins p
        LEFT JOIN protein_metabolites pm ON p.id = pm.protein_id
        WHERE pm.metabolite_id IS NULL
    """)
    return {row[0] for row in cursor.fetchall() if row[0]}  # Set of unlinked uniprot_ids

def link_unlinked_proteins(xml_file, conn, unlinked_proteins):
    if not os.path.exists(xml_file):
        logger.warning(f"File not found: {xml_file}")
        return

    cursor = conn.cursor()
    ns = "{http://www.hmdb.ca}"
    context = ET.iterparse(xml_file, events=("start", "end"))
    link_count = 0
    found_proteins = set()
    processed_proteins = 0

    for event, elem in context:
        if event == "end" and elem.tag == f"{ns}protein":
            processed_proteins += 1
            uniprot_id = safe_text(elem.find(f"{ns}uniprot_id"))
            if not uniprot_id:
                continue  # Skip if no uniprot_id

            # Only process if this protein is in our unlinked set
            if uniprot_id in unlinked_proteins:
                found_proteins.add(uniprot_id)
                cursor.execute("SELECT id FROM proteins WHERE uniprot_id = %s", (uniprot_id,))
                prot_result = cursor.fetchone()
                if prot_result:
                    protein_id = prot_result[0]
                    met_assoc_root = elem.find(f"{ns}metabolite_associations")
                    if met_assoc_root:
                        for met in met_assoc_root.findall(f"{ns}metabolite"):
                            hmdb_id = safe_text(met.find(f"{ns}accession"))
                            if hmdb_id:
                                cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hmdb_id,))
                                met_result = cursor.fetchone()
                                if met_result:
                                    metabolite_id = met_result[0]
                                    cursor.execute("""
                                        INSERT INTO protein_metabolites (metabolite_id, protein_id)
                                        VALUES (%s, %s)
                                        ON CONFLICT DO NOTHING
                                    """, (metabolite_id, protein_id))
                                    if cursor.rowcount > 0:
                                        link_count += 1
                                        logger.info(f"Linked protein {uniprot_id} to metabolite {hmdb_id}")
                                else:
                                    logger.warning(f"Metabolite {hmdb_id} not found for protein {uniprot_id}")
                        if not met_assoc_root.findall(f"{ns}metabolite"):
                            logger.info(f"Protein {uniprot_id} has empty <metabolite_associations>")
                    else:
                        logger.info(f"Protein {uniprot_id} has no <metabolite_associations>")
                else:
                    logger.error(f"Protein {uniprot_id} not found in database (should not happen)")

            elem.clear()

    # Report proteins not found in the XML
    missing_proteins = unlinked_proteins - found_proteins
    for uniprot_id in missing_proteins:
        logger.info(f"Protein {uniprot_id} not found in {xml_file}")

    conn.commit()
    cursor.close()
    logger.info(f"Processed {xml_file}: {processed_proteins} proteins, {link_count} new links")
    if missing_proteins:
        logger.info(f"{len(missing_proteins)} unlinked proteins not found in {xml_file}")
    if link_count == 0 and not missing_proteins:
        logger.info(f"All 827 unlinked proteins checked in {xml_file} have no metabolite associations")

if __name__ == "__main__":
    conn = connect_db()
    try:
        cursor = conn.cursor()
        unlinked_proteins = get_unlinked_proteins(cursor)
        cursor.close()
        logger.info(f"Found {len(unlinked_proteins)} unlinked proteins in database")

        for xml_file in DATA_FILES:
            logger.info(f"Starting processing {xml_file} for unlinked proteins...")
            link_unlinked_proteins(xml_file, conn, unlinked_proteins)
    finally:
        conn.close()
        logger.info("Finished processing.")