#!/usr/bin/env python3
import os
import psycopg2
from psycopg2.extras import Json, execute_values
from lxml import etree
from tqdm import tqdm
import math

# Database connection details (update these as needed)
DB_NAME = "hmdb_full"
DB_USER = "postgres"
DB_PASSWORD = "your_password"  # Replace with your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

DATA_FILES = ["./data/hmdb_metabolites.xml"]  # Path to your XML file

# SQL statements for the database
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS xml_elements (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    tag TEXT NOT NULL,
    attributes JSONB,
    text TEXT,
    parent_id INTEGER REFERENCES xml_elements(id)
);
"""

CLEAR_FILE_DATA = "DELETE FROM xml_elements WHERE file_name = %s;"
INSERT_ELEMENT = "INSERT INTO xml_elements (file_name, tag, attributes, text, parent_id) VALUES %s RETURNING id;"
UPDATE_ELEMENT_TEXT = "UPDATE xml_elements SET text = %s WHERE id = %s;"

# Configuration
BATCH_SIZE = 1000  # Number of elements to batch before committing to the database
ESTIMATED_AVG_ELEMENT_SIZE = 500  # Average size of an element in bytes (adjust if needed)

def estimate_total_elements(file_path, avg_element_size=ESTIMATED_AVG_ELEMENT_SIZE):
    """Estimate the number of XML elements based on file size."""
    total_size = os.path.getsize(file_path)
    estimated_total = math.ceil(total_size / avg_element_size)
    return estimated_total

def create_table(conn):
    """Create the database table if it doesn’t exist."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
    conn.commit()

def clear_file_data(conn, file_name):
    """Clear existing data for this file from the database."""
    with conn.cursor() as cur:
        cur.execute(CLEAR_FILE_DATA, (file_name,))
    conn.commit()

def insert_element_batch(conn, element_batch):
    """Insert a batch of elements into the database."""
    with conn.cursor() as cur:
        ids = execute_values(cur, INSERT_ELEMENT, element_batch, template="(%s, %s, %s, %s, %s)", fetch=True)
        conn.commit()
        return [row[0] for row in ids]

def batch_update_element_text(conn, update_list):
    """Update the text of elements in batch."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPDATE_ELEMENT_TEXT, update_list)
        conn.commit()

def process_xml_file(conn, file_path):
    """Process the XML file and insert data into the database."""
    file_name = os.path.basename(file_path)
    print(f"Estimating total elements in: {file_name}")

    # Estimate total elements based on file size
    estimated_total_elements = estimate_total_elements(file_path)
    estimated_total_events = estimated_total_elements * 2  # Count both start and end events
    print(f"Estimated total elements: {estimated_total_elements}, Estimated total events: {estimated_total_events}")

    # Start processing with a progress bar
    print(f"Processing file: {file_name}")
    clear_file_data(conn, file_name)
    pbar = tqdm(total=estimated_total_events, desc=f"Processing {file_name}", unit="event", smoothing=0.1)

    parent_stack = []  # Track parent element IDs
    element_batch = []  # Batch for start events
    update_batch = []   # Batch for end event text updates

    # Stream the XML file
    context = etree.iterparse(file_path, events=("start", "end"), recover=True)
    for event, elem in context:
        try:
            if event == "start":
                # Prepare data for a new element
                parent_id = parent_stack[-1] if parent_stack else None
                initial_text = elem.text.strip() if elem.text and elem.text.strip() else ""
                element_data = (file_name, elem.tag, Json(dict(elem.attrib)) if elem.attrib else None, initial_text, parent_id)
                element_batch.append(element_data)
                if len(element_batch) >= BATCH_SIZE:
                    ids = insert_element_batch(conn, element_batch)
                    parent_stack.extend(ids)
                    element_batch.clear()
                    pbar.update(BATCH_SIZE)
            elif event == "end":
                # Update text when the element ends
                full_text = " ".join(elem.itertext()).strip()
                if parent_stack:
                    current_id = parent_stack.pop()
                    update_batch.append((full_text, current_id))
                    if len(update_batch) >= BATCH_SIZE:
                        batch_update_element_text(conn, update_batch)
                        update_batch.clear()
                elem.clear()  # Free up memory
                pbar.update(1)
        except Exception as e:
            print(f"Error processing element: {e}")
            continue

    # Handle any remaining batches
    if element_batch:
        ids = insert_element_batch(conn, element_batch)
        parent_stack.extend(ids)
        pbar.update(len(element_batch))

    if update_batch:
        batch_update_element_text(conn, update_batch)

    pbar.close()
    print(f"Finished processing file: {file_name}")

def main():
    """Main function to connect to the database and process files."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
    except Exception as e:
        print("Error connecting to the database:", e)
        return

    create_table(conn)
    
    for file_path in DATA_FILES:
        if os.path.exists(file_path):
            process_xml_file(conn, file_path)
        else:
            print(f"File not found: {file_path}")

    conn.close()
    print("Data extraction and insertion complete.")

if __name__ == "__main__":
    main()