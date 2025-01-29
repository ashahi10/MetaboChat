import sqlite3
import os

# Use the same database path as in your project
DB_PATH = os.path.abspath("../data/metabolites.db")
print(f"Using database path: {DB_PATH}")

def list_tables():
    """
    List all tables in the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]

def describe_table(table_name):
    """
    Get the schema of a table.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    schema = cursor.fetchall()
    conn.close()
    return schema

def count_rows(table_name):
    """
    Count the number of rows in a table.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    row_count = cursor.fetchone()[0]
    conn.close()
    return row_count

def sample_data(table_name, limit=5):
    """
    Fetch sample data from a table.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    rows = cursor.fetchall()
    conn.close()
    return rows

def explore_database():
    """
    Automate the exploration of the database.
    """
    print("\n--- Exploring the Database ---")
    tables = list_tables()
    print(f"\nTables in the database: {tables}")

    for table in tables:
        print(f"\n--- Schema of table '{table}' ---")
        schema = describe_table(table)
        for column in schema:
            print(f"Column: {column[1]}, Type: {column[2]}")

        row_count = count_rows(table)
        print(f"\nTotal rows in table '{table}': {row_count}")

        print(f"\n--- Sample data from table '{table}' ---")
        rows = sample_data(table)
        for row in rows:
            print(row)

if __name__ == "__main__":
    explore_database()
