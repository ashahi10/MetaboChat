import sqlite3
import time

db_path = "/Users/adityashahi/Desktop/MetaboChat/MetaboChat/Metabochat/data/metabolites.db"

start_time = time.time()
conn = sqlite3.connect(db_path)
end_time = time.time()

print(f"Database connection time: {end_time - start_time:.4f} seconds")

conn.execute("VACUUM;")  # Clean up fragmented space
conn.execute("ANALYZE;")  # Optimize query execution
conn.close()

print("Database optimization complete!")