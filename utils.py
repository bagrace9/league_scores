import os
import sqlite3

def ensure_db_exists(db_path='league_scores.db'):
    """Ensure the SQLite database file exists."""
    if not os.path.exists(db_path):
        open(db_path, 'w').close()

def connect_to_sqlite(db_path='league_scores.db'):
    """Establish a connection to the SQLite database."""
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        return None
