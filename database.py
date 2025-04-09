from utils import connect_to_sqlite

def run_create_script(script_path='sql/create_scripts.sql'):
    """Run the SQL script to create necessary database objects."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn, open(script_path, 'r') as sql_file:
            conn.executescript(sql_file.read())
    finally:
        conn.close()

def create_league(league_name, is_handicap, league_url, cash_percentage, entry_fee):
    """Insert a new league into the database."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn:
            conn.execute(
                """
                INSERT INTO leagues (league_name, league_is_handicap, league_url, league_cash_percentage, league_entry_fee)
                VALUES (?, ?, ?, ?, ?)
                """,
                (league_name, is_handicap, league_url, cash_percentage, entry_fee)
            )
    finally:
        conn.close()

def update_league(league_id, league_name, is_handicap, league_url, cash_percentage, entry_fee):
    """Update an existing league in the database."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn:
            conn.execute(
                """
                UPDATE leagues
                SET league_name = ?, league_is_handicap = ?, league_url = ?, league_cash_percentage = ?, league_entry_fee = ?
                WHERE id = ?
                """,
                (league_name, is_handicap, league_url, cash_percentage, entry_fee, league_id)
            )
    finally:
        conn.close()

def fetch_leagues():
    """Fetch all leagues from the database."""
    conn = connect_to_sqlite()
    if conn is None:
        return []

    try:
        with conn:
            return conn.execute("SELECT id, league_name FROM leagues").fetchall()
    finally:
        conn.close()

def execute_sql_script(script_path):
    """Execute a given SQL script."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn, open(script_path, 'r') as sql_file:
            conn.executescript(sql_file.read())
    finally:
        conn.close()

def execute_sql(sql):
    """Execute a given SQL script."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        conn.executescript(sql)
    finally:
        conn.close()        

def execute_update_points_script(league_id, script_path='sql/update_points.sql'):
    """Execute the SQL script to update points for a specific league."""
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn, open(script_path, 'r') as sql_file:
            script = sql_file.read().replace("{league_id}", str(league_id))
            conn.executescript(script)
    finally:
        conn.close()

def fetch_league_url(league_id):
    """Fetch the URL of a league by its ID."""
    conn = connect_to_sqlite()
    if conn is None:
        return None

    try:
        with conn:
            result = conn.execute(
                "SELECT league_url FROM leagues WHERE id = ?", (league_id,)
            ).fetchone()
            return result[0] if result else None
    finally:
        conn.close()

def fetch_league_by_id(league_id):
    """Fetch a league's details by its ID."""
    conn = connect_to_sqlite()
    if conn is None:
        return None

    try:
        with conn:
            result = conn.execute(
                """
                SELECT league_name AS name, league_is_handicap AS is_handicap, 
                       league_url AS url, league_cash_percentage AS cash_percentage, 
                       league_entry_fee AS entry_fee
                FROM leagues
                WHERE id = ?
                """,
                (league_id,)
            ).fetchone()
            if result:
                return {
                    "name": result[0],
                    "is_handicap": result[1],
                    "url": result[2],
                    "cash_percentage": result[3],
                    "entry_fee": result[4],
                }
            return None
    finally:
        conn.close()
