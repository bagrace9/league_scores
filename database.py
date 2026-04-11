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

def create_league(league_name, league_urls, cash_percentage, entry_fee, is_handicap, handicap_minimum_rounds, handicap_rounds_considered,handicap_years_lookback,handicap_base_score,handicap_multiplier):
    """Insert a new league into the database."""
    if isinstance(league_urls, str):
        league_urls = [league_urls] if league_urls else []
    
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO leagues (league_name, 
                                    league_cash_percentage, 
                                    league_entry_fee, 
                                    league_is_handicap, 
                                    handicap_minimum_rounds, 
                                    handicap_rounds_considered, 
                                    handicap_years_lookback,
                                    handicap_base_score,
                                    handicap_multiplier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (league_name, cash_percentage, entry_fee, is_handicap, handicap_minimum_rounds, handicap_rounds_considered,handicap_years_lookback, handicap_base_score, handicap_multiplier)
            )
            league_id = cursor.lastrowid
            if league_urls:
                conn.executemany(
                    "INSERT INTO league_urls (league_id, url) VALUES (?, ?)",
                    [(league_id, url) for url in league_urls]
                )
    finally:
        conn.close()

def update_league(league_id, league_name, league_urls, cash_percentage, entry_fee, is_handicap, handicap_minimum_rounds, handicap_rounds_considered,handicap_years_lookback,handicap_base_score,handicap_multiplier):
    """Update an existing league in the database."""
    if isinstance(league_urls, str):
        league_urls = [league_urls] if league_urls else []
    
    conn = connect_to_sqlite()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        with conn:
            conn.execute(
                """
                UPDATE leagues
                SET league_name = ?
                  , league_cash_percentage = ?
                  , league_entry_fee = ?                  
                  , league_is_handicap = ?
                  , handicap_minimum_rounds = ?
                  , handicap_rounds_considered = ?
                  , handicap_years_lookback = ?
                  , handicap_base_score = ?
                  , handicap_multiplier = ?
                WHERE id = ?
                """,
                (league_name, cash_percentage, entry_fee, is_handicap, handicap_minimum_rounds, handicap_rounds_considered,handicap_years_lookback,handicap_base_score,handicap_multiplier, league_id)
            )
            conn.execute("DELETE FROM league_urls WHERE league_id = ?", (league_id,))
            if league_urls:
                conn.executemany(
                    "INSERT INTO league_urls (league_id, url) VALUES (?, ?)",
                    [(league_id, url) for url in league_urls]
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

def fetch_league_urls(league_id):
    """Fetch all URLs for a given league."""
    conn = connect_to_sqlite()
    if conn is None:
        return []

    try:
        with conn:
            rows = conn.execute(
                "SELECT url FROM league_urls WHERE league_id = ? ORDER BY id",
                (league_id,)
            ).fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()

def fetch_league_url(league_id):
    """Fetch the primary URL of a league by its ID."""
    urls = fetch_league_urls(league_id)
    return urls[0] if urls else None

def fetch_league_by_id(league_id):
    """Fetch a league's details by its ID."""
    conn = connect_to_sqlite()
    if conn is None:
        return None

    try:
        with conn:
            result = conn.execute(
                """
                SELECT league_name AS name, 
                       league_is_handicap AS is_handicap, 
                       league_cash_percentage AS cash_percentage, 
                       league_entry_fee AS entry_fee,
                       handicap_minimum_rounds AS handicap_minimum_rounds,
                       handicap_rounds_considered AS handicap_rounds_considered,
                       handicap_years_lookback AS handicap_years_lookback,
                       handicap_base_score AS handicap_base_score,
                       handicap_multiplier AS handicap_multiplier
                FROM leagues
                WHERE id = ?
                """,
                (league_id,)
            ).fetchone()
            if result:
                urls = fetch_league_urls(league_id)
                return {
                    "name": result[0],
                    "is_handicap": result[1],
                    "url": urls[0] if urls else "",
                    "urls": urls,
                    "cash_percentage": result[2],
                    "entry_fee": result[3],
                    "handicap_minimum_rounds": result[4],
                    "handicap_rounds_considered": result[5],
                    "handicap_years_lookback": result[6],
                    "handicap_base_score": result[7],
                    "handicap_multiplier": result[8]
                }
            return None
    finally:
        conn.close()
