"""
Database access layer for the league scores application.

Provides functions for connecting to PostgreSQL and performing all read/write
operations for leagues, events, raw scores, hole scores, handicaps, and payouts.
"""
import logging
import math
import re
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_db_connection_string
from utils import format_league_urls, parse_league_urls

logger = logging.getLogger(__name__)


def connect_to_postgresql():
    """Establish a connection to the PostgreSQL database."""
    try:
        connection_string = get_db_connection_string()
        return psycopg2.connect(connection_string)
    except psycopg2.Error as e:
        logger.error("Error connecting to PostgreSQL database: %s", e)
        return None


def _execute_script(script_path):
    """Read and execute a SQL script file within a single transaction."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        with open(script_path, 'r', encoding='utf-8') as sql_file:
            cursor.execute(sql_file.read())
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def run_create_script(script_path='sql/create_perm_tables.sql'):
    """Run the SQL script to create necessary database objects."""
    _execute_script(script_path)


def create_league(
        league_name,
        league_urls,
        cash_percentage,
        entry_fee,
        is_handicap,
        handicap_minimum_rounds,
        handicap_rounds_considered,
        handicap_years_lookback,
        handicap_base_score,
        handicap_multiplier,
):
    """Insert a new league into the database."""
    urls_text = format_league_urls(league_urls)

    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO leagues (
                  league_name
                , league_urls
                , league_cash_percentage
                , league_entry_fee
                , league_is_handicap
                , handicap_minimum_rounds
                , handicap_rounds_considered
                , handicap_years_lookback
                , handicap_base_score
                , handicap_multiplier
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING league_id
            """,
            (
                league_name,
                urls_text,
                cash_percentage,
                entry_fee,
                is_handicap,
                handicap_minimum_rounds,
                handicap_rounds_considered,
                handicap_years_lookback,
                handicap_base_score,
                handicap_multiplier,
            ),
        )
        league_id = cursor.fetchone()[0]
        conn.commit()
        return league_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def update_league(
        league_id,
        league_name,
        league_urls,
        cash_percentage,
        entry_fee,
        is_handicap,
        handicap_minimum_rounds,
        handicap_rounds_considered,
        handicap_years_lookback,
        handicap_base_score,
        handicap_multiplier,
):
    """Update an existing league in the database."""
    urls_text = format_league_urls(league_urls)

    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE leagues
            SET league_name = %s
              , league_urls = %s
              , league_cash_percentage = %s
              , league_entry_fee = %s
              , league_is_handicap = %s
              , handicap_minimum_rounds = %s
              , handicap_rounds_considered = %s
              , handicap_years_lookback = %s
              , handicap_base_score = %s
              , handicap_multiplier = %s
            WHERE league_id = %s
            """,
            (
                league_name,
                urls_text,
                cash_percentage,
                entry_fee,
                is_handicap,
                handicap_minimum_rounds,
                handicap_rounds_considered,
                handicap_years_lookback,
                handicap_base_score,
                handicap_multiplier,
                league_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_leagues():
    """Fetch all leagues from the database."""
    conn = connect_to_postgresql()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT league_id FROM leagues")
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def fetch_imported_event_urls(league_id=None):
    """Fetch imported export URLs, optionally filtered by league."""
    conn = connect_to_postgresql()
    if conn is None:
        return set()

    try:
        cursor = conn.cursor()
        if league_id is not None:
            cursor.execute(
                "SELECT export_url FROM events WHERE is_imported = TRUE AND league_id = %s",
                (league_id,),
            )
        else:
            cursor.execute("SELECT export_url FROM events WHERE is_imported = TRUE")
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()
        conn.close()


def fetch_event_by_url(export_url):
    """Fetch a single event record by its export URL."""
    conn = connect_to_postgresql()
    if conn is None:
        return None

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM events WHERE export_url = %s", (export_url,))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()


def _derive_event_name_from_filename(filename):
    """Derive event name from downloaded filename."""
    stem = Path(filename).stem
    name = re.sub(r"[-_](\d{4}-\d{2}-\d{2}|\d{8})_\d{8}_\d{6}$", "", stem)
    return name.replace("-", " ").strip()


def _normalize_column_name(name):
    """Lowercase and strip all non-alphanumeric/underscore characters from a column name."""
    return re.sub(r"[^a-z0-9_]", "", str(name).lower())


def _load_import_dataframe(file_path):
    """Load raw export as-is, then expose normalized columns for flexible mapping."""
    df = pd.read_excel(file_path)
    if df.empty:
        return df

    column_map = {col: _normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=column_map)
    return df


def _normalize_row(row):
    """Return a copy of a DataFrame row dict with all keys lowercased and stripped."""
    return {str(k).strip().lower(): v for k, v in row.items()}


def _pick_value(row, keys, default=None):
    """Return the first non-None, non-empty value found in a row dict using the given key candidates."""
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _to_int(value):
    """Coerce a value to int, returning None for empty or un-parseable inputs.

    Converts through float first to handle decimal strings like '3.0' that
    Excel sometimes produces for integer-valued cells.
    """
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_text(value):
    """Coerce a value to a stripped string, returning None for empty or null inputs."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _extract_hole_number(column_name):
    """Extract a hole number (1-36) from a normalized column name like 'hole_7'.

    Returns None if the column is not a hole column or the number is out of range.
    """
    key = str(column_name).strip().lower()
    match = re.match(r"^hole_(\d+)$", key)
    if match:
        hole_number = int(match.group(1))
        return hole_number if 1 <= hole_number <= 36 else None
    return None


def import_downloaded_file(league_id, downloaded_file):
    """Import one downloaded leaderboard file into events, raw_scores, and hole_scores."""
    df = _load_import_dataframe(downloaded_file.filepath)
    if df.empty:
        raise ValueError(f"No rows found in file: {downloaded_file.filepath}")

    available_columns = set(df.columns)
    required_columns = {
        "division",
        "name",
        "username",
        "round_total_score",
    }
    missing_columns = sorted(required_columns - available_columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in file {downloaded_file.filepath}: {', '.join(missing_columns)}"
        )

    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()

        event_name = _derive_event_name_from_filename(downloaded_file.filename)
        num_players = int(df['name'].notna().sum()) if 'name' in df.columns else len(df)

        cursor.execute(
            """
            INSERT INTO events (
                  league_id
                , event_name
                , event_end_date
                , export_url
                , is_downloaded
                , file_name
                , file_path
                , num_players
                , download_date
                , is_imported
                , import_date
                , is_excluded
            ) VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP, FALSE)
            RETURNING event_id
            """,
            (
                league_id,
                event_name,
                downloaded_file.event_end_date,
                downloaded_file.export_url,
                downloaded_file.filename,
                downloaded_file.filepath,
                num_players,
                downloaded_file.download_date,
            ),
        )
        event_id = cursor.fetchone()[0]

        # Insert one raw_score row per player, followed by their individual hole scores.
        for raw_row in df.to_dict(orient="records"):
            row = _normalize_row(raw_row)

            division = _to_text(row.get("division"))
            player_name = _to_text(row.get("name"))
            player_username = _to_text(row.get("username"))
            raw_score = _to_int(row.get("round_total_score"))

            if player_name is None:
                continue

            cursor.execute(
                """
                INSERT INTO raw_scores (
                      event_id
                    , league_id
                    , division
                    , player_name
                    , player_username
                    , raw_score
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING raw_score_id
                """,
                (
                    event_id,
                    league_id,
                    division,
                    player_name,
                    player_username,
                    raw_score,
                ),
            )
            raw_score_id = cursor.fetchone()[0]

            # Insert a hole_score row for each hole column present in the export.
            for column_name, value in row.items():
                hole_number = _extract_hole_number(column_name)
                hole_score = _to_int(value)
                if hole_number is None or hole_score is None:
                    continue

                cursor.execute(
                    """
                    INSERT INTO hole_scores (
                          raw_score_id
                        , hole_number
                        , hole_score
                    ) VALUES (%s, %s, %s)
                    """,
                    (raw_score_id, hole_number, hole_score),
                )

        conn.commit()
        return event_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def insert_event_record(
        league_id,
        event_name,
        export_url,
        event_end_date=None,
        num_players=None,
        is_imported=False,
):
    """Insert an event record to the database."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO events (
                  league_id
                , event_name
                , export_url
                , event_end_date
                , num_players
                , is_imported
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING event_id
            """,
            (
                league_id,
                event_name,
                export_url,
                event_end_date,
                num_players,
                is_imported,
            ),
        )
        event_id = cursor.fetchone()[0]
        conn.commit()
        return event_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def mark_event_imported_by_url(export_url):
    """Mark an existing event record as imported by export URL."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE events SET is_imported = TRUE WHERE export_url = %s",
            (export_url,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def update_event_file_metadata(event_id, file_name, file_path):
    """Update file metadata for an event after moving the imported file."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE events
            SET file_name = %s
              , file_path = %s
              , update_time = CURRENT_TIMESTAMP
            WHERE event_id = %s
            """,
            (file_name, file_path, event_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_sql_script(script_path):
    """Execute a given SQL script."""
    _execute_script(script_path)


def execute_sql(sql):
    """Execute a given SQL statement."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_update_points_script(league_id, script_path='sql/drop_create_final_scores.sql'):
    """Execute the final scores merge SQL script for a specific league."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        with open(script_path, 'r', encoding='utf-8') as sql_file:
            script = sql_file.read().replace("{league_id}", str(league_id))
            cursor.execute(script)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_league_urls(league_id):
    """Fetch all URLs for a given league."""
    conn = connect_to_postgresql()
    if conn is None:
        return []

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT league_urls FROM leagues WHERE league_id = %s", (league_id,))
        row = cursor.fetchone()
        if not row:
            return []
        urls_text = row['league_urls'] if isinstance(row, dict) else row[0]
        return parse_league_urls(urls_text)
    finally:
        cursor.close()
        conn.close()


def fetch_league_url(league_id):
    """Fetch the primary URL of a league by its ID."""
    urls = fetch_league_urls(league_id)
    return urls[0] if urls else None


def fetch_league_by_id(league_id):
    """Fetch a league's details by its ID."""
    conn = connect_to_postgresql()
    if conn is None:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT league_name AS name, 
                   league_urls,
                   league_is_handicap AS is_handicap, 
                   league_cash_percentage AS cash_percentage, 
                   league_entry_fee AS entry_fee,
                   handicap_minimum_rounds AS handicap_minimum_rounds,
                   handicap_rounds_considered AS handicap_rounds_considered,
                   handicap_years_lookback AS handicap_years_lookback,
                   handicap_base_score AS handicap_base_score,
                   handicap_multiplier AS handicap_multiplier
            FROM leagues
            WHERE league_id = %s
            """,
            (league_id,)
        )
        result = cursor.fetchone()
        if result:
            urls_text = result[1]
            urls = parse_league_urls(urls_text)
            return {
                "name": result[0],
                "is_handicap": result[2],
                "url": urls[0] if urls else "",
                "urls": urls,
                "cash_percentage": result[3],
                "entry_fee": result[4],
                "handicap_minimum_rounds": result[5],
                "handicap_rounds_considered": result[6],
                "handicap_years_lookback": result[7],
                "handicap_base_score": result[8],
                "handicap_multiplier": result[9],
            }
        return None
    finally:
        cursor.close()
        conn.close()



def create_payout_table():
    """Drop and recreate the payouts table.

    Builds payout percentages for 1-100 players using an exponential decay model.
    The top ceil(n / 3) finishers are paid, with each successive position receiving
    64% of the prior position's share. Pre-computing these values here lets the
    scoring SQL join directly without recalculating on every run.
    """
    payout_rows = []
    # Geometric decay factor: each paid position earns this fraction of the previous one's share.
    decay = 0.64

    for n_players in range(1, 101):
        n_winners = math.ceil(n_players / 3)
        weights = [decay ** i for i in range(n_winners)]
        total_weight = sum(weights)

        for position, weight in enumerate(weights, start=1):
            payout_fraction = weight / total_weight
            payout_rows.append(
                {
                    'n_players': n_players,
                    'position': position,
                    'weight': round(weight, 12),
                    'percentage': round(payout_fraction, 12),
                    'payout_percent': round(payout_fraction * 100, 4),
                }
            )

    payouts_df = pd.DataFrame(payout_rows)

    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS payouts")
        cursor.execute(
            """
            CREATE TABLE payouts (
                  n_players INTEGER NOT NULL
                , position INTEGER NOT NULL
                , weight NUMERIC(20, 12) NOT NULL
                , percentage NUMERIC(20, 12) NOT NULL
                , payout_percent NUMERIC(9, 4) NOT NULL
                , create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                , PRIMARY KEY (n_players, position)
            )
            """
        )

        cursor.executemany(
            """
            INSERT INTO payouts (
                  n_players
                , position
                , weight
                , percentage
                , payout_percent
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [tuple(row) for row in payouts_df[['n_players', 'position', 'weight', 'percentage', 'payout_percent']].itertuples(index=False, name=None)],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def payouts_table_exists():
    """Return True when the payouts table already exists in the current schema."""
    conn = connect_to_postgresql()
    if conn is None:
        raise Exception("Failed to connect to the database.")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                                    AND table_name = 'payouts'
            )
            """
        )
        return bool(cursor.fetchone()[0])
    finally:
        cursor.close()
        conn.close()
    





