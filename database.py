"""
Database access layer for the league scores application.

Provides functions for connecting to BigQuery and performing all read/write
operations for leagues, events, raw scores, hole scores, handicaps, and payouts.
"""
import logging
import math
import re
import uuid
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from config import _to_bool
from config import get_bigquery_config
from utils import format_league_urls, parse_league_urls

logger = logging.getLogger(__name__)

_bq_client_cache = None


def _get_bigquery_client():
    global _bq_client_cache
    if _bq_client_cache is not None:
        return _bq_client_cache
    cfg = get_bigquery_config()
    credentials_path = cfg.get('credentials_path')
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        _bq_client_cache = bigquery.Client(
            project=cfg['project_id'],
            location=cfg.get('location'),
            credentials=credentials,
        )
    else:
        _bq_client_cache = bigquery.Client(project=cfg['project_id'], location=cfg.get('location'))
    return _bq_client_cache


def _bq_dataset_ref():
    cfg = get_bigquery_config()
    return f"{cfg['project_id']}.{cfg['dataset']}"


def _bq_default_dataset():
    return bigquery.DatasetReference.from_string(_bq_dataset_ref())


def _bq_table(table_name):
    return f"`{_bq_dataset_ref()}.{table_name}`"


def _bq_table_id(table_name):
    return f"{_bq_dataset_ref()}.{table_name}"


def _run_bigquery_sql(sql, query_parameters=None):
    client = _get_bigquery_client()
    job_config = bigquery.QueryJobConfig(default_dataset=_bq_default_dataset())
    if query_parameters:
        job_config.query_parameters = query_parameters
    return list(client.query(sql, job_config=job_config).result())


def _substitute_dataset(sql_text):
    return sql_text.replace('{dataset_name}', _bq_dataset_ref())


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
    league_id = str(uuid.uuid4())

    _run_bigquery_sql(
        f"""
        INSERT INTO {_bq_table('leagues')} (
            league_id,
            league_name,
            league_urls,
            league_cash_percentage,
            league_entry_fee,
            league_is_handicap,
            handicap_minimum_rounds,
            handicap_rounds_considered,
            handicap_years_lookback,
            handicap_base_score,
            handicap_multiplier,
            create_time,
            update_time
        )
        VALUES (
            @league_id,
            @league_name,
            @league_urls,
            @league_cash_percentage,
            @league_entry_fee,
            @league_is_handicap,
            @handicap_minimum_rounds,
            @handicap_rounds_considered,
            @handicap_years_lookback,
            @handicap_base_score,
            @handicap_multiplier,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """,
        [
            bigquery.ScalarQueryParameter('league_id', 'STRING', league_id),
            bigquery.ScalarQueryParameter('league_name', 'STRING', league_name),
            bigquery.ScalarQueryParameter('league_urls', 'STRING', urls_text),
            bigquery.ScalarQueryParameter('league_cash_percentage', 'NUMERIC', cash_percentage),
            bigquery.ScalarQueryParameter('league_entry_fee', 'NUMERIC', entry_fee),
            bigquery.ScalarQueryParameter('league_is_handicap', 'BOOL', bool(is_handicap)),
            bigquery.ScalarQueryParameter('handicap_minimum_rounds', 'INT64', handicap_minimum_rounds),
            bigquery.ScalarQueryParameter('handicap_rounds_considered', 'INT64', handicap_rounds_considered),
            bigquery.ScalarQueryParameter('handicap_years_lookback', 'INT64', handicap_years_lookback),
            bigquery.ScalarQueryParameter('handicap_base_score', 'INT64', handicap_base_score),
            bigquery.ScalarQueryParameter('handicap_multiplier', 'NUMERIC', handicap_multiplier),
        ],
    )
    return league_id


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

    _run_bigquery_sql(
        f"""
        UPDATE {_bq_table('leagues')}
        SET league_name = @league_name,
            league_urls = @league_urls,
            league_cash_percentage = @league_cash_percentage,
            league_entry_fee = @league_entry_fee,
            league_is_handicap = @league_is_handicap,
            handicap_minimum_rounds = @handicap_minimum_rounds,
            handicap_rounds_considered = @handicap_rounds_considered,
            handicap_years_lookback = @handicap_years_lookback,
            handicap_base_score = @handicap_base_score,
            handicap_multiplier = @handicap_multiplier,
            update_time = CURRENT_TIMESTAMP()
        WHERE league_id = @league_id
        """,
        [
            bigquery.ScalarQueryParameter('league_name', 'STRING', league_name),
            bigquery.ScalarQueryParameter('league_urls', 'STRING', urls_text),
            bigquery.ScalarQueryParameter('league_cash_percentage', 'NUMERIC', cash_percentage),
            bigquery.ScalarQueryParameter('league_entry_fee', 'NUMERIC', entry_fee),
            bigquery.ScalarQueryParameter('league_is_handicap', 'BOOL', bool(is_handicap)),
            bigquery.ScalarQueryParameter('handicap_minimum_rounds', 'INT64', handicap_minimum_rounds),
            bigquery.ScalarQueryParameter('handicap_rounds_considered', 'INT64', handicap_rounds_considered),
            bigquery.ScalarQueryParameter('handicap_years_lookback', 'INT64', handicap_years_lookback),
            bigquery.ScalarQueryParameter('handicap_base_score', 'INT64', handicap_base_score),
            bigquery.ScalarQueryParameter('handicap_multiplier', 'NUMERIC', handicap_multiplier),
            bigquery.ScalarQueryParameter('league_id', 'STRING', str(league_id)),
        ],
    )


def fetch_leagues():
    """Fetch all leagues from the database."""
    rows = _run_bigquery_sql(f"SELECT league_id FROM {_bq_table('leagues')}")
    return [(row['league_id'],) for row in rows]


def fetch_imported_event_urls(league_id=None):
    """Fetch imported export URLs, optionally filtered by league."""
    if league_id is not None:
        rows = _run_bigquery_sql(
            f"""
            SELECT export_url
            FROM {_bq_table('events')}
            WHERE is_imported = TRUE
              AND league_id = @league_id
            """,
            [bigquery.ScalarQueryParameter('league_id', 'STRING', str(league_id))],
        )
    else:
        rows = _run_bigquery_sql(
            f"SELECT export_url FROM {_bq_table('events')} WHERE is_imported = TRUE"
        )
    return {row['export_url'] for row in rows}


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
    return df.rename(columns=column_map)


def _normalize_row(row):
    """Return a copy of a DataFrame row dict with all keys lowercased and stripped."""
    return {str(k).strip().lower(): v for k, v in row.items()}


def _to_int(value):
    """Coerce a value to int, returning None for empty or un-parseable inputs."""
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
    """Extract a hole number (1-36) from a normalized column name like hole_7."""
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
    required_columns = {"division", "name", "username", "round_total_score"}
    missing_columns = sorted(required_columns - available_columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in file {downloaded_file.filepath}: {', '.join(missing_columns)}"
        )

    client = _get_bigquery_client()
    event_id = str(uuid.uuid4())
    event_name = _derive_event_name_from_filename(downloaded_file.filename)
    num_players = int(df['name'].notna().sum()) if 'name' in df.columns else len(df)

    event_row = {
        'event_id': event_id,
        'league_id': str(league_id),
        'event_name': event_name,
        'event_end_date': downloaded_file.event_end_date.isoformat() if downloaded_file.event_end_date else None,
        'export_url': downloaded_file.export_url,
        'is_downloaded': True,
        'file_name': downloaded_file.filename,
        'file_path': downloaded_file.filepath,
        'num_players': num_players,
        'download_date': downloaded_file.download_date.isoformat() if downloaded_file.download_date else None,
        'is_imported': False,
        'is_excluded_from_handicap': False,
        'is_excluded_from_points': False,
        'is_no_handicap_applied': False,
        'buy_in_override_ammt': None,
        'points_multiplier': None,
    }

    job = client.load_table_from_json([event_row], _bq_table_id('events'))
    job.result()
    if job.errors:
        raise Exception(f"Failed to insert event row: {job.errors}")

    raw_score_rows = []
    hole_score_rows = []

    for raw_row in df.to_dict(orient="records"):
        row = _normalize_row(raw_row)

        division = _to_text(row.get("division"))
        player_name = _to_text(row.get("name"))
        player_username = _to_text(row.get("username"))
        raw_score = _to_int(row.get("round_total_score"))

        if player_name is None:
            continue

        raw_score_id = str(uuid.uuid4())
        raw_score_rows.append(
            {
                'raw_score_id': raw_score_id,
                'event_id': event_id,
                'league_id': str(league_id),
                'division': division,
                'player_name': player_name,
                'player_username': player_username,
                'raw_score': raw_score,
            }
        )

        for column_name, value in row.items():
            hole_number = _extract_hole_number(column_name)
            hole_score = _to_int(value)
            if hole_number is None or hole_score is None:
                continue
            hole_score_rows.append(
                {
                    'hole_score_id': str(uuid.uuid4()),
                    'raw_score_id': raw_score_id,
                    'hole_number': hole_number,
                    'hole_score': hole_score,
                }
            )

    if raw_score_rows:
        job = client.load_table_from_json(raw_score_rows, _bq_table_id('raw_scores'))
        job.result()
        if job.errors:
            raise Exception(f"Failed to insert raw score rows: {job.errors}")

    if hole_score_rows:
        job = client.load_table_from_json(hole_score_rows, _bq_table_id('hole_scores'))
        job.result()
        if job.errors:
            raise Exception(f"Failed to insert hole score rows: {job.errors}")

    _run_bigquery_sql(
        f"""
        UPDATE {_bq_table('events')}
        SET is_imported = TRUE,
            update_time = CURRENT_TIMESTAMP()
        WHERE event_id = @event_id
        """,
        [bigquery.ScalarQueryParameter('event_id', 'STRING', event_id)],
    )

    return event_id


def update_event_file_metadata(event_id, file_name, file_path):
    """Update file metadata for an event after moving the imported file."""
    _run_bigquery_sql(
        f"""
        UPDATE {_bq_table('events')}
        SET file_name = @file_name,
            file_path = @file_path,
            update_time = CURRENT_TIMESTAMP()
        WHERE event_id = @event_id
        """,
        [
            bigquery.ScalarQueryParameter('file_name', 'STRING', file_name),
            bigquery.ScalarQueryParameter('file_path', 'STRING', file_path),
            bigquery.ScalarQueryParameter('event_id', 'STRING', str(event_id)),
        ],
    )


def execute_sql_script(script_path):
    """Read and execute a SQL script file."""
    with open(script_path, 'r', encoding='utf-8') as sql_file:
        script = _substitute_dataset(sql_file.read())
    _run_bigquery_sql(script)


def fetch_league_urls(league_id):
    """Fetch all URLs for a given league."""
    rows = _run_bigquery_sql(
        f"SELECT league_urls FROM {_bq_table('leagues')} WHERE league_id = @league_id LIMIT 1",
        [bigquery.ScalarQueryParameter('league_id', 'STRING', str(league_id))],
    )
    if not rows:
        return []
    return parse_league_urls(rows[0]['league_urls'])


def fetch_league_by_id(league_id):
    """Fetch a league's details by its ID."""
    rows = _run_bigquery_sql(
        f"""
        SELECT
            league_name AS name,
            league_urls,
            league_is_handicap AS is_handicap,
            league_cash_percentage AS cash_percentage,
            league_entry_fee AS entry_fee,
            handicap_minimum_rounds,
            handicap_rounds_considered,
            handicap_years_lookback,
            handicap_base_score,
            handicap_multiplier
        FROM {_bq_table('leagues')}
        WHERE league_id = @league_id
        LIMIT 1
        """,
        [bigquery.ScalarQueryParameter('league_id', 'STRING', str(league_id))],
    )
    if not rows:
        return None

    row = rows[0]
    urls = parse_league_urls(row['league_urls'])
    return {
        'name': row['name'],
        'is_handicap': row['is_handicap'],
        'url': urls[0] if urls else '',
        'urls': urls,
        'cash_percentage': row['cash_percentage'],
        'entry_fee': row['entry_fee'],
        'handicap_minimum_rounds': row['handicap_minimum_rounds'],
        'handicap_rounds_considered': row['handicap_rounds_considered'],
        'handicap_years_lookback': row['handicap_years_lookback'],
        'handicap_base_score': row['handicap_base_score'],
        'handicap_multiplier': row['handicap_multiplier'],
    }


def create_payout_table():
    """Drop and recreate the payouts table."""
    payout_rows = []
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
                    'weight': round(weight, 9),
                    'percentage': round(payout_fraction, 9),
                    'payout_percent': round(payout_fraction * 100, 4),
                }
            )

    payouts_df = pd.DataFrame(payout_rows)

    _run_bigquery_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_bq_table('payouts')} (
            n_players INT64 NOT NULL,
            position INT64 NOT NULL,
            weight NUMERIC,
            percentage NUMERIC,
            payout_percent NUMERIC,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    _run_bigquery_sql(f"DELETE FROM {_bq_table('payouts')} WHERE TRUE")

    rows = payouts_df[['n_players', 'position', 'weight', 'percentage', 'payout_percent']].to_dict(orient='records')
    bq_client = _get_bigquery_client()
    job = bq_client.load_table_from_json(rows, _bq_table_id('payouts'))
    job.result()
    if job.errors:
        raise Exception(f"Failed to populate payouts table: {job.errors}")


def payouts_table_exists():
    """Return True when the payouts table already exists in the configured dataset."""
    dataset_ref = _bq_dataset_ref()

    sql =  f"""
        SELECT COUNT(1) AS table_count
        FROM `{dataset_ref}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = 'payouts'
        """
    rows = _run_bigquery_sql(sql)
    
    return bool(rows and rows[0]['table_count'] > 0)


def apply_event_updates():
    """Reset event update fields, then apply per-event overrides from the event_updates table."""
    # Reset defaults
    _run_bigquery_sql(
        f"""
        UPDATE {_bq_table('events')} 
        SET is_excluded_from_handicap = FALSE, 
            is_no_handicap_applied = FALSE, 
            is_excluded_from_points = FALSE, 
            points_multiplier = NULL, 
            buy_in_override_ammt = NULL 
        WHERE TRUE
        """
    )

    # Single UPDATE statement from the event_updates table managed by the admin app
    sql = f"""
        UPDATE {_bq_table('events')} t
        SET 
            is_excluded_from_handicap = COALESCE(s.is_excluded_from_handicap, t.is_excluded_from_handicap),
            is_no_handicap_applied = COALESCE(s.is_no_handicap_applied, t.is_no_handicap_applied),
            is_excluded_from_points = COALESCE(s.is_excluded_from_points, t.is_excluded_from_points),
            points_multiplier = COALESCE(s.points_multiplier, t.points_multiplier),
            buy_in_override_ammt = COALESCE(s.buy_in_override_ammt, t.buy_in_override_ammt),
            update_time = CURRENT_TIMESTAMP()
        FROM {_bq_table('event_updates')} s
        WHERE t.export_url = s.export_url
    """
    _run_bigquery_sql(sql)
    logger.info("Applied event updates from database overrides.")


def _bq_table_exists(table_name):
    """Checks if a BigQuery table exists."""
    dataset_ref = _bq_dataset_ref()
    sql = f"""
        SELECT 1
        FROM `{dataset_ref}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = @table_name
    """
    rows = _run_bigquery_sql(sql, [bigquery.ScalarQueryParameter('table_name', 'STRING', table_name)])
    return bool(rows)


def _get_bq_table_ddl(table_name):
    """Fetches the DDL (CREATE TABLE statement) for a BigQuery table."""
    dataset_ref = _bq_dataset_ref()
    sql = f"""
        SELECT ddl
        FROM `{dataset_ref}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = @table_name
    """
    rows = _run_bigquery_sql(sql, [bigquery.ScalarQueryParameter('table_name', 'STRING', table_name)])
    return rows[0]['ddl'] if rows else None

def normalize_table_definition(ddl):
    """Extracts the schema block from DDL and simplifies it for comparison."""
    if not ddl:
        return None

    start = ddl.find('(')
    if start == -1: return None

    # Find matching closing paren for the column block to ignore OPTIONS (...)
    depth, end = 0, -1
    for i in range(start, len(ddl)):
        if ddl[i] == '(': depth += 1
        elif ddl[i] == ')': depth -= 1
        if depth == 0:
            end = i
            break

    if end == -1: return None

    # Extract content, lowercase, remove noise, and collapse whitespace
    content = ddl[start+1:end].lower()
    content = content.replace("default generate_uuid()", "")
    return re.sub(r"\s+", " ", content).strip()

def sync_permanent_table_schemas():
    """
    Compares the schema of permanent tables with their _template counterparts.
    If a permanent table's schema differs from its _template, the permanent table is dropped
    and recreated with the _template's schema.
    """
    dataset_ref = _bq_dataset_ref()
    sql = f"""
        SELECT table_name
        FROM `{dataset_ref}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '%_template'
    """
    template_table_rows = _run_bigquery_sql(sql)

    if not template_table_rows:
        logger.info("No _template tables found for schema synchronization.")
        return

    for row in template_table_rows:
        template_table_name = row['table_name']
        main_table_name = template_table_name.replace('_template', '')
        logger.info(f"Checking schema for main table '{main_table_name}' against template '{template_table_name}'...")

        template_ddl = _get_bq_table_ddl(template_table_name)
        main_table_exists = _bq_table_exists(main_table_name)
        
        if main_table_exists:
            compared_template_ddl = normalize_table_definition(template_ddl)
            compared_main_ddl = normalize_table_definition(_get_bq_table_ddl(main_table_name))
            
            if compared_template_ddl != compared_main_ddl:
                logger.warning(f"Schema mismatch detected for '{main_table_name}'. Template DDL: {compared_template_ddl}, Main DDL: {compared_main_ddl}. Dropping and recreating '{main_table_name}'.")
                create_statement = template_ddl.replace(f"`{_bq_dataset_ref()}.{template_table_name}`", f"`{_bq_dataset_ref()}.{main_table_name}`")
                _run_bigquery_sql(f"DROP TABLE IF EXISTS {_bq_table(main_table_name)}")
                _run_bigquery_sql(create_statement)
                
            else:
                logger.info(f"Schema for '{main_table_name}' matches template. No action needed.")
                continue

        else:
            logger.info(f"Main table '{main_table_name}' does not exist. Creating from template.")
            create_statement = template_ddl.replace(f"`{_bq_dataset_ref()}.{template_table_name}`", f"`{_bq_dataset_ref()}.{main_table_name}`")
            _run_bigquery_sql(create_statement)



def drop_template_tables():
    """Drops all tables in the configured dataset that end with '_template'."""
    dataset_ref = _bq_dataset_ref()
    sql = f"""
        SELECT table_name
        FROM `{dataset_ref}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '%_template'
    """
    rows = _run_bigquery_sql(sql)

    if rows:
        logger.info(f"Found {len(rows)} template tables to drop.")
        for row in rows:
            table_to_drop = row['table_name']
            _run_bigquery_sql(f"DROP TABLE IF EXISTS {_bq_table(table_to_drop)}")
            logger.info(f"Dropped table: {table_to_drop}")
    else:
        logger.info("No template tables found to drop.")
        
def prune_abandoned_scores():
    """Delete raw and hole scores for events or raw scores that do not exist."""
    raw_result = _run_bigquery_sql(f"""
        DELETE FROM {_bq_table('raw_scores')} rs
        WHERE NOT EXISTS (
            SELECT 1 FROM {_bq_table('events')} e
            WHERE e.event_id = rs.event_id
        )
    """)
    
    hole_result = _run_bigquery_sql(f"""
        DELETE FROM {_bq_table('hole_scores')} hs
        WHERE NOT EXISTS (
            SELECT 1 FROM {_bq_table('raw_scores')} rs
            WHERE rs.raw_score_id = hs.raw_score_id
        )
    """)

    raw_deleted = raw_result[0]['num_deleted_rows'] if raw_result else 0
    hole_deleted = hole_result[0]['num_deleted_rows'] if hole_result else 0

    if raw_deleted or hole_deleted:
        logger.warning(f"Pruned abandoned scores: {raw_deleted} raw_scores, {hole_deleted} hole_scores")
    else:
        logger.info("No abandoned scores found.")