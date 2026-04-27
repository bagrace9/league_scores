"""League bootstrap utilities.

Loads optional league configs from disk and seeds the leagues table only when
no leagues currently exist in the database.
"""
import json
import logging

import database
from config import get_leagues_bootstrap_config_path

logger = logging.getLogger(__name__)
DEFAULTS = {
    'league_cash_percentage': 0,
    'league_entry_fee': 0,
    'league_is_handicap': False,
    'handicap_minimum_rounds': 0,
    'handicap_rounds_considered': 0,
    'handicap_years_lookback': 0,
    'handicap_base_score': 0,
    'handicap_multiplier': 0,
}


def _normalize_league_entry(row):
    league_name = str(row.get('league_name') or row.get('name') or '').strip()
    urls = row.get('league_urls') or row.get('urls') or []
    if isinstance(urls, str):
        urls = [part.strip() for part in urls.split('|') if part.strip()]
    if not isinstance(urls, list):
        urls = []
    urls = [str(url).strip() for url in urls if str(url).strip()]

    if not league_name or not urls:
        return None

    league = {'league_name': league_name, 'league_urls': urls}
    for key, default in DEFAULTS.items():
        league[key] = row.get(key, default)
    return league


def load_league_bootstrap_configs(config_path=None):
    config_path = config_path or get_leagues_bootstrap_config_path()
    if not config_path.exists():
        return []

    try:
        with config_path.open('r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as error:
        logger.error(f"Could not parse league bootstrap config {config_path}: {error}")
        return []

    if isinstance(payload, dict):
        payload = payload.get('leagues', [])

    if not isinstance(payload, list):
        logger.error(f"League bootstrap config must be a JSON list (or object with 'leagues'): {config_path}")
        return []

    normalized = []
    for index, row in enumerate(payload, start=1):
        if not isinstance(row, dict):
            logger.warning(f"Skipping league config entry #{index}: expected object")
            continue

        league = _normalize_league_entry(row)
        if league is None:
            logger.warning(f"Skipping league config entry #{index}: missing league_name or league_urls")
            continue

        normalized.append(league)

    return normalized


def bootstrap_leagues_if_empty(config_path=None):
    config_path = config_path or get_leagues_bootstrap_config_path()
    league_rows = database.fetch_leagues()
    if league_rows:
        return league_rows

    league_configs = load_league_bootstrap_configs(config_path=config_path)
    if not league_configs:
        logger.warning(
            f"No leagues in database and no bootstrap config found at {config_path}."
        )
        return []

    created_count = 0
    for league in league_configs:
        try:
            database.create_league(
                league['league_name'],
                league['league_urls'],
                league['league_cash_percentage'],
                league['league_entry_fee'],
                league['league_is_handicap'],
                league['handicap_minimum_rounds'],
                league['handicap_rounds_considered'],
                league['handicap_years_lookback'],
                league['handicap_base_score'],
                league['handicap_multiplier'],
            )
            created_count += 1
        except Exception as error:
            logger.error(f"Failed creating bootstrap league '{league['league_name']}': {error}")

    if created_count:
        logger.info(
            f"Created {created_count} league(s) from bootstrap config: {config_path}"
        )

    return database.fetch_leagues()
