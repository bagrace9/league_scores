import os
from pathlib import Path

CONFIG_FILE_PATH = Path(__file__).parent / 'config' / 'db_config.txt'
REQUIRED_BIGQUERY_KEYS = ('GCP_PROJECT_ID', 'BIGQUERY_DATASET')
OPTIONAL_ENV_KEYS = (
    'BIGQUERY_LOCATION',
    'GOOGLE_APPLICATION_CREDENTIALS',
    'GCS_BUCKET',
    'ARCHIVE_IMPORTED_FILES',
    'EVENT_UPDATES_PATH',
    'LEAGUES_BOOTSTRAP_PATH',
)


def _parse_config_text(text):
    """Parse key=value lines from config file text."""
    config = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        config[key.strip()] = value.strip()
    return config


def _load_config_from_gcs(gcs_uri):
    """Download and parse a config txt file from GCS."""
    from google.cloud import storage as gcs_module
    client = gcs_module.Client()
    bucket_name, blob_path = gcs_uri[len('gs://'):].split('/', 1)
    blob = client.bucket(bucket_name).blob(blob_path)
    return _parse_config_text(blob.download_as_text(encoding='utf-8'))


def _to_bool(value, default=False):
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in ('1', 'true', 'yes', 'y', 'on'):
        return True
    if normalized in ('0', 'false', 'no', 'n', 'off'):
        return False
    return default


_config_cache: dict | None = None


def load_db_config(config_path=None):
    """Load settings from a config file (local or GCS) plus optional env var overrides.

    Result is cached after the first call. Pass config_path only on the first call
    (or to intentionally reload from a different source).

    Resolution order (highest priority last, so env vars win):
    1. GCS config file — if DB_CONFIG_GCS_URI env var is set, or config_path starts with gs://
    2. Local config file — config_path or the default config/db_config.txt
    3. Individual environment variables
    """
    global _config_cache
    if _config_cache is not None and config_path is None:
        return _config_cache
    config = {}

    gcs_uri = os.getenv('DB_CONFIG_GCS_URI', '').strip()
    if config_path and str(config_path).startswith('gs://'):
        gcs_uri = str(config_path)

    if gcs_uri:
        config = _load_config_from_gcs(gcs_uri)
    else:
        path = Path(config_path) if config_path else CONFIG_FILE_PATH
        if path.exists():
            with path.open('r', encoding='utf-8') as f:
                config = _parse_config_text(f.read())

    # Individual env vars override file values (useful for local dev overrides)
    for key in (*REQUIRED_BIGQUERY_KEYS, *OPTIONAL_ENV_KEYS):
        env_value = os.getenv(key)
        if env_value is not None and env_value.strip() != '':
            config[key] = env_value.strip()

    missing = [key for key in REQUIRED_BIGQUERY_KEYS if not config.get(key, '').strip()]
    if missing:
        source = gcs_uri or (str(config_path) if config_path else str(CONFIG_FILE_PATH))
        raise ValueError(
            f"Missing required config values: {', '.join(missing)} (source: {source})"
        )

    if config_path is None:
        _config_cache = config
    return config


def get_bigquery_config(config_path=None):
    """Return validated BigQuery configuration values."""
    config = load_db_config(config_path)

    return {
        'project_id': config['GCP_PROJECT_ID'],
        'dataset': config['BIGQUERY_DATASET'],
        'location': config.get('BIGQUERY_LOCATION', 'US'),
        'credentials_path': config.get('GOOGLE_APPLICATION_CREDENTIALS'),
    }


def get_storage_config(config_path=None):
    """Return optional archive storage configuration values."""
    config = load_db_config(config_path)
    bucket = config.get('GCS_BUCKET')
    event_updates_path = (config.get('EVENT_UPDATES_PATH') or 'config/event_updates.csv').strip('/')
    event_updates_gcs_uri = f"gs://{bucket}/{event_updates_path}" if bucket and event_updates_path else ''

    return {
        'bucket': bucket,
        'archive_files': _to_bool(config.get('ARCHIVE_IMPORTED_FILES'), default=True),
        'event_updates_gcs_uri': event_updates_gcs_uri,
    }


def get_leagues_bootstrap_config_path(config_path=None):
    """Return the location of the optional leagues bootstrap JSON file."""
    config = load_db_config(config_path)
    path_value = (config.get('LEAGUES_BOOTSTRAP_PATH') or 'config/league_configs.json').strip()
    if path_value.startswith('gs://'):
        return path_value
    bucket = config.get('GCS_BUCKET')
    if bucket:
        return f"gs://{bucket}/{path_value.strip('/')}"
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path(__file__).parent / path
