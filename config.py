import os
from pathlib import Path

CONFIG_FILE_PATH = Path(__file__).parent / 'config' / 'db_config.txt'
REQUIRED_BIGQUERY_KEYS = ('GCP_PROJECT_ID', 'BIGQUERY_DATASET')
OPTIONAL_ENV_KEYS = (
    'BIGQUERY_LOCATION',
    'GOOGLE_APPLICATION_CREDENTIALS',
    'GCS_BUCKET',
    'GCS_PREFIX',
    'ARCHIVE_IMPORTED_FILES',
    'UPLOAD_LOG_TO_GCS',
    'LOG_GCS_PREFIX',
)


def _to_bool(value, default=True):
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in ('1', 'true', 'yes', 'y', 'on'):
        return True
    if normalized in ('0', 'false', 'no', 'n', 'off'):
        return False
    return default


def load_db_config(config_path=None):
    """Load BigQuery settings from environment and/or config file.

    Environment variables take precedence over file values.
    """
    path = Path(config_path) if config_path else CONFIG_FILE_PATH
    config = {}

    if path.exists():
        with path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' not in line:
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()

    for key in REQUIRED_BIGQUERY_KEYS:
        env_value = os.getenv(key)
        if env_value is not None and env_value.strip() != '':
            config[key] = env_value.strip()

    for key in OPTIONAL_ENV_KEYS:
        env_value = os.getenv(key)
        if env_value is not None and env_value.strip() != '':
            config[key] = env_value.strip()

    missing = [key for key in REQUIRED_BIGQUERY_KEYS if key not in config or str(config[key]).strip() == '']
    if missing:
        if not path.exists():
            raise FileNotFoundError(
                f"Database config file not found: {path}. Missing required values: {', '.join(missing)}"
            )
        raise ValueError(f"Missing required database config values: {', '.join(missing)}")

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

    return {
        'bucket': config.get('GCS_BUCKET'),
        'prefix': config.get('GCS_PREFIX', ''),
        'archive_files': _to_bool(config.get('ARCHIVE_IMPORTED_FILES'), default=True),
        'upload_log_to_gcs': _to_bool(config.get('UPLOAD_LOG_TO_GCS'), default=False),
        'log_prefix': config.get('LOG_GCS_PREFIX', ''),
    }
