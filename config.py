import os
from pathlib import Path

CONFIG_FILE_PATH = Path(__file__).parent / 'config' / 'db_config.txt'
REQUIRED_DB_KEYS = ('DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD')


def load_db_config(config_path=None):
    """Load PostgreSQL connection settings from environment and/or config file.

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

    for key in REQUIRED_DB_KEYS:
        env_value = os.getenv(key)
        if env_value is not None and env_value.strip() != '':
            config[key] = env_value.strip()

    missing = [key for key in REQUIRED_DB_KEYS if key not in config or str(config[key]).strip() == '']
    if missing:
        if not path.exists():
            raise FileNotFoundError(
                f"Database config file not found: {path}. Missing required values: {', '.join(missing)}"
            )
        raise ValueError(f"Missing required database config values: {', '.join(missing)}")

    return config


def get_db_connection_string(config_path=None):
    """Build a PostgreSQL connection string from config values."""
    config = load_db_config(config_path)
    return (
        f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@"
        f"{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
    )
