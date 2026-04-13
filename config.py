from pathlib import Path

CONFIG_FILE_PATH = Path(__file__).parent / 'config' / 'db_config.txt'


def load_db_config(config_path=None):
    """Load PostgreSQL connection settings from the config file."""
    path = Path(config_path) if config_path else CONFIG_FILE_PATH
    config = {}

    if not path.exists():
        raise FileNotFoundError(f"Database config file not found: {path}")

    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, value = line.split('=', 1)
            config[key.strip()] = value.strip()

    return config


def get_db_connection_string(config_path=None):
    """Build a PostgreSQL connection string from config values."""
    config = load_db_config(config_path)
    return (
        f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@"
        f"{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
    )
