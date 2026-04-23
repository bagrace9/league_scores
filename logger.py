"""
Logging configuration for the league scores application.

Sets up a timestamped file handler alongside a console handler so every
run produces a dated log file in the logs/ directory.
"""
import logging
from datetime import datetime
from pathlib import Path

from google.cloud import storage

LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging(level=logging.INFO):
    """Configure logging to a timestamped file and the console."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOG_DIR / f'league_scores_{timestamp}.log'

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True,
    )

    logger = logging.getLogger('league_scores')
    logger.log_file_path = str(log_file)
    logger.info(f'Log file created: {log_file}')
    return logger


def upload_log_to_gcs(logger, bucket_name, prefix=''):
    """Upload the active run log file to GCS and return its gs:// URI."""
    log_file_path = getattr(logger, 'log_file_path', None)
    if not log_file_path:
        raise ValueError('Logger has no log_file_path configured.')

    log_path = Path(log_file_path)
    if not log_path.exists():
        raise FileNotFoundError(f'Log file not found: {log_file_path}')

    # Flush open handlers so the uploaded file includes the latest log lines.
    for handler in logging.getLogger().handlers:
        try:
            handler.flush()
        except Exception:
            pass

    clean_prefix = prefix.strip('/') if prefix else ''
    blob_parts = [part for part in [clean_prefix, 'logs', log_path.name] if part]
    blob_name = '/'.join(blob_parts)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_filename(str(log_path))

    return f'gs://{bucket_name}/{blob_name}'
