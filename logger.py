"""
Logging configuration for the league scores application.

Sets up a timestamped file handler alongside a console handler so every
run produces a dated log file in the logs/ directory.
"""
import logging
from datetime import datetime
from pathlib import Path

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
    logger.info(f'Log file created: {log_file}')
    return logger
