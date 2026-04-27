"""
Logging configuration for the league scores application.
"""
import logging


def setup_logging(level=logging.INFO):
    """Configure logging to stdout."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        handlers=[logging.StreamHandler()],
        force=True,
    )
    return logging.getLogger('league_scores')
