"""
Utility functions for the league scores application.

Includes URL parsing/formatting for league configuration storage.
"""


def parse_league_urls(urls_text):
    """Parse league URLs stored as pipe-delimited text."""
    if not urls_text:
        return []
    return [url.strip() for url in urls_text.split('|') if url.strip()]


def format_league_urls(league_urls):
    """Format league URLs as a pipe-delimited string for storage."""
    if league_urls is None:
        return None
    if isinstance(league_urls, str):
        return league_urls.strip()
    return '|'.join(url.strip() for url in league_urls if url and url.strip())