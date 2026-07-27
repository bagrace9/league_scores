"""
UDisc web scraping utilities for the league scores pipeline.

Handles league schedule pagination, event leaderboard link discovery,
and leaderboard export file downloads.
"""
import logging
import os
import re
from datetime import date, datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
EXPORTS_DIR = os.path.join(os.path.dirname(__file__), 'exports')


def fetch_page_content(url):
    """Fetch and parse the HTML content of a URL.

    Raises requests.HTTPError if the server returns a non-2xx status.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')


def _extract_year(value):
    """Extract a 4-digit year from a string, if present."""
    if not value:
        return None
    match = re.search(r"(19|20)\d{2}", str(value))
    return int(match.group(0)) if match else None


def _get_year_context(anchor):
    """Inspect nearby text for a candidate event link to find a year."""
    if anchor is None:
        return None

    candidates = []
    for node in [anchor, anchor.parent, anchor.find_parent()]:
        if node is None:
            continue
        text = node.get_text(' ', strip=True)
        if text:
            candidates.append(text)

    for text in candidates:
        year = _extract_year(text)
        if year is not None:
            return year

    return None


def get_event_links(url):
    """Return all event leaderboard URLs from a league's schedule pages.

    Paginates through the schedule until a page adds no new links. Only events
    from the previous calendar year onward are included to limit scope and avoid
    reprocessing old data.
    """
    links = []
    seen = set()
    last_len = -1
    page = 1
    lookback_year = datetime.now().year - 1
    while len(links) > last_len:
        page_url = f"{url}/schedule?page={page}"
        last_len = len(links)
        soup = fetch_page_content(page_url)
        if not soup:
            break

        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            if not link.startswith('/events') or '/leaderboard' not in link:
                continue

            year = _get_year_context(a_tag)
            if year is None:
                continue

            if year >= lookback_year:
                full_url = 'https://udisc.com' + link
                if full_url not in seen:
                    seen.add(full_url)
                    links.append(full_url)

        page += 1
    return links

def find_download_links_on_page(page_url):
    """Find event leaderboard download links and end date on a UDisc event page.

    Returns a tuple of (links, event_end_date) where event_end_date is a date
    parsed from the page's <time datetime> elements, or None if not found.
    For multi-day events the last <time> element is the end date; for single-day
    events the only <time> element serves as both start and end.
    """
    links = []
    seen = set()
    soup = fetch_page_content(page_url)
    if not soup:
        return [], None

    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        if '/events' in link and '/leaderboard' in link:
            event_url = urljoin(page_url, link)
            if event_url in seen:
                continue
            seen.add(event_url)
            links.append(event_url)

    event_end_date = None
    time_elements = soup.find_all('time', datetime=True)
    if time_elements:
        try:
            event_end_date = date.fromisoformat(time_elements[-1]['datetime'])
        except (ValueError, KeyError):
            pass

    return links, event_end_date


def download_event_data(export_url, download_dir=None):
    """Download Excel file from the event's leaderboard export URL."""
    try:
        # Download the Excel file
        response = requests.get(export_url, timeout=60)
        response.raise_for_status()

        target_dir = download_dir or EXPORTS_DIR

        # Create export directory if it doesn't exist
        os.makedirs(target_dir, exist_ok=True)
        
        # Get default filename from response header or use fallback
        content_disposition = response.headers.get('Content-Disposition', '')
        if 'filename=' in content_disposition:
            default_filename = content_disposition.split('filename=')[1].strip('"')
            base_name = default_filename.rsplit('.', 1)[0]  # Remove extension
        else:
            base_name = 'udisc_export'
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.xlsx"
        filepath = os.path.join(target_dir, filename)
        
        # Save the file
        with open(filepath, 'wb') as f:
            f.write(response.content)
        logger.info(f"Saved event spreadsheet to {filepath}")
        
        return {
            'export_url': export_url,
            'filename': filename,
            'filepath': filepath,
            'success': True
        }
        
    except requests.RequestException as e:
        return {
            'export_url': export_url,
            'error': str(e),
            'success': False
        }
