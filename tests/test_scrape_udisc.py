"""Tests for scrape_udisc.py — scraping and download utilities."""
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import requests

from scrape_udisc import (
    fetch_page_content,
    get_event_links,
    find_download_links_on_page,
    download_event_data,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_response(content: bytes = b"", status: int = 200, headers: dict = None, raise_for_status=False):
    """Build a minimal mock requests.Response."""
    resp = MagicMock()
    resp.content = content
    resp.status_code = status
    resp.headers = headers or {}
    if raise_for_status:
        resp.raise_for_status.side_effect = requests.HTTPError("404")
    else:
        resp.raise_for_status.return_value = None
    return resp


def make_soup_html(links=None, time_dates=None):
    """Build a minimal HTML page with anchor tags and optional time elements."""
    link_tags = ""
    for href, year_label in (links or []):
        link_tags += f'<a href="{href}"><span class="ml-2 font-normal text-sm text-subtle">{year_label}</span></a>\n'
    time_tags = "".join(f'<time datetime="{d}"></time>' for d in (time_dates or []))
    return f"<html><body>{link_tags}{time_tags}</body></html>".encode()


# ---------------------------------------------------------------------------
# fetch_page_content
# ---------------------------------------------------------------------------

class TestFetchPageContent:
    def test_returns_beautifulsoup_on_success(self):
        html = b"<html><body><p>Hello</p></body></html>"
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            soup = fetch_page_content("https://udisc.com/leagues/foo/schedule")
        assert soup.find("p").text == "Hello"

    def test_raises_on_http_error(self):
        with patch("scrape_udisc.requests.get", return_value=make_response(raise_for_status=True)):
            with pytest.raises(requests.HTTPError):
                fetch_page_content("https://udisc.com/bad-url")


# ---------------------------------------------------------------------------
# get_event_links
# ---------------------------------------------------------------------------

class TestGetEventLinks:
    def _mock_get(self, pages: list[bytes]):
        """Return a side_effect list that serves each page once, then an empty body."""
        responses = [make_response(p) for p in pages]
        # Append an empty page so pagination terminates
        responses.append(make_response(b"<html></html>"))
        return responses

    def test_collects_valid_event_links(self):
        current_year = 2026
        page1 = make_soup_html([
            ("/events/abc/leaderboard", str(current_year)),
            ("/events/def/leaderboard", str(current_year - 1)),
        ])
        with patch("scrape_udisc.requests.get", side_effect=self._mock_get([page1])):
            links = get_event_links("https://udisc.com/leagues/foo")

        assert any("events/abc/leaderboard" in l for l in links)
        assert any("events/def/leaderboard" in l for l in links)

    def test_skips_links_without_year_span(self):
        html = b'<html><body><a href="/events/abc/leaderboard">No year span here</a></body></html>'
        with patch("scrape_udisc.requests.get", side_effect=self._mock_get([html])):
            links = get_event_links("https://udisc.com/leagues/foo")
        assert links == []

    def test_skips_links_older_than_lookback_year(self):
        old_year = 2010
        page = make_soup_html([("/events/old/leaderboard", str(old_year))])
        with patch("scrape_udisc.requests.get", side_effect=self._mock_get([page])):
            links = get_event_links("https://udisc.com/leagues/foo")
        assert links == []

    def test_deduplicates_links(self):
        current_year = 2026
        page = make_soup_html([
            ("/events/abc/leaderboard", str(current_year)),
            ("/events/abc/leaderboard", str(current_year)),  # duplicate
        ])
        with patch("scrape_udisc.requests.get", side_effect=self._mock_get([page])):
            links = get_event_links("https://udisc.com/leagues/foo")
        assert links.count("https://udisc.com/events/abc/leaderboard") == 1

    def test_skips_non_event_links(self):
        current_year = 2026
        page = make_soup_html([("/blog/post", str(current_year))])
        with patch("scrape_udisc.requests.get", side_effect=self._mock_get([page])):
            links = get_event_links("https://udisc.com/leagues/foo")
        assert links == []


# ---------------------------------------------------------------------------
# find_download_links_on_page
# ---------------------------------------------------------------------------

class TestFindDownloadLinksOnPage:
    def test_finds_leaderboard_links(self):
        html = b"""
        <html><body>
          <a href="/events/abc/leaderboard/export">Export</a>
          <a href="/events/abc/leaderboard">View</a>
        </body></html>
        """
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            links, _ = find_download_links_on_page("https://udisc.com/events/abc")
        assert len(links) == 2

    def test_parses_event_end_date_from_last_time_element(self):
        html = b"""
        <html><body>
          <time datetime="2024-06-10"></time>
          <time datetime="2024-06-15"></time>
        </body></html>
        """
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            _, end_date = find_download_links_on_page("https://udisc.com/events/abc")
        assert end_date == date(2024, 6, 15)

    def test_end_date_is_none_when_no_time_elements(self):
        html = b"<html><body></body></html>"
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            _, end_date = find_download_links_on_page("https://udisc.com/events/abc")
        assert end_date is None

    def test_deduplicates_leaderboard_links(self):
        html = b"""
        <html><body>
          <a href="/events/abc/leaderboard/export">Export</a>
          <a href="/events/abc/leaderboard/export">Export again</a>
        </body></html>
        """
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            links, _ = find_download_links_on_page("https://udisc.com/events/abc")
        assert len(links) == 1

    def test_returns_empty_list_when_no_links(self):
        html = b"<html><body><a href='/unrelated'>Foo</a></body></html>"
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            links, _ = find_download_links_on_page("https://udisc.com/events/abc")
        assert links == []

    def test_invalid_datetime_attribute_yields_none_date(self):
        html = b'<html><body><time datetime="not-a-date"></time></body></html>'
        with patch("scrape_udisc.requests.get", return_value=make_response(html)):
            _, end_date = find_download_links_on_page("https://udisc.com/events/abc")
        assert end_date is None


# ---------------------------------------------------------------------------
# download_event_data
# ---------------------------------------------------------------------------

class TestDownloadEventData:
    def test_successful_download_with_content_disposition(self, tmp_path):
        export_url = "https://udisc.com/events/abc/leaderboard/export"
        headers = {"Content-Disposition": 'attachment; filename="MyEvent-2024-06-15.xlsx"'}
        resp = make_response(content=b"xlsx-bytes", headers=headers)

        with patch("scrape_udisc.requests.get", return_value=resp):
            result = download_event_data(export_url, download_dir=str(tmp_path))

        assert result["success"] is True
        assert result["export_url"] == export_url
        assert result["filename"].endswith(".xlsx")
        assert Path(result["filepath"]).exists()

    def test_successful_download_fallback_filename(self, tmp_path):
        export_url = "https://udisc.com/events/abc/leaderboard/export"
        resp = make_response(content=b"xlsx-bytes", headers={})

        with patch("scrape_udisc.requests.get", return_value=resp):
            result = download_event_data(export_url, download_dir=str(tmp_path))

        assert result["success"] is True
        assert "udisc_export" in result["filename"]

    def test_network_error_returns_failure_dict(self):
        export_url = "https://udisc.com/events/abc/leaderboard/export"
        with patch("scrape_udisc.requests.get", side_effect=requests.RequestException("timeout")):
            result = download_event_data(export_url)

        assert result["success"] is False
        assert result["export_url"] == export_url
        assert "timeout" in result["error"]

    def test_filename_includes_timestamp(self, tmp_path):
        export_url = "https://udisc.com/events/abc/leaderboard/export"
        headers = {"Content-Disposition": 'attachment; filename="Event.xlsx"'}
        resp = make_response(content=b"x", headers=headers)

        with patch("scrape_udisc.requests.get", return_value=resp):
            result = download_event_data(export_url, download_dir=str(tmp_path))

        # Timestamp pattern: YYYYMMDD_HHMMSS
        import re
        assert re.search(r"\d{8}_\d{6}", result["filename"])
