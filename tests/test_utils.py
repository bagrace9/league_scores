"""Tests for utils.py — URL parsing and formatting helpers."""
import pytest
from utils import parse_league_urls, format_league_urls


class TestParseLeagueUrls:
    def test_single_url(self):
        assert parse_league_urls("https://udisc.com/leagues/foo") == ["https://udisc.com/leagues/foo"]

    def test_multiple_pipe_delimited_urls(self):
        result = parse_league_urls("https://udisc.com/a|https://udisc.com/b|https://udisc.com/c")
        assert result == ["https://udisc.com/a", "https://udisc.com/b", "https://udisc.com/c"]

    def test_strips_whitespace_around_urls(self):
        result = parse_league_urls("  https://udisc.com/a  |  https://udisc.com/b  ")
        assert result == ["https://udisc.com/a", "https://udisc.com/b"]

    def test_empty_string_returns_empty_list(self):
        assert parse_league_urls("") == []

    def test_none_returns_empty_list(self):
        assert parse_league_urls(None) == []

    def test_pipes_with_no_content_between_them_are_ignored(self):
        result = parse_league_urls("https://udisc.com/a||https://udisc.com/b")
        assert result == ["https://udisc.com/a", "https://udisc.com/b"]

    def test_only_pipes_returns_empty_list(self):
        assert parse_league_urls("|||") == []

    def test_single_url_with_trailing_pipe(self):
        result = parse_league_urls("https://udisc.com/a|")
        assert result == ["https://udisc.com/a"]


class TestFormatLeagueUrls:
    def test_list_of_urls_joined_with_pipe(self):
        result = format_league_urls(["https://udisc.com/a", "https://udisc.com/b"])
        assert result == "https://udisc.com/a|https://udisc.com/b"

    def test_single_url_list(self):
        assert format_league_urls(["https://udisc.com/a"]) == "https://udisc.com/a"

    def test_string_input_returned_stripped(self):
        assert format_league_urls("  https://udisc.com/a  ") == "https://udisc.com/a"

    def test_none_returns_none(self):
        assert format_league_urls(None) is None

    def test_empty_list_returns_empty_string(self):
        assert format_league_urls([]) == ""

    def test_list_with_blank_entries_are_ignored(self):
        result = format_league_urls(["https://udisc.com/a", "", "  ", "https://udisc.com/b"])
        assert result == "https://udisc.com/a|https://udisc.com/b"

    def test_urls_are_stripped_of_whitespace(self):
        result = format_league_urls(["  https://udisc.com/a  ", "  https://udisc.com/b  "])
        assert result == "https://udisc.com/a|https://udisc.com/b"

    def test_roundtrip_parse_then_format(self):
        original = "https://udisc.com/a|https://udisc.com/b"
        assert format_league_urls(parse_league_urls(original)) == original
