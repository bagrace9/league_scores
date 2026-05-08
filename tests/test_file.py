"""Tests for file.py — File domain object."""
import shutil
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from file import File


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_file(**kwargs) -> File:
    defaults = dict(
        export_url="https://udisc.com/events/abc/leaderboard/export",
        filename="My-League-2024-06-15_20240615_120000.xlsx",
        filepath="/tmp/My-League-2024-06-15_20240615_120000.xlsx",
        success=True,
        error=None,
        event_end_date=date(2024, 6, 15),
    )
    defaults.update(kwargs)
    return File(**defaults)


# ---------------------------------------------------------------------------
# _parse_download_date_from_filename
# ---------------------------------------------------------------------------

class TestParseDownloadDateFromFilename:
    def test_parses_standard_timestamp_suffix(self):
        dt = File._parse_download_date_from_filename("My-Event-2024-06-15_20240615_143022.xlsx")
        assert dt == datetime(2024, 6, 15, 14, 30, 22)

    def test_parses_with_8digit_date_prefix(self):
        dt = File._parse_download_date_from_filename("My-Event-20240615_20240615_090000.xlsx")
        assert dt == datetime(2024, 6, 15, 9, 0, 0)

    def test_returns_none_when_no_timestamp(self):
        assert File._parse_download_date_from_filename("plainfile.xlsx") is None

    def test_returns_none_for_malformed_timestamp(self):
        assert File._parse_download_date_from_filename("event_99999999_999999.xlsx") is None

    def test_ignores_extension_when_parsing(self):
        # The regex only looks at the stem so extension doesn't matter
        dt = File._parse_download_date_from_filename("event-2024-01-01_20240101_083045.csv")
        assert dt == datetime(2024, 1, 1, 8, 30, 45)

    def test_no_match_returns_none(self):
        assert File._parse_download_date_from_filename("") is None


# ---------------------------------------------------------------------------
# from_download_result
# ---------------------------------------------------------------------------

class TestFromDownloadResult:
    def test_creates_file_from_full_result(self):
        result = {
            "export_url": "https://udisc.com/export",
            "filename": "event_20240101_083045.xlsx",
            "filepath": "/tmp/event_20240101_083045.xlsx",
            "success": True,
            "error": None,
            "event_end_date": date(2024, 1, 1),
        }
        f = File.from_download_result(result)
        assert f.export_url == "https://udisc.com/export"
        assert f.success is True
        assert f.event_end_date == date(2024, 1, 1)

    def test_defaults_success_to_false_when_missing(self):
        f = File.from_download_result({"export_url": "x", "filename": "", "filepath": ""})
        assert f.success is False

    def test_missing_keys_use_defaults(self):
        f = File.from_download_result({})
        assert f.export_url is None
        assert f.filename == ""
        assert f.filepath == ""


# ---------------------------------------------------------------------------
# exists / get_file_size
# ---------------------------------------------------------------------------

class TestExistsAndFileSize:
    def test_exists_true_when_file_on_disk(self, tmp_path):
        p = tmp_path / "test.xlsx"
        p.write_bytes(b"hello")
        f = make_file(filepath=str(p))
        assert f.exists() is True

    def test_exists_false_when_file_missing(self):
        f = make_file(filepath="/nonexistent/path/file.xlsx")
        assert f.exists() is False

    def test_get_file_size_returns_bytes(self, tmp_path):
        p = tmp_path / "test.xlsx"
        p.write_bytes(b"hello world")
        f = make_file(filepath=str(p))
        assert f.get_file_size() == 11

    def test_get_file_size_returns_none_for_missing_file(self):
        f = make_file(filepath="/no/such/file.xlsx")
        assert f.get_file_size() is None


# ---------------------------------------------------------------------------
# delete_from_disk
# ---------------------------------------------------------------------------

class TestDeleteFromDisk:
    def test_deletes_existing_file(self, tmp_path):
        p = tmp_path / "to_delete.xlsx"
        p.write_bytes(b"data")
        f = make_file(filepath=str(p))
        result = f.delete_from_disk()
        assert result is True
        assert not p.exists()

    def test_returns_true_when_file_already_gone(self):
        f = make_file(filepath="/nonexistent/file.xlsx")
        assert f.delete_from_disk() is True

    def test_returns_false_on_os_error(self, tmp_path):
        p = tmp_path / "file.xlsx"
        p.write_bytes(b"x")
        f = make_file(filepath=str(p))
        with patch("pathlib.Path.unlink", side_effect=OSError("perm denied")):
            assert f.delete_from_disk() is False


# ---------------------------------------------------------------------------
# move_to_directory
# ---------------------------------------------------------------------------

class TestMoveToDirectory:
    def test_moves_file_and_updates_metadata(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        dst = tmp_path / "dst"
        file_path = src / "event_20240101_083045.xlsx"
        file_path.write_bytes(b"data")
        f = make_file(filepath=str(file_path), filename=file_path.name)

        result = f.move_to_directory(str(dst))

        assert result is True
        assert Path(f.filepath).exists()
        assert str(dst) in f.filepath

    def test_returns_false_when_source_missing(self, tmp_path):
        f = make_file(filepath="/no/such/file.xlsx")
        result = f.move_to_directory(str(tmp_path))
        assert result is False

    def test_renames_on_collision(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        dst = tmp_path / "dst"
        dst.mkdir()
        # Pre-place a file at the expected destination
        (dst / "event_20240101_083045.xlsx").write_bytes(b"existing")

        file_path = src / "event_20240101_083045.xlsx"
        file_path.write_bytes(b"new")
        f = make_file(filepath=str(file_path), filename=file_path.name)

        result = f.move_to_directory(str(dst))
        assert result is True
        # Original should not be overwritten; a renamed copy should exist
        assert (dst / "event_20240101_083045.xlsx").read_bytes() == b"existing"


# ---------------------------------------------------------------------------
# __str__ / __repr__
# ---------------------------------------------------------------------------

class TestStringRepresentation:
    def test_str_success(self):
        f = make_file(filename="myfile.xlsx", success=True)
        assert "✓" in str(f)
        assert "myfile.xlsx" in str(f)

    def test_str_failure(self):
        f = make_file(filename="myfile.xlsx", success=False)
        assert "✗" in str(f)

    def test_repr_contains_key_fields(self):
        f = make_file(filename="myfile.xlsx", success=True)
        r = repr(f)
        assert "myfile.xlsx" in r
        assert "success=True" in r
