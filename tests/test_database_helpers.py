"""Tests for pure/helper functions in database.py.

BigQuery-hitting functions are not tested here — those belong in integration
tests. This file covers the deterministic helpers that do string manipulation,
data normalization, and payout math without any network calls.
"""
import math
import re
import uuid
from datetime import date
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# We import the private helpers directly since they carry clear contracts.
import database
from database import (
    _derive_event_name_from_filename,
    _normalize_column_name,
    _to_int,
    _to_text,
    _extract_hole_number,
    normalize_table_definition,
    prepare_import_data,
)


# ---------------------------------------------------------------------------
# _derive_event_name_from_filename
# ---------------------------------------------------------------------------

class TestDeriveEventNameFromFilename:
    def test_strips_timestamp_suffix_with_iso_date(self):
        name = _derive_event_name_from_filename("My-League-Event-2024-06-15_20240615_120000.xlsx")
        assert "2024-06-15" not in name
        assert "20240615" not in name
        assert "My" in name

    def test_strips_timestamp_suffix_with_8digit_date(self):
        name = _derive_event_name_from_filename("Cool-Event_20240615_20240615_083045.xlsx")
        assert "20240615" not in name

    def test_replaces_hyphens_with_spaces(self):
        name = _derive_event_name_from_filename("Disc-Golf-League_20240615_20240615_083045.xlsx")
        assert "-" not in name
        assert "Disc Golf League" in name

    def test_no_timestamp_returns_stem(self):
        name = _derive_event_name_from_filename("plainfile.xlsx")
        assert name == "plainfile"

    def test_empty_extension_handled(self):
        name = _derive_event_name_from_filename("no-ext")
        assert isinstance(name, str)


# ---------------------------------------------------------------------------
# _normalize_column_name
# ---------------------------------------------------------------------------

class TestNormalizeColumnName:
    def test_lowercases(self):
        assert _normalize_column_name("PlayerName") == "playername"

    def test_strips_spaces(self):
        assert _normalize_column_name("Player Name") == "playername"

    def test_removes_special_chars(self):
        assert _normalize_column_name("Round Total Score!") == "roundtotalscore"

    def test_preserves_underscores(self):
        assert _normalize_column_name("player_username") == "player_username"

    def test_handles_numeric_suffix(self):
        assert _normalize_column_name("Hole 7") == "hole7"

    def test_handles_non_string_input(self):
        result = _normalize_column_name(123)
        assert result == "123"


# ---------------------------------------------------------------------------
# _to_int
# ---------------------------------------------------------------------------

class TestToInt:
    def test_converts_integer(self):
        assert _to_int(5) == 5

    def test_converts_float_string(self):
        assert _to_int("3.0") == 3

    def test_converts_negative(self):
        assert _to_int(-2) == -2

    def test_returns_none_for_none(self):
        assert _to_int(None) is None

    def test_returns_none_for_blank_string(self):
        assert _to_int("  ") is None

    def test_returns_none_for_non_numeric_string(self):
        assert _to_int("abc") is None

    def test_converts_string_integer(self):
        assert _to_int("42") == 42


# ---------------------------------------------------------------------------
# _to_text
# ---------------------------------------------------------------------------

class TestToText:
    def test_strips_whitespace(self):
        assert _to_text("  hello  ") == "hello"

    def test_returns_none_for_none(self):
        assert _to_text(None) is None

    def test_returns_none_for_blank_string(self):
        assert _to_text("   ") is None

    def test_converts_non_string(self):
        assert _to_text(42) == "42"

    def test_returns_stripped_string(self):
        assert _to_text("Alice") == "Alice"


# ---------------------------------------------------------------------------
# _extract_hole_number
# ---------------------------------------------------------------------------

class TestExtractHoleNumber:
    def test_extracts_valid_hole_number(self):
        assert _extract_hole_number("hole_7") == 7

    def test_extracts_hole_1(self):
        assert _extract_hole_number("hole_1") == 1

    def test_extracts_hole_18(self):
        assert _extract_hole_number("hole_18") == 18

    def test_extracts_hole_36(self):
        assert _extract_hole_number("hole_36") == 36

    def test_returns_none_for_hole_0(self):
        assert _extract_hole_number("hole_0") is None

    def test_returns_none_for_hole_37(self):
        assert _extract_hole_number("hole_37") is None

    def test_returns_none_for_non_hole_column(self):
        assert _extract_hole_number("player_name") is None

    def test_returns_none_for_empty_string(self):
        assert _extract_hole_number("") is None

    def test_handles_uppercase_input(self):
        # The function lowercases the key before matching, so Hole_7 still matches
        assert _extract_hole_number("Hole_7") == 7


# ---------------------------------------------------------------------------
# normalize_table_definition
# ---------------------------------------------------------------------------

class TestNormalizeTableDefinition:
    SAMPLE_DDL = """
    CREATE TABLE `project.dataset.leagues_template`
    (
      league_id STRING DEFAULT GENERATE_UUID(),
      league_name STRING,
      create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    OPTIONS (...)
    """

    def test_returns_none_for_none_input(self):
        assert normalize_table_definition(None) is None

    def test_returns_none_when_no_opening_paren(self):
        assert normalize_table_definition("CREATE TABLE foo") is None

    def test_strips_generate_uuid_noise(self):
        result = normalize_table_definition(self.SAMPLE_DDL)
        assert "generate_uuid()" not in result

    def test_lowercases_output(self):
        result = normalize_table_definition(self.SAMPLE_DDL)
        assert result == result.lower()

    def test_collapses_whitespace(self):
        result = normalize_table_definition(self.SAMPLE_DDL)
        assert "  " not in result

    def test_two_equivalent_ddls_produce_same_normalized_form(self):
        ddl1 = "CREATE TABLE `p.d.t` (\n  col1 STRING,\n  col2 INT64\n)"
        ddl2 = "CREATE TABLE `p.d.t` ( col1 STRING, col2 INT64 )"
        assert normalize_table_definition(ddl1) == normalize_table_definition(ddl2)

    def test_different_columns_produce_different_forms(self):
        ddl1 = "CREATE TABLE t (col1 STRING)"
        ddl2 = "CREATE TABLE t (col2 INT64)"
        assert normalize_table_definition(ddl1) != normalize_table_definition(ddl2)


# ---------------------------------------------------------------------------
# prepare_import_data
# ---------------------------------------------------------------------------

class TestPrepareImportData:
    """Tests for the spreadsheet-parsing logic in prepare_import_data."""

    def _make_downloaded_file(self, filepath, export_url="https://udisc.com/export",
                               filename="event_20240615_20240615_120000.xlsx",
                               event_end_date=date(2024, 6, 15)):
        f = MagicMock()
        f.filepath = filepath
        f.export_url = export_url
        f.filename = filename
        f.event_end_date = event_end_date
        f.download_date = None
        return f

    def _write_valid_excel(self, path):
        # Use underscore-separated names so _normalize_column_name produces
        # the expected column keys: round_total_score, hole_1, hole_2, etc.
        df = pd.DataFrame({
            "Division": ["MPO", "FPO"],
            "Name": ["Alice", "Bob"],
            "Username": ["alice123", "bob456"],
            "Round_Total_Score": [54, 60],
            "Hole_1": [3, 4],
            "Hole_2": [4, 5],
        })
        df.to_excel(path, index=False)

    def test_returns_none_for_empty_file(self, tmp_path):
        path = tmp_path / "empty.xlsx"
        pd.DataFrame().to_excel(path, index=False)
        df_file = self._make_downloaded_file(str(path))
        with patch.object(database, "_load_import_dataframe", return_value=pd.DataFrame()):
            result = prepare_import_data("league-1", df_file)
        assert result is None

    def test_raises_on_missing_required_columns(self, tmp_path):
        path = tmp_path / "bad.xlsx"
        pd.DataFrame({"SomeColumn": [1, 2]}).to_excel(path, index=False)
        df_file = self._make_downloaded_file(str(path))

        bad_df = pd.DataFrame({"somecolumn": [1, 2]})
        with patch.object(database, "_load_import_dataframe", return_value=bad_df):
            with pytest.raises(ValueError, match="Missing required columns"):
                prepare_import_data("league-1", df_file)

    def test_returns_expected_structure(self, tmp_path):
        path = tmp_path / "valid.xlsx"
        self._write_valid_excel(path)
        df_file = self._make_downloaded_file(str(path))

        result = prepare_import_data("league-1", df_file)
        assert result is not None
        assert "event_id" in result
        assert "event_row" in result
        assert "raw_score_rows" in result
        assert "hole_score_rows" in result

    def test_raw_score_rows_count_matches_players(self, tmp_path):
        path = tmp_path / "valid.xlsx"
        self._write_valid_excel(path)
        df_file = self._make_downloaded_file(str(path))

        result = prepare_import_data("league-1", df_file)
        assert len(result["raw_score_rows"]) == 2

    def test_hole_score_rows_are_populated(self, tmp_path):
        path = tmp_path / "valid.xlsx"
        self._write_valid_excel(path)
        df_file = self._make_downloaded_file(str(path))

        result = prepare_import_data("league-1", df_file)
        # 2 players × 2 holes = 4 hole score rows
        assert len(result["hole_score_rows"]) == 4

    def test_event_id_reused_when_provided(self, tmp_path):
        path = tmp_path / "valid.xlsx"
        self._write_valid_excel(path)
        df_file = self._make_downloaded_file(str(path))
        fixed_id = str(uuid.uuid4())

        result = prepare_import_data("league-1", df_file, event_id=fixed_id)
        assert result["event_id"] == fixed_id
        assert result["event_row"]["event_id"] == fixed_id

    def test_skips_rows_with_no_player_name(self, tmp_path):
        path = tmp_path / "mixed.xlsx"
        df = pd.DataFrame({
            "Division": ["MPO", None],
            "Name": ["Alice", None],
            "Username": ["alice123", None],
            "Round_Total_Score": [54, None],
        })
        df.to_excel(path, index=False)
        df_file = self._make_downloaded_file(str(path))

        result = prepare_import_data("league-1", df_file)
        assert result is not None
        assert len(result["raw_score_rows"]) == 1
