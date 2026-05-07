"""Tests for config.py — configuration loading and resolution."""
import os
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

import config
from config import (
    _parse_config_text,
    _to_bool,
    get_bigquery_config,
    get_leagues_bootstrap_config_path,
    get_storage_config,
    load_db_config,
)


# ---------------------------------------------------------------------------
# _to_bool
# ---------------------------------------------------------------------------

class TestToBool:
    @pytest.mark.parametrize("value", ["True", "1", "yes", "Y", "On", "TRUE", "YES"])
    def test_truthy_values(self, value):
        assert _to_bool(value) is True

    @pytest.mark.parametrize("value", ["False", "0", "no", "N", "Off", "FALSE", "NO"])
    def test_falsy_values(self, value):
        assert _to_bool(value) is False

    def test_none_returns_default_false(self):
        assert _to_bool(None) is False

    def test_none_with_custom_default_true(self):
        assert _to_bool(None, default=True) is True

    def test_empty_string_returns_default(self):
        assert _to_bool("") is False

    def test_unrecognised_string_returns_default(self):
        assert _to_bool("random_garbage") is False


# ---------------------------------------------------------------------------
# _parse_config_text
# ---------------------------------------------------------------------------

class TestParseConfigText:
    def test_parses_key_value_pairs(self):
        text = "GCP_PROJECT_ID=my-project\nBIGQUERY_DATASET=my-dataset"
        result = _parse_config_text(text)
        assert result == {"GCP_PROJECT_ID": "my-project", "BIGQUERY_DATASET": "my-dataset"}

    def test_ignores_comment_lines(self):
        text = "# this is a comment\nGCP_PROJECT_ID=my-project"
        result = _parse_config_text(text)
        assert "GCP_PROJECT_ID" in result
        assert len(result) == 1

    def test_ignores_blank_lines(self):
        text = "\n\nGCP_PROJECT_ID=x\n\n"
        result = _parse_config_text(text)
        assert result == {"GCP_PROJECT_ID": "x"}

    def test_ignores_lines_without_equals(self):
        text = "NOT_A_KEY\nGCP_PROJECT_ID=x"
        result = _parse_config_text(text)
        assert "NOT_A_KEY" not in result

    def test_value_with_equals_sign_preserved(self):
        text = "KEY=value=with=equals"
        result = _parse_config_text(text)
        assert result["KEY"] == "value=with=equals"

    def test_strips_whitespace_from_keys_and_values(self):
        text = "  GCP_PROJECT_ID  =  my-project  "
        result = _parse_config_text(text)
        assert result["GCP_PROJECT_ID"] == "my-project"

    def test_empty_string_returns_empty_dict(self):
        assert _parse_config_text("") == {}


# ---------------------------------------------------------------------------
# load_db_config
# ---------------------------------------------------------------------------

class TestLoadDbConfig:
    def setup_method(self):
        config._config_cache = None

    def test_loads_from_local_file(self):
        file_text = "GCP_PROJECT_ID=test-project\nBIGQUERY_DATASET=test-dataset"
        with patch("config.Path.exists", return_value=True), \
             patch("pathlib.Path.open", mock_open(read_data=file_text)), \
             patch.dict(os.environ, {}, clear=True):
            result = load_db_config()
        assert result["GCP_PROJECT_ID"] == "test-project"
        assert result["BIGQUERY_DATASET"] == "test-dataset"

    def test_env_vars_override_file_values(self):
        file_text = "GCP_PROJECT_ID=file-project\nBIGQUERY_DATASET=file-dataset"
        with patch("config.Path.exists", return_value=True), \
             patch("pathlib.Path.open", mock_open(read_data=file_text)), \
             patch.dict(os.environ, {"GCP_PROJECT_ID": "env-project"}, clear=False):
            config._config_cache = None
            result = load_db_config()
        assert result["GCP_PROJECT_ID"] == "env-project"
        assert result["BIGQUERY_DATASET"] == "file-dataset"

    def test_env_only_when_no_file(self):
        with patch("config.Path.exists", return_value=False), \
             patch.dict(os.environ, {
                 "GCP_PROJECT_ID": "env-proj",
                 "BIGQUERY_DATASET": "env-ds",
             }, clear=True):
            result = load_db_config()
        assert result["GCP_PROJECT_ID"] == "env-proj"

    def test_raises_when_required_keys_missing(self):
        with patch("config.Path.exists", return_value=False), \
             patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing required config values"):
                load_db_config()

    def test_raises_lists_all_missing_keys(self):
        with patch("config.Path.exists", return_value=False), \
             patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
                load_db_config()

    def test_result_is_cached_on_second_call(self):
        file_text = "GCP_PROJECT_ID=p\nBIGQUERY_DATASET=d"
        with patch("config.Path.exists", return_value=True), \
             patch("pathlib.Path.open", mock_open(read_data=file_text)) as m, \
             patch.dict(os.environ, {}, clear=True):
            load_db_config()
            load_db_config()
        # file should only be opened once due to caching
        assert m.call_count == 1

    def test_gcs_uri_param_triggers_gcs_load(self):
        config._config_cache = None
        with patch("config._load_config_from_gcs", return_value={
            "GCP_PROJECT_ID": "gcs-proj",
            "BIGQUERY_DATASET": "gcs-ds",
        }) as mock_gcs, \
             patch.dict(os.environ, {}, clear=True):
            result = load_db_config(config_path="gs://bucket/config.txt")
        mock_gcs.assert_called_once_with("gs://bucket/config.txt")
        assert result["GCP_PROJECT_ID"] == "gcs-proj"

    def test_db_config_gcs_uri_env_var_triggers_gcs_load(self):
        config._config_cache = None
        with patch("config._load_config_from_gcs", return_value={
            "GCP_PROJECT_ID": "env-gcs-proj",
            "BIGQUERY_DATASET": "env-gcs-ds",
        }) as mock_gcs, \
             patch.dict(os.environ, {"DB_CONFIG_GCS_URI": "gs://bucket/config.txt"}, clear=True):
            result = load_db_config()
        mock_gcs.assert_called_once_with("gs://bucket/config.txt")
        assert result["GCP_PROJECT_ID"] == "env-gcs-proj"


# ---------------------------------------------------------------------------
# get_bigquery_config
# ---------------------------------------------------------------------------

class TestGetBigqueryConfig:
    def setup_method(self):
        config._config_cache = None

    def test_returns_required_keys(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "proj",
            "BIGQUERY_DATASET": "ds",
        }):
            result = get_bigquery_config()
        assert result["project_id"] == "proj"
        assert result["dataset"] == "ds"

    def test_location_defaults_to_us(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "proj",
            "BIGQUERY_DATASET": "ds",
        }):
            result = get_bigquery_config()
        assert result["location"] == "US"

    def test_location_can_be_overridden(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "proj",
            "BIGQUERY_DATASET": "ds",
            "BIGQUERY_LOCATION": "EU",
        }):
            result = get_bigquery_config()
        assert result["location"] == "EU"

    def test_credentials_path_included_when_set(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "proj",
            "BIGQUERY_DATASET": "ds",
            "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/creds.json",
        }):
            result = get_bigquery_config()
        assert result["credentials_path"] == "/path/to/creds.json"


# ---------------------------------------------------------------------------
# get_storage_config
# ---------------------------------------------------------------------------

class TestGetStorageConfig:
    def test_archive_files_defaults_to_true(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
        }):
            result = get_storage_config()
        assert result["archive_files"] is True

    def test_archive_files_can_be_disabled(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
            "ARCHIVE_IMPORTED_FILES": "false",
        }):
            result = get_storage_config()
        assert result["archive_files"] is False

    def test_bucket_is_none_when_not_set(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
        }):
            result = get_storage_config()
        assert result["bucket"] is None

    def test_bucket_returned_when_set(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
            "GCS_BUCKET": "my-bucket",
        }):
            result = get_storage_config()
        assert result["bucket"] == "my-bucket"


# ---------------------------------------------------------------------------
# get_leagues_bootstrap_config_path
# ---------------------------------------------------------------------------

class TestGetLeaguesBootstrapConfigPath:
    def test_defaults_to_local_path_when_no_bucket(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
        }):
            result = get_leagues_bootstrap_config_path()
        assert isinstance(result, Path)
        assert "league_configs.json" in str(result)

    def test_returns_gcs_path_when_bucket_configured(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
            "GCS_BUCKET": "my-bucket",
        }):
            result = get_leagues_bootstrap_config_path()
        assert str(result).startswith("gs://my-bucket/")

    def test_returns_gcs_path_when_leagues_path_is_gcs_uri(self):
        with patch("config.load_db_config", return_value={
            "GCP_PROJECT_ID": "p", "BIGQUERY_DATASET": "d",
            "LEAGUES_BOOTSTRAP_PATH": "gs://other-bucket/leagues.json",
        }):
            result = get_leagues_bootstrap_config_path()
        assert result == "gs://other-bucket/leagues.json"
