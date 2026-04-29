import os
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

import config
from config import _to_bool, load_db_config, REQUIRED_BIGQUERY_KEYS


def test_to_bool():
    assert _to_bool('True') is True
    assert _to_bool('1') is True
    assert _to_bool('yes') is True
    assert _to_bool('Y') is True
    assert _to_bool('On') is True
    assert _to_bool('False') is False
    assert _to_bool('0') is False
    assert _to_bool('no') is False
    assert _to_bool('N') is False
    assert _to_bool('Off') is False
    assert _to_bool(None) is False  # Default is False
    assert _to_bool('random_string') is False
    assert _to_bool('') is False


@patch('config.Path.exists', return_value=True)
@patch('builtins.open', new_callable=mock_open, read_data="GCP_PROJECT_ID=test-project\nBIGQUERY_DATASET=test-dataset")
def test_load_db_config_from_local_file(mock_file_open, mock_path_exists):
    # Clear cache for testing
    config._config_cache = None
    
    config_data = load_db_config()
    assert config_data['GCP_PROJECT_ID'] == 'test-project'
    assert config_data['BIGQUERY_DATASET'] == 'test-dataset'
    mock_path_exists.assert_called_once()
    mock_file_open.assert_called_once()


@patch('config._load_config_from_gcs')
def test_load_db_config_from_gcs_uri_param(mock_load_gcs):
    # Clear cache for testing
    config._config_cache = None

    mock_load_gcs.return_value = {
        'GCP_PROJECT_ID': 'gcs-project',
        'BIGQUERY_DATASET': 'gcs-dataset',
    }
    config_data = load_db_config(config_path='gs://my-bucket/config.txt')
    assert config_data['GCP_PROJECT_ID'] == 'gcs-project'
    mock_load_gcs.assert_called_once_with('gs://my-bucket/config.txt')


@patch.dict(os.environ, {'GCP_PROJECT_ID': 'env-project', 'BIGQUERY_DATASET': 'env-dataset'})
@patch('config.Path.exists', return_value=False) # Ensure no local file is read
def test_load_db_config_from_env_vars(mock_path_exists):
    # Clear cache for testing
    config._config_cache = None

    config_data = load_db_config()
    assert config_data['GCP_PROJECT_ID'] == 'env-project'
    assert config_data['BIGQUERY_DATASET'] == 'env-dataset'
    mock_path_exists.assert_called_once() # Still checks for default local path


def test_load_db_config_missing_required_keys_from_file():
    # Clear cache for testing
    config._config_cache = None

    # Mock an empty config file
    with patch('config.Path.exists', return_value=True), \
         patch('builtins.open', new_callable=mock_open, read_data=""):
        with pytest.raises(ValueError, match="Missing required config values: GCP_PROJECT_ID, BIGQUERY_DATASET"):
            load_db_config()


def test_load_db_config_missing_required_keys_from_env():
    # Clear cache for testing
    config._config_cache = None

    # Mock missing env vars
    with patch.dict(os.environ, {}, clear=True): # Clear all env vars
        with patch('config.Path.exists', return_value=False):
            with pytest.raises(ValueError, match="Missing required config values: GCP_PROJECT_ID, BIGQUERY_DATASET"):
                load_db_config()