-- =============================================================================
-- BigQuery create_perm_tables.sql
-- Creates permanent application tables for dataset configured as default dataset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `{dataset_name}.leagues` (
    league_id STRING DEFAULT GENERATE_UUID(),
    league_name STRING,
    league_urls STRING,
    league_entry_fee NUMERIC,
    league_cash_percentage NUMERIC,
    league_is_handicap BOOL,
    handicap_minimum_rounds INT64,
    handicap_rounds_considered INT64,
    handicap_years_lookback INT64,
    handicap_base_score INT64,
    handicap_multiplier NUMERIC,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{dataset_name}.events` (
    event_id STRING DEFAULT GENERATE_UUID(),
    league_id STRING,
    event_name STRING,
    event_end_date DATE,
    export_url STRING,
    is_downloaded BOOL,
    file_name STRING,
    file_path STRING,
    num_players INT64,
    download_date TIMESTAMP,
    is_imported BOOL,
    import_date TIMESTAMP,
    is_excluded BOOL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{dataset_name}.raw_scores` (
    raw_score_id STRING DEFAULT GENERATE_UUID(),
    event_id STRING,
    league_id STRING,
    division STRING,
    player_name STRING,
    player_username STRING,
    raw_score INT64,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{dataset_name}.hole_scores` (
    hole_score_id STRING DEFAULT GENERATE_UUID(),
    raw_score_id STRING,
    hole_number INT64,
    hole_score INT64,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
