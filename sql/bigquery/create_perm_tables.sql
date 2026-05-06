-- =============================================================================
-- BigQuery create_perm_tables.sql
-- Creates permanent application tables for dataset configured as default dataset.
-- =============================================================================



CREATE OR REPLACE TABLE `{dataset_name}.leagues_template` (
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

CREATE OR REPLACE TABLE `{dataset_name}.events_template` (
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
    is_excluded_from_handicap BOOL, -- exclude this score from future handicap calculations
    is_excluded_from_points BOOL,   -- exclude this score from season points totals
    is_no_handicap_applied BOOL,    -- do not apply handicap to this score    
    points_multiplier NUMERIC,      -- multiply the points. for example, double points for league Finals
    buy_in_override_ammt NUMERIC,   -- override the buy-in amount for this event, which is used for calculating winnings. if null, will use league_entry_fee
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



CREATE OR REPLACE TABLE `{dataset_name}.raw_scores_template` (
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

CREATE OR REPLACE TABLE `{dataset_name}.hole_scores_template` (
    hole_score_id STRING DEFAULT GENERATE_UUID(),
    raw_score_id STRING,
    hole_number INT64,
    hole_score INT64,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE If NOT EXISTS `{dataset_name}.event_updates_template` (
    export_url STRING,
    is_excluded_from_handicap BOOL,
    is_no_handicap_applied BOOL,
    is_excluded_from_points BOOL,
    points_multiplier NUMERIC,
    buy_in_override_ammt NUMERIC,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);