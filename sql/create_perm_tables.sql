
-- =============================================================================
-- create_perm_tables.sql
-- Creates all permanent application tables if they do not already exist.
-- Safe to re-run on an existing database without side effects.
-- =============================================================================

create TABLE if not exists raw_scores (
            raw_score_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id INTEGER,
            league_id INTEGER,
            division TEXT,
            player_name TEXT,
            player_username TEXT,
            raw_score INTEGER,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
;


CREATE table if not exists leagues (
            league_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            league_name TEXT,         
            league_urls TEXT,   
            league_entry_fee numeric(8,4),
            league_cash_percentage numeric(8,4),
            league_is_handicap BOOLEAN,
            handicap_minimum_rounds int,
            handicap_rounds_considered int,
            handicap_years_lookback int,
            handicap_base_score int,
            handicap_multiplier numeric(8,4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
;


create table IF NOT EXISTS events (
            event_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            league_id INTEGER,
            event_name TEXT,
            event_end_date date,
            export_url TEXT,
            is_downloaded BOOLEAN,
            file_name TEXT,
            file_path TEXT,
            num_players INTEGER,
            download_date TIMESTAMP,
            is_imported BOOLEAN,
            import_date TIMESTAMP,
            is_excluded BOOLEAN,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) 
;


create table IF NOT EXISTS hole_scores (
            hole_score_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            raw_score_id INTEGER,
            hole_number INTEGER,
            hole_score INTEGER,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
;







