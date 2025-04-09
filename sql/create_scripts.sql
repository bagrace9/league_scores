

Drop table if exists impt_raw_scores
;


CREATE table if not exists impt_raw_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id INTEGER,
            start_date_str TEXT,
            end_date_str TEXT,
            start_date date,
            end_date date,
            event TEXT,
            division TEXT,
            player TEXT,
            score INTEGER,
            points_multiplyer INTEGER,
            handicap_excluded BOOLEAN,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )

;

CREATE table if not exists scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id INTEGER,
            start_date date,
            end_date date,
            year INTEGER,
            event TEXT,
            division TEXT,
            player TEXT,
            raw_score INTEGER,
            points_multiplyer float,
            handicap int,
            adjusted_score int,
            place int,
            points int,
            handicap_excluded BOOLEAN,
            next_handicap int,
            season_points int,
            season_place int,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
;


CREATE table if not exists leagues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            league_name TEXT,
            league_is_handicap BOOLEAN,
            league_url TEXT,
            league_cash_percentage float,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )