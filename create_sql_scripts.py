import os

def create_table_create_file():
    file_path = "sql/create_scripts.sql"

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Define the SQL script as a multiline string
    sql_script = """
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
            league_url TEXT,
            league_entry_fee float,
            league_cash_percentage float,
            league_is_handicap BOOLEAN,
            handicap_minimum_rounds int,
            handicap_rounds_considered int,
            handicap_years_lookback int,
            handicap_base_score int,
            handicap_multiplier float,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
;
"""

    # Write the SQL script to the file
    with open(file_path, "w") as file:
        file.write(sql_script)


def create_replace_scores_file():
    file_path = "sql/replace_scores.sql"

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Define the SQL script as a multiline string
    sql_script = """
delete from scores as s1
where exists (
  						select 1
              from impt_raw_scores irs
              where s1.event = irs.event
             );

insert into scores(league_id,
                       start_date,
                       end_date,
                       event,
                       division,
                       player,
                       raw_score,
                       points_multiplyer,
                       handicap_excluded)
select league_id,
			 start_date,
       end_date,
       event,
       division,
       player,
       score,
       points_multiplyer,
       handicap_excluded
from impt_raw_scores
"""

    # Write the SQL script to the file
    with open(file_path, "w") as file:
        file.write(sql_script)



def create_update_points_file():
    file_path = "sql/update_points.sql"

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Define the SQL script as a multiline string
    sql_script = """
update scores
set year = strftime('%Y', end_date)
where league_id = {league_id}
;

update scores as s1
set handicap = case when (select handicap
                          from leagues l
                          where l.id = s1.league_id
                          )
                          = 0
                     then 0
                     when (select count(1)
                           from scores s2
                           where s2.player = s1.player
                             and s2.league_id = s1.league_id
                             and s2.end_date < s1.end_date
                             and s2.year >= s1.year -1
                           	 and s2.handicap_excluded = 0
                           )
                           < 3
                     then 0
                     else (select  max(round((avg(x.raw_score) - 49) * 0.8 , 0) ,0)
                           from(select s3.raw_score,
                                row_number() over (order by end_date desc) rn
                           			from scores s3
                                where s3.player = s1.player
                                  and s3.league_id = s1.league_id
                                  and s3.end_date < s1.end_date
                                  and s3.year >= s1.year -1
                                  and s3.handicap_excluded = 0
                                ) x
                           where x.rn <=3
                           )
                      end
where s1."year" = strftime('%Y', datetime('now'))
  and league_id = {league_id}
;
  
                           
update scores as s1
set adjusted_score = raw_score - handicap
where s1.year = strftime('%Y', datetime('now'))
  and league_id = {league_id}

;
                           
update scores as s1
set place = (select count(1) + 1
             from scores s2
             where s2.event = s1.event
               and s2.league_id = s1.league_id
             	 and s2.adjusted_score < s1.adjusted_score
               and s2.division = s1.division
             )
   	,points = (select count(1)
             from scores s2
             where s2.event = s1.event
               and s2.league_id = s1.league_id
             	 and s2.adjusted_score >= s1.adjusted_score
               and s2.division = s1.division
             )
where s1.year = strftime('%Y', datetime('now'))     
  and league_id = {league_id}
       
;



update scores as s1
set next_handicap = case when (select handicap
                          from leagues l
                          where l.id = s1.league_id
                          )
                          = 0
                     then 0
                     when (select count(1)
                           from scores s2
                           where s2.player = s1.player
                             and s2.league_id = s1.league_id
                             and s2.end_date <= s1.end_date
                             and s2.year >= s1.year -1
                             and s2.handicap_excluded = 0
                           )
                           < 3
                     then 0
                     else (select  max(round((avg(x.raw_score) - 49) * 0.8 , 0) ,0)
                           from(select s3.raw_score,
                                row_number() over (order by end_date desc) rn
                           			from scores s3
                                where s3.player = s1.player
                                  and s3.league_id = s1.league_id
                                  and s3.end_date <= s1.end_date
                                  and s3.year >= s1.year -1
                                  and s3.handicap_excluded = 0
                                ) x
                           where x.rn <=3
                           )
                      end
where s1.year = strftime('%Y', datetime('now'))                    
  and league_id = {league_id}

;


update scores as s1
set season_points = (select sum(points)
                     from scores s2
                     where s2.player = s1.player
                       and s2.league_id = s1.league_id
                       and s2.end_date <= s1.end_date
                       and s2.year >= s1.year
                     )
where s1."year" = strftime('%Y', datetime('now'))
  and league_id = {league_id}             
  
;


update scores as s1
set season_place = (select count(distinct player) + 1
                     from scores s2
                     where s2.event = s1.event
                       and s2.league_id = s1.league_id
                       and s2.season_points > s1.season_points
                       and s2.division = s1.division
                     )
where s1.year = strftime('%Y', datetime('now'))     
  and league_id = {league_id}
;
"""

    # Write the SQL script to the file
    with open(file_path, "w") as file:
        file.write(sql_script)

def create_sql_files():
    # Create the SQL files
    create_table_create_file()
    create_replace_scores_file()
    create_update_points_file() 