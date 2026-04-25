create or replace table season_players as(
    select rs.player_username
         , rs.player_name 
         , league_id
         , max(e.event_end_date) as last_played_date
      from raw_scores rs
      join events  e using (event_id)
      join leagues l using (league_id)
    where e.is_excluded = false
      and e.year = extract(year from current_date()) 
    group by rs.player_username
           , rs.player_name
           , league_id
);