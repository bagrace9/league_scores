create or replace table season_log as(
    select rs.player_username
         , rs.player_name 
         , league_id
         , e.event_id
         , e.event_name
         , e.event_end_date
         , e.year
         , fs.division
         , fs.raw_score
         , fs.handicap
         , fs.adjusted_score
         , fs.place
         , fs.points
         , fs.payout
         , fs.
      from season_players sp
      cross join events  e
      join leagues l        using (league_id)
      join final_scores fs    using (event_id)
    where e.is_excluded = false
      and e.year = extract(year from current_date())

; 