create or replace table season_players_summary as(
    select adj.player_username
         , adj.player_name 
         , adj.league_id
         , adj.league_name
         , adj.division
         , max(adj.event_end_date) as last_played_date
         , round(avg(adj.raw_score), 2) as average_raw_score
         , count(*) as events_played
         , sum(adj.points) as season_points
         , rank() over (partition by adj.league_id, adj.division order by sum(adj.points) desc) as season_place
      from adjusted_scores adj
    where adj.year = extract(year from current_date()) 
    group by adj.player_username
           , adj.player_name
           , adj.league_id
           , adj.league_name
           , adj.division
);