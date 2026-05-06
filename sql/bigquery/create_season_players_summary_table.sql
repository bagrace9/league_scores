CREATE OR REPLACE TABLE `{dataset_name}.season_players_summary` AS
WITH player_stats AS (
    SELECT adj.player_username
         , adj.player_name 
         , adj.league_id
         , adj.league_name
         , adj.division
         , max(adj.event_end_date) as last_played_date
         , round(avg(adj.raw_score), 2) as average_raw_score
         , count(*) as events_played
         , sum(adj.points) as season_points
         , sum(adj.payout) as season_winnings
         , h.next_handicap
         , h.next_raw_scores_considered as next_handicap_scores
      from adjusted_scores adj
      left join handicaps h
        on  h.player_username = adj.player_username
        and h.league_id = adj.league_id
        and h.handicap_rank = 1
    where adj.year = extract(year from current_date()) 
    group by adj.player_username
           , adj.player_name
           , adj.league_id
           , adj.league_name
           , adj.division
           , h.next_handicap
           , h.next_raw_scores_considered
)
SELECT 
      player_username
    , player_name
    , league_id
    , league_name
    , division
    , last_played_date
    , average_raw_score
    , events_played
    , season_points
    , rank() over (partition by league_id, division order by season_points desc) as season_place
    , season_winnings
    , next_handicap
    , next_handicap_scores
FROM player_stats;