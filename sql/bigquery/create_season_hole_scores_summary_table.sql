create or replace table season_hole_scores_summary as
    select adj.player_username
         , adj.player_name
         , concat(adj.player_name, ' (', adj.player_username, ')') as player_display_name
         , l.league_id
         , l.league_name
         , hs.hole_number
         , hs.hole_score
         , e.event_name
         , e.event_end_date
         , e.year
      from adjusted_scores adj
      join season_event_summary  e 
          on e.event_id = adj.event_id
          and e.year = 2026
      join leagues l
          on l.league_id = adj.league_id
      join hole_scores hs 
          on hs.raw_score_id = adj.raw_score_id