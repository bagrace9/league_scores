create or replace table season_log as
    select sp.player_username
         , sp.player_name
         , l.league_id
         , l.league_name
         , e.event_id
         , e.event_name
         , e.event_end_date
         , extract(year from e.event_end_date) as year
         , first_value(adj.division IGNORE NULLS)
              over (partition by sp.player_username, l.league_id, EXTRACT(YEAR FROM e.event_end_date)
                        order by e.event_end_date desc
                     rows between current row and unbounded following
                    ) as division
         , adj.raw_score
         , coalesce(adj.handicap, 0) as handicap
         , adj.adjusted_score
         , adj.handicap_scores
         , adj.place
         , adj.points
         , adj.payout
         , coalesce(
                    first_value(adj.next_handicap IGNORE NULLS)
                          over (partition by sp.player_username, l.league_id, EXTRACT(YEAR FROM e.event_end_date)
                                    order by e.event_end_date  
                                rows between current row and unbounded following
                                )
                    ,0) as next_handicap
         , first_value(adj.next_handicap_scores IGNORE NULLS)
              over (partition by sp.player_username, l.league_id, EXTRACT(YEAR FROM e.event_end_date)
                        order by e.event_end_date desc
                     rows between current row and unbounded following
                    ) as next_handicap_scores
         , coalesce(
                    first_value(adj.season_points_as_of_event IGNORE NULLS)
                          over (partition by sp.player_username, l.league_id, EXTRACT(YEAR FROM e.event_end_date)
                                    order by e.event_end_date desc
                                rows between current row and unbounded following
                                )
                    ,0) as season_points_as_of_event

      from season_players_summary sp
      join events  e --join only on league to ensure we get all players even if they missed some events
          on e.league_id = sp.league_id
      join leagues l        on l.league_id = e.league_id
      left join adjusted_scores adj 
                            on adj.event_id = e.event_id
                           and adj.player_username = sp.player_username
    where e.is_excluded = false
      and EXTRACT(year FROM e.event_end_date) = EXTRACT(year FROM CURRENT_DATE())

; 


