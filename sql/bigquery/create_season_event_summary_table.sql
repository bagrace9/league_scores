create or replace table season_event_summary as
select e.event_id
     , e.league_id
     , l.league_name
     , e.event_name
     , e.event_end_date
    , extract(year from e.event_end_date) as year
    , count(*) as num_players
    , round(avg(adj.raw_score), 2) as average_raw_score
    , rank() over (partition by extract(year from e.event_end_date), e.league_id order by avg(adj.raw_score) desc) as event_rank_in_league
from  events e
join adjusted_scores adj
    on adj.event_id = e.event_id
join leagues l
    on l.league_id = e.league_id

where e.is_excluded_from_points = false
  and extract(year from e.event_end_date) = (select max(extract(year from event_end_date)) 
                                               from events
                                               where is_excluded_from_points = false
                                               )
    GROUP BY
        e.event_id
     , e.league_id
     , l.league_name
     , e.event_name
     , e.event_end_date                                               