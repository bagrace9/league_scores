create or replace table `{dataset_name}.season_event_summary` as
select e.event_id
     , e.league_id
     , l.league_name
     , e.event_name
     , e.event_end_date
    , extract(year from e.event_end_date) as year
    , count(*) as num_players
    , round(avg(adj.raw_score), 2) as average_raw_score
    , rank() over (partition by extract(year from e.event_end_date), e.league_id order by e.event_end_date desc) as event_rank_in_league
from  events e
join adjusted_scores adj
    on adj.event_id = e.event_id
join leagues l
    on l.league_id = e.league_id

where extract(year from e.event_end_date) = extract(year from current_date())
    GROUP BY
        e.event_id
     , e.league_id
     , l.league_name
     , e.event_name
     , e.event_end_date                                               