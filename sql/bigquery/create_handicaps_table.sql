-- =============================================================================
-- BigQuery create_handicaps_table.sql
-- Rebuilds the handicaps table from scratch each run.
-- =============================================================================


CREATE OR REPLACE TABLE `{dataset_name}.handicaps` AS


WITH scores_in_handicap_calc AS (
    SELECT

         rs.raw_score_id
        , rs.league_id
        , rs.raw_score
        , rs.player_username
        , e.event_end_date
    FROM raw_scores rs
    JOIN events e
        ON e.event_id = rs.event_id
    JOIN leagues l
        ON l.league_id = rs.league_id
    WHERE l.league_is_handicap = TRUE
    and e.is_excluded_from_handicap = FALSE
    and extract(year from e.event_end_date) >= extract(year from current_date()) - l.handicap_years_lookback

),



scores_to_handicap AS (
    SELECT
          l.league_id
        , l.handicap_minimum_rounds
        , l.handicap_rounds_considered
        , l.handicap_years_lookback
        , l.handicap_base_score
        , l.handicap_multiplier
        , rs.raw_score_id
        , rs.raw_score
        , rs.player_username
        , e.event_end_date
        ,(select count(*)
            from scores_in_handicap_calc shc
            where shc.player_username = rs.player_username
            and shc.league_id = rs.league_id
            and shc.event_end_date < e.event_end_date
            ) previous_weeks
    FROM raw_scores rs
    JOIN events e
        ON e.event_id = rs.event_id
    JOIN leagues l
        ON l.league_id = rs.league_id
    WHERE l.league_is_handicap = TRUE
    and e.is_no_handicap_applied = FALSE
    and extract(year from e.event_end_date) >= extract(year from current_date()) - l.handicap_years_lookback

)



, joined_scores as (
    SELECT
          
         stc.raw_score_id
        ,stc.handicap_base_score
        ,stc.handicap_multiplier
        ,stc.league_id
        ,stc.player_username
        ,stc.event_end_date
        ,stc.raw_score
        ,shc.raw_score as hcap_raw_score
        ,stc.previous_weeks
        ,stc.handicap_minimum_rounds
        ,shc.event_end_date hcap_end_date
    FROM scores_to_handicap stc
    join scores_in_handicap_calc shc
        on shc.player_username = stc.player_username
       and shc.league_id = stc.league_id
       and shc.event_end_date <= stc.event_end_date

    qualify row_number() over (partition by stc.raw_score_id order by shc.event_end_date desc) <= stc.handicap_rounds_considered 
)



,next_handicap as ( 
select 
        js.league_id
        , js.player_username
        , js.event_end_date
        , js.raw_score
        , js.raw_score_id
        , js.previous_weeks
        , js.handicap_minimum_rounds
        , case when js.handicap_minimum_rounds -1 > js.previous_weeks 
               then 0
               else greatest(
                        cast( 
                              round(
                                      (avg(js.hcap_raw_score) - js.handicap_base_score) * js.handicap_multiplier
                                  , 0)
                              as int
                              ) 
                          ,0)
               end AS next_handicap
        , string_agg(cast(js.hcap_raw_score as string),',' order by js.event_end_date desc) as next_raw_scores_considered
        
from joined_scores js
group by 
          js.league_id
        , js.player_username
        , js.event_end_date
        , js.raw_score
        , js.raw_score_id
        , js.handicap_base_score
        , js.handicap_multiplier
        , js.previous_weeks
        , js.handicap_minimum_rounds
)


select nh.raw_score_id
         ,nh.raw_score
         ,nh.next_handicap
         ,nh.next_raw_scores_considered
         ,nh.league_id
         ,nh.player_username
         ,nh.event_end_date
         
    , last_value(nh.next_handicap ignore nulls) 
           over (partition by nh.league_id, nh.player_username 
                     order by nh.event_end_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) as handicap

    , last_value(nh.next_raw_scores_considered ignore nulls) 
           over (partition by nh.league_id, nh.player_username 
                     order by nh.event_end_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) as raw_scores_considered
    , row_number() over (partition by nh.league_id, nh.player_username order by nh.event_end_date desc) as handicap_rank
    , CURRENT_TIMESTAMP() AS create_time
from next_handicap nh