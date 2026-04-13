-- =============================================================================
-- merge_into_final_scores.sql
-- Rebuilds the final_scores table from raw data each run.
--
-- For every non-excluded event, calculates:
--   place   – finishing position per league/division/date (1 = best score)
--   points  – league points using descending rank (winner earns most points)
--   payout  – cash payout joined from the pre-computed payouts lookup table
-- =============================================================================

drop table if exists final_scores cascade
;

create table if not exists final_scores as

with scores_with_ranks as (
    SELECT
          rs.event_id
        , e.event_name
        , rs.league_id
        , l.league_name
        , e.event_end_date AS end_date
        , extract(year from e.event_end_date) AS year
        , rs.division
        , rs.player_name
        , rs.player_username
        , rs.raw_score
        , h.handicap AS handicap
        , COALESCE(rs.raw_score - h.handicap, rs.raw_score) AS adjusted_score
        -- place: 1 = lowest adjusted score (best finish)
        , rank() OVER (PARTITION BY rs.league_id, rs.division, e.event_end_date ORDER BY COALESCE(rs.raw_score - h.handicap, rs.raw_score)) AS place
        -- points: descending rank so the winner earns the highest point value (equal to field size)
        , rank() OVER (PARTITION BY rs.league_id, rs.division, e.event_end_date ORDER BY COALESCE(rs.raw_score - h.handicap, rs.raw_score) DESC) AS points
        , count(*) OVER (PARTITION BY rs.league_id, rs.division, e.event_end_date) AS num_players

    
    FROM raw_scores rs

    JOIN events e
        ON e.event_id = rs.event_id
        
    join leagues l
        on l.league_id = rs.league_id

    LEFT JOIN handicaps h
        ON rs.raw_score_id = h.raw_score_id

    WHERE e.is_excluded = false
)

SELECT
          swr.event_id
        , swr.event_name
        , swr.league_id
        , swr.league_name
        , swr.end_date
        , swr.year
        , swr.division
        , swr.player_name
        , swr.player_username
        , swr.raw_score
        , swr.handicap AS handicap
        , swr.adjusted_score
        , swr.place
        , swr.points
        -- payout = total pot after league cut * pre-computed position percentage.
        -- Only populated for events from 2016 onward (start of consistent data).
        , case when swr.year >= 2016
               then round((swr.num_players * l.league_entry_fee) * (1 - (l.league_cash_percentage / 100)) * pp.percentage, 0)
               else null
               end AS payout

FROM scores_with_ranks swr
left join payouts pp
       on pp.n_players = swr.num_players
      and pp.position = swr.place
join leagues l
    on l.league_id = swr.league_id

;