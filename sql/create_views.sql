-- =============================================================================
-- create_views.sql
-- Creates or replaces reporting views on top of final_scores.
-- Run after merge_into_final_scores.sql so the underlying table is current.
-- =============================================================================

-- All final scores for the current season; convenient filtered starting point.
create or replace view current_year_scores as
SELECT
      event_id
    , event_name
    , league_id
    , league_name
    , end_date
    , year
    , division
    , player_name
    , player_username
    , raw_score
    , handicap
    , adjusted_score
    , place
    , points
    , payout
FROM final_scores
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
;

-- Season standings: cumulative points per player, league, and division.
create or replace view current_year_total_points as
SELECT
      player_username
    , player_name
    , league_name
    , division
    , sum(points) as total_points
FROM final_scores
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY
      player_username
    , player_name
    , league_name
    , division
;