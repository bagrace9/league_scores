-- =============================================================================
-- BigQuery create_views.sql
-- Creates reporting views over final_scores.
-- =============================================================================

CREATE OR REPLACE VIEW current_year_scores AS
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
    , season_points_as_of_event
FROM {dataset_name}.final_scores
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE());

CREATE OR REPLACE VIEW current_year_total_points AS
SELECT
      player_username
    , player_name
    , league_name
    , division
    , SUM(points) AS total_points
    , AVG(fs.raw_score) AS average_raw_score
    , ARRAY_AGG(next_handicap ORDER BY end_date DESC, event_id DESC LIMIT 1)[OFFSET(0)] AS current_handicap
    , ARRAY_AGG(next_handicap_scores ORDER BY end_date DESC, event_id DESC LIMIT 1)[OFFSET(0)] AS handicap_scores
FROM {dataset_name}.final_scores fs
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY
      player_username
    , player_name
    , league_name
    , division;
