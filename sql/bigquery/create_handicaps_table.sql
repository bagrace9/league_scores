-- =============================================================================
-- BigQuery create_handicaps_table.sql
-- Rebuilds the handicaps table from scratch each run.
-- =============================================================================


CREATE or replace TABLE handicaps AS
WITH ranked_scores AS (
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
        , ROW_NUMBER() OVER (
            PARTITION BY l.league_id, rs.player_username
            ORDER BY e.event_end_date DESC
        ) AS rn
    FROM raw_scores rs
    LEFT JOIN events e
        ON e.event_id = rs.event_id
    JOIN leagues l
        ON l.league_id = rs.league_id
    WHERE e.is_excluded_from_handicap = FALSE
      AND l.league_is_handicap = TRUE
),
next_handicaps AS (
    SELECT
          rs1.league_id
        , rs1.raw_score_id
        , rs1.player_username
        , rs1.event_end_date
        , rs1.raw_score
        , CASE
            WHEN COUNT(*) < rs1.handicap_minimum_rounds THEN 0
            ELSE ROUND((AVG(rs2.raw_score) - rs1.handicap_base_score) * rs1.handicap_multiplier, 0)
          END AS next_handicap
        , STRING_AGG(CAST(rs2.raw_score AS STRING), ',' ORDER BY rs2.rn) AS next_handicap_scores
        , COUNT(*) AS lookback_count
    FROM ranked_scores rs1
    JOIN ranked_scores rs2
        ON rs2.league_id = rs1.league_id
       AND rs2.player_username = rs1.player_username
       AND rs2.rn BETWEEN rs1.rn AND rs1.rn + rs1.handicap_rounds_considered - 1
    WHERE EXTRACT(YEAR FROM rs1.event_end_date) >= EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    GROUP BY
          rs1.league_id
        , rs1.player_username
        , rs1.raw_score_id
        , rs1.handicap_base_score
        , rs1.handicap_multiplier
        , rs1.handicap_minimum_rounds
        , rs1.event_end_date
        , rs1.raw_score
)
SELECT
      league_id
    , raw_score_id
    , player_username
    , CAST(GREATEST(next_handicap, 0) AS INT64) AS next_handicap
    , next_handicap_scores
    , event_end_date
    , raw_score
    , CAST(
        LAG(GREATEST(next_handicap, 0)) OVER (
        PARTITION BY league_id, player_username
        ORDER BY event_end_date
            ) 
        AS INT64
        ) AS handicap
    , LAG(next_handicap_scores) OVER (
        PARTITION BY league_id, player_username
        ORDER BY event_end_date
      ) AS handicap_scores
    , CURRENT_TIMESTAMP() AS create_time
FROM next_handicaps;
