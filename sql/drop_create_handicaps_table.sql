-- =============================================================================
-- drop_create_handicaps_table.sql
-- Rebuilds the handicaps table from scratch each run.
--
-- Algorithm:
--   1. Rank each player's scores per league, most-recent first (rn = 1).
--   2. For each round rs1, the lookback window rs2 contains the N rounds
--      scored immediately before it (rn between rs1.rn+1 and rs1.rn+N).
--   3. handicap = (avg of lookback window - base_score) * multiplier.
--   4. Players who have not met the minimum required rounds within the
--      configured lookback period are excluded entirely.
-- =============================================================================

Drop TABLE if exists handicaps cascade
;


CREATE table if not exists handicaps AS
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
    join leagues l
      ON l.league_id = rs.league_id
    where e.is_excluded = false 
      and l.league_is_handicap = true
      and e.is_excluded = false

)
, next_handicaps AS (
SELECT
      rs1.league_id
    , rs1.raw_score_id
    , rs1.player_username
    , rs1.event_end_date
    , rs1.raw_score
    , case when count(*) < rs1.handicap_minimum_rounds
           then 0
           else ROUND((AVG(rs2.raw_score) - rs1.handicap_base_score) * rs1.handicap_multiplier, 0) 
           end AS next_handicap
    , STRING_AGG(rs2.raw_score::text, ',' order by rs2.rn) AS next_handicap_scores
    , COUNT(*) AS lookback_count
FROM ranked_scores rs1
-- rs2 is the lookback window: the N rounds scored just before rs1's round.
join ranked_scores rs2
    on rs2.league_id = rs1.league_id
    and rs2.player_username = rs1.player_username
    and rs2.rn BETWEEN rs1.rn and rs1.rn + rs1.handicap_rounds_considered -1
where EXTRACT(YEAR FROM rs1.event_end_date) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
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
    , greatest(next_handicap, 0) AS next_handicap
    , next_handicap_scores
    , event_end_date
    , raw_score
    ,lag(greatest(next_handicap, 0)) 
            OVER (
            PARTITION BY league_id, player_username
            ORDER BY event_end_date
         ) AS handicap
    ,lag(next_handicap_scores) OVER (
            PARTITION BY league_id, player_username
            ORDER BY event_end_date
    ) AS handicap_scores
  
    , CURRENT_TIMESTAMP AS create_time
FROM next_handicaps


;
