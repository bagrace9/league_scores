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

Drop TABLE if exists handicaps
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
        , rs.player_username
        , rs.raw_score
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
      and EXTRACT(YEAR FROM e.event_end_date) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
      and l.league_is_handicap = true
      and e.is_excluded = false

      --- only consider players for handicap calculation if they have the 
      --- minimum required rounds either this or last year
      and exists (select 1
                  from raw_scores rs2
                  join events e2
                      on rs2.event_id = e2.event_id
                  where rs2.league_id = l.league_id
                    and rs2.player_username = rs.player_username
                    and EXTRACT(YEAR FROM e2.event_end_date) >= EXTRACT(YEAR FROM CURRENT_DATE) - l.handicap_years_lookback
                    and e2.is_excluded = false
                  group by 
                        rs2.league_id
                      , rs2.player_username
                      , EXTRACT(YEAR FROM e2.event_end_date)
                  having count(1) >= l.handicap_minimum_rounds
                    )
)
SELECT
      rs1.league_id
    , rs1.raw_score_id
    , rs1.player_username
    , ROUND((AVG(rs2.raw_score) - rs1.handicap_base_score) * rs1.handicap_multiplier, 0) AS handicap
    , CURRENT_TIMESTAMP AS create_time
FROM ranked_scores rs1
-- rs2 is the lookback window: the N rounds scored just before rs1's round.
join ranked_scores rs2
    on rs2.league_id = rs1.league_id
    and rs2.player_username = rs1.player_username
    and rs2.rn BETWEEN rs1.rn + 1 and rs1.rn + rs1.handicap_rounds_considered
    -- Only produce a handicap row for each player's most-recent rounds.
    WHERE rs1.rn <= rs1.handicap_rounds_considered
GROUP BY
      rs1.league_id
    , rs1.player_username
    ,rs1.raw_score_id
    ,rs1.handicap_base_score
    ,rs1.handicap_multiplier
-- Exclude players whose average does not exceed the base score
-- (already scoring at or below scratch; no positive handicap applies).
having (AVG(rs2.raw_score) > rs1.handicap_base_score)

;
