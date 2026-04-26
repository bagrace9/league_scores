-- =============================================================================
-- BigQuery create_final_scores.sql
-- Rebuilds final_scores from raw scores and handicaps each run.
-- =============================================================================


CREATE OR REPLACE TABLE `{dataset_name}.adjusted_scores` AS
WITH scores_with_ranks AS (
    SELECT
          rs.raw_score_id
        , rs.event_id
        , e.event_end_date 
        , extract(year from e.event_end_date) as year
        , rs.league_id
        , rs.division
        , rs.player_name
        , rs.player_username
        , rs.raw_score
        , h.handicap
        , h.handicap_scores
        , h.next_handicap
        , h.next_handicap_scores
        , COALESCE(rs.raw_score - h.handicap, rs.raw_score) AS adjusted_score

        , RANK() OVER (
                      PARTITION BY rs.league_id, rs.division, rs.event_id
                      ORDER BY COALESCE(rs.raw_score - h.handicap, rs.raw_score)
                    ) AS place

        , COUNT(*) OVER (
                          PARTITION BY rs.league_id, rs.division,rs.event_id, COALESCE(rs.raw_score - h.handicap, rs.raw_score)
                        ) AS tie_count

        , RANK() OVER (
                        PARTITION BY rs.league_id, rs.division, rs.event_id
                        ORDER BY COALESCE(rs.raw_score - h.handicap, rs.raw_score) DESC
                      ) AS points

        , COUNT(*) OVER (PARTITION BY rs.league_id, rs.division, rs.event_id) AS num_players
    FROM raw_scores rs
    join events e 
        on e.event_id = rs.event_id
    LEFT JOIN handicaps h
        ON rs.raw_score_id = h.raw_score_id
    WHERE e.is_excluded = FALSE
),
payouts_with_ties AS (
    SELECT
          swr.raw_score_id
        , COALESCE(SUM(pp.percentage), 0) / swr.tie_count AS split_percentage
    FROM scores_with_ranks swr
    LEFT JOIN payouts pp
        ON pp.n_players = swr.num_players
       AND pp.position BETWEEN swr.place AND (swr.place + swr.tie_count - 1)
    GROUP BY
          swr.raw_score_id
        , swr.tie_count
)


SELECT
      swr.event_id
    , swr.league_id
    , l.league_name
    , swr.event_end_date
    , swr.year
    , swr.division
    , swr.player_name
    , swr.player_username
    , swr.raw_score
    , swr.handicap
    , swr.handicap_scores
    , swr.next_handicap
    , swr.next_handicap_scores
    , swr.adjusted_score
    , swr.place
    , swr.points
    , CASE
        WHEN swr.year >= 2016 THEN ROUND((swr.num_players * l.league_entry_fee) * (1 - (l.league_cash_percentage / 100)) * pwt.split_percentage, 0)
        ELSE NULL
      END AS payout
    , sum(points) over (partition by swr.league_id, swr.division, swr.player_username , swr.year order by swr.event_end_date) as season_points_as_of_event

FROM scores_with_ranks swr
LEFT JOIN payouts_with_ties pwt
    ON pwt.raw_score_id = swr.raw_score_id
JOIN leagues l
    ON l.league_id = swr.league_id

;