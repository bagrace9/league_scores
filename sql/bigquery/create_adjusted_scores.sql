-- =============================================================================
-- BigQuery create_adjusted_scores.sql
-- Rebuilds adjusted_scores from raw scores and handicaps each run.
-- =============================================================================


CREATE OR REPLACE TABLE `{dataset_name}.adjusted_scores` AS
WITH scores_with_ranks AS (
    SELECT
          rs.raw_score_id
        , rs.event_id
        , rs.league_id
        , rs.division
        , rs.player_name
        , rs.player_username
        , rs.raw_score
        , h.handicap
        , COALESCE(rs.raw_score - h.handicap, rs.raw_score) AS adjusted_score

        , RANK() OVER (
                      PARTITION BY rs.league_id, rs.division, rs.event_id
                      ORDER BY COALESCE(rs.raw_score - h.handicap, rs.raw_score)
                    ) AS place

        , COUNT(*) OVER (
                          PARTITION BY rs.league_id, rs.division,rs.event_id, COALESCE(rs.raw_score - h.handicap, rs.raw_score)
                        ) AS tie_count

        , COUNT(*) OVER (PARTITION BY rs.league_id, rs.division, rs.event_id) AS num_players
    FROM raw_scores rs
    LEFT JOIN handicaps h
        ON rs.raw_score_id = h.raw_score_id
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
),

scores_with_payouts AS (
SELECT
      swr.raw_score_id
    , swr.event_id
    , swr.league_id
    , l.league_name
    , e.event_end_date
    , extract(year from e.event_end_date) as year
    , swr.division
    , swr.player_name
    , swr.player_username
    , swr.raw_score
    , swr.handicap
    , swr.adjusted_score
    , swr.place
    , case when e.is_excluded_from_points = true 
           then 0
           else (swr.num_players - swr.place + 1) * COALESCE(e.points_multiplier, 1)
      end as points
    , CASE
        -- Only track payouts for events from 2016 onwards (historical league tracking start)
        WHEN extract(year from e.event_end_date) >= 2016
        THEN ROUND(swr.num_players * COALESCE(e.buy_in_override_ammt, l.league_entry_fee) * (1 - (l.league_cash_percentage / 100)) * pwt.split_percentage, 0)
        ELSE 0
      END AS payout

FROM scores_with_ranks swr
join events e
    on e.event_id = swr.event_id
LEFT JOIN payouts_with_ties pwt
    ON pwt.raw_score_id = swr.raw_score_id
JOIN leagues l
    ON l.league_id = swr.league_id

)



SELECT
      swp.raw_score_id
    , swp.event_id
    , swp.league_id
    , swp.league_name
    , swp.event_end_date
    , swp.year
    , swp.division
    , swp.player_name
    , swp.player_username
    , swp.raw_score
    , swp.handicap
    , swp.adjusted_score
    , swp.place
    , swp.points
    , swp.payout
    ,sum(points) 
           over (partition by swp.league_id, swp.division, swp.player_username , swp.year order by swp.event_end_date) as season_points_as_of_event
    

FROM scores_with_payouts swp
;