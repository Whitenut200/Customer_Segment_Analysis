create table churn_moving as
WITH user_history AS (
    SELECT
        user_id,
        snapshot_date,
        segment AS curr_segment,
        "LTV",
        LAG(segment) OVER (
            PARTITION BY user_id
            ORDER BY snapshot_date
        ) AS prev_segment,
        LAG("LTV") OVER (
            PARTITION BY user_id
            ORDER BY snapshot_date
        ) AS prev_LTV
    FROM monthly_churn
),
transition AS (
    SELECT
        snapshot_date,
        user_id,
        prev_segment,
        curr_segment,
        "LTV",
        prev_LTV
    FROM user_history
    WHERE prev_segment IS NOT NULL
),
agg_pair AS (
    SELECT
        snapshot_date,
        prev_segment,
        curr_segment,
        COUNT(DISTINCT user_id) AS move_users,
        SUM("LTV")             AS curr_total_ltv,
        SUM(prev_LTV)          AS prev_total_ltv
    FROM transition
    GROUP BY snapshot_date, prev_segment, curr_segment
),
agg_prev AS (
    SELECT
        snapshot_date,
        prev_segment,
        COUNT(DISTINCT user_id) AS prev_total_users
    FROM transition
    GROUP BY snapshot_date, prev_segment
)
SELECT
    p.snapshot_date,
    p.prev_segment,
    p.curr_segment,
    p.move_users,
    a.prev_total_users,
    ROUND(
        p.move_users * 1.0 / a.prev_total_users * 100,
        2
    ) AS transition_rate_pct,
    p.curr_total_ltv,
    p.prev_total_ltv
FROM agg_pair p
JOIN agg_prev a
  ON p.snapshot_date = a.snapshot_date
 AND p.prev_segment  = a.prev_segment
ORDER BY
    p.snapshot_date,
    p.prev_segment,
    p.curr_segment;
