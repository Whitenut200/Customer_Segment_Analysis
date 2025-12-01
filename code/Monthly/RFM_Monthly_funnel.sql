create table public.monthly_rfm_detail_funnel as
with base as(
select 
	user_id,
	snapshot_date as snapshot_month,
	rfm_segment,
	month_monetary as monetary,
	lag(rfm_segment) over (partition by user_id order by snapshot_date) as prev_segment,
	lag(month_monetary) over (partition by user_id order by snapshot_date) as prev_monetary
from public.new_monthly_rfm),
transitions AS (
    SELECT
        snapshot_month,
        user_id,
        prev_segment,
        rfm_segment AS curr_segment,
        prev_monetary,
        monetary AS curr_monetary,
        (monetary - prev_monetary) AS diff_amount,
        CASE WHEN prev_monetary > 0
            THEN (monetary - prev_monetary) / prev_monetary::numeric
            ELSE NULL
        END AS uplift_pct
    FROM base
    WHERE prev_segment IS NOT NULL
)
SELECT
    snapshot_month,
    prev_segment,
    curr_segment,
    COUNT(*) AS transition_cnt,
    SUM(prev_monetary) AS total_prev_amount,
    SUM(curr_monetary) AS total_curr_amount,
    SUM(diff_amount) AS total_diff_amount,
    AVG(diff_amount) AS avg_diff_amount,
    AVG(uplift_pct) AS avg_uplift_pct
FROM transitions
GROUP BY 1,2,3
ORDER BY prev_segment, curr_segment;
