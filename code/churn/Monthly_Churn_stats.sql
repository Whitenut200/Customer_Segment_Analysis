create table monthly_churn_stats as(
select
	snapshot_date,
	segment,
	count(distinct user_id) as cnt,
	sum("LTV") as total_LTV
--	,count(distinct user_id)/sum("LTV") as ARPU
from public.monthly_churn group by 1,2)
