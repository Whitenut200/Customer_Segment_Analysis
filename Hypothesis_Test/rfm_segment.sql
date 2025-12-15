-- **H0:** 세그먼트(VIP/우수/성장/일반) 간 초기 유지율에 차이가 없다.
-- **H1:** 세그먼트 간 초기 유지율에는 유의한 차이가 있다.

create table analysis.rfm_segment as
with base as(
select
	rfm_segment,
	cast(cohort_date as date) as curr_period,
	SUM(case when cohort_age=0 then active_number end ) as total_number,
	sum(case when cohort_age=1 then active_number end) as action_number
from mart.cohort_rfm_stat group by 1,2)
select rfm_segment,curr_period, action_number, (total_number-action_number) as non_action_number
from base where curr_period in ('2020-03-01','2020-02-01') and rfm_segment is not null
