-- **H0:** 상위 세그먼트 고객의 monetary·Recency은 변하지 않았다.
-- **H1:** 최근 Cohort의 상위 세그먼트 monetary·Recency은 유의하게 감소했다.

create table analysis.high_segment_rm as
select 
case when snapshot_date in ('2019-10-31','2019-11-30','2019-12-31','2020-01-31','2020-02-29') then 'past'
when snapshot_date in ('2020-03-31','2020-04-30') then 'current' end as point_of_view,
user_id,
avg(recency) as avg_recency,
avg(month_monetary) as avg_montetary
from mart.new_monthly_rfm where rfm_segment in ('VIP','성장 고객','우수 고객') group by 1,2;
