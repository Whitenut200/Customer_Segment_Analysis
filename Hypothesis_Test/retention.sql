-- **H0:** 최근 Cohort의 초기 유지율(1개월 Retention)은 과거와 차이가 없다.
-- **H1:** 최근 Cohort의 초기 유지율은 과거 Cohort 대비 유의하게 낮다.

create table analysis.retention as
with base as
(select 
cohort_date,
first_user,
active_users
from mart.cohort_stat 
where cohort_age =1 and cohort_Date!='2020-04-01'
)
select
'past' as "point of view",
    AVG(CASE WHEN cohort_date IN 
        ('2019-10-01','2019-11-01','2019-12-01','2020-01-01')
        THEN first_user END) AS first_user,
    AVG(CASE WHEN cohort_date IN 
    	('2019-10-01','2019-11-01','2019-12-01','2020-01-01')
    	then active_users end) as active_user
 from base
 union all
 select
 'current' as "point of view",
    AVG(CASE WHEN cohort_date IN 
        ('2020-03-01','2020-02-01')
        THEN first_user END) AS first_user,
    AVG(CASE WHEN cohort_date IN 
        ('2020-03-01','2020-02-01')
        THEN active_users END) AS active_user
FROM base;
