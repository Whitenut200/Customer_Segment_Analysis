create table churn_user_ltv_stat as
with churn_user as
(select 
 user_id
from monthly_churn where snapshot_date ='2020-04-30' and segment='이탈 고객'),
churn_user_stat as(
select
	count(distinct c.user_id) as churn_total_cnt,
	sum("LTV") as churn_ago_total_LTV
from churn_user C  left join monthly_churn M 
on C.user_id=M.user_id
),
action_ARPU as(
select 	
	count(distinct case when segment='활성 고객' then user_id end ) as four_action_cnt,
	sum("LTV")/count(distinct user_id) as four_ARPU
from monthly_churn mc where snapshot_date='2020-04-30'),
churn_ratio_stat as(
select 
	snapshot_date,
	move_users 
from churn_moving where curr_segment='이탈 고객' and prev_segment!= '이탈 고객' and snapshot_date in ('2020-02-29','2020-03-31','2020-04-30')
),
monthly_user as(
select	
	snapshot_date,
	count(distinct user_id) as cnt
from monthly_churn 
where snapshot_date in ('2020-02-29','2020-03-31','2020-04-30') 
group by 1 ),
churn_ratio as(
select 
sum(case when c.snapshot_date ='2020-02-29' then move_users end ) / 
sum(case when M.snapshot_date = '2020-02-29' then cnt end) as two_month,
sum(case when c.snapshot_date ='2020-03-31' then move_users end ) / 
sum(case when M.snapshot_date = '2020-03-31' then cnt end) as three_month,
sum(case when c.snapshot_date ='2020-04-30' then move_users end ) / 
sum(case when M.snapshot_date = '2020-04-30' then cnt end) as four_month
from churn_ratio_stat C join monthly_user M
on c.snapshot_date=m.snapshot_date)
select
churn_total_cnt,
churn_ago_total_LTV,
four_action_cnt,
two_month,
three_month,
four_month,
four_ARPU,
four_action_cnt*(two_month+three_month+four_month)/3 * four_ARPU as lose_LTV
from action_ARPU cross join churn_ratio
cross join churn_user_stat ;
