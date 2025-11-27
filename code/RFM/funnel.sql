create table monthly_rfm_funnel as(
with table2 as (
select 
	user_id,
	max(case when snapshot_date ='2019-10-31' then rfm_segment end) as "2019-10",
	max(case when snapshot_date ='2019-11-30' then rfm_segment end) as "2019-11",
	max(case when snapshot_date ='2019-12-31' then rfm_segment end) as "2019-12",
	max(case when snapshot_date ='2020-01-31' then rfm_segment end) as "2020-01",
	max(case when snapshot_date ='2020-02-29' then rfm_segment end) as "2020-02",
	max(case when snapshot_date ='2020-03-31' then rfm_segment end) as "2020-03",
	max(case when snapshot_date ='2020-04-30' then rfm_segment end) as "2020-04"
	from monthly_rfm_userid_catagory group by 1) 
select * from table2 )
