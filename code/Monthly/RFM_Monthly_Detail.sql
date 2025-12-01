drop table public.monthly_rfm_detail_stat;

create table public.monthly_rfm_detail_stat as
select
	snapshot_date,
	rfm_segment,
	avg(recency) as avg_recency,
	avg(month_freq) as avg_freqency,
	avg(month_monetary) as arpu,
	sum(month_monetary) as total_monetary,
	sum(month_monetary) / sum(sum(month_monetary)) over (partition by snapshot_date ) as monetary_ratio
	from public.new_monthly_rfm group by 1,2;
