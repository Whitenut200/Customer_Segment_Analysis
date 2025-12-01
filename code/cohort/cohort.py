from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# DB 정보
db_url = "jdbc:postgresql://localhost:5432/postgres"
# db_table = "public.REM_base"
db_user = ""
db_password = ""
db_driver = "org.postgresql.Driver"

# Spark / Delta 세션
builder = (
    SparkSession.builder
    .appName("delta-metrics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.jars", r"D:\project\segment\postgresql-42.7.8.jar")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

DELTA_PATH = "D:/project/segment/warehouse/events_delta"

# Delta 읽기 (+ 특정 월 필터)
events_df = (
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)

# 전체 기간
events_df.createOrReplaceTempView("all_events") # TempView등록 (이름 설정)


# SQL로 지표산출
cohort_sql = """
with month_date as(
select 
    user_id,
    event_time,
    price
from all_events
where event_type='purchase'),
first_date as(
select 
    user_id,
    date_trunc('month', MIN(event_time)) AS cohort_date
from month_date
group by user_id),
user_month as(
select
    user_id,
    date_trunc('month', event_time) as order_date,
    sum(price) as revenue
from month_date
group by 1,2),
cohort_base as(
select
    F.user_id,
    F.cohort_date,
    U.order_date,
    CAST(months_between(U.order_date, F.cohort_date) as INT) as cohort_age,
    revenue
from first_date F join user_month U 
on F.user_id=U.user_id),
cohort_age as(
select 
    cohort_date,
    cohort_age,
    count(distinct user_id) as active_users,
    sum(revenue) as period_revenue
from cohort_base
group by 1,2),
cohort_first as(
select
    cohort_date,
    count(distinct user_id) as first_user
from cohort_base
group by 1),
cohort_stat as(
select
    a.cohort_date,
    b.first_user,
    a.cohort_age,
    a.active_users,
    a.period_revenue,
    a.active_users * 1.0 / b.first_user as retention_rate,
    a.period_revenue * 1.0 / b.first_user as revenue_per_user
from cohort_age a
join cohort_first B 
on a.cohort_date= b.cohort_date)
select 
    cast(cohort_date as DATE) as cohort_date,
    cohort_age,
    first_user,
    active_users,
    retention_rate,
    period_revenue,
    revenue_per_user, 
    SUM(period_revenue) OVER (
        PARTITION BY cohort_date
        ORDER BY cohort_age
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    SUM(revenue_per_user) OVER (
        PARTITION BY cohort_date
        ORDER BY cohort_age
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue_per_user    
from cohort_stat 
order by 1,2
"""

cohort_df = spark.sql(cohort_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
cohort_df.createOrReplaceTempView("cohort_stat")


# DB 적재
# 한개의 테이블만 적재
try:
    print("post greSQL 적재 시작")

    (cohort_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "cohort_stat")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .mode("append")
        .save()
    )

    print(" {dbtable} 적재 완료")

except Exception as e:
    print("적재 실패")
    print(e)


