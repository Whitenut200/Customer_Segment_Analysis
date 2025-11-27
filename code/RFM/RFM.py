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

# Delta 읽기
events_df = (
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)


# 전체 기간
events_df.createOrReplaceTempView("all_events") # TempView등록 (이름 설정)


# SQL로 지표산출
RFM_base_sql = """
with standard_date as (
    SELECT 
        date_add(max(to_date(event_time)),1) as standard_date
    from all_events
    where event_type='purchase'
),
RFM_base as(
    SELECT
        A.user_id,
            
        -- Recency
        max(A.event_time) as last_order_ts,
        max(to_date(A.event_time)) as last_order_date,
            
        -- 차이
        sd.standard_date,
        datediff(sd.standard_date, max(to_date(A.event_time))) as recency,
            
        -- Frequency
        count(*) as frequency,
                           
        -- Monetary
        sum(A.price) as monetary
            
    FROM all_events A 
    cross join standard_date sd
    where event_type='purchase'
    GROUP BY a.user_id,sd.standard_date)
select 
    user_id, last_order_ts, last_order_date, standard_date,
    recency, frequency, monetary FROM RFM_base
"""

daily_metrics_df = spark.sql(RFM_base_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
daily_metrics_df.createOrReplaceTempView("rfm_base")

# partition by 를 쓰면 같은 recency끼리 묶여서 숫자가 부여됨
# asc는 높을 수록 점수가 높고, desc는 낮을 수록 점수가 높음
RFM_sql ="""
with RFM_score as(
select
    user_id,
    recency,
    frequency,
    monetary,
    ntile(5) over (order by recency desc) as recency_score, 
    ntile(5) over (order by frequency asc) as frequency_score, 
    ntile(5) over (order by monetary asc) as monetary_score
from rfm_base
),
RFM_user as(
select 
    user_id,
    CASE
        WHEN recency_score = 5 AND frequency_score = 5 AND monetary_score = 5 
            THEN 'VIP'

        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
            THEN '우수 고객'

        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3
            THEN '성장 고객'

        WHEN recency_score = 5 AND frequency_score = 1
            THEN '신규 고객'

        WHEN recency_score = 1 AND frequency_score <= 2
            THEN '이탈 고위험 고객'

        WHEN recency_score = 2 AND frequency_score <= 2
            THEN '이탈 예정 고객'

        ELSE '일반 고객'
    END AS rfm_segment,
    recency,
    recency_score,
    frequency,
    frequency_score,
    monetary,
    monetary_score
from RFM_score)
select * from RFM_user
"""
RFM_score_df = spark.sql(RFM_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
RFM_score_df.createOrReplaceTempView("rfm_score")


RFM_stat_sql ="""
select
rfm_segment,
count(*) as cnt,
COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS user_ratio
from rfm_score
group by rfm_segment
"""

RFM_stat_df = spark.sql(RFM_stat_sql)
print("RFM_stat_sql 예시")
RFM_stat_df.show(20, truncate=False)

# 여기서 Temp View로 등록 후 바로 사용 가능
RFM_stat_df.createOrReplaceTempView("rfm_stat")

RFM_sales_sql ="""
WITH rfm_merged AS (
    SELECT
        a.user_id,
        b.rfm_segment,
        a.frequency,
        a.recency,
        a.monetary
    FROM rfm_base a
    LEFT JOIN rfm_score b
        ON a.user_id = b.user_id
),
rfm_sales AS (
    SELECT
        rfm_segment,
        COUNT(DISTINCT user_id)                          AS user_cnt,         -- 세그먼트별 유저 수
        SUM(monetary)                                    AS monetary,        -- 총 매출
        SUM(monetary) * 1.0 / SUM(SUM(monetary)) OVER () AS monetary_ratio,  -- 전체 대비 매출 비중
        AVG(monetary)                                    AS arpu,            -- 유저당 평균 매출
        AVG(frequency)                                   AS frequency_avg,   -- 평균 주문 횟수
        AVG(recency)                                     AS recency_avg      -- 평균 Recency(일)
    FROM rfm_merged
    GROUP BY rfm_segment
)
SELECT *
FROM rfm_sales

"""
RFM_sales_df = spark.sql(RFM_sales_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
RFM_sales_df.createOrReplaceTempView("rfm_detail_stat")



# 테이블 목록
tables_to_write = [
    ("public.rfm_base", daily_metrics_df),
    ("public.rfm_score", RFM_score_df),
    ("public.rfm_stat", RFM_stat_df),
    ("public.rfm_detail_stat", RFM_sales_df)
]

# PostgreSQL 테이블 존재 여부 확인 함수
def table_exists(table_name: str) -> bool:
    """public 스키마에 테이블이 존재하는지 확인"""
    
    schema, tbl = table_name.split(".")   # "public.rfm_base" → public / rfm_base

    check_sql = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='{schema}'
      AND table_name='{tbl}'
    """

    try:
        result = (spark.read
                    .format("jdbc")
                    .option("url", db_url)
                    .option("user", db_user)
                    .option("password", db_password)
                    .option("query", check_sql)
                    .load())
        return result.count() > 0
    except:
        return False


# 적재
for table_name, df in tables_to_write:
    try:
        print(f"\n[{table_name}] 적재 준비")

        if table_exists(table_name):
            print(f" [{table_name}] 이미 존재 → 적재 스킵")
            continue

        print(f"[{table_name}] 테이블 없음 → 신규 생성 & 적재 진행")

        (df.write
           .format("jdbc")
           .option("url", db_url)
           .option("dbtable", table_name)
           .option("user", db_user)
           .option("password", db_password)
           .option("driver", db_driver)
           .mode("overwrite")     # 존재 안 하면 overwrite로 새로 생성
           .save()
        )

        print(f"[{table_name}] 적재 완료!")

    except Exception as e:
        print(f"[{table_name}] 적재 중 에러 발생:")
        print(e)

