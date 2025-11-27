from pyspark.sql import SparkSession, functions as F
from functools import reduce
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
    .master("local[*]")  # 로컬 전체 코어 사용
    # Delta Lake 설정 (기존 그대로 유지)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # JDBC(PostgreSQL) 드라이버
    .config("spark.jars", r"D:\project\segment\postgresql-42.7.8.jar")
    # 메모리/성능 설정 추가
    .config("spark.driver.memory", "12g")              # 드라이버 메모리 증가
    .config("spark.sql.shuffle.partitions", "300")     # 셔플 파티션 수 조정
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

DELTA_PATH = "D:/project/segment/warehouse/events_delta"

# 1. Delta 읽기
events_df = (
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)

snapshot_months = [
    "2019-10-31", "2019-11-30", "2019-12-31",
    "2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"
]
all_data = []

for snap in snapshot_months:

    # snapshot 기준 이전까지 데이터 (누적) - Recency용
    history_df = events_df.filter(F.to_date("event_time") <= F.lit(snap))
    history_df.createOrReplaceTempView("hist_events")

    # Recency 계산 (누적 기준)
    # standard_date = hist_events에서 가장 마지막 구매일 + 1일
    last_purchase_sql = """
    WITH standard_date AS (
        SELECT 
            date_add(MAX(to_date(event_time)), 1) AS standard_date
        FROM hist_events
        WHERE event_type = 'purchase'
    ),
    last_purchase AS (
        SELECT
            user_id,
            MAX(to_date(event_time)) AS last_purchase
        FROM hist_events
        WHERE event_type = 'purchase'
        GROUP BY user_id
    )
    SELECT 
        l.user_id,
        s.standard_date,
        datediff(s.standard_date, l.last_purchase) AS recency
    FROM last_purchase l
    CROSS JOIN standard_date s
    """
    recency_df = spark.sql(last_purchase_sql)

    # 해당 월의 F/M 계산 (당월 기준)
    # snap이 '2019-11-30'이면 2019-11 전체만 포함
    RFM_base_monthly = f"""
    WITH month_stat AS (
        SELECT
            user_id,
            COUNT(*)  AS month_freq,
            SUM(price) AS month_monetary
        FROM hist_events
        WHERE event_type = 'purchase'
          AND date_trunc('month', to_date(event_time)) 
              = date_trunc('month', DATE '{snap}')
        GROUP BY user_id
    )
    SELECT * FROM month_stat
    """
    monthly_FM_df = spark.sql(RFM_base_monthly)

    # Recency + F/M 조인 + snapshot_date 컬럼 추가
    base_df = (
        monthly_FM_df
        .join(recency_df, on="user_id", how="left")
        .withColumn("snapshot_date", F.lit(snap))
    )

    all_data.append(base_df)

# 모든 스냅샷 Union
if not all_data:
    raise ValueError("스냅샷 데이터가 없습니다.")

monthly_rfm_base_df = reduce(lambda df1, df2: df1.unionByName(df2), all_data)
monthly_rfm_base_df.createOrReplaceTempView("monthly_rfm_base")

# RFM 스코어 + 세그먼트 분류
monthly_RFM_SQL = """
WITH rfm_score AS (
    SELECT
        user_id,
        snapshot_date,
        recency,
        month_freq,
        month_monetary,
        ntile(5) OVER (
            PARTITION BY snapshot_date 
            ORDER BY recency DESC
        ) AS recency_score,
        ntile(5) OVER (
            PARTITION BY snapshot_date 
            ORDER BY month_freq ASC
        ) AS frequency_score,
        ntile(5) OVER (
            PARTITION BY snapshot_date 
            ORDER BY month_monetary ASC
        ) AS monetary_score
    FROM monthly_rfm_base
),
rfm_user AS (
    SELECT 
        user_id,
        snapshot_date,
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
        month_freq,
        frequency_score,
        month_monetary,
        monetary_score
    FROM rfm_score
)
SELECT * FROM rfm_user
"""

monthly_RFM_df = spark.sql(monthly_RFM_SQL)

# PostgreSQL 적재
try:
    print("PostgreSQL 적재 시작")

    (monthly_RFM_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "new_monthly_rfm")   # 하나의 테이블에 월별 append
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .mode("append")
        .save()
    )

    print("monthly_rfm 전체 적재 완료")

except Exception as e:
    print("적재 실패")
    print(e)
