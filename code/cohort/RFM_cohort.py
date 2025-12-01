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

# Delta 읽기 (+ 특정 월 필터)
events_df = (
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)

# 전체 기간
events_df.createOrReplaceTempView("all_events") # TempView등록 (이름 설정)

new_monthly_rfm_df = (
    spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", "public.new_monthly_rfm")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .load()
)
new_monthly_rfm_df.createOrReplaceTempView("new_monthly_rfm")


# SQL로 지표산출
cohort_sql = """
WITH month_date AS (
    SELECT 
        user_id,
        event_time,
        price
    FROM all_events
    WHERE event_type = 'purchase'
),

first_date AS (
    SELECT 
        user_id,
        date_trunc('month', MIN(event_time)) AS cohort_date
    FROM month_date
    GROUP BY user_id
),

user_month AS (
    SELECT
        user_id,
        date_trunc('month', event_time) AS order_date,
        SUM(price) AS revenue
    FROM month_date
    GROUP BY user_id, date_trunc('month', event_time)
),

cohort_base AS (
    SELECT
        f.user_id,
        f.cohort_date,
        u.order_date,
        CAST(months_between(u.order_date, f.cohort_date) AS INT) AS cohort_age,
        u.revenue
    FROM first_date f
    JOIN user_month u 
        ON f.user_id = u.user_id
),

monthly_rfm AS (
    SELECT 
        user_id,
        date_trunc('month', snapshot_date) AS snap_month,
        rfm_segment
    FROM new_monthly_rfm
    GROUP BY user_id, date_trunc('month', snapshot_date), rfm_segment
),

cohort_rfm_join AS (
    SELECT 
        c.user_id,
        c.cohort_date,
        m.snap_month,
        c.order_date,
        c.cohort_age,
        c.revenue,
        m.rfm_segment
    FROM cohort_base c
    LEFT JOIN monthly_rfm m
        ON c.user_id = m.user_id
       AND c.order_date = m.snap_month
),

cohort_rfm_stat AS (
    SELECT 
        rfm_segment,
        cohort_date,
        order_date,
        cohort_age,
        COUNT(DISTINCT user_id) AS active_number,
        SUM(revenue)          AS active_revenue
    FROM cohort_rfm_join
    GROUP BY rfm_segment, cohort_date, cohort_age,order_date
) 
SELECT 
    rfm_segment,
    cohort_date,
    order_date,
    cohort_age,
    active_number,
    active_number * 1.0
       / FIRST_VALUE(active_number) OVER (
             PARTITION BY rfm_segment, cohort_date
             ORDER BY cohort_age
         ) AS cnt_retention,
    active_revenue * 1.0
       / FIRST_VALUE(active_revenue) OVER (
             PARTITION BY rfm_segment, cohort_date
             ORDER BY cohort_age
         ) AS revenue_retention
FROM cohort_rfm_stat;
"""

cohort_df = spark.sql(cohort_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
cohort_df.createOrReplaceTempView("cohort_rfm_stat")


# DB 적재
# 한개의 테이블만 적재
try:
    print("post greSQL 적재 시작")

    (cohort_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "cohort_rfm_stat")
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


