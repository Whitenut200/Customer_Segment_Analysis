from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# DB 정보
db_url = "jdbc:postgresql://localhost:5432/postgres"
# db_table = "public.REM_base"
db_user = ""
db_password = ""
db_driver = "org.postgresql.Driver"

# Spark / Delta
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

# Delta 읽기
events_df = ( 
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)

# 월별 세그먼트 분류
snapshot_months = [
    "2019-10-31", "2019-11-30", "2019-12-31",
    "2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"
]

new_monthly_rfm_df = (
    spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", "public.new_monthly_rfm")  # 예: "public.all_events"
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .load()
)
new_monthly_rfm_df.createOrReplaceTempView("new_monthly_rfm")

for snap in snapshot_months:
    print(f"{snap} 계산 시작")

    # snapshot 기준 이전까지 데이터 필터링
    history_df = events_df.filter(F.to_date("event_time") <= F.lit(snap))
    monthly_rfm_user = new_monthly_rfm_df.filter(F.to_date("snapshot_date") == F.lit(snap))

    history_df.createOrReplaceTempView("all_events")
    monthly_rfm_user.createOrReplaceTempView("monthly_rfm_user")

    monthly_rfm_user_sql = f"""
    WITH user_category AS (
        SELECT 
            user_id,
            category_id
        FROM all_events
        WHERE event_type = 'purchase'
        GROUP BY user_id, category_id
    ),
    last_code_per_category AS (
        SELECT
            category_id,
            category_code,
            MAX(event_time) AS last_time
        FROM all_events
        WHERE event_type = 'purchase'
          AND category_code IS NOT NULL
          AND to_date(event_time) <= DATE '{snap}'
        GROUP BY category_id, category_code
    ),
    category_dim_at_snapshot AS (
        SELECT
            category_id,
            category_code
        FROM (
            SELECT
                category_id,
                category_code,
                last_time,
                ROW_NUMBER() OVER(
                    PARTITION BY category_id
                    ORDER BY last_time DESC
                ) AS rn
            FROM last_code_per_category
        ) t
        WHERE rn = 1
    ),
    category_event AS (
        SELECT
            uc.user_id,
            uc.category_id,
            dim.category_code,
            DATE '{snap}' AS snapshot_date
        FROM user_category uc
        LEFT JOIN category_dim_at_snapshot dim
          ON uc.category_id = dim.category_id
    ),
    base AS (
        SELECT 
            c.snapshot_date,
            c.category_id,
            c.category_code,
            s.user_id,
            s.rfm_segment,
            s.recency,
            s.month_freq,
            s.month_monetary
        FROM category_event c
        LEFT JOIN monthly_rfm_user s 
          ON c.snapshot_date = s.snapshot_date
         AND c.user_id      = s.user_id
    ),
    category_segment AS (
        SELECT 
            snapshot_date,
            category_id,
            category_code,
            rfm_segment,
            AVG(recency)          AS avg_recency,
            SUM(month_freq)       AS sum_frequency,
            SUM(month_monetary)   AS sum_monetary
        FROM base
        GROUP BY
            snapshot_date,
            category_id,
            category_code,
            rfm_segment
    ),
    category_segment_with_ratio AS (
        SELECT
            cs.*,
            cs.sum_monetary * 1.0
              / SUM(cs.sum_monetary) OVER (PARTITION BY cs.snapshot_date) AS monetary_ratio
        FROM category_segment cs
    )
    SELECT * FROM category_segment_with_ratio
    """

    monthly_rfm_category_df = spark.sql(monthly_rfm_user_sql)


    try:
        print("PostgreSQL 적재 시작")

        (monthly_rfm_category_df
            .write
            .format("jdbc")
            .option("url", db_url)
            .option("dbtable", "monthly_rfm_category")
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", db_driver)
            .mode("append")
            .save()
        )

        print(f"monthly_rfm_category_test ({snap}) 적재 완료")

    except Exception as e:
        print("적재 실패")
        print(e)
