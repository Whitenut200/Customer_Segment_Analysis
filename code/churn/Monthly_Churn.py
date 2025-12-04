from pyspark.sql import SparkSession, functions as F
from functools import reduce
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

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
    # Delta Lake 설정
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

snapshot_months = [
    "2019-10-31", "2019-11-30", "2019-12-31",
    "2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"
]
all_data = []

for snap in snapshot_months:
    history_df = events_df.filter(F.to_date("event_time") <= F.lit(snap))
    history_df.createOrReplaceTempView("hist_events")
  
    # standard_date = hist_events에서 가장 마지막 구매일 + 1일
    churn_sql = f"""
WITH orderdate AS (
    SELECT 
        user_id,
        to_date(event_time) AS order_date,
        price
    FROM hist_events
    WHERE event_type = 'purchase'
),

first_purchase AS (
    SELECT
        user_id,
        DATE '{snap}' AS snapshot_date,
        '신규 고객' AS segment,
        MIN(order_date) AS first_date
    FROM orderdate
    GROUP BY user_id, DATE '{snap}'
    HAVING date_trunc('month', MIN(order_date)) = date_trunc('month', DATE '{snap}')
),

standard_date AS (
    SELECT
        date_add(MAX(to_date(event_time)), 1) AS standard_date
    FROM hist_events
),

recency AS (
    SELECT
        O.user_id,
        DATE '{snap}' AS snapshot_date,
        MAX(O.order_date) AS last_date,
        datediff(MAX(S.standard_date), MAX(O.order_date)) AS recency
    FROM orderdate O
    CROSS JOIN standard_date S
    GROUP BY O.user_id, DATE '{snap}'
),

segment AS (
    SELECT
        r.user_id,
        r.snapshot_date,
        CASE 
            WHEN r.recency <= 60 THEN '활성 고객'
            WHEN r.recency > 60 AND r.recency <= 90 THEN '이탈 위험 고객'
            WHEN r.recency > 90 THEN '이탈 고객'
        END AS segment
    FROM recency r
    LEFT ANTI JOIN first_purchase fp 
      ON r.user_id = fp.user_id
     AND r.snapshot_date = fp.snapshot_date
),

total AS (
    SELECT user_id, snapshot_date, segment, 1 AS priority
    FROM first_purchase

    UNION ALL

    SELECT user_id, snapshot_date, segment, 2 AS priority
    FROM segment
),

final_segment AS (
    SELECT
        user_id,
        snapshot_date,
        segment
    FROM (
        SELECT
            user_id,
            snapshot_date,
            segment,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, snapshot_date
                ORDER BY priority
            ) AS rn
        FROM total
    ) t
    WHERE rn = 1
)

SELECT 
    f.user_id,
    f.snapshot_date,
    f.segment,
    COALESCE(SUM(o.price), 0) AS LTV
FROM final_segment f
LEFT JOIN orderdate o
  ON f.user_id = o.user_id
 AND date_trunc('month', f.snapshot_date) = date_trunc('month', o.order_date)
GROUP BY f.user_id, f.snapshot_date, f.segment
"""
    churn_df = spark.sql(churn_sql)

    all_data.append(churn_df)

# 모든 월 데이터 union
if len(all_data) == 0:
    raise ValueError("all_data 없음")

final_df = reduce(DataFrame.unionByName, all_data)

# PostgreSQL 적재
try:
    print("PostgreSQL 적재 시작")

    (final_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "Monthly_churun")   # 하나의 테이블에 월별 append
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
