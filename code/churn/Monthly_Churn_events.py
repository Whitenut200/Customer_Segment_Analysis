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

monthly_churn_df = (
    spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", "public.monthly_churn") 
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .load()
)
monthly_churn_df.createOrReplaceTempView("monthly_churn")

snapshot_months = [
    "2019-10-31", "2019-11-30", "2019-12-31",
    "2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"
]

all_data = []

for snap in snapshot_months:
    churn_interval_sql = f"""
    WITH churn_users AS (
        SELECT DISTINCT
            user_id,
            segment
        FROM monthly_churn
        WHERE snapshot_date = DATE '{snap}' 
    ),
    events_7d AS (
        SELECT
            e.user_id,
            c.segment,
            e.event_type,
            '7 day' AS interval,
            COUNT(*) AS event_cnt
        FROM all_events e
        JOIN churn_users c ON e.user_id = c.user_id
        WHERE e.event_time >= DATE '{snap}' - INTERVAL '7 day'
          AND e.event_time <  DATE '{snap}'
        GROUP BY e.user_id, c.segment, e.event_type
    ),
    events_3d AS (
        SELECT
            e.user_id,
            c.segment,
            e.event_type,
            '3 day' AS interval,
            COUNT(*) AS event_cnt
        FROM all_events e
        JOIN churn_users c ON e.user_id = c.user_id
        WHERE e.event_time >= DATE '{snap}' - INTERVAL '3 day'
          AND e.event_time <  DATE '{snap}'
        GROUP BY e.user_id, c.segment, e.event_type
    ),
    events_14d AS (
        SELECT
            e.user_id,
            c.segment,
            e.event_type,
            '14 day' AS interval,
            COUNT(*) AS event_cnt
        FROM all_events e
        JOIN churn_users c ON e.user_id = c.user_id
        WHERE e.event_time >= DATE '{snap}' - INTERVAL '14 day'
          AND e.event_time <  DATE '{snap}'
        GROUP BY e.user_id, c.segment, e.event_type
    ),
    total AS (
        SELECT * FROM events_3d
        UNION ALL
        SELECT * FROM events_7d
        UNION ALL
        SELECT * FROM events_14d
    )
    SELECT
        segment,
        event_type,
        interval,
        SUM(event_cnt) AS event_cnt
    FROM total
    GROUP BY segment, event_type, interval
    """

    churn_interval_df = spark.sql(churn_interval_sql)
    all_data.append(churn_interval_df)

# 모든 월 데이터 union
if len(all_data) == 0:
    raise ValueError("all_data 없음")

final_df = reduce(DataFrame.unionByName, all_data)

final_df.show()

churn_category_sql = """
WITH last_purchase AS (
    SELECT
        user_id,
        MAX(event_time) AS last_purchase_time
    FROM all_events
    WHERE event_type = 'purchase'
    GROUP BY user_id
),
user_segment AS (
    SELECT
        user_id,
        segment,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM monthly_churn
)
SELECT
    us.segment,
    e.category_id,
    e.category_code,
    COUNT(*) AS last_purchase_cnt
FROM last_purchase lp
JOIN all_events e
  ON lp.user_id = e.user_id
 AND lp.last_purchase_time = e.event_time
JOIN user_segment us
  ON lp.user_id = us.user_id
 AND us.rn = 1
GROUP BY
    us.segment,
    e.category_id,
    e.category_code
ORDER BY
    us.segment,
    last_purchase_cnt DESC
"""

churn_category_df = spark.sql(churn_category_sql)

all_data2=[]
for snap in snapshot_months:
    churn_funnel_sql = f"""
    with base as(
    select
        user_id,
        snapshot_date,
        segment
    from monthly_churn
    where snapshot_date=date '{snap}'
    group by 1,2,3
    ),
    events as(
    select
        user_id,
        event_type,
        date '{snap}' as snapshot_date,
        count(*) as cnt
    from all_events 
    where date_trunc('month',date '{snap}') = date_trunc('month',to_date(event_time))
    group by 1,2,3 
    )
    select B.snapshot_date , segment, event_type, sum(cnt) as total_count
    from base B join events E 
    on B.user_id=E.user_id
    group by 1,2,3
    """
    churn_funnel_df = spark.sql(churn_funnel_sql)
    all_data2.append(churn_funnel_df)

# 모든 월 데이터 union
if len(all_data2) == 0:
    raise ValueError("all_data2 없음")
 
final_df2 = reduce(DataFrame.unionByName, all_data2)

final_df2.show()


# 테이블 목록
tables_to_write = [
    ("public.churn_interval", final_df),
    ("public.churn_category", churn_category_df)
    ("public.churn_monthly_funnel", final_df2)
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


# 변경된 적재 루프
for table_name, df in tables_to_write:
    try:

        if table_exists(table_name):
            print(f" [{table_name}] 이미 존재 → 적재 스킵")
            continue

        print(f" [{table_name}] 테이블 없음 → 신규 생성 & 적재 진행")

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

        print(f" [{table_name}] 적재 완료!")

    except Exception as e:
        print(f" [{table_name}] 적재 중 에러 발생:")
        print(e)
