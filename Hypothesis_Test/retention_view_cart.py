from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# DB 정보
db_url = "jdbc:postgresql://localhost:5432/postgres"
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

# Delta 읽기
events_df = (
    spark.read
         .format("delta")
         .load(DELTA_PATH)
)

# 전체 기간
events_df.createOrReplaceTempView("all_events") # TempView등록 (이름 설정)


# SQL로 지표산출
retention_sql = """
with first_purchase as(
select 
    user_id,
    to_date(date_trunc('month', MIN(event_time))) AS first_date
from all_events where event_type='purchase' group by 1
),
ageisone as(
select
    a.user_id,
    f.first_date,
    e.category_id,
    a.event_type
from all_events a 
join first_purchase f -- 첫구매가 없으면 고려 하지 않음 left join X
on a.user_id=f.user_id 
where to_date(date_trunc('month', a.event_time)) = add_months(f.first_date, 1)
    )
select 
    user_id,
    first_date,
    count(distinct category_id) as category_cnt,
    sum(case when event_type='view' then 1 else 0 end) as view_cnt,
    sum(case when event_type='cart' then 1 else 0 end) as cart_cnt,
    MAX(case when event_type='purchase' then 1 else 0 end) as TF_purchase
from ageisone group by 1,2
"""

retention_df = spark.sql(retention_sql)

# 여기서 Temp View로 등록 후 바로 사용 가능
retention_df.createOrReplaceTempView("retention")


# DB 적재
# 한개의 테이블만 적재
try:
    print("post greSQL 적재 시작")

    (retention_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "analysis.retention_view_cart")
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

