# **H0:** 상위 세그먼트 신규 유입 고객의 행동 지표(view/cart/purchase)는 과거와 차이가 없다.
# **H1:** 최근 유입된 상위 세그먼트 고객은 과거보다 행동 지표가 낮아 유지율도 낮다.


from pyspark.sql import SparkSession, functions as F
from functools import reduce
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

events_df.createOrReplaceTempView("all_events") # TempView등록 (이름 설정)

all_data = []

new_monthly_rfm_df = (
    spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", "mart.new_monthly_rfm")  # 예: "public.all_events"
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .load()
)
new_monthly_rfm_df.createOrReplaceTempView("new_monthly_rfm")

view_cart_sql = """
    with date as(
    select
        last_day(to_date(event_time)) AS snapshot_date,
        user_id,
        event_type
    from all_events
    ),
    segment as(
    select 
        to_date(snapshot_date) AS snapshot_date,
        user_id,
        rfm_segment
    from new_monthly_rfm
    ),
    total as(
    select
        d.snapshot_date,
        d.user_id,
        s.rfm_segment,
        sum(case when d.event_type='view' then 1 else 0 end) as view_cnt,
        sum(case when d.event_type='cart' then 1 else 0 end) as cart_cnt,
        sum(case when d.event_type='purchase' then 1 else 0 end) as purchase_cnt
        from date D join segment S
        on D.snapshot_date=S.snapshot_date and D.user_id=S.user_id
        group by 1,2,3
    )
    select
    case when snapshot_date in (DATE '2019-10-31',DATE '2019-11-30',DATE '2019-12-31',DATE '2020-01-31',DATE '2020-02-29') then 'past'
    when snapshot_date in (DATE '2020-03-31',DATE '2020-04-30') then 'current' end as point_of_view,
    user_id,
    view_cnt, cart_cnt, purchase_cnt 
    from total where rfm_segment in ('VIP','성장 고객','우수 고객') 
    """

view_cart_df = spark.sql(view_cart_sql)

# PostgreSQL 적재
try:
    print("PostgreSQL 적재 시작")

    (view_cart_df
        .write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", "analysis.high_segmemt_vcp")   # 하나의 테이블에 월별 append
    
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
