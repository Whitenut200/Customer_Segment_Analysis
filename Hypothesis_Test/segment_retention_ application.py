import statsmodels.formula.api as smf
import pandas as pd
from sqlalchemy import create_engine


# 공통 설정

DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = ""
SCHEMA = "mart" 

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 데이터 로드

def data_load(table: str, schema: str = SCHEMA, columns=None, where=None):
    col_sql = ", ".join([f'"{c}"' for c in columns]) if columns else "*"
    sql = f'SELECT {col_sql} FROM "{schema}"."{table}"'
    if where:
        sql += f" WHERE {where}"
    return pd.read_sql(sql, con=engine)


df=data_load("cohort_rfm_stat")

need = ["cnt_retention", "cohort_age", "rfm_segment", "cohort_date"]
d = df.dropna(subset=need).copy()

d = d.reset_index(drop=True)


# 범주형 지정
d["rfm_segment"] = d["rfm_segment"].astype("category")
d["cohort_age"] = d["cohort_age"].astype(int)
d["cohort_month"] = d["cohort_date"].dt.to_period("M").astype(str)

# Mixed model: retention ~ cohort_age * segment + (1 | cohort_month)
model = smf.mixedlm(
    "cnt_retention ~ cohort_age * C(rfm_segment)",
    data=d,
    groups=d["cohort_month"]  # random intercept
)

res = model.fit(reml=False)  # 보통 비교/검정용이면 ML(reml=False) 많이 씀
print(res.summary())
