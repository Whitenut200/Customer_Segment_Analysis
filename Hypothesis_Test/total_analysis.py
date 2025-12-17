from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from sqlalchemy import text
from scipy.stats import chi2_contingency, wilcoxon
import statsmodels.formula.api as smf
import statsmodels.api as sm
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from scipy.stats import spearmanr

# =========================
# 공통 설정
# =========================
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = "5432"
DB_NAME = ""
SCHEMA = "analysis"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# =========================
# 데이터 로드
# =========================
def data_load(table: str, schema: str = SCHEMA, columns=None, where=None):
    col_sql = ", ".join([f'"{c}"' for c in columns]) if columns else "*"
    sql = f'SELECT {col_sql} FROM "{schema}"."{table}"'
    if where:
        sql += f" WHERE {where}"
    return pd.read_sql(sql, con=engine) 

# =========================
# Chi-square
# =========================
def chi2_rate_test(
    df: pd.DataFrame,
    group_col: str,
    total_col: str,
    success_col: str,
    correction: bool = False,
):
    # 필수 컬럼 체크
    need = {group_col, total_col, success_col}
    if not need.issubset(df.columns):
        raise ValueError(f"필수 컬럼 누락: {need - set(df.columns)}")
      
    # 결측값 제거
    sub = df[[group_col, total_col, success_col]].dropna().copy()

    sub[total_col] = pd.to_numeric(sub[total_col], errors="coerce")
    sub[success_col] = pd.to_numeric(sub[success_col], errors="coerce")
    sub = sub.dropna()

    # 전체 인원수가 활성인원수 보다 높지 않은 것만 가져오기
    # 이론상 맞지 않음
    sub = sub[sub[success_col] <= sub[total_col]]

    # 이탈자 산출
    sub["failure"] = sub[total_col] - sub[success_col]

    contingency = (
        sub.set_index(group_col)[[success_col, "failure"]]
        .astype(int)
    )

    # 카이제곱 검정
    chi2, p, dof, expected = chi2_contingency(contingency.values, correction=correction)

    n = contingency.values.sum()
    r, k = contingency.shape
    v = float(np.sqrt((chi2 / n) / min(r - 1, k - 1))) if min(r - 1, k - 1) > 0 else None

    # 잔차 (어느 그룹이 차이를 만들었는지)
    resid = (contingency.values - expected) / np.sqrt(expected)
    resid_df = pd.DataFrame(resid, index=contingency.index, columns=contingency.columns)

    result = {
        "test": "chi_square",
        "chi2": float(chi2),
        "p_value": float(p),
        "dof": int(dof),
        "cramers_v": v,
        "n": int(n),
    }
    return result, contingency, resid_df


# =========================
# ANOVA (그룹별 평균 비교) + Tukey 사후검정
# =========================

def initial_retention_anova(
    df,
    segment_col="rfm_segment",
    month_col="curr_period",
    active_col="action_number",
    non_active_col="non_action_number",
    curr_period=("2020-02-01", "2020-03-01"),
    alpha=0.05
):
    """
    초기 유지율(월별)을 표본으로 사용하는 One-way ANOVA
    
    Parameters
    ----------
    df : DataFrame
        segment, snapshot_month, active_cnt, unactive_cnt 포함
    initial_months : tuple
        초기 구간으로 사용할 월 (예: ('2020-02','2020-03'))
    """

    # 필수 컬럼 체크
    required_cols = {segment_col, month_col, active_col, non_active_col}
  
    if not required_cols.issubset(df.columns):
        raise ValueError(f"필수 컬럼 누락: {required_cols - set(df.columns)}")

    df = df.copy()

    # 2. 유지율 계산
    df["total_cnt"] = df[active_col] + df[non_active_col]
    df["retention_rate"] = df[active_col] / df["total_cnt"]

    df_init = df.copy()

    if df_init.empty:
        raise ValueError("초기 기간 데이터가 비어 있습니다.")
      
    # ANOVA 실행
    model = smf.ols(
        f"retention_rate ~ C({segment_col})",
        data=df_init
    ).fit()

    anova_table = sm.stats.anova_lm(model, typ=2)

    f_stat = anova_table.loc[f"C({segment_col})", "F"]
    p_value = anova_table.loc[f"C({segment_col})", "PR(>F)"]

    # 효과크기 (eta squared)
    ss_between = anova_table.loc[f"C({segment_col})", "sum_sq"]
    ss_total = anova_table["sum_sq"].sum()
    eta_squared = ss_between / ss_total if ss_total > 0 else None

    # 사후검정 (Tukey HSD)

    tukey = pairwise_tukeyhsd(
        endog=df_init["retention_rate"],
        groups=df_init[segment_col],
        alpha=alpha
    )
    tukey_df = pd.DataFrame(
        tukey.summary().data[1:],
        columns=tukey.summary().data[0]
    )

    # 결과 출력
    print("\n" + "="*60)
    print("초기 유지율 세그먼트 간 차이 ANOVA")
    print("="*60)
    print(anova_table)
    print(f"\nF-statistic : {f_stat:.4f}")
    print(f"p-value     : {p_value:.6f}")
    print(f"eta^2       : {eta_squared:.3f}")
    print("\n[Tukey HSD 사후검정]")
    print(tukey_df)
    print("="*60)

    # 반환
    result = {
        "test_name": "Initial Retention ANOVA",
        "initial_months": curr_period,
        "F": float(f_stat),
        "p_value": float(p_value),
        "eta_squared": float(eta_squared),
        "significant": p_value < alpha,
        "n_obs": int(len(df_init)),
        "n_groups": int(df_init[segment_col].nunique())
    }

    return result, anova_table, tukey_df, df_init



# =========================
# Logistic Regression (statsmodels: OR/p-value 해석용)
# 범주형은 categorical_cols에 넣으면 자동 C() 처리
# =========================
def logistic_regression_sm(df, target_col, numeric_cols=None, categorical_cols=None):
    numeric_cols = numeric_cols or []
    categorical_cols = categorical_cols or []

    need = {target_col, *numeric_cols, *categorical_cols}
    if not need.issubset(df.columns):
        raise ValueError(f"필수 컬럼 누락: {need - set(df.columns)}")

    sub = df[[target_col] + numeric_cols + categorical_cols].dropna().copy()
    sub[target_col] = pd.to_numeric(sub[target_col], errors="coerce").astype(int)
    sub = sub.dropna()

    # formula 만들기: 수치형은 그대로, 범주형은 C(col)
    terms = []
    terms += numeric_cols
    terms += [f"C({c})" for c in categorical_cols]
    formula = f"{target_col} ~ " + " + ".join(terms) if terms else f"{target_col} ~ 1"

    model = smf.logit(formula=formula, data=sub).fit(disp=0)

    # OR & CI
    params = model.params
    conf = model.conf_int()
    or_df = pd.DataFrame({
        "term": params.index,
        "coef": params.values,
        "odds_ratio": np.exp(params.values),
        "p_value": model.pvalues.values,
        "ci_low_or": np.exp(conf[0].values),
        "ci_high_or": np.exp(conf[1].values),
    }).sort_values("p_value", ascending=True)

    result = {
        "test": "logistic_regression",
        "n": int(len(sub)),
        "pseudo_r2": float(model.prsquared),
        "llf": float(model.llf),
        "aic": float(model.aic),
        "formula": formula,
    }
    return result, or_df, model
# =========================
# 5) 상관분석 
# =========================
def cohort_spearman_analysis(
    df,
    age_col="cohort_age",
    freq_col="avg_freq",
    arpu_col="arpu",
    min_age=1,
    max_age=3,
    min_active_users=100
):
    # 필터링
    sub = df[
        (df[age_col].between(min_age, max_age)) &
        (df["active_users"] >= min_active_users)
    ][[age_col, freq_col, arpu_col]].dropna()

    # Spearman
    rho_freq, p_freq = spearmanr(sub[age_col], sub[freq_col])
    rho_arpu, p_arpu = spearmanr(sub[age_col], sub[arpu_col])

    result = {
        "avg_freq": {
            "spearman_rho": rho_freq,
            "p_value": p_freq,
            "significant": p_freq < 0.05
        },
        "arpu": {
            "spearman_rho": rho_arpu,
            "p_value": p_arpu,
            "significant": p_arpu < 0.05
        },
        "n": len(sub)
    }

    return result



# =========================
# Wilcoxon signed-rank (paired)
# =========================

# 전처리
def prepare_wilcoxon_data(
    df,
    id_col="user_id",
    period_col="point_of_view",
    value_col="value",
    before_label="past",
    after_label="current"
):
    """
    Wilcoxon signed-rank test용 데이터 생성
    long -> wide (before / after)
    """

    wide = (
        df
        .pivot_table(
            index=id_col,
            columns=period_col,
            values=value_col,
            aggfunc="mean"   # 동일 user_id + period 중복 시 평균
        )
        .reset_index()
    )

    # before / after 모두 존재하는 사용자만
    wide = wide[[id_col, before_label, after_label]].dropna()

    wide = wide.rename(
        columns={
            before_label: "before_value",
            after_label: "after_value"
        }
    )

    return wide


def wilcoxon_signed_rank(
    df: pd.DataFrame,
    before_col: str,
    after_col: str,
):
    need = {before_col, after_col}
    if not need.issubset(df.columns):
        raise ValueError(f"필수 컬럼 누락: {need - set(df.columns)}")

    sub = df[[before_col, after_col]].dropna().copy()
    sub[before_col] = pd.to_numeric(sub[before_col], errors="coerce")
    sub[after_col] = pd.to_numeric(sub[after_col], errors="coerce")
    sub = sub.dropna()

    stat, p = wilcoxon(sub[before_col], sub[after_col], zero_method="wilcox", alternative="two-sided")

    diff = sub[after_col] - sub[before_col]
    n = len(diff)
    median_diff = float(np.median(diff))
    mean_diff = float(np.mean(diff))

    # rank-biserial correlation (간단 효과크기)
    rbc = float(1.0 - (2.0 * stat / (n * (n + 1)))) if n > 0 else None

    result = {
        "test": "wilcoxon_signed_rank",
        "stat": float(stat),
        "p_value": float(p),
        "n": int(n),
        "median_diff": median_diff,
        "mean_diff": mean_diff,
        "rank_biserial": rbc,
    }
    return result


results = []

# =========================
#          실행
# =========================

# (1) 카이제곱: POV별 유지율 차이
df1 = data_load("retention")  # point_of_view, first_uesr, active_user
r1, ct, resid = chi2_rate_test(df1, "point_of_view", "first_user", "active_user")

print("\n" + "="*60)
print("[Chi-square] 초기 Cohort 유지율 하락 가설")
print("="*60)
print("Contingency Table:\n", ct)
print("\nResiduals:\n", resid)
print("\nResult:", r1)

results.append({**r1, "name": "초기 Cohort 유지율 하락 가설"})


# (2) ANOVA: 세그먼트별 ARPU 평균 차이
df2 = data_load("rfm_segment2")  # segment, arpu
anova_res, anova_tbl, tukey_df, df_used = initial_retention_anova(df2)

print("\n" + "="*60)
print("[ANOVA] 세그먼트 간 초기 유지율 차이")
print("="*60)
print("ANOVA result:", anova_res)

results.append({**anova_res, "name": "세그먼트 간 초기 유지율 차이 가설"})


# (3) 로지스틱: 활성(1)/비활성(0) 예측
df3 = data_load("retention_view_cart_2")  # active_flag, recency, freq, amount, segment 등
r, or_df, model = logistic_regression_sm(
    df3,
    target_col="TF_purchase",
    numeric_cols=["category_cnt", "view_cnt", "cart_cnt"]
    # categorical_cols=["segment"] # 범주형 
)
results.append({**r, "name": "행동 지표(초기 경험) 기반 가설"})
print("\n" + "="*60)
print("[Logistic Regression] 행동 지표 기반 초기 유지율")
print("="*60)

print("Model Info")
for k, v in r.items():
    print(f"{k:12s}: {v}")

print(" Odds Ratio Table (중요)")
print(or_df)

print(" statsmodels summary")
print(model.summary())

sig_df = or_df[or_df["p_value"] < 0.05]

print("Significant Variables (p < 0.05)")
if sig_df.empty:
    print("유의한 변수 없음")
else:
    print(sig_df[["term", "odds_ratio", "p_value", "ci_low_or", "ci_high_or"]])



# (4) 상관분석: 코호트나이와 연속형 사이의 관계
df4 = data_load("age") 

spearman_res = cohort_spearman_analysis(
    df4,
    age_col="cohort_age",
    freq_col="avg_freq",
    arpu_col="arpu",
    min_age=1,
    max_age=4,
    min_active_users=100
)

print("\n" + "="*60)
print("[상관분석] Cohort Age에 따른 구매 행동 차이 가설")
print("="*60)

print("Cohort Age ↔ avg_freq:", spearman_res["avg_freq"])
print("Cohort Age ↔ arpu    :", spearman_res["arpu"])
print("표본 수:", spearman_res["n"])

# (원하면 요약 results에 넣기 - 구조 맞춰서)
results.append({
    "test": "spearman",
    "name": "Cohort Age에 따른 구매 행동 차이 가설",
    "n": spearman_res["n"],
    "freq_rho": float(spearman_res["avg_freq"]["spearman_rho"]),
    "freq_p": float(spearman_res["avg_freq"]["p_value"]),
    "arpu_rho": float(spearman_res["arpu"]["spearman_rho"]),
    "arpu_p": float(spearman_res["arpu"]["p_value"]),
})

summary_df = pd.DataFrame(results)
print(summary_df)



# (5) Wilcoxon: 여러 지표 한번에 (추천 구조)
def run_one_wilcoxon(table, metric_col, schema=SCHEMA):
    df = data_load(table, schema=schema)  
    df = df.rename(columns={metric_col: "value"})

    df_wide = prepare_wilcoxon_data(
        df,
        id_col="user_id",
        period_col="point_of_view",
        value_col="value",
        before_label="past",
        after_label="current"
    )

    w_res = wilcoxon_signed_rank(
        df=df_wide,
        before_col="before_value",
        after_col="after_value"
    )
    w_res["table"] = table
    w_res["metric"] = metric_col
    return w_res


tests = [
    ("high_segment_rm", "avg_recency"),
    ("high_segment_rm", "avg_montetary"),  
    ("high_segment_vcp", "view_cnt"),     
    ("high_segment_vcp", "cart_cnt"),
    ("high_segment_vcp", "purchase_cnt")
]

print("\n" + "="*60)
print("[Wilcoxon] 최근 vs 과거 지표 차이")
print("="*60)

wilcoxon_results = [run_one_wilcoxon(t, m) for t, m in tests]
wilcoxon_df = pd.DataFrame(wilcoxon_results).sort_values("p_value")
print("\n[Wilcoxon 결과]")
print(wilcoxon_df)

# 요약 results에도 합치기 (원하면)
for row in wilcoxon_results:
    results.append({**row, "name": f"Wilcoxon 변화: {row['metric']}"})


# 전체 요약표
summary_df = pd.DataFrame(results)
summary_df
