# -*- coding: utf-8 -*-
"""데일리 파이프라인 쿼리 매니페스트 (전 섹션 결정적 재추출).
러너: 각 QUERIES[name] 을 mcp athena 로 실행 → daily_pipeline/tmp/{name}.json 에 결과 data 배열 저장.
그 후 build.py 가 tmp/*.json 을 읽어 tf-data.json + tf-mattress-data.json 재구성.
모든 날짜는 DATE_ADD 상대 → 매일 최신. object_idx BETWEEN 0 AND 100. net GMV=필터없음.
"""

SELF = ['3918642','3640244','3640123','1089824','3607491','3121605','3898593','3898584','3748221']
COMP = ['767440','2636441','1930788','442026','676405','2731307','329364','1590911','2352818']
ALL18 = SELF + COMP
SELF_SRPKW = ['3918642','3640244','3640123','1089824','3607491','3121605','3898593','3898584','3748221']  # 키워드추세는 자사만
MATTRESS = ['1089824','3607491','3121605']  # basic/refine/studio 매트리스
FRAMES_STUDIO_OPT = ['3898593','3898584','3748221']  # attach_frame_option 대상(프레임옵션 매트리스)

def _in(pids):
    return ",".join("'%s'" % p for p in pids)
def _inb(pids):
    return ",".join(str(int(p)) for p in pids)

ALL_S = _in(ALL18)          # 문자열 IN
ALL_B = _inb(ALL18)         # bigint IN
SELF_S = _in(SELF_SRPKW)
MAT_S = _in(MATTRESS)
MAT_B = _inb(MATTRESS)
FOPT_S = _in(FRAMES_STUDIO_OPT)

# 최근 N개월 yyyymm 파티션 (문자열 비교). GMV/orders 6개월치 커버.
YM6 = "yyyymm >= date_format(DATE_ADD('month',-6,CURRENT_DATE),'%Y%m')"
YM_AGES = "yyyymm >= '202501' AND yyyymm <= date_format(CURRENT_DATE,'%Y%m')"

QUERIES = {}

# ============ daily (funnel / uv / gmv) — 18상품, 최근 50일 (build가 history 보존 병합) ============
QUERIES['daily_funnel'] = f"""
SELECT period dt, CAST(product_id AS VARCHAR) pid, SUM(impression_count_all) imp, SUM(pdpview_count_all) pdp, SUM(purchase_count_all) purchase
FROM ba_preserved.comm_product_perform
WHERE product_id IN ({ALL_B}) AND period_base='day'
  AND period >= CAST(DATE_ADD('day',-50,CURRENT_DATE) AS VARCHAR)
  AND {YM6}
GROUP BY period, product_id"""

QUERIES['daily_uv'] = f"""
SELECT CAST(base_dt AS VARCHAR) dt, id pid, SUM(impression_user_join) imp_uv, SUM(pdpview_user_join) pdp_uv, SUM(purchase_user_join) buy_uv
FROM ba_preserved.commerce_daily_user_count_v3
WHERE id IN ({ALL_S}) AND base='product' AND paid_type='total'
  AND base_dt >= CAST(DATE_ADD('day',-50,CURRENT_DATE) AS VARCHAR)
GROUP BY base_dt, id"""

QUERIES['daily_gmv'] = f"""
SELECT CAST(base_dt AS VARCHAR) dt, CAST(product_id AS BIGINT) pid, SUM(gmv) gmv, SUM(gross_profit) gp, SUM(option_quantity) qty
FROM ba_preserved.commerce_gross_profit_orders
WHERE CAST(product_id AS BIGINT) IN ({ALL_B}) AND {YM6} AND base_dt >= DATE_ADD('day',-50,CURRENT_DATE)
GROUP BY base_dt, product_id"""

# ============ SRP: base 14d (rank=AVG, best=P5, imp, ctr) — matrix+keywordRank 공통 소스 ============
# 단일 쿼리로 P5 1회 산출(approx_percentile 실행간 흔들림 방지 → matrix/keywordRank 일치)
QUERIES['srp_base14'] = f"""
SELECT object_id pid, query_keyword kw,
  AVG(object_idx) rank, CAST(approx_percentile(object_idx,0.05) AS INT) best,
  AVG(monthly_sales_score) score, SUM(is_clicked)*1.0/COUNT(*) ctr, COUNT(*) imp
FROM search.commerce_srp_dataset_daily_v0_0_2
WHERE date >= CAST(DATE_ADD('day',-14,CURRENT_DATE) AS VARCHAR)
  AND object_id IN ({ALL_S}) AND object_idx BETWEEN 0 AND 100
GROUP BY object_id, query_keyword HAVING COUNT(*) >= 20"""

# 통합검색량(통검) qc: query_qc_mart INTEGRATED, 2w(qc)+4w(qc4). 최신 date 파티션.
QUERIES['qc_integrated'] = f"""
SELECT q.search_keyword kw,
  MAX(CASE WHEN q.period='2w' THEN q.qc END) qc2w,
  MAX(CASE WHEN q.period='4w' THEN q.qc END) qc4w
FROM search.query_qc_mart q
WHERE q.date = (SELECT max(date) FROM search.query_qc_mart)
  AND q.source='INTEGRATED' AND q.period IN ('2w','4w')
  AND q.search_keyword IN (
    SELECT DISTINCT query_keyword FROM search.commerce_srp_dataset_daily_v0_0_2
    WHERE date >= CAST(DATE_ADD('day',-14,CURRENT_DATE) AS VARCHAR)
      AND object_id IN ({ALL_S}) AND object_idx BETWEEN 0 AND 100)
GROUP BY q.search_keyword"""

# srpKeywords 자사 14d 키워드추세 (일별)
QUERIES['srp_kwtrend'] = f"""
SELECT date dt, object_id pid, query_keyword kw, AVG(object_idx) rank, CAST(approx_percentile(object_idx,0.05) AS INT) best, AVG(monthly_sales_score) score
FROM search.commerce_srp_dataset_daily_v0_0_2
WHERE date >= CAST(DATE_ADD('day',-14,CURRENT_DATE) AS VARCHAR)
  AND object_id IN ({SELF_S}) AND object_idx BETWEEN 0 AND 100
GROUP BY date, object_id, query_keyword HAVING COUNT(*) >= 10"""

# scoreTs / featTs 60d — self/comp 분할 (18*60=1080>1000)
for tag, grp in [('self', SELF), ('comp', COMP)]:
    gs = _in(grp)
    QUERIES[f'scorets_{tag}'] = f"""
SELECT date dt, object_id pid, AVG(monthly_sales_score) score
FROM search.commerce_srp_dataset_daily_v0_0_2
WHERE date >= CAST(DATE_ADD('day',-60,CURRENT_DATE) AS VARCHAR)
  AND object_id IN ({gs}) AND object_idx BETWEEN 0 AND 100
GROUP BY date, object_id"""
    QUERIES[f'featts_{tag}'] = f"""
SELECT date dt, object_id pid, AVG(review_count_score) review, AVG(sell_cnt_28_day_score) sell28,
  AVG(view_cnt_28_day_score) view28, AVG(sell_per_view_28_day_score) spv28, AVG(wish_count_180_day_score) wish,
  AVG(card_count_180_day_score) card, CAST(AVG(qc_rank_4w) AS DOUBLE) qc_rank
FROM search.commerce_srp_dataset_daily_v0_0_2
WHERE date >= CAST(DATE_ADD('day',-60,CURRENT_DATE) AS VARCHAR)
  AND object_id IN ({gs}) AND object_idx BETWEEN 0 AND 100
GROUP BY date, object_id"""

# ============ inflow 14d per-day — 4 윈도우 분할(18상품×14일×채널 >1000 방지) ============
_INFLOW_CASE = """CASE WHEN COALESCE(inferred_page_id,page_id) LIKE 'SRP%' AND object_section_id IN ('SEARCH_CAROUSEL_HOME','STORE_SRP_AD_PRODUCTION','STORE_SRP_AD_MID') THEN '검색광고'
     WHEN COALESCE(inferred_page_id,page_id) LIKE 'SRP%' THEN '검색'
     WHEN COALESCE(inferred_page_id,page_id)='CATEGORY' AND object_section_id IN ('STORE_CATEGORY_CAROUSEL','MD_PICK_ITEM') THEN '카테고리광고'
     WHEN COALESCE(inferred_page_id,page_id)='CATEGORY' THEN '카테고리'
     WHEN COALESCE(inferred_page_id,page_id)='PDP' THEN 'PDP추천'
     WHEN COALESCE(inferred_page_id,page_id) IN ('HOME','SHOPPINGHOME') THEN '홈'
     WHEN COALESCE(inferred_page_id,page_id)='BRAND_DETAIL' THEN '브랜드관'
     WHEN COALESCE(inferred_page_id,page_id)='EXHIBITION_DETAIL' THEN '기획전'
     WHEN COALESCE(inferred_page_id,page_id) LIKE 'MYPACKAGE%' THEN '마이패키지'
     WHEN COALESCE(inferred_page_id,page_id)='TODAYSDEAL' THEN '오늘의딜'
     WHEN COALESCE(inferred_page_id,page_id)='AD_PRODUCT_LIST' THEN '광고리스트'
     WHEN COALESCE(inferred_page_id,page_id)='BEST' THEN '베스트'
     WHEN COALESCE(inferred_page_id,page_id) LIKE 'CDP%' THEN '컨텐츠'
     WHEN COALESCE(inferred_page_id,page_id) LIKE 'BRAZE%' THEN '푸시'
     WHEN COALESCE(inferred_page_id,page_id)='CART' OR COALESCE(inferred_page_id,page_id) LIKE 'MYPAGE%' OR COALESCE(inferred_page_id,page_id) LIKE 'MYSHOPPING%' OR COALESCE(inferred_page_id,page_id)='ORDER_RESULT' OR COALESCE(inferred_page_id,page_id)='RECENT_VIEWED_GOODS' OR COALESCE(inferred_page_id,page_id) LIKE 'INTERESTINFO%' THEN '재방문'
     ELSE '기타' END"""
_INFLOW_WINDOWS = [(14,12),(11,9),(8,6),(5,1)]  # (start_days_ago, end_days_ago) 각 3~4일
for i,(a,b) in enumerate(_INFLOW_WINDOWS):
    QUERIES[f'inflow_w{i}'] = f"""
WITH m AS (
  SELECT date, object_id pid, {_INFLOW_CASE} inflow, category, uuid
  FROM log.analyst_log_table
  WHERE date BETWEEN CAST(DATE_ADD('day',-{a},CURRENT_DATE) AS VARCHAR) AND CAST(DATE_ADD('day',-{b},CURRENT_DATE) AS VARCHAR)
    AND object_id IN ({ALL_S}) AND category IN ('IMPRESSION','CLICK') AND object_type='PRODUCTION')
SELECT date dt, pid, inflow,
  SUM(CASE WHEN category='IMPRESSION' THEN 1 ELSE 0 END) imp,
  SUM(CASE WHEN category='CLICK' THEN 1 ELSE 0 END) click,
  COUNT(DISTINCT CASE WHEN category='CLICK' THEN uuid END) click_uv
FROM m GROUP BY date, pid, inflow"""

# inflowCvr 7d (채널 click→같은날 같은상품 구매 attribution, UV)
QUERIES['inflow_cvr'] = f"""
WITH ic AS (
  SELECT date dt, object_id pid, uuid, user_id,
    CASE WHEN COALESCE(inferred_page_id,page_id) LIKE 'SRP%' THEN '검색'
         WHEN COALESCE(inferred_page_id,page_id)='CATEGORY' THEN '카테고리'
         WHEN COALESCE(inferred_page_id,page_id)='PDP' THEN 'PDP추천'
         WHEN COALESCE(inferred_page_id,page_id) IN ('HOME','SHOPPINGHOME') THEN '홈'
         WHEN COALESCE(inferred_page_id,page_id)='BRAND_DETAIL' THEN '브랜드관'
         WHEN COALESCE(inferred_page_id,page_id)='EXHIBITION_DETAIL' THEN '기획전'
         WHEN COALESCE(inferred_page_id,page_id)='AD_PRODUCT_LIST' THEN '광고리스트'
         WHEN COALESCE(inferred_page_id,page_id)='BEST' THEN '베스트'
         WHEN COALESCE(inferred_page_id,page_id) LIKE 'CDP%' THEN '컨텐츠'
         ELSE '기타' END inflow
  FROM log.analyst_log_table
  WHERE date >= CAST(DATE_ADD('day',-7,CURRENT_DATE) AS VARCHAR)
    AND object_id IN ({ALL_S}) AND category='CLICK' AND object_type='PRODUCTION'),
pp AS (
  SELECT DISTINCT CAST(base_dt AS VARCHAR) dt, CAST(user_id AS VARCHAR) user_id, CAST(product_id AS BIGINT) pid
  FROM ba_preserved.commerce_gross_profit_orders
  WHERE {YM6} AND base_dt >= DATE_ADD('day',-7,CURRENT_DATE) AND user_id IS NOT NULL)
SELECT i.dt, i.pid, i.inflow, COUNT(DISTINCT i.uuid) click_uv,
  COUNT(DISTINCT CASE WHEN p.user_id IS NOT NULL THEN i.uuid END) buy_uv
FROM ic i LEFT JOIN pp p ON i.dt=p.dt AND CAST(i.user_id AS VARCHAR)=p.user_id AND CAST(i.pid AS BIGINT)=p.pid
GROUP BY i.dt, i.pid, i.inflow"""

# ages 18상품 202501~현재 월누적 (월요일만 실행; 러너가 요일체크)
QUERIES['ages'] = f"""
WITH buyers AS (
  SELECT DISTINCT CAST(o.product_id AS BIGINT) pid, CAST(o.user_id AS BIGINT) uid
  FROM ba_preserved.commerce_gross_profit_orders o
  WHERE {YM_AGES.replace('yyyymm','o.yyyymm')} AND CAST(o.product_id AS BIGINT) IN ({ALL_B})
    AND o.option_quantity>0 AND o.user_id IS NOT NULL)
SELECT b.pid,
  CASE WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 25 THEN '20-24'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 30 THEN '25-29'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 35 THEN '30-34'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 40 THEN '35-39'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 45 THEN '40-44'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 50 THEN '45-49'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 55 THEN '50-54'
       WHEN 2026-CAST(SUBSTR(u.birthday,1,4) AS INT) < 60 THEN '55-59'
       ELSE '60+' END ag, COUNT(DISTINCT b.uid) cnt
FROM buyers b JOIN dump_member.verified_users u ON u.user_id=b.uid
WHERE u.birthday IS NOT NULL AND LENGTH(u.birthday)>=4 AND CAST(SUBSTR(u.birthday,1,4) AS INT) BETWEEN 1930 AND 2010
GROUP BY 1,2"""

# ============ benchmarks: bedding / mattress / bed (7d UV, 가격밴드 PB vs 3P) ============
_BENCH_CATE = {'bedding':"cate_d3='이불/이불커버'", 'mattress':"cate_d2='매트리스(150T이상)'", 'bed':"cate_d2='침대'"}
_BENCH_BANDS = {
 'bedding':"CASE WHEN p.selling_cost<30000 THEN '01_<30k' WHEN p.selling_cost<50000 THEN '02_30-50k' WHEN p.selling_cost<70000 THEN '03_50-70k' WHEN p.selling_cost<100000 THEN '04_70-100k' ELSE '05_100k+' END",
 'mattress':"CASE WHEN p.selling_cost<200000 THEN '01_<20만' WHEN p.selling_cost<400000 THEN '02_20-40만' WHEN p.selling_cost<700000 THEN '03_40-70만' WHEN p.selling_cost<1200000 THEN '04_70-120만' ELSE '05_120만+' END",
 'bed':"CASE WHEN p.selling_cost<500000 THEN '01_<50만' WHEN p.selling_cost<800000 THEN '02_50-80만' WHEN p.selling_cost<1200000 THEN '03_80-120만' ELSE '04_120만+' END",
}
for bk in ['bedding','mattress','bed']:
    QUERIES[f'bench_{bk}_bands'] = f"""
SELECT CASE WHEN p.brand_name='오늘의집 layer' THEN 'PB' ELSE '3P' END pb_flag,
  {_BENCH_BANDS[bk]} price_band, COUNT(DISTINCT p.product_id) prod_cnt,
  SUM(uc.impression_user_join) imp_uv, SUM(uc.pdpview_user_join) pdp_uv, SUM(uc.purchase_user_join) buy_uv
FROM ba_preserved.commerce_daily_user_count_v3 uc
JOIN ba_preserved.comm_product_info_latest p ON TRY_CAST(uc.id AS BIGINT)=p.product_id
WHERE uc.base_dt BETWEEN DATE_ADD('day',-7,CURRENT_DATE) AND DATE_ADD('day',-1,CURRENT_DATE)
  AND uc.base='product' AND uc.paid_type='total' AND {_BENCH_CATE[bk]}
GROUP BY 1,2"""
    QUERIES[f'bench_{bk}_lineup'] = f"""
SELECT CAST(uc.id AS VARCHAR) pid, p.product_name, p.selling_cost, p.brand_name,
  SUM(uc.impression_user_join) imp_uv, SUM(uc.pdpview_user_join) pdp_uv, SUM(uc.purchase_user_join) buy_uv
FROM ba_preserved.commerce_daily_user_count_v3 uc
JOIN ba_preserved.comm_product_info_latest p ON TRY_CAST(uc.id AS BIGINT)=p.product_id
WHERE uc.base_dt BETWEEN DATE_ADD('day',-7,CURRENT_DATE) AND DATE_ADD('day',-1,CURRENT_DATE)
  AND uc.base='product' AND uc.paid_type='total' AND p.brand_name='오늘의집 layer' AND {_BENCH_CATE[bk]}
GROUP BY 1,2,3,4"""

# ============ 매트리스 객단가 decomp (3mo, 옵션명 조인, 커버 제외) ============
QUERIES['decomp'] = f"""
WITH mo AS (
  SELECT product_id, option_id, SUM(gmv) gmv, SUM(option_quantity) qty
  FROM ba_preserved.commerce_gross_profit_orders
  WHERE product_id IN ({MAT_S}) AND yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m')
  GROUP BY product_id, option_id),
nm AS (SELECT id, arbitrary(explain) explain, arbitrary(explain2) explain2 FROM ba_preserved.commerce_snapshot_production_options
       WHERE base_dt >= DATE_ADD('day',-120,CURRENT_DATE) GROUP BY id)
SELECT mo.product_id pid, nm.explain, nm.explain2, mo.qty, mo.gmv,
  CAST(mo.gmv AS DOUBLE)/NULLIF(mo.qty,0) unit
FROM mo LEFT JOIN nm ON CAST(mo.option_id AS BIGINT)=nm.id
WHERE mo.qty>0"""

# attach_frame_option: 프레임(3 studio) 옵션 중 매트리스 옵션 월별
QUERIES['attach_frame_option'] = f"""
WITH fo AS (
  SELECT product_id, option_id, yyyymm, SUM(gmv) gmv, SUM(option_quantity) qty
  FROM ba_preserved.commerce_gross_profit_orders
  WHERE product_id IN ({FOPT_S}) AND {YM6}
  GROUP BY product_id, option_id, yyyymm),
nm AS (SELECT id, arbitrary(explain) explain FROM ba_preserved.commerce_snapshot_production_options
       WHERE base_dt >= DATE_ADD('day',-160,CURRENT_DATE) GROUP BY id)
SELECT fo.yyyymm,
  CASE WHEN nm.explain LIKE '%매트리스%' AND nm.explain NOT LIKE '%패드%' AND nm.explain NOT LIKE '%밀림방지%' THEN 'attach' ELSE 'frame' END typ,
  SUM(fo.gmv) gmv, SUM(fo.qty) qty
FROM fo LEFT JOIN nm ON CAST(fo.option_id AS BIGINT)=nm.id
WHERE fo.qty>0 GROUP BY fo.yyyymm, 2"""

# ============ 합구매 cosell (프레임→매트리스) tier 유저단위 ============
# frame tier 목록은 동적: layer 침대 프레임(수납/일반, 매트리스포함 제외)
_FRAME_CTE = """frames AS (
  SELECT CAST(product_id AS VARCHAR) pid,
    CASE WHEN product_name LIKE '%basic /%' THEN 'basic' WHEN product_name LIKE '%refine /%' THEN 'refine'
         WHEN product_name LIKE '%studio /%' THEN 'studio' ELSE 'etc' END tier
  FROM ba_preserved.comm_product_info_latest
  WHERE brand_name='오늘의집 layer' AND cate_d2='침대' AND cate_d3 IN ('수납침대','일반침대') AND product_name NOT LIKE '%매트리스 포함%')"""
_MAT3 = "yyyymm >= date_format(DATE_ADD('month',-6,CURRENT_DATE),'%Y%m')"
_FRAME3 = "yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m')"

QUERIES['cosell_tier_summary'] = f"""
WITH {_FRAME_CTE},
fu AS (SELECT DISTINCT f.tier, CAST(o.user_id AS BIGINT) uid FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
       WHERE {_FRAME3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
mu AS (SELECT CAST(o.user_id AS BIGINT) uid, MAX(CASE WHEN p.brand_name='오늘의집 layer' THEN 1 ELSE 0 END) any_our
       FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
       WHERE p.cate_d2='매트리스(150T이상)' AND {_MAT3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1)
SELECT fu.tier, COUNT(DISTINCT fu.uid) frame_users,
  COUNT(DISTINCT CASE WHEN mu.uid IS NOT NULL THEN fu.uid END) bought_mat,
  COUNT(DISTINCT CASE WHEN mu.any_our=1 THEN fu.uid END) our_mat,
  COUNT(DISTINCT CASE WHEN mu.uid IS NOT NULL AND COALESCE(mu.any_our,0)=0 THEN fu.uid END) comp_only
FROM fu LEFT JOIN mu ON fu.uid=mu.uid GROUP BY fu.tier"""

QUERIES['cosell_tier_dest'] = f"""
WITH {_FRAME_CTE},
fu AS (SELECT DISTINCT f.tier, CAST(o.user_id AS BIGINT) uid FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
       WHERE {_FRAME3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
mp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.brand_name, SUM(o.option_quantity) qty, SUM(o.gmv) gmv
       FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
       WHERE p.cate_d2='매트리스(150T이상)' AND {_MAT3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3),
j AS (SELECT fu.tier, mp.pid, mp.brand_name, mp.uid, mp.qty, mp.gmv FROM fu JOIN mp ON fu.uid=mp.uid),
agg AS (SELECT tier, pid, brand_name, COUNT(DISTINCT uid) users, SUM(qty) qty, SUM(gmv) gmv,
  ROW_NUMBER() OVER (PARTITION BY tier ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY tier, pid, brand_name)
SELECT tier, pid, brand_name, users, qty, gmv FROM agg WHERE rn<=6"""

# ============ 역방향 매트리스→프레임 tier 유저단위 ============
QUERIES['m2f_summary'] = f"""
WITH mu AS (SELECT DISTINCT CASE product_id WHEN '1089824' THEN 'basic' WHEN '3607491' THEN 'refine' WHEN '3121605' THEN 'studio' END tier, CAST(user_id AS BIGINT) uid
       FROM ba_preserved.commerce_gross_profit_orders WHERE product_id IN ({MAT_S}) AND {_FRAME3} AND option_quantity>0 AND user_id IS NOT NULL),
frames AS (SELECT CAST(product_id AS BIGINT) pid, CASE WHEN brand_name='오늘의집 layer' THEN 1 ELSE 0 END is_self
       FROM ba_preserved.comm_product_info_latest WHERE cate_d2='침대' AND cate_d3 IN ('수납침대','일반침대') AND product_name NOT LIKE '%매트리스 포함%'),
fu AS (SELECT CAST(o.user_id AS BIGINT) uid, MAX(fr.is_self) any_our FROM ba_preserved.commerce_gross_profit_orders o JOIN frames fr ON CAST(o.product_id AS BIGINT)=fr.pid
       WHERE {_MAT3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1)
SELECT mu.tier, COUNT(DISTINCT mu.uid) mat_users,
  COUNT(DISTINCT CASE WHEN fu.uid IS NOT NULL THEN mu.uid END) bought_frame,
  COUNT(DISTINCT CASE WHEN fu.any_our=1 THEN mu.uid END) our_frame,
  COUNT(DISTINCT CASE WHEN fu.uid IS NOT NULL AND COALESCE(fu.any_our,0)=0 THEN mu.uid END) comp_only
FROM mu LEFT JOIN fu ON mu.uid=fu.uid WHERE mu.tier IS NOT NULL GROUP BY mu.tier"""

QUERIES['m2f_dest'] = f"""
WITH mu AS (SELECT DISTINCT CASE product_id WHEN '1089824' THEN 'basic' WHEN '3607491' THEN 'refine' WHEN '3121605' THEN 'studio' END tier, CAST(user_id AS BIGINT) uid
       FROM ba_preserved.commerce_gross_profit_orders WHERE product_id IN ({MAT_S}) AND {_FRAME3} AND option_quantity>0 AND user_id IS NOT NULL),
fp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.product_name, p.brand_name, SUM(o.gmv) gmv
       FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
       WHERE p.cate_d2='침대' AND p.cate_d3 IN ('수납침대','일반침대') AND p.product_name NOT LIKE '%매트리스 포함%'
         AND {_MAT3.replace('yyyymm','o.yyyymm')} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3,4),
j AS (SELECT mu.tier, fp.pid, fp.product_name, fp.brand_name, fp.uid, fp.gmv FROM mu JOIN fp ON mu.uid=fp.uid WHERE mu.tier IS NOT NULL),
agg AS (SELECT tier, pid, product_name, brand_name, COUNT(DISTINCT uid) users, SUM(gmv) gmv,
  ROW_NUMBER() OVER (PARTITION BY tier ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY tier, pid, product_name, brand_name)
SELECT tier, pid, product_name, brand_name, users, gmv FROM agg WHERE rn<=6"""

if __name__ == '__main__':
    print(f"QUERIES: {len(QUERIES)}")
    for k in QUERIES: print(' -', k)
