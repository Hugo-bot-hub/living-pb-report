# -*- coding: utf-8 -*-
"""데일리 파이프라인 쿼리 매니페스트 (전 섹션 결정적 재추출).
러너: 각 QUERIES[name] 을 mcp athena 로 실행 → daily_pipeline/tmp/{name}.json 에 결과 data 배열 저장.
그 후 build.py 가 tmp/*.json 을 읽어 tf-data.json + tf-mattress-data.json 재구성.
모든 날짜는 DATE_ADD 상대 → 매일 최신. object_idx BETWEEN 0 AND 100. net GMV=필터없음.
"""

SELF = ['3918642','3640244','3640123','1089824','3607491','3121605','3898593','3898584','3748221','2518275']
COMP = ['767440','2636441','1930788','442026','676405','2731307','329364','1590911','2352818']
ALL18 = SELF + COMP
SELF_SRPKW = ['3918642','3640244','3640123','1089824','3607491','3121605','3898593','3898584','3748221','2518275']  # 키워드추세는 자사만
MATTRESS = ['1089824','3607491','3121605']  # basic/refine/studio 매트리스
# attach(프레임옵션 매트리스) 대상 프레임. 2518275=refine 빅수납호텔(2026-07-01 재런칭).
# basic 1243313·refine 3858646는 볼륨 최대 + 부착률 상위 → tier 비교 기준선으로 포함(studio가 왜 0%인지 대조군).
FRAMES_ATTACH = ['3898593','3898584','3748221','2518275','1243313','3858646']

def _in(pids):
    return ",".join("'%s'" % p for p in pids)
def _inb(pids):
    return ",".join(str(int(p)) for p in pids)

ALL_S = _in(ALL18)          # 문자열 IN
ALL_B = _inb(ALL18)         # bigint IN
SELF_S = _in(SELF_SRPKW)
MAT_S = _in(MATTRESS)
MAT_B = _inb(MATTRESS)
FOPT_S = _in(FRAMES_ATTACH)

# 🔴 옵션명 매트리스 판정 단일 정본 (attach_frame_option / cosell 공용).
# '매트리스 미포함'·'매트리스커버'·토퍼 등 오탐 제외 필수 — 2518275 "Q/K (매트리스 미포함)" 4,785만이
# 약한 필터(패드·밀림방지만 제외)에선 attach로 오분류됨(2026-07-16 발견).
_MAT_OPT_NOT = ['패드','밀림방지','커버','토퍼','미포함','별도','불포함','제외','선택안함','미선택']
def _ismat(col):
    return "(%s LIKE '%%매트리스%%' AND %s)" % (col, " AND ".join("%s NOT LIKE '%%%s%%'" % (col, w) for w in _MAT_OPT_NOT))
# 프레임 액세서리 — 프레임 본체 수량이 attach율 분모라 액세서리가 섞이면 왜곡.
# ⚠️ 키워드 선정 근거(2026-07-16 옵션 전수 확인). 본체와 액세서리를 가르는 경계가 미묘함:
#   · '조명형'/'서랍형'은 400 시리즈 패널(15~25만) = 액세서리.
#     반면 bare '조명'·'서랍'은 본체에도 붙음 — "LED 조명 헤드 SS"(41만)·"Q/K (양방서랍)"(98만)·
#     "슈퍼싱글(3서랍)"은 전부 본체. bare 키워드를 쓰면 본체가 액세서리로 빠져 부착률이 부풀려짐.
_ACC_WORDS = ['패널','협탁','선반','서랍형','조명형','화장대','룸스프레이','패드','커버','토퍼','밀림방지']
_ISACC = lambda col: "(%s)" % " OR ".join("%s LIKE '%%%s%%'" % (col, w) for w in _ACC_WORDS)

# 🔴 옵션명 조회 정본. arbitrary(explain)는 400일 창에서 실행마다 다른 값(공백 포함)을 돌려줘
# 분류가 비결정적이 됨(같은 옵션이 어떤 런에선 ''로 잡혀 frame 로 오분류). 최신 비공백 이름으로 고정.
_OPT_NM = """(SELECT id, max_by(explain, base_dt) explain FROM ba_preserved.commerce_snapshot_production_options
   WHERE base_dt >= DATE_ADD('day',-400,CURRENT_DATE) AND explain IS NOT NULL AND TRIM(explain) <> '' GROUP BY id)"""

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

# ============ 경쟁 레이더: 핵심 키워드 신규경쟁자·부스트/광고 의심 자동감지 (7d, idx<=20) ============
# 목적: 지정경쟁사만 보는 현 대시보드가 놓치는 신규 진입자/프로모·광고로 순위만 산 상품 감지.
# 판정은 build.py: 순위 높은데 sell28·리뷰 바닥 = boost_suspect. SRP원장은 광고/오가닉 미분리 → 괴리로 추론.
_RADAR_KW = "'매트리스','침대프레임','수납침대','이불','차렵이불','냉감이불'"
QUERIES['kw_radar'] = f"""
WITH s AS (
  SELECT query_keyword kw, object_id pid, AVG(object_idx) rnk, COUNT(*) imp, SUM(is_clicked) clk,
    MAX(monthly_sales_score) sales_score, MAX(sell_cnt_28_day) sell28, MAX(review_count) rev, MAX(review_avg) ravg
  FROM search.commerce_srp_dataset_daily_v0_0_2
  WHERE query_keyword IN ({_RADAR_KW})
    AND date >= CAST(DATE_ADD('day',-7,CURRENT_DATE) AS VARCHAR) AND object_idx <= 20
  GROUP BY query_keyword, object_id HAVING COUNT(*) > 40)
SELECT s.kw, s.pid, p.brand_name brand, p.product_name name, p.selling_cost price,
  ROUND(s.rnk,1)+1 rank, s.imp, ROUND(s.clk*100.0/s.imp,1) ctr,
  ROUND(s.sales_score,3) sales_score, CAST(s.sell28 AS INT) sell28, s.rev, ROUND(s.ravg,2) ravg
FROM s LEFT JOIN ba_preserved.comm_product_info_latest p ON TRY_CAST(s.pid AS BIGINT)=p.product_id
ORDER BY s.kw, rank"""

# ============ 리드 퍼널(표준 통일): 가구 리드가치(gross·상품매칭·성숙 코호트) + 의도등급 T1~T4 ============
# 리드가치 = 찜한 distinct 유저가 [찜월 forward 3개월] 내 [그 찜한 상품]을 산 실현 GMV ÷ 찜 유저. (통합공식 문서 정의블록)
# 코호트 c0=date_trunc(month,오늘)-3M. 찜/PDP형성=코호트월[c0,c0+1M) · vd/구매관찰=[c0,c0+3M). 리드가치=T3+T4(찜리드).
# ⚠️ user_pdp_facts.base_dt=date, user_scrap_facts.base_dt=VARCHAR. scan~10GB/run. 하반기 리드퍼널 문서 §5-3 + 리드가치 표준.
_FURN_LEAD = '1089824,3607491,3121605,1243313,3748221,3898593,3898584,2518275'
QUERIES['lead_funnel'] = f"""
WITH cm AS (SELECT date_trunc('month',CURRENT_DATE)-interval '3' month AS c0),
pdpf AS (
  SELECT CAST(p.user_id AS BIGINT) user_id, p.product_id, COUNT(DISTINCT p.base_dt) vd,
    MAX(CASE WHEN p.base_dt < cm.c0+interval '1' month THEN 1 ELSE 0 END) in_cm
  FROM ba_preserved.user_pdp_facts p, cm
  WHERE p.base_dt>=cm.c0 AND p.base_dt<cm.c0+interval '3' month
    AND p.product_id IN ({_FURN_LEAD}) AND p.user_id IS NOT NULL
  GROUP BY 1,2),
scr AS (
  SELECT CAST(s.user_id AS BIGINT) user_id, TRY_CAST(s.object_id AS BIGINT) product_id
  FROM ba_preserved.user_scrap_facts s, cm
  WHERE s.base_dt>=CAST(cm.c0 AS VARCHAR) AND s.base_dt<CAST(cm.c0+interval '1' month AS VARCHAR)
    AND s.object_type='PRODUCTION' AND TRY_CAST(s.object_id AS BIGINT) IN ({_FURN_LEAD}) AND s.user_id IS NOT NULL
  GROUP BY 1,2),
buy AS (
  SELECT CAST(o.user_id AS BIGINT) user_id, CAST(o.product_id AS BIGINT) product_id, SUM(o.gmv) gmv
  FROM ba_preserved.commerce_gross_profit_orders o, cm
  WHERE CAST(o.product_id AS BIGINT) IN ({_FURN_LEAD})
    AND o.base_dt>=cm.c0 AND o.base_dt<cm.c0+interval '3' month AND o.yyyymm>=date_format(cm.c0,'%Y%m') AND o.user_id IS NOT NULL
  GROUP BY 1,2),
leads AS (
  SELECT COALESCE(s.user_id,p.user_id) user_id, COALESCE(s.product_id,p.product_id) product_id,
    CASE WHEN s.user_id IS NOT NULL THEN 1 ELSE 0 END scrapped, COALESCE(p.vd,0) vd
  FROM scr s FULL OUTER JOIN (SELECT * FROM pdpf WHERE in_cm=1) p ON s.user_id=p.user_id AND s.product_id=p.product_id)
SELECT CAST(l.product_id AS VARCHAR) pid,
  CASE WHEN l.scrapped=1 AND l.vd>=2 THEN 'T4' WHEN l.scrapped=1 THEN 'T3' WHEN l.vd>=2 THEN 'T2' ELSE 'T1' END tier,
  COUNT(*) leads, SUM(CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END) conv, COALESCE(SUM(b.gmv),0) gmv
FROM leads l LEFT JOIN buy b ON l.user_id=b.user_id AND l.product_id=b.product_id
GROUP BY l.product_id, CASE WHEN l.scrapped=1 AND l.vd>=2 THEN 'T4' WHEN l.scrapped=1 THEN 'T3' WHEN l.vd>=2 THEN 'T2' ELSE 'T1' END
ORDER BY pid, tier"""

# ============ 리드 생성: 노출→리드 도달률(90일) + 월별 신규리드 성장추이(6개월) — 가구만 ============
# reach_to_lead = 리드유저(찜 or PDP재방문2일↑) / PDP도달유저(90일). new lead월 = (user,product)가 처음 리드된 달(첫찜 or 2번째방문일).
# 단일 쿼리로 둘 다 산출(6개월 파티션 1회 스캔). ym='_REACH90' 행=도달률. scan~35GB/run. 오가닉 미구분(데이터 한계).
QUERIES['lead_growth'] = f"""
WITH pdp AS (
  SELECT CAST(user_id AS BIGINT) uid, product_id pid, base_dt bd
  FROM ba_preserved.user_pdp_facts
  WHERE base_dt >= date_trunc('month', DATE_ADD('month',-5,CURRENT_DATE))
    AND product_id IN ({_FURN_LEAD}) AND user_id IS NOT NULL
  GROUP BY CAST(user_id AS BIGINT), product_id, base_dt),
pagg AS (
  SELECT uid, pid, element_at(array_sort(array_agg(bd)),2) second_pdp,
    MAX(CASE WHEN bd >= DATE_ADD('day',-90,CURRENT_DATE) THEN 1 ELSE 0 END) pdp90
  FROM pdp GROUP BY uid, pid),
scr AS (
  SELECT CAST(user_id AS BIGINT) uid, TRY_CAST(object_id AS BIGINT) pid, MIN(CAST(base_dt AS DATE)) first_scrap
  FROM ba_preserved.user_scrap_facts
  WHERE base_dt >= CAST(date_trunc('month', DATE_ADD('month',-5,CURRENT_DATE)) AS VARCHAR)
    AND object_type='PRODUCTION' AND TRY_CAST(object_id AS BIGINT) IN ({_FURN_LEAD}) AND user_id IS NOT NULL
  GROUP BY CAST(user_id AS BIGINT), TRY_CAST(object_id AS BIGINT)),
j AS (
  SELECT COALESCE(p.uid,s.uid) uid, COALESCE(p.pid,s.pid) pid, COALESCE(p.pdp90,0) pdp90,
    CASE WHEN s.first_scrap IS NULL THEN p.second_pdp WHEN p.second_pdp IS NULL THEN s.first_scrap
         ELSE LEAST(s.first_scrap,p.second_pdp) END lead_date
  FROM pagg p FULL OUTER JOIN scr s ON p.uid=s.uid AND p.pid=s.pid)
SELECT CAST(pid AS VARCHAR) pid, date_format(lead_date,'%Y%m') ym, COUNT(*) new_leads, 0 pdp_users, 0 lead_users
FROM j WHERE lead_date IS NOT NULL AND lead_date >= date_trunc('month', DATE_ADD('month',-5,CURRENT_DATE))
GROUP BY pid, date_format(lead_date,'%Y%m')
UNION ALL
SELECT CAST(pid AS VARCHAR) pid, '_REACH90' ym, 0,
  SUM(CASE WHEN pdp90=1 THEN 1 ELSE 0 END), SUM(CASE WHEN pdp90=1 AND lead_date IS NOT NULL THEN 1 ELSE 0 END)
FROM j GROUP BY pid
ORDER BY pid, ym"""

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
WHERE uc.base_dt BETWEEN CAST(DATE_ADD('day',-7,CURRENT_DATE) AS VARCHAR) AND CAST(DATE_ADD('day',-1,CURRENT_DATE) AS VARCHAR)
  AND uc.base='product' AND uc.paid_type='total' AND {_BENCH_CATE[bk]}
GROUP BY 1,2"""
    QUERIES[f'bench_{bk}_lineup'] = f"""
SELECT CAST(uc.id AS VARCHAR) pid, p.product_name, p.selling_cost, p.brand_name,
  SUM(uc.impression_user_join) imp_uv, SUM(uc.pdpview_user_join) pdp_uv, SUM(uc.purchase_user_join) buy_uv
FROM ba_preserved.commerce_daily_user_count_v3 uc
JOIN ba_preserved.comm_product_info_latest p ON TRY_CAST(uc.id AS BIGINT)=p.product_id
WHERE uc.base_dt BETWEEN CAST(DATE_ADD('day',-7,CURRENT_DATE) AS VARCHAR) AND CAST(DATE_ADD('day',-1,CURRENT_DATE) AS VARCHAR)
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

# attach_frame_option: 프레임 옵션을 매트리스/본체/액세서리 3분류로 월별·상품별 집계.
# 🔴 2026-07-16 개편: ①매트리스 판정을 _ismat 정본으로 교체(미포함·커버 오탐 제거)
#   ②상품별(pid) 분해 — 프레임별 attach율 비교가 핵심 지표(studio 0~2% vs basic/refine 21~52%)
#   ③액세서리(패널·협탁) 분리 — 본체 수량이 attach율 분모라 액세서리 섞이면 왜곡
# 스냅샷 창 160→400일: 160일은 옵션명 매칭 62%뿐(cosell 정정 때 확인), 400일 81.4%.
QUERIES['attach_frame_option'] = f"""
WITH fo AS (
  SELECT CAST(product_id AS VARCHAR) pid, option_id, yyyymm, SUM(gmv) gmv, SUM(option_quantity) qty
  FROM ba_preserved.commerce_gross_profit_orders
  WHERE product_id IN ({FOPT_S}) AND {YM6}
  GROUP BY product_id, option_id, yyyymm),
nm AS {_OPT_NM}
SELECT fo.pid, fo.yyyymm,
  CASE WHEN {_ismat('nm.explain')} THEN 'attach'
       WHEN {_ISACC('nm.explain')} THEN 'acc'
       WHEN nm.explain IS NULL THEN 'unknown'
       ELSE 'frame' END typ,
  SUM(fo.gmv) gmv, SUM(fo.qty) qty
FROM fo LEFT JOIN nm ON CAST(fo.option_id AS BIGINT)=nm.id
GROUP BY fo.pid, fo.yyyymm, 3"""

# ============ 합구매 cosell — 옵션매트리스 복구 + 프레임 전수(수납/일반/깔판) ============
# 정정: 프레임 PID 안 추가옵션 매트리스(basic/refine 프레임의 "본넬매트리스" 등)를 복구.
#       옵션명 스냅샷 400일 창(160일은 매칭 62%). 프레임 전수 = 수납침대+일반침대+깔판(refine 저상형).
_FRAME_FILTER_LAYER = "brand_name='오늘의집 layer' AND cate_d2='침대' AND cate_d3 IN ('수납침대','일반침대','깔판') AND product_name NOT LIKE '%매트리스 포함%' AND product_name NOT LIKE '%사용 X%'"
_FRAME_FILTER_ALL = "cate_d2='침대' AND cate_d3 IN ('수납침대','일반침대','깔판') AND product_name NOT LIKE '%매트리스 포함%' AND product_name NOT LIKE '%사용 X%'"
_TIER = "CASE WHEN product_name LIKE '%basic /%' THEN 'basic' WHEN product_name LIKE '%refine /%' THEN 'refine' WHEN product_name LIKE '%studio /%' THEN 'studio' ELSE 'etc' END"
_ISMAT = _ismat('s.explain')  # 정본 단일화(구 인라인 정의와 동일 판정)
_SNAP400 = "s.base_dt >= DATE_ADD('day',-400,CURRENT_DATE)"
_MAT3 = "o.yyyymm >= date_format(DATE_ADD('month',-6,CURRENT_DATE),'%Y%m')"
_FRAME3 = "o.yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m')"
_YM6O = "o.yyyymm >= date_format(DATE_ADD('month',-6,CURRENT_DATE),'%Y%m')"

# 프레임→매트리스 tier 요약 (옵션매트 복구 = mu OR has_opt)
QUERIES['cosell_tier_summary'] = f"""
WITH frames AS (SELECT CAST(product_id AS VARCHAR) pid, product_id pid_b, {_TIER} tier
  FROM ba_preserved.comm_product_info_latest WHERE {_FRAME_FILTER_LAYER}),
fo AS (SELECT DISTINCT f.tier, CAST(o.user_id AS BIGINT) uid, o.option_id
  FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
  WHERE {_FRAME3} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
nm AS (SELECT s.id, MAX(CASE WHEN {_ISMAT} THEN 1 ELSE 0 END) is_mat
  FROM ba_preserved.commerce_snapshot_production_options s JOIN frames fr ON s.production_id=fr.pid_b
  WHERE {_SNAP400} GROUP BY s.id),
fu AS (SELECT fo.tier, fo.uid, MAX(COALESCE(nm.is_mat,0)) has_opt
  FROM fo LEFT JOIN nm ON CAST(fo.option_id AS BIGINT)=nm.id GROUP BY fo.tier, fo.uid),
mu AS (SELECT CAST(o.user_id AS BIGINT) uid, MAX(CASE WHEN p.brand_name='오늘의집 layer' THEN 1 ELSE 0 END) any_our
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE p.cate_d2='매트리스(150T이상)' AND {_MAT3} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1)
SELECT fu.tier, COUNT(DISTINCT fu.uid) frame_users,
  COUNT(DISTINCT CASE WHEN mu.uid IS NOT NULL OR fu.has_opt=1 THEN fu.uid END) bought_mat,
  COUNT(DISTINCT CASE WHEN mu.any_our=1 OR fu.has_opt=1 THEN fu.uid END) our_mat,
  COUNT(DISTINCT CASE WHEN (mu.uid IS NOT NULL AND COALESCE(mu.any_our,0)=0) AND COALESCE(fu.has_opt,0)=0 THEN fu.uid END) comp_only,
  COUNT(DISTINCT CASE WHEN fu.has_opt=1 THEN fu.uid END) opt_mat
FROM fu LEFT JOIN mu ON fu.uid=mu.uid GROUP BY fu.tier"""

# 별도 매트리스PID 행선지 (경쟁유출 확인용; 프레임필터 전수화)
QUERIES['cosell_tier_dest'] = f"""
WITH frames AS (SELECT CAST(product_id AS VARCHAR) pid, {_TIER} tier
  FROM ba_preserved.comm_product_info_latest WHERE {_FRAME_FILTER_LAYER}),
fu AS (SELECT DISTINCT f.tier, CAST(o.user_id AS BIGINT) uid FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
  WHERE {_FRAME3} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
mp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.brand_name, SUM(o.option_quantity) qty, SUM(o.gmv) gmv
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE p.cate_d2='매트리스(150T이상)' AND {_MAT3} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3),
j AS (SELECT fu.tier, mp.pid, mp.brand_name, mp.uid, mp.qty, mp.gmv FROM fu JOIN mp ON fu.uid=mp.uid),
agg AS (SELECT tier, pid, brand_name, COUNT(DISTINCT uid) users, SUM(qty) qty, SUM(gmv) gmv,
  ROW_NUMBER() OVER (PARTITION BY tier ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY tier, pid, brand_name)
SELECT tier, pid, brand_name, users, qty, gmv FROM agg WHERE rn<=6"""

# 역방향 매트리스→프레임 tier (프레임필터 전수화)
QUERIES['m2f_summary'] = f"""
WITH mu AS (SELECT DISTINCT CASE product_id WHEN '1089824' THEN 'basic' WHEN '3607491' THEN 'refine' WHEN '3121605' THEN 'studio' END tier, CAST(user_id AS BIGINT) uid
  FROM ba_preserved.commerce_gross_profit_orders WHERE product_id IN ({MAT_S}) AND yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m') AND option_quantity>0 AND user_id IS NOT NULL),
frames AS (SELECT CAST(product_id AS BIGINT) pid, CASE WHEN brand_name='오늘의집 layer' THEN 1 ELSE 0 END is_self
  FROM ba_preserved.comm_product_info_latest WHERE {_FRAME_FILTER_ALL}),
fu AS (SELECT CAST(o.user_id AS BIGINT) uid, MAX(fr.is_self) any_our FROM ba_preserved.commerce_gross_profit_orders o JOIN frames fr ON CAST(o.product_id AS BIGINT)=fr.pid
  WHERE {_MAT3} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1)
SELECT mu.tier, COUNT(DISTINCT mu.uid) mat_users,
  COUNT(DISTINCT CASE WHEN fu.uid IS NOT NULL THEN mu.uid END) bought_frame,
  COUNT(DISTINCT CASE WHEN fu.any_our=1 THEN mu.uid END) our_frame,
  COUNT(DISTINCT CASE WHEN fu.uid IS NOT NULL AND COALESCE(fu.any_our,0)=0 THEN mu.uid END) comp_only
FROM mu LEFT JOIN fu ON mu.uid=fu.uid WHERE mu.tier IS NOT NULL GROUP BY mu.tier"""

QUERIES['m2f_dest'] = f"""
WITH mu AS (SELECT DISTINCT CASE product_id WHEN '1089824' THEN 'basic' WHEN '3607491' THEN 'refine' WHEN '3121605' THEN 'studio' END tier, CAST(user_id AS BIGINT) uid
  FROM ba_preserved.commerce_gross_profit_orders WHERE product_id IN ({MAT_S}) AND yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m') AND option_quantity>0 AND user_id IS NOT NULL),
fp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.product_name, p.brand_name, SUM(o.gmv) gmv
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE {_FRAME_FILTER_ALL} AND {_MAT3} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3,4),
j AS (SELECT mu.tier, fp.pid, fp.product_name, fp.brand_name, fp.uid, fp.gmv FROM mu JOIN fp ON mu.uid=fp.uid WHERE mu.tier IS NOT NULL),
agg AS (SELECT tier, pid, product_name, brand_name, COUNT(DISTINCT uid) users, SUM(gmv) gmv,
  ROW_NUMBER() OVER (PARTITION BY tier ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY tier, pid, product_name, brand_name)
SELECT tier, pid, product_name, brand_name, users, gmv FROM agg WHERE rn<=6"""

# 경쟁 프레임(데일리리빙) 구매자 → 매트리스 행선지
QUERIES['q1_comp_frame_dest'] = f"""
WITH fu AS (SELECT DISTINCT CAST(user_id AS BIGINT) uid FROM ba_preserved.commerce_gross_profit_orders
  WHERE product_id='2352818' AND yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m') AND option_quantity>0 AND user_id IS NOT NULL),
mp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.product_name, p.brand_name, SUM(o.gmv) gmv
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE p.cate_d2='매트리스(150T이상)' AND {_YM6O} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3,4),
j AS (SELECT mp.pid, mp.product_name, mp.brand_name, mp.uid, mp.gmv FROM fu JOIN mp ON fu.uid=mp.uid),
agg AS (SELECT pid, product_name, brand_name, COUNT(DISTINCT uid) users, SUM(gmv) gmv, ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY 1,2,3)
SELECT (SELECT COUNT(*) FROM fu) buyers, pid, product_name, brand_name, users, gmv FROM agg WHERE rn<=8 ORDER BY users DESC"""

# 경쟁 매트리스(수면밀도) 구매자 → 프레임 행선지 (프레임 전수화)
QUERIES['q2_comp_mat_dest'] = f"""
WITH mu AS (SELECT DISTINCT CAST(user_id AS BIGINT) uid FROM ba_preserved.commerce_gross_profit_orders
  WHERE product_id='1590911' AND yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m') AND option_quantity>0 AND user_id IS NOT NULL),
fp AS (SELECT CAST(o.user_id AS BIGINT) uid, CAST(o.product_id AS BIGINT) pid, p.product_name, p.brand_name, SUM(o.gmv) gmv
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE {_FRAME_FILTER_ALL} AND {_YM6O} AND o.option_quantity>0 AND o.user_id IS NOT NULL GROUP BY 1,2,3,4),
j AS (SELECT fp.pid, fp.product_name, fp.brand_name, fp.uid, fp.gmv FROM mu JOIN fp ON mu.uid=fp.uid),
agg AS (SELECT pid, product_name, brand_name, COUNT(DISTINCT uid) users, SUM(gmv) gmv, ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT uid) DESC) rn FROM j GROUP BY 1,2,3)
SELECT (SELECT COUNT(*) FROM mu) buyers, pid, product_name, brand_name, users, gmv FROM agg WHERE rn<=8 ORDER BY users DESC"""

# 브랜드 생태계 락인 — 옵션매트리스 복구(전 브랜드 프레임), 같은브랜드 합구매율
QUERIES['q3_brand_eco'] = f"""
WITH frames AS (SELECT CAST(product_id AS VARCHAR) pid, product_id pid_b, brand_name b
  FROM ba_preserved.comm_product_info_latest WHERE {_FRAME_FILTER_ALL}),
fo AS (SELECT DISTINCT f.b, CAST(o.user_id AS BIGINT) uid, o.option_id
  FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
  WHERE {_YM6O} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
fq AS (SELECT f.b, o.option_id, SUM(o.option_quantity) qty
  FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON o.product_id=f.pid
  WHERE {_YM6O} AND o.option_quantity>0 GROUP BY f.b, o.option_id),
nm AS (SELECT s.id, MAX(CASE WHEN {_ISMAT} THEN 1 ELSE 0 END) is_mat
  FROM ba_preserved.commerce_snapshot_production_options s JOIN frames fr ON s.production_id=fr.pid_b
  WHERE {_SNAP400} GROUP BY s.id),
fu AS (SELECT fo.b, fo.uid, MAX(COALESCE(nm.is_mat,0)) has_opt FROM fo LEFT JOIN nm ON CAST(fo.option_id AS BIGINT)=nm.id GROUP BY fo.b, fo.uid),
mr AS (SELECT fq.b, ROUND(SUM(CASE WHEN nm.id IS NOT NULL THEN fq.qty ELSE 0 END)*100.0/NULLIF(SUM(fq.qty),0),1) match_pct
       FROM fq LEFT JOIN nm ON CAST(fq.option_id AS BIGINT)=nm.id GROUP BY fq.b),
mb AS (SELECT DISTINCT CAST(o.user_id AS BIGINT) uid, p.brand_name b
  FROM ba_preserved.commerce_gross_profit_orders o JOIN ba_preserved.comm_product_info_latest p ON CAST(o.product_id AS BIGINT)=p.product_id
  WHERE p.cate_d2='매트리스(150T이상)' AND {_YM6O} AND o.option_quantity>0 AND o.user_id IS NOT NULL)
SELECT fu.b brand, COUNT(DISTINCT fu.uid) frame_buyers,
  COUNT(DISTINCT CASE WHEN mb.uid IS NOT NULL OR fu.has_opt=1 THEN fu.uid END) same_brand_mat,
  ROUND(COUNT(DISTINCT CASE WHEN mb.uid IS NOT NULL OR fu.has_opt=1 THEN fu.uid END)*100.0/COUNT(DISTINCT fu.uid),1) rate_pct,
  MAX(mr.match_pct) opt_join_match_pct
FROM fu LEFT JOIN mb ON fu.uid=mb.uid AND fu.b=mb.b LEFT JOIN mr ON fu.b=mr.b
GROUP BY fu.b HAVING COUNT(DISTINCT fu.uid)>=50 ORDER BY same_brand_mat DESC LIMIT 12"""

QUERIES['q4_attach_detail'] = f"""
WITH frames AS (SELECT product_id pid_b, {_TIER} tier FROM ba_preserved.comm_product_info_latest WHERE {_FRAME_FILTER_LAYER}),
fo AS (SELECT o.product_id, o.option_id, SUM(o.gmv) gmv, SUM(o.option_quantity) qty
  FROM ba_preserved.commerce_gross_profit_orders o JOIN frames f ON CAST(o.product_id AS BIGINT)=f.pid_b
  WHERE o.yyyymm >= date_format(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m') AND o.option_quantity>0 GROUP BY o.product_id, o.option_id),
nm AS (SELECT s.id, arbitrary(s.explain) explain, arbitrary({_TIER.replace('product_name','s2.product_name')}) tier
  FROM ba_preserved.commerce_snapshot_production_options s
  JOIN ba_preserved.comm_product_info_latest s2 ON s.production_id=s2.product_id
  WHERE {_SNAP400} GROUP BY s.id)
SELECT fo.product_id frame_pid, nm.tier, nm.explain option_name, fo.qty, fo.gmv
FROM fo LEFT JOIN nm ON CAST(fo.option_id AS BIGINT)=nm.id
WHERE nm.explain LIKE '%매트리스%' AND nm.explain NOT LIKE '%패드%' AND nm.explain NOT LIKE '%밀림방지%' AND nm.explain NOT LIKE '%커버%' AND nm.explain NOT LIKE '%토퍼%' AND fo.qty>0
ORDER BY fo.product_id, fo.gmv DESC"""

# Q1 데일리리빙 프레임 옵션매트리스 (행선지 자사옵션 행 추가용)
QUERIES['q1_opt'] = f"""
WITH fb AS (SELECT CAST(user_id AS BIGINT) uid, option_id, gmv, option_quantity qty
  FROM ba_preserved.commerce_gross_profit_orders o WHERE o.product_id='2352818' AND {_FRAME3} AND o.option_quantity>0 AND o.user_id IS NOT NULL),
nm AS (SELECT s.id, MAX(CASE WHEN {_ISMAT} THEN 1 ELSE 0 END) is_mat
  FROM ba_preserved.commerce_snapshot_production_options s WHERE s.production_id=2352818 AND {_SNAP400} GROUP BY s.id)
SELECT COUNT(DISTINCT fb.uid) users, SUM(fb.gmv) gmv, SUM(fb.qty) qty
FROM fb JOIN nm ON CAST(fb.option_id AS BIGINT)=nm.id WHERE nm.is_mat=1"""

if __name__ == '__main__':
    print(f"QUERIES: {len(QUERIES)}")
    for k in QUERIES: print(' -', k)
