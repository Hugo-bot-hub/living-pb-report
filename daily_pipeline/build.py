# -*- coding: utf-8 -*-
"""결정적 빌더: daily_pipeline/tmp/{name}.json (Athena data 2D배열) → tf-data.json + tf-mattress-data.json 재구성.
모든 변환·반올림·정렬·불변식 하드코딩. 기존 파일에서 products config·daily history 보존.
tmp 파일 없으면 해당 섹션은 기존 값 유지(부분 실행 안전).
"""
import json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp')
TF = os.environ.get('TF_DATA_PATH', os.path.join(ROOT, 'tf-data.json'))
MAT = os.environ.get('MAT_DATA_PATH', os.path.join(ROOT, 'tf-mattress-data.json'))
TMP = os.environ.get('TF_TMP', TMP)

SELF = ['3918642','3640244','3640123','1089824','3607491','3121605','3898593','3898584','3748221','2518275']
COMP = ['767440','2636441','1930788','442026','676405','2731307','329364','1590911','2352818']
ALL18 = SELF + COMP
LAYER = '오늘의집 layer'

def load(name):
    p = os.path.join(TMP, name + '.json')
    if not os.path.exists(p): return None
    with open(p, encoding='utf-8') as f:
        d = json.load(f)
    return d.get('data', d) if isinstance(d, dict) else d

def rows(name, cols):
    data = load(name)
    if data is None: return None
    return [dict(zip(cols, r)) for r in data]

def rows_req(name, cols):
    # 빈 결과(=상류 소스 동결/타임아웃)를 None 취급 → 해당 섹션 기존 값 보존.
    # 2026-07-21: SRP 소스(commerce_srp_dataset_daily_v0_0_2) 07-06 동결이 빈 배열을 반환해
    # srpMatrix/srpKeywords/srpKeywordRank/keywordRadar를 조용히 덮어쓴 사고 재발 방지.
    r = rows(name, cols)
    return r if r else None

def i(x):
    if x in (None,''): return 0
    return int(float(x))
def f(x):
    if x in (None,''): return 0.0
    return float(x)

with open(TF, encoding='utf-8') as fp: D = json.load(fp)
with open(MAT, encoding='utf-8') as fp: M = json.load(fp)
changed = []

# ---------- daily (funnel∩UV + GMV leftjoin, history 보존) ----------
fn = rows('daily_funnel', ['dt','pid','imp','pdp','purchase'])
uv = rows('daily_uv', ['dt','pid','imp_uv','pdp_uv','buy_uv'])
gm = rows('daily_gmv', ['dt','pid','gmv','gp','qty'])
if fn is not None and uv is not None and gm is not None:
    fmap = {(r['dt'],str(r['pid'])):r for r in fn}
    umap = {(r['dt'],str(r['pid'])):r for r in uv}
    gmap = {(r['dt'],str(r['pid'])):r for r in gm}
    for pid in ALL18:
        old = {r['dt']:r for r in D['daily'].get(pid, [])}
        for (dt,p),fr in fmap.items():
            if p != pid: continue
            ur = umap.get((dt,p))
            if ur is None: continue  # funnel∩UV 6필드 필수
            gr = gmap.get((dt,p), {})
            old[dt] = {'dt':dt, 'gmv':i(gr.get('gmv')), 'gp':round(f(gr.get('gp'))), 'qty':i(gr.get('qty')),
                       'imp':i(fr['imp']), 'pdp':i(fr['pdp']), 'purchase':i(fr['purchase']),
                       'imp_uv':i(ur['imp_uv']), 'pdp_uv':i(ur['pdp_uv']), 'buy_uv':i(ur['buy_uv'])}
        D['daily'][pid] = [old[k] for k in sorted(old)]
    changed.append('daily')

# ---------- SRP base (matrix + keywordRank) ----------
base = rows_req('srp_base14', ['pid','kw','rank','best','score','ctr','imp'])
qc = rows_req('qc_integrated', ['kw','qc2w','qc4w'])
if base is not None:
    # srpMatrix: pid별 imp 상위 12
    by_pid = {}
    for r in base:
        by_pid.setdefault(str(r['pid']), []).append(r)
    matrix = []
    for pid, rs in by_pid.items():
        for r in sorted(rs, key=lambda x: -i(x['imp']))[:12]:
            matrix.append({'pid':str(pid), 'kw':r['kw'], 'rank':round(f(r['rank']),2), 'best':i(r['best']),
                           'score':round(f(r['score']),3), 'ctr':round(f(r['ctr'])*100,2), 'imp':i(r['imp'])})
    D['srpMatrix'] = matrix
    changed.append('srpMatrix')
    # srpKeywordRank: qc(통합검색 2w) 조인, pid별 qc 상위 15
    if qc is not None:
        qmap = {r['kw']:r for r in qc}
        krank = {}
        for pid, rs in by_pid.items():
            lst = []
            for r in rs:
                q = qmap.get(r['kw'])
                if not q: continue
                lst.append({'kw':r['kw'], 'qc':i(q['qc2w']), 'rank':round(f(r['rank']),1), 'best':i(r['best']),
                            'imp':i(r['imp']), 'ctr':round(f(r['ctr'])*100,2), 'qc_4w_int':i(q['qc4w'])})
            lst.sort(key=lambda x:-x['qc'])
            krank[pid] = lst[:15]
        D['srpKeywordRank'] = krank
        changed.append('srpKeywordRank')

# ---------- srpKeywords (자사 14d 키워드추세) ----------
kt = rows_req('srp_kwtrend', ['dt','pid','kw','rank','best','score'])
if kt is not None:
    sk = {}
    for r in kt:
        sk.setdefault(str(r['pid']), []).append({'dt':r['dt'],'kw':r['kw'],'rank':round(f(r['rank']),1),'best':i(r['best']),'score':round(f(r['score']),3)})
    for pid in sk: sk[pid].sort(key=lambda x:(x['dt'],x['kw']), reverse=True)
    D['srpKeywords'] = sk
    changed.append('srpKeywords')

# ---------- keyword_radar (경쟁 레이더: 신규경쟁자·부스트/광고 의심 자동감지) ----------
kr = rows_req('kw_radar', ['kw','pid','brand','name','price','rank','imp','ctr','buy','rev'])
if kr is not None:
    KW_ORDER = ['매트리스','침대프레임','수납침대','이불','차렵이불','냉감이불']
    radar = {}
    for r in kr:
        pid = str(r['pid']); rank = f(r['rank']); buy = i(r['buy']); rev = i(r['rev'])
        # 자사 판정 = 숏헤드 로스터(SELF) OR 브랜드명이 layer (로스터 밖 layer 상품도 '우리'로 인식, 경쟁사 오인 방지)
        is_self = (pid in SELF) or (r['brand'] == LAYER)
        tags = []
        if is_self:
            tags.append('ours')
        else:
            if pid in COMP: tags.append('known_comp')
            # 부스트/광고 의심 = 노출 상위(근사순위<=10)인데 주간판매(<3)·리뷰(<1000) 바닥.
            if rank <= 10 and buy < 3 and rev < 1000: tags.append('boost_suspect')
            if rev < 300: tags.append('new_entrant')
        radar.setdefault(r['kw'], []).append({'pid':pid,'brand':r['brand'] or '','name':r['name'] or '',
            'price':i(r['price']),'rank':round(rank,1),'imp':i(r['imp']),'ctr':round(f(r['ctr']),1),
            'buy':buy,'rev':rev,'is_self':is_self,'tags':tags})
    out = {}
    for kw in radar: out[kw] = sorted(radar[kw], key=lambda x:x['rank'])[:12]
    new_threats = 0; boost_cnt = 0; our_best = {}
    for kw, lst in out.items():
        for x in lst:
            if 'new_entrant' in x['tags']: new_threats += 1
            if 'boost_suspect' in x['tags'] and not x['is_self']: boost_cnt += 1
            if x['is_self'] and (kw not in our_best or x['rank'] < our_best[kw]): our_best[kw] = x['rank']
    D['keywordRadar'] = {'keywords':[k for k in KW_ORDER if k in out]+[k for k in out if k not in KW_ORDER],
        'data':out,'summary':{'new_threats':new_threats,'boost_suspect':boost_cnt,'our_best_rank':our_best},
        'window':'최근 7일 노출 기준 (상위 15위)',
        'note':'노출순위 = 주간 노출량 기준 근사순위(검색 원장에 절대순위 없음). ⚠️부스트/광고 의심 = 노출 상위인데 주간판매·리뷰 바닥. 소스: commerce_srp_query_product_metrics_v0_1.'}
    changed.append('keywordRadar')

# ---------- lead_funnel (표준통일: 가구 리드가치 gross·상품매칭·성숙코호트 + 의도등급 T1~T4) ----------
lf = rows('lead_funnel', ['pid','tier','leads','conv','gmv'])
if lf is not None:
    LF_NAME = {'1243313':'basic 침대프레임'}
    byp = {}
    for r in lf:
        byp.setdefault(str(r['pid']), {})[r['tier']] = {'leads':i(r['leads']),'conv':i(r['conv']),'gmv':i(r['gmv'])}
    prods = []; total_hot_out = 0; tot_scrap_leads = 0; tot_scrap_gmv = 0
    for pid, tiers in byp.items():
        t = {}
        for tk in ['T1','T2','T3','T4']:
            v = tiers.get(tk, {'leads':0,'conv':0,'gmv':0}); leads = v['leads']; conv = v['conv']; gmv = v['gmv']
            t[tk] = {'leads':leads,'conv':conv,'gmv':gmv,'rate':round(conv*100.0/leads,1) if leads else 0.0,
                     'value':round(gmv/leads) if leads else 0,'outstanding':leads-conv}
        total_leads = sum(t[tk]['leads'] for tk in t)
        scrap_leads = t['T3']['leads']+t['T4']['leads']; scrap_conv = t['T3']['conv']+t['T4']['conv']
        scrap_gmv = t['T3']['gmv']+t['T4']['gmv']
        lead_value = round(scrap_gmv/scrap_leads) if scrap_leads else 0
        tot_scrap_leads += scrap_leads; tot_scrap_gmv += scrap_gmv
        hot_out = t['T4']['outstanding']; total_hot_out += hot_out
        name = D['products'].get(pid,{}).get('shortName') or LF_NAME.get(pid, pid)
        prods.append({'pid':pid,'name':name,'tiers':t,'summary':{'total_leads':total_leads,
            'scrap_to_buy_rate':round(scrap_conv*100.0/scrap_leads,1) if scrap_leads else 0.0,
            'scrap_leads':scrap_leads,'lead_value':lead_value,'hot_uncaptured':hot_out}})
    prods.sort(key=lambda x:(-x['summary']['lead_value'],x['pid']))
    D['leadFunnel'] = {'products':prods,'window':'찜월→forward 3개월 성숙 코호트',
        'summary':{'furniture_hot_uncaptured':total_hot_out,'bench_scrap_to_buy':6.9,
            'lead_value_blended':round(tot_scrap_gmv/tot_scrap_leads) if tot_scrap_leads else 0},
        'note':'리드가치 = 찜한 distinct 유저가 찜월 forward 3개월 내 그 찜한 상품을 산 실현 GMV ÷ 찜 유저(gross·상품매칭·성숙 코호트). vs 리드 CAC로 매체 투자 판정(정의블록=통합공식 문서). T4 찜+재방문/T3 찜만/T2 재방문만/T1 1회. 찜→구매=찜리드(T3+T4) 전환율(벤치 6.9%). 로그인 유저·비로그인 제외.'}
    changed.append('leadFunnel')

# ---------- lead_growth (노출→리드 도달률 90일 + 월별 신규리드 성장추이 6개월) ----------
lg = rows('lead_growth', ['pid','ym','new_leads','pdp_users','lead_users'])
if lg is not None:
    LFN = {'1243313':'basic 침대프레임'}
    byp = {}; reach = {}
    for r in lg:
        pid = str(r['pid'])
        if r['ym'] == '_REACH90':
            pu = i(r['pdp_users']); lu = i(r['lead_users'])
            reach[pid] = {'pdp_users':pu,'lead_users':lu,'reach_to_lead':round(lu*100.0/pu,1) if pu else 0.0}
        else:
            byp.setdefault(pid, []).append({'ym':r['ym'],'new_leads':i(r['new_leads'])})
    all_ym = sorted({m['ym'] for lst in byp.values() for m in lst})
    partial_ym = all_ym[-1] if all_ym else None
    prods = []; total_monthly = {}
    for pid, months in byp.items():
        months.sort(key=lambda x:x['ym'])
        for m in months: total_monthly[m['ym']] = total_monthly.get(m['ym'],0) + m['new_leads']
        comp = [m for m in months if m['ym'] != partial_ym]  # 완료월만 추세계산(당월 부분 제외)
        trend = None
        if len(comp) >= 2 and comp[-2]['new_leads']:
            trend = round((comp[-1]['new_leads']-comp[-2]['new_leads'])/comp[-2]['new_leads']*100)
        rc = reach.get(pid, {})
        name = D['products'].get(pid,{}).get('shortName') or LFN.get(pid, pid)
        prods.append({'pid':pid,'name':name,'reach_to_lead':rc.get('reach_to_lead',0),
            'pdp_users':rc.get('pdp_users',0),'lead_users':rc.get('lead_users',0),'monthly':months,'trend_pct':trend})
    prods.sort(key=lambda x:(-x['reach_to_lead'],x['pid']))
    D['leadGrowth'] = {'products':prods,'total_monthly':[{'ym':y,'new_leads':total_monthly[y]} for y in sorted(total_monthly)],
        'partial_ym':partial_ym,
        'note':'리드화율=리드유저(찜 or PDP재방문2일↑)/PDP도달유저(90일). 월별 신규리드=그 달 처음 리드된 user×product(첫 찜 or 2번째 방문일). 오가닉/비오가닉 미구분(데이터 한계). 마지막 달=진행중(부분). 로그인 유저 기준.'}
    changed.append('leadGrowth')

# ---------- scoreTs / featTs (self+comp 병합) ----------
sc = (rows('scorets_self',['dt','pid','score']) or []) + (rows('scorets_comp',['dt','pid','score']) or [])
if sc:
    ts = {}
    for r in sc: ts.setdefault(str(r['pid']),[]).append({'dt':r['dt'],'score':round(f(r['score']),3)})
    for pid in ts: ts[pid].sort(key=lambda x:x['dt'])
    D['scoreTs'] = ts; changed.append('scoreTs')
ff = (rows('featts_self',['dt','pid','review','sell28','view28','spv28','wish','card','qc_rank']) or []) + \
     (rows('featts_comp',['dt','pid','review','sell28','view28','spv28','wish','card','qc_rank']) or [])
if ff:
    ft = {}
    for r in ff:
        ft.setdefault(str(r['pid']),[]).append({'dt':r['dt'],'review':round(f(r['review']),4),'sell28':round(f(r['sell28']),4),
            'view28':round(f(r['view28']),4),'spv28':round(f(r['spv28']),4),'wish':round(f(r['wish']),4),
            'card':round(f(r['card']),4),'qc_rank':round(f(r['qc_rank']),2)})
    for pid in ft: ft[pid].sort(key=lambda x:x['dt'])
    D['srpFeatureTs'] = ft; changed.append('srpFeatureTs')

# ---------- inflow (4윈도우 concat) ----------
inf = []
ok_inf = True
for w in range(4):
    r = rows(f'inflow_w{w}', ['dt','pid','inflow','imp','click','click_uv'])
    if r is None: ok_inf = False; break
    inf += r
if ok_inf and inf:
    D['inflow'] = sorted([{'dt':r['dt'],'pid':str(r['pid']),'inflow':r['inflow'],'imp':i(r['imp']),'click':i(r['click']),'click_uv':i(r['click_uv'])} for r in inf],
                         key=lambda x:(x['dt'],x['pid'],-x['imp'],x['inflow']))  # imp 동점 → 채널명으로 완전순서
    changed.append('inflow')

# ---------- inflowCvr ----------
cv = rows('inflow_cvr', ['dt','pid','inflow','click_uv','buy_uv'])
if cv is not None:
    agg = {}
    dts = set()
    for r in cv:
        dts.add(r['dt'])
        cell = agg.setdefault(str(r['pid']),{}).setdefault(r['inflow'],{'click_uv':0,'buy_uv':0})
        cell['click_uv']+=i(r['click_uv']); cell['buy_uv']+=i(r['buy_uv'])
    for pid in agg:
        for ch,c in agg[pid].items():
            c['cvr']=round(c['buy_uv']/c['click_uv']*100,2) if c['click_uv'] else 0.0
    ds=sorted(dts)
    D['inflowCvr']={'period':f"{ds[0]} ~ {ds[-1]}" if ds else '', 'note':'Same-day click → purchase attribution (UV 기준, 7일)', 'data':agg}
    changed.append('inflowCvr')

# ---------- ages (월요일만 tmp 존재) ----------
ag = rows('ages', ['pid','ag','cnt'])
if ag is not None:
    AGE=['20-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60+']
    cnt={p:{k:0 for k in AGE} for p in ALL18}
    for r in ag:
        p=str(r['pid'])
        if p in cnt and r['ag'] in cnt[p]: cnt[p][r['ag']]=i(r['cnt'])
    buyer=[]; parts=[]
    for pid in ALL18:
        s=sum(cnt[pid].values())
        age={k:(round(cnt[pid][k]/s*100,1) if s else 0.0) for k in AGE}
        sn=D['products'].get(pid,{}).get('shortName',pid)
        e={'pid':pid,'product':f'{sn} ({pid})','sample':s,'age':age}
        if s<100: e['warn']=True
        buyer.append(e)
        if pid in SELF: parts.append(f"{sn} 25-34세 {round(age['25-29']+age['30-34'],1)}% (샘플 {s})")
    D['ages']['buyer']=buyer
    D['ages']['insight']=' | '.join(parts)
    changed.append('ages')

# ---------- benchmarks (bedding/mattress/bed) ----------
BANDS = {
 'bedding':[('01_<30k','<3만원'),('02_30-50k','3-5만원'),('03_50-70k','5-7만원'),('04_70-100k','7-10만원'),('05_100k+','10만원+')],
 'mattress':[('01_<20만','<20만'),('02_20-40만','20-40만'),('03_40-70만','40-70만'),('04_70-120만','70-120만'),('05_120만+','120만+')],
 'bed':[('01_<50만','<50만'),('02_50-80만','50-80만'),('03_80-120만','80-120만'),('04_120만+','120만+')],
}
def band_of(bk, price):
    p=price
    if bk=='bedding': return '01_<30k' if p<30000 else '02_30-50k' if p<50000 else '03_50-70k' if p<70000 else '04_70-100k' if p<100000 else '05_100k+'
    if bk=='mattress': return '01_<20만' if p<200000 else '02_20-40만' if p<400000 else '03_40-70만' if p<700000 else '04_70-120만' if p<1200000 else '05_120만+'
    return '01_<50만' if p<500000 else '02_50-80만' if p<800000 else '03_80-120만' if p<1200000 else '04_120만+'
def pct(n,d): return round(n/d*100,2) if d else 0.0
def ratio(a,b): return round(a/b,2) if b else 0.0

benchmarks={}
for bk in ['bedding','mattress','bed']:
    bd = rows(f'bench_{bk}_bands', ['pb_flag','price_band','prod_cnt','imp_uv','pdp_uv','buy_uv'])
    lu = rows(f'bench_{bk}_lineup', ['pid','product_name','selling_cost','brand_name','imp_uv','pdp_uv','buy_uv'])
    if bd is None or lu is None:
        benchmarks[bk] = D.get('benchmarks',{}).get(bk); continue
    bmap={}
    for r in bd: bmap[(r['pb_flag'],r['price_band'])]={'cnt':i(r['prod_cnt']),'imp':i(r['imp_uv']),'pdp':i(r['pdp_uv']),'buy':i(r['buy_uv'])}
    p3tot=[0,0,0]; pbands=[]
    for band,label in BANDS[bk]:
        pb=bmap.get(('PB',band),{'cnt':0,'imp':0,'pdp':0,'buy':0}); p3=bmap.get(('3P',band),{'cnt':0,'imp':0,'pdp':0,'buy':0})
        p3tot=[p3tot[0]+p3['imp'],p3tot[1]+p3['pdp'],p3tot[2]+p3['buy']]
        pb_ctr=pct(pb['pdp'],pb['imp']); p3_ctr=pct(p3['pdp'],p3['imp']); pb_cvr=pct(pb['buy'],pb['pdp']); p3_cvr=pct(p3['buy'],p3['pdp'])
        pbands.append({'band':band,'label':label,'pb_prod_cnt':pb['cnt'],'p3_prod_cnt':p3['cnt'],
            'pb_imp_uv':pb['imp'],'pb_pdp_uv':pb['pdp'],'pb_buy_uv':pb['buy'],'p3_imp_uv':p3['imp'],'p3_pdp_uv':p3['pdp'],'p3_buy_uv':p3['buy'],
            'pb_ctr':pb_ctr,'p3_ctr':p3_ctr,'ctr_ratio':ratio(pb_ctr,p3_ctr),'pb_cvr':pb_cvr,'p3_cvr':p3_cvr,'cvr_ratio':ratio(pb_cvr,p3_cvr)})
    cate_ctr=pct(p3tot[1],p3tot[0]); cate_cvr=pct(p3tot[2],p3tot[1])
    bl=[]; sp=[]
    band_lu={b['band']:b for b in pbands}
    for r in lu:
        iu,pu,bu=i(r['imp_uv']),i(r['pdp_uv']),i(r['buy_uv'])
        ctr=pct(pu,iu); cvr=pct(bu,pu); price=i(r['selling_cost'])
        line = 'basic' if 'basic /' in (r['product_name'] or '') else 'refine' if 'refine /' in (r['product_name'] or '') else 'studio' if 'studio /' in (r['product_name'] or '') else ''
        is_self = str(r['pid']) in SELF
        bl.append({'pid':str(r['pid']),'name':r['product_name'],'line':line,'price':price,'is_self':is_self,'imp_uv':iu,'pdp_uv':pu,'buy_uv':bu,'ctr':ctr,'cvr':cvr})
        if is_self:
            bd2=band_of(bk,price); bb=band_lu.get(bd2,{})
            sp.append({'pid':str(r['pid']),'name':r['product_name'],'line':line,'price':price,'price_band':bd2,'ctr':ctr,'cvr':cvr,
                'imp_uv':iu,'pdp_uv':pu,'buy_uv':bu,'cate_p3_ctr':cate_ctr,'cate_p3_cvr':cate_cvr,
                'cate_ctr_ratio':ratio(ctr,cate_ctr),'cate_cvr_ratio':ratio(cvr,cate_cvr),
                'band_p3_ctr':bb.get('p3_ctr',0.0),'band_p3_cvr':bb.get('p3_cvr',0.0),
                'band_ctr_ratio':ratio(ctr,bb.get('p3_ctr',0)),'band_cvr_ratio':ratio(cvr,bb.get('p3_cvr',0))})
    # Athena 가 lu 를 어떤 순서로 주든 동일 출력이 되도록 정렬(내용은 같고 순서만 흔들리던 것)
    bl.sort(key=lambda x:(-x['imp_uv'],x['pid'])); sp.sort(key=lambda x:(-x['imp_uv'],x['pid']))
    benchmarks[bk]={'self_products':sp,'price_bands':pbands,'pb_lineup':bl,
        'cate_3p_avg':{'imp_uv':p3tot[0],'pdp_uv':p3tot[1],'buy_uv':p3tot[2],'ctr':cate_ctr,'cvr':cate_cvr}}
if any(f'bench_{b}_bands' for b in ['bedding','mattress','bed'] if load(f'bench_{b}_bands') is not None):
    D['benchmarks']=benchmarks
    if benchmarks.get('bedding'): D['categoryBenchmark']=benchmarks['bedding']
    changed.append('benchmarks')

# ---------- 매트리스 decomp (tf-mattress-data.json) ----------
def parse_size(ex2, ex):
    t=(ex2 or '')+' '+(ex or '')
    for kw,sz in [('라지킹','LK'),('슈퍼싱글','SS'),('퀸','Q'),('킹','K'),('더블','D'),('싱글','S'),('LK','LK'),('SS','SS'),(' Q',' Q'),(' K',' K'),(' D',' D'),(' S',' S')]:
        if kw in t: return sz.strip()
    return ''
def parse_hard(ex):
    e=ex or ''
    if '미디엄소프트' in e or '미디엄 소프트' in e or '포근한' in e: return '미디엄소프트'
    if '미디엄하드' in e or '미디엄 하드' in e or '단단한' in e: return '미디엄하드'
    if '미디엄' in e: return '미디엄'
    return ''
dc = rows('decomp', ['pid','explain','explain2','qty','gmv','unit'])
if dc is not None:
    decomp={}
    for r in dc:
        ex=r['explain'] or ''
        if '커버' in ex: continue  # 방수커버 제외
        decomp.setdefault(str(r['pid']),[]).append({'name':ex,'size':parse_size(r['explain2'],ex),'hardness':parse_hard(ex),
            'qty':i(r['qty']),'unit':round(f(r['unit'])),'gmv':i(r['gmv'])})
    for pid in decomp: decomp[pid].sort(key=lambda x:-x['gmv'])
    M['decomp']=decomp; changed.append('decomp')

# 프레임 PID→표시명. attach_rate_by_frame·attach_frame_option_detail 공용이라 두 섹션보다 먼저 정의.
FRAME_NM={'1243313':'basic 바른수납','1800535':'basic 접이식철제','3858646':'refine 빅수납','3652507':'refine 수납',
 '3621320':'refine 파티션','2518275':'refine 빅수납호텔','3602793':'refine 저상형','3898593':'studio 코타수납',
 '3898584':'studio 코타평상','3748221':'studio 페이브수납','3116503':'studio 페이브솔리드','3146345':'studio 페이브패브릭','3146405':'studio 페이브패브릭'}

# ---------- attach_frame_option ----------
# 쿼리가 pid·typ(attach/frame/acc) 3분류로 확장(2026-07-16). 산출물 2개:
#  ① attach_frame_option = 월별 attach 합계 flat array (기존 차트 하위호환, 구조 불변)
#  ② attach_rate_by_frame = 프레임별 월별 부착률(attach_qty/frame_qty). acc(패널·협탁)는 분모에서 제외.
af = rows('attach_frame_option', ['pid','yyyymm','typ','gmv','qty'])
if af is not None:
    mm={}
    for r in af:
        if r['typ']!='attach': continue
        y=r['yyyymm']; e=mm.setdefault(y,{'yyyymm':y,'gmv':0,'qty':0})
        e['gmv']+=i(r['gmv']); e['qty']+=i(r['qty'])
    # attach 없는 달도 0으로
    allym=sorted({r['yyyymm'] for r in af})
    M['attach_frame_option']=[mm.get(y,{'yyyymm':y,'gmv':0,'qty':0}) for y in allym]
    changed.append('attach_frame_option')

    # ② 프레임별 부착률
    cell={}
    for r in af:
        k=(str(r['pid']), r['yyyymm']); c=cell.setdefault(k,{})
        t=r['typ']; c[t+'_gmv']=c.get(t+'_gmv',0)+i(r['gmv']); c[t+'_qty']=c.get(t+'_qty',0)+i(r['qty'])
    byf={}
    for (pid,ym),c in sorted(cell.items()):
        fq=c.get('frame_qty',0); aq=c.get('attach_qty',0)
        byf.setdefault(pid,[]).append({'yyyymm':ym,'frame_qty':fq,'attach_qty':aq,'acc_qty':c.get('acc_qty',0),
            'unknown_qty':c.get('unknown_qty',0),
            'frame_gmv':c.get('frame_gmv',0),'attach_gmv':c.get('attach_gmv',0),'acc_gmv':c.get('acc_gmv',0),
            'rate_pct':round(aq*100.0/fq,1) if fq>0 else 0.0})
    prods=[]; tot_unknown=0
    for pid,ms in byf.items():
        tf=sum(m['frame_qty'] for m in ms); ta=sum(m['attach_qty'] for m in ms)
        uk=sum(m['unknown_qty'] for m in ms); tot_unknown+=uk
        prods.append({'pid':pid,'name':FRAME_NM.get(pid,pid),'months':ms,'frame_qty':tf,'attach_qty':ta,
            'attach_gmv':sum(m['attach_gmv'] for m in ms),'unknown_qty':uk,
            'rate_pct':round(ta*100.0/tf,1) if tf>0 else 0.0})
    prods.sort(key=lambda x:(-x['rate_pct'],x['pid']))
    # unknown = 옵션명 미매칭. frame 에 섞으면 분모가 부풀어 부착률이 과소 → 별도 분리하고 규모를 노출.
    M['attach_rate_by_frame']={'products':prods,'window':'최근 6개월','unknown_qty':tot_unknown,
        'note':'부착률=같은 PID 안에서 매트리스 옵션 수량 ÷ 프레임 본체 수량. 액세서리(패널·협탁·400 서랍/선반형·패드·커버)는 분모·분자 모두 제외. '
               '옵션명 "매트리스 미포함"·"매트리스커버"는 매트리스로 세지 않음. 100% 초과 가능(프레임 1대에 매트리스 2개 등). '
               '(2026-07-16: 오탐 필터·비결정 옵션명·본체/액세서리 경계 수정)'}
    changed.append('attach_rate_by_frame')
    if tot_unknown:
        print('  [warn] attach_rate_by_frame: 옵션명 미매칭 %d개 — 부착률 분모에서 제외됨' % tot_unknown)

# ---------- 프레임옵션 매트리스 상세 (전 layer 프레임·옵션별·tier) ----------
ad=rows('q4_attach_detail',['frame_pid','tier','option_name','qty','gmv'])
opt_by_tier={}
if ad is not None:
    items=[{'frame_pid':str(x['frame_pid']),'frame_name':FRAME_NM.get(str(x['frame_pid']),str(x['frame_pid'])),
            'tier':x.get('tier') or 'etc','option_name':x['option_name'],'qty':i(x['qty']),'gmv':i(x['gmv'])} for x in ad]
    items.sort(key=lambda x:(x['tier'],x['frame_pid'],-x['gmv'],x['option_name']))  # 동점 gmv → 이름으로 완전순서
    for x in items:
        t=x['tier']; opt_by_tier.setdefault(t,{'qty':0,'gmv':0}); opt_by_tier[t]['qty']+=x['qty']; opt_by_tier[t]['gmv']+=x['gmv']
    M['attach_frame_option_detail']={'items':items,
        'total':{'qty':sum(t['qty'] for t in items),'gmv':sum(t['gmv'] for t in items)},
        'window':'최근 3개월','note':'전 layer 침대프레임(basic/refine/studio)의 추가옵션 매트리스 전수. 상품별·옵션별 세부.'}
    changed.append('attach_frame_option_detail')

# ---------- cosell_by_tier (프레임→매트리스, 옵션매트리스 복구) ----------
NAME={'1590911':'수면밀도','1089824':'basic매트리스(우리)','329364':'휴도','858905':'웰퍼니쳐',
'2737888':'지누스','425266':'먼데이하우스','3607491':'refine매트리스(우리)','1525743':'웰퍼니쳐','3121605':'studio매트리스(우리)'}
cs=rows('cosell_tier_summary',['tier','frame_users','bought_mat','our_mat','comp_only','opt_mat'])
cd=rows('cosell_tier_dest',['tier','pid','brand_name','users','qty','gmv'])
if cs is not None and cd is not None:
    dest={}
    for r in cd:
        dest.setdefault(r['tier'],[]).append({'pid':str(r['pid']),'name':NAME.get(str(r['pid']),r['brand_name']),'brand':r['brand_name'],
            'is_self':(r['brand_name']==LAYER),'users':i(r['users']),'qty':i(r['qty']),'gmv':i(r['gmv'])})
    out={}; tot=[0,0,0,0,0]
    for r in cs:
        t=r['tier']
        if t not in ('basic','refine','studio'): continue
        fu,bm,om,co,op=i(r['frame_users']),i(r['bought_mat']),i(r['our_mat']),i(r['comp_only']),i(r['opt_mat'])
        tot=[tot[0]+fu,tot[1]+bm,tot[2]+om,tot[3]+co,tot[4]+op]
        dst=dest.get(t,[])
        if op>0:  # 자사 프레임옵션 매트리스를 dest 행으로
            ob=opt_by_tier.get(t,{'qty':0,'gmv':0})
            dst=[{'pid':'opt','name':'프레임옵션 매트리스(자사)','brand':LAYER,'is_self':True,'users':op,'qty':ob['qty'],'gmv':ob['gmv']}]+dst
        dst=sorted(dst,key=lambda x:(-x['gmv'],str(x['pid'])))
        out[t]={'frame_users':fu,'bought_mat':bm,'our':om,'comp_only':co,'opt_mat':op,
            'rate_pct':round(bm/fu*100,1) if fu else 0.0,'leak_pct':round(co/bm*100,1) if bm else 0.0,'dest':dst}
    M['cosell_by_tier']=out
    M['cosell_summary']={'method':'유저단위(별도 매트리스PID OR 프레임옵션 매트리스). 전 layer 침대프레임(수납/일반/깔판).',
        'frame_users':tot[0],'bought_mat':tot[1],'rate_pct':round(tot[1]/tot[0]*100,1) if tot[0] else 0.0,
        'our':tot[2],'comp_only':tot[3],'opt_mat':tot[4],'leak_pct':round(tot[3]/tot[1]*100,1) if tot[1] else 0.0,
        'market_rate_pct':35.1,'window':'프레임 최근3개월 / 매트리스 최근6개월 (유저단위, 옵션매트 복구·400일창)',
        'note':'모수 착시였음(옵션매트 누락). 정정 44%로 시장(35%) 상위. 우리(옵션매트 포함) 포착 vs 경쟁 유출이 핵심. 옵션매트리스=자사 포착.'}
    changed.append('cosell_by_tier')

# ---------- mat_to_frame_by_tier (역방향, 프레임 전수) ----------
ms=rows('m2f_summary',['tier','mat_users','bought_frame','our_frame','comp_only'])
md=rows('m2f_dest',['tier','pid','product_name','brand_name','users','gmv'])
if ms is not None and md is not None:
    dest={}
    for r in md:
        dest.setdefault(r['tier'],[]).append({'pid':str(r['pid']),'name':r['product_name'],'brand':r['brand_name'],
            'is_self':(r['brand_name']==LAYER),'users':i(r['users']),'gmv':i(r['gmv'])})
    for t in dest: dest[t].sort(key=lambda x:-x['users'])
    out={}
    for r in ms:
        t=r['tier']
        if t not in ('basic','refine','studio'): continue
        mu,bf,of_,co=i(r['mat_users']),i(r['bought_frame']),i(r['our_frame']),i(r['comp_only'])
        out[t]={'mat_users':mu,'bought_frame':bf,'our':of_,'comp_only':co,
            'rate_pct':round(bf/mu*100,1) if mu else 0.0,'leak_pct':round(co/bf*100,1) if bf else 0.0,'dest':dest.get(t,[])}
    M['mat_to_frame_by_tier']=out
    M['mat_to_frame_summary']={'method':'우리 매트리스 구매자→프레임 구매(유저단위, 프레임 전수).','note':'매트리스→프레임 방향 유출 추적.'}
    changed.append('mat_to_frame_by_tier')

# ---------- 합구매 경쟁자 관점 (comp_frame_dest + 옵션매트 행 / comp_mattress_dest) ----------
def _dest_block(nm_, comp_pid, comp_name):
    r=rows(nm_, ['buyers','pid','product_name','brand_name','users','gmv'])
    if r is None: return None
    dest=[{'pid':str(x['pid']),'name':x['product_name'],'brand':x['brand_name'],
           'is_self':(x['brand_name']==LAYER),'users':i(x['users']),'gmv':i(x['gmv'])} for x in r]
    dest.sort(key=lambda x:-x['users'])
    return {'comp_pid':comp_pid,'comp_name':comp_name,'buyers':(i(r[0]['buyers']) if r else 0),'dest':dest[:8]}
cf=_dest_block('q1_comp_frame_dest','2352818','데일리리빙 드레스덴(프레임 최대경쟁)')
qo=rows('q1_opt',['users','gmv','qty'])
if cf and qo:  # 데일리리빙 프레임옵션 매트리스(=데일리리빙 자체) 행 추가
    o=qo[0]
    cf['dest']=sorted(cf['dest']+[{'pid':'opt','name':'데일리리빙 프레임옵션 매트리스','brand':'데일리리빙','is_self':False,'users':i(o['users']),'gmv':i(o['gmv'])}],key=lambda x:-x['users'])[:9]
if cf: M['comp_frame_dest']=cf; changed.append('comp_frame_dest')
cm=_dest_block('q2_comp_mat_dest','1590911','수면밀도(매트리스 최대경쟁)')
if cm: M['comp_mattress_dest']=cm; changed.append('comp_mattress_dest')

# ---------- 브랜드 생태계 락인 (옵션매트 복구) ----------
be=rows('q3_brand_eco',['brand','frame_buyers','same_brand_mat','rate_pct','opt_join_match_pct'])
if be is not None:
    def _mp(x):
        v=x.get('opt_join_match_pct'); return f(v) if v not in (None,'') else None
    M['brand_ecosystem']=[{'brand':x['brand'],'is_self':(x['brand']==LAYER),'frame_buyers':i(x['frame_buyers']),
        'same_brand_mat':i(x['same_brand_mat']),'rate_pct':f(x['rate_pct']),
        'opt_match_pct':_mp(x),'underest':(_mp(x) is not None and _mp(x)<90)} for x in be]
    changed.append('brand_ecosystem')

# ---------- meta + lastUpdate ----------
last = max((r['dt'] for pid in SELF for r in D['daily'].get(pid,[])), default=D['meta'].get('lastUpdate'))
D['meta']['lastUpdate']=last
D['meta']['version']='daily'
D['meta']['srpWindow']={'matrix':'7d','keywords':'7d','keywordRank':'7d','ts':'60d'}
D['meta']['qcSource']='INTEGRATED(통합검색)'
D['meta']['srpSource']='query_object_metrics_with_order_mart(노출가중)+search_object_with_order_mart(P5)'

# ---------- SRP 신선도 감시 (소스 동결 시 조용한 낡음 방지) ----------
# srp_base14/kw_radar는 매일 실행. 소스(SRP 마트)가 동결되면 rows_req가 빈결과→기존값 보존(블랭크는 막지만 낡음).
# 그 경우 해당 섹션이 changed에 없다 = 이번 런에서 갱신 실패 → WARN 출력(run.ps1이 tf-alert.log로 전달).
_srp_crit=['srpMatrix','keywordRadar','srpKeywordRank']
_srp_stale=[s for s in _srp_crit if s not in changed]
if _srp_stale:
    D['meta']['srpStaleWarn']={'sections':_srp_stale,'since':D['meta'].get('srpRefreshed','?')}
    print('WARN: SRP sections not refreshed this run:', _srp_stale, '- SRP 소스(query_object_metrics_with_order_mart/search_object_with_order_mart) 동결/장애 확인 필요')
else:
    D['meta'].pop('srpStaleWarn',None)
    D['meta']['srpRefreshed']=last

# ---------- 불변식 검증 ----------
assert isinstance(D['srpMatrix'],list) and isinstance(D['inflow'],list) and isinstance(D['ages']['buyer'],list)
for bk in ['bedding','mattress','bed']:
    b=D['benchmarks'].get(bk)
    if b: assert isinstance(b['self_products'],list) and isinstance(b['price_bands'],list) and isinstance(b['pb_lineup'],list)
m3=sum(r['gmv'] for r in D['daily']['3918642'] if r['dt'].startswith('2026-03'))
assert m3==11553700, f"3월정합 실패 3918642={m3}"
m3b=sum(r['gmv'] for r in D['daily']['3640244'] if r['dt'].startswith('2026-03'))
assert m3b==45449200, f"3월정합 실패 3640244={m3b}"

with open(TF,'w',encoding='utf-8') as fp: json.dump(D,fp,ensure_ascii=False,indent=2)
with open(MAT,'w',encoding='utf-8') as fp: json.dump(M,fp,ensure_ascii=False,indent=2)
print('BUILD OK. sections updated:', changed)
print('lastUpdate:', last, '| daily pids:', len(D['daily']), '| srpMatrix:', len(D['srpMatrix']), '| 3월정합 OK')
