# -*- coding: utf-8 -*-
import json, os, sys

OUT = r"C:\Users\yys\living-pb-report\scripts\_tf_out"
DATA = r"C:\Users\yys\living-pb-report\tf-data.json"
TODAY = "2026-07-02"
YDAY = "2026-07-01"
PERIOD7 = "2026-06-25 ~ 2026-07-01"
PIDS = ['3918642','3640244','767440','2636441','1930788','442026']

def load(name):
    p = os.path.join(OUT, name + ".json")
    with open(p, encoding="utf-8") as f:
        j = json.load(f)
    if j.get("status") != "SUCCEEDED" and j.get("status") != "SUCCESS":
        # accept whatever status if data present
        if not j.get("data"):
            raise RuntimeError(f"{name}: status={j.get('status')} err={j.get('error')}")
    cols = [c["name"] for c in j["columns"]]
    rows = [dict(zip(cols, r)) for r in j["data"]]
    return rows

def num(x):
    if x is None or x == "": return None
    try:
        f = float(x)
        return int(f) if f == int(f) else f
    except: return x

d = json.load(open(DATA, encoding="utf-8"))
report = {}

# ---------- 1. daily merge (A + B + B_uv) ----------
A = load("A_gmv"); B = load("B_funnel"); UV = load("B_uv")
gmv = {}
for r in A:
    gmv[(str(r["dt"]), str(r["product_id"]))] = (num(r["gmv"]), num(r["gp"]), num(r["qty"]))
fun = {}
for r in B:
    fun[(str(r["dt"]), str(r["product_id"]))] = (num(r["imp"]), num(r["pdp"]), num(r["purchase"]))
uv = {}
for r in UV:
    uv[(str(r["dt"]), str(r["pid"]))] = (num(r["imp_uv"]), num(r["pdp_uv"]), num(r["buy_uv"]))

# March validation on raw A
mar = {p: 0 for p in PIDS}
for (dt, pid), (g, gp, q) in gmv.items():
    if dt.startswith("2026-03") and pid in mar:
        mar[pid] += (g or 0)
report["march_3918642"] = mar["3918642"]
report["march_3640244"] = mar["3640244"]
expect = {"3918642": 11553700, "3640244": 45449200}
ok = (mar["3918642"] == expect["3918642"] and mar["3640244"] == expect["3640244"])
report["march_ok"] = ok
if not ok:
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit("MARCH VALIDATION FAILED — abort, no write")

added = {}
for pid in PIDS:
    rows = {row["dt"]: row for row in d["daily"][pid]}
    all_dts = set(dt for (dt, p) in gmv if p == pid) | set(dt for (dt, p) in fun if p == pid) | set(dt for (dt, p) in uv if p == pid)
    for dt in all_dts:
        if dt > YDAY:  # exclude today / future
            continue
        g = gmv.get((dt, pid)); fv = fun.get((dt, pid)); uvv = uv.get((dt, pid))
        if not (g and fv and uvv):
            continue
        # PB self products have gp=None (원가 미반영) → coerce to 0.0, don't drop the row.
        gp_val = g[1] if g[1] is not None else 0.0
        if g[0] is None or g[2] is None or None in fv or None in uvv:
            continue
        newrow = {"dt": dt, "gmv": g[0], "gp": gp_val, "qty": g[2],
                  "imp": fv[0], "pdp": fv[1], "purchase": fv[2],
                  "imp_uv": uvv[0], "pdp_uv": uvv[1], "buy_uv": uvv[2]}
        if dt not in rows:
            added.setdefault(pid, []).append(dt)
        rows[dt] = newrow
    d["daily"][pid] = [rows[k] for k in sorted(rows)]
report["daily_added"] = added
maxdt = max(d["daily"][p][-1]["dt"] for p in PIDS)
report["maxdt"] = maxdt

# ---------- 2. srpKeywords (C) upsert ----------
C = load("C_srpkw")
for pid in ['3918642','3640244']:
    cur = {(x["dt"], x["kw"]): x for x in d["srpKeywords"][pid]}
    for r in C:
        if str(r["object_id"]) != pid: continue
        rec = {"dt": str(r["date"]), "kw": r["query_keyword"],
               "rank": round(float(r["avg_rank"]), 1), "best": int(float(r["best_rank"])),
               "score": round(float(r["selling_score"]), 3) if r["selling_score"] not in (None,"") else None}
        cur[(rec["dt"], rec["kw"])] = rec
    d["srpKeywords"][pid] = sorted(cur.values(), key=lambda x: (x["dt"], x["kw"]))
report["srpKeywords_dts"] = {pid: sorted({x["dt"] for x in d["srpKeywords"][pid]})[-2:] for pid in ['3918642','3640244']}

# ---------- 3. inflow (D1+D2) full replace, per-day flat, pid str ----------
inflow = []
for nm in ["D1_inflow", "D2_inflow"]:
    for r in load(nm):
        inflow.append({"dt": str(r["date"]), "pid": str(r["pid"]), "inflow": r["inflow"],
                       "imp": num(r["imp"]), "click": num(r["click"]), "click_uv": num(r["click_uv"])})
d["inflow"] = inflow
report["inflow_rows"] = len(inflow)
report["inflow_dts"] = sorted({x["dt"] for x in inflow})

# ---------- 4. inflowCvr (G) ----------
G = load("G_cvr")
cvr = {}
for r in G:
    pid = str(r["pid"]); ch = r["inflow"]
    c = cvr.setdefault(pid, {}).setdefault(ch, {"click_uv": 0, "buy_uv": 0})
    c["click_uv"] += int(num(r["click_uv"]) or 0)
    c["buy_uv"] += int(num(r["buy_uv"]) or 0)
for pid in cvr:
    for ch, c in cvr[pid].items():
        c["cvr"] = round(c["buy_uv"] / c["click_uv"] * 100, 2) if c["click_uv"] else 0.0
d["inflowCvr"]["data"] = cvr
d["inflowCvr"]["period"] = PERIOD7
report["inflowCvr_pids"] = list(cvr.keys())

# ---------- 5. categoryBenchmark (H + I) ----------
H = load("H_bands"); I = load("I_lineup")
bands = {}
for r in H:
    band = r["price_band"]; flag = r["pb_flag"]
    b = bands.setdefault(band, {})
    b[flag] = {"cnt": int(num(r["prod_cnt"]) or 0), "imp": int(num(r["imp_uv"]) or 0),
               "pdp": int(num(r["pdp_uv"]) or 0), "buy": int(num(r["buy_uv"]) or 0)}
def ctr(p, i): return round(p / i * 100, 2) if i else 0.0
def cvr_(b, p): return round(b / p * 100, 2) if p else 0.0
# cate_3p_avg
t = {"imp": 0, "pdp": 0, "buy": 0}
for band, sides in bands.items():
    if "3P" in sides:
        t["imp"] += sides["3P"]["imp"]; t["pdp"] += sides["3P"]["pdp"]; t["buy"] += sides["3P"]["buy"]
cate = {"imp_uv": t["imp"], "pdp_uv": t["pdp"], "buy_uv": t["buy"], "ctr": ctr(t["pdp"], t["imp"]), "cvr": cvr_(t["buy"], t["pdp"])}
d["categoryBenchmark"]["cate_3p_avg"] = cate
# price_bands
label_map = {x["band"]: x["label"] for x in d["categoryBenchmark"]["price_bands"]}
pbout = []
band_p3 = {}
for band in ["01_<30k", "02_30-50k", "03_50-70k", "04_70-100k", "05_100k+"]:
    sides = bands.get(band, {})
    pb = sides.get("PB", {"cnt": 0, "imp": 0, "pdp": 0, "buy": 0})
    p3 = sides.get("3P", {"cnt": 0, "imp": 0, "pdp": 0, "buy": 0})
    pb_ctr, pb_cvr = ctr(pb["pdp"], pb["imp"]), cvr_(pb["buy"], pb["pdp"])
    p3_ctr, p3_cvr = ctr(p3["pdp"], p3["imp"]), cvr_(p3["buy"], p3["pdp"])
    band_p3[band] = (p3_ctr, p3_cvr)
    pbout.append({"band": band, "label": label_map.get(band, band),
        "pb_prod_cnt": pb["cnt"], "p3_prod_cnt": p3["cnt"],
        "pb_imp_uv": pb["imp"], "pb_pdp_uv": pb["pdp"], "pb_buy_uv": pb["buy"],
        "p3_imp_uv": p3["imp"], "p3_pdp_uv": p3["pdp"], "p3_buy_uv": p3["buy"],
        "pb_ctr": pb_ctr, "p3_ctr": p3_ctr, "ctr_ratio": round(pb_ctr / p3_ctr, 2) if p3_ctr else 0.0,
        "pb_cvr": pb_cvr, "p3_cvr": p3_cvr, "cvr_ratio": round(pb_cvr / p3_cvr, 2) if p3_cvr else 0.0})
d["categoryBenchmark"]["price_bands"] = pbout
# pb_lineup (keep name/line/price/is_self, refresh UV)
iuv = {str(r["pid"]): (int(num(r["imp_uv"]) or 0), int(num(r["pdp_uv"]) or 0), int(num(r["buy_uv"]) or 0)) for r in I}
for x in d["categoryBenchmark"]["pb_lineup"]:
    u = iuv.get(str(x["pid"]))
    if u:
        x["imp_uv"], x["pdp_uv"], x["buy_uv"] = u
        x["ctr"] = ctr(u[1], u[0]); x["cvr"] = cvr_(u[2], u[1])
# self_products (keep meta, refresh UV + ratios)
for x in d["categoryBenchmark"]["self_products"]:
    u = iuv.get(str(x["pid"]))
    if u:
        x["imp_uv"], x["pdp_uv"], x["buy_uv"] = u
        x["ctr"] = ctr(u[1], u[0]); x["cvr"] = cvr_(u[2], u[1])
    x["cate_p3_ctr"] = cate["ctr"]; x["cate_p3_cvr"] = cate["cvr"]
    x["cate_ctr_ratio"] = round(x["ctr"] / cate["ctr"], 2) if cate["ctr"] else 0.0
    x["cate_cvr_ratio"] = round(x["cvr"] / cate["cvr"], 2) if cate["cvr"] else 0.0
    bp = band_p3.get(x["price_band"], (0, 0))
    x["band_p3_ctr"], x["band_p3_cvr"] = bp
    x["band_ctr_ratio"] = round(x["ctr"] / bp[0], 2) if bp[0] else 0.0
    x["band_cvr_ratio"] = round(x["cvr"] / bp[1], 2) if bp[1] else 0.0
d["categoryBenchmark"]["period"] = PERIOD7
report["cate_3p_avg"] = cate

# ---------- 6. ages (E) Monday only — flat array, keep product label ----------
_ages_path = os.path.join(OUT, "E_ages.json")
if os.path.exists(_ages_path):
    E = load("E_ages")
    agg = {}
    for r in E:
        pid = str(r["pid"]); ag = r["age_group"]; c = int(num(r["cnt"]) or 0)
        agg.setdefault(pid, {})[ag] = c
    order = ["20-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60+"]
    for b in d["ages"]["buyer"]:
        pid = str(b["pid"])
        if pid not in agg: continue
        counts = agg[pid]; tot = sum(counts.values())
        b["sample"] = tot
        b["age"] = {k: round(counts.get(k, 0) / tot * 100, 1) for k in order} if tot else {k: 0.0 for k in order}
        if tot < 100: b["warn"] = True
        elif "warn" in b: del b["warn"]
    d["meta"]["agesUpdate"] = TODAY
    report["ages_samples"] = {str(b["pid"]): b["sample"] for b in d["ages"]["buyer"]}
else:
    report["ages_skipped"] = "E not run (non-Monday)"

# ---------- 7. srpMatrix (F) flat array ----------
F = load("F_matrix")
mx = []
for r in F:
    mx.append({"pid": str(r["object_id"]), "kw": r["query_keyword"],
               "rank": round(float(r["avg_rank"]), 2), "best": int(float(r["best_rank"])),
               "score": round(float(r["score"]), 3) if r["score"] not in (None,"") else None,
               "ctr": round(float(r["ctr"]) * 100, 1), "imp": int(float(r["imp"]))})
d["srpMatrix"] = mx
report["srpMatrix_rows"] = len(mx)

# ---------- 8. scoreTs upsert ----------
S = load("scoreTs")
for pid in d["scoreTs"]:
    cur = {x["dt"]: x for x in d["scoreTs"][pid]}
    for r in S:
        if str(r["pid"]) != pid: continue
        cur[str(r["dt"])] = {"dt": str(r["dt"]), "score": round(float(r["score"]), 3) if r["score"] not in (None,"") else None}
    d["scoreTs"][pid] = [cur[k] for k in sorted(cur)]
report["scoreTs_last"] = {pid: d["scoreTs"][pid][-1]["dt"] for pid in d["scoreTs"]}

# ---------- 9. srpFeatureTs upsert ----------
SF = load("srpFeatureTs")
for pid in d["srpFeatureTs"]:
    cur = {x["dt"]: x for x in d["srpFeatureTs"][pid]}
    for r in SF:
        if str(r["pid"]) != pid: continue
        def rd(v, n=4):
            return round(float(v), n) if v not in (None, "") else None
        cur[str(r["dt"])] = {"dt": str(r["dt"]), "review": rd(r["review"]), "sell28": rd(r["sell28"]),
            "view28": rd(r["view28"]), "spv28": rd(r["spv28"]), "wish": rd(r["wish"]),
            "card": rd(r["card"]), "qc_rank": rd(r["qc_rank"], 2)}
    d["srpFeatureTs"][pid] = [cur[k] for k in sorted(cur)]
report["srpFeatureTs_last"] = {pid: d["srpFeatureTs"][pid][-1]["dt"] for pid in d["srpFeatureTs"]}

# ---------- 10. meta ----------
d["meta"]["lastUpdate"] = maxdt
d["meta"]["srpUpdate"] = TODAY
d["meta"]["inflowUpdate"] = TODAY

# ---------- structure guards ----------
assert isinstance(d["ages"]["buyer"], list), "ages.buyer must be array"
assert isinstance(d["srpMatrix"], list), "srpMatrix must be array"
assert isinstance(d["categoryBenchmark"]["self_products"], list)
assert isinstance(d["categoryBenchmark"]["price_bands"], list)
assert isinstance(d["categoryBenchmark"]["pb_lineup"], list)
assert isinstance(d["inflow"], list)
assert all(isinstance(x["pid"], str) for x in d["inflow"])

# yesterday GMV for report
report["yday_gmv_3918642"] = next((r["gmv"] for r in d["daily"]["3918642"] if r["dt"] == YDAY), None)
report["yday_gmv_3640244"] = next((r["gmv"] for r in d["daily"]["3640244"] if r["dt"] == YDAY), None)

json.dump(d, open(DATA, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(json.dumps(report, ensure_ascii=False, indent=2))
