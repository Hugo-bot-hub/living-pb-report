# -*- coding: utf-8 -*-
import json, io, os

ROOT = r"C:\Users\yys\living-pb-report"
RAW = os.path.join(ROOT, "raw_2026_06_07")
PATH = os.path.join(ROOT, "tf-data.json")
PIDS = ["3918642", "3640244", "767440", "2636441", "1930788", "442026"]
LASTDT = "2026-06-07"

def load(name):
    return json.load(io.open(os.path.join(RAW, name + ".json"), encoding="utf-8"))["data"]

d = json.load(io.open(PATH, encoding="utf-8"))

# ---------- 1. daily: append 2026-06-07 (gmv+funnel+uv all present) ----------
A = {}  # pid -> {gmv,gp,qty}
for dt, pid, gmv, gp, qty in load("A"):
    if dt == LASTDT:
        A[str(pid)] = (int(gmv), round(float(gp), 2), int(qty))
B = {}
for dt, pid, imp, pdp, pur in load("B"):
    if dt == LASTDT:
        B[str(pid)] = (int(imp), int(pdp), int(pur))
BUV = {}
for dt, pid, iu, pu, bu in load("BUV"):
    if dt == LASTDT:
        BUV[str(pid)] = (int(iu), int(pu), int(bu))

daily_added = []
for pid in PIDS:
    if pid in A and pid in B and pid in BUV:  # only append fully-populated rows
        if d["daily"][pid] and d["daily"][pid][-1]["dt"] == LASTDT:
            continue  # already present
        gmv, gp, qty = A[pid]; imp, pdp, pur = B[pid]; iu, pu, bu = BUV[pid]
        d["daily"][pid].append({"dt": LASTDT, "gmv": gmv, "gp": gp, "qty": qty,
            "imp": imp, "pdp": pdp, "purchase": pur,
            "imp_uv": iu, "pdp_uv": pu, "buy_uv": bu})
        daily_added.append(pid)

# ---------- 2. srpKeywords (self 2, last 14d from C) ----------
sk = {"3918642": [], "3640244": []}
for date, oid, kw, avg_rank, best_rank, sscore in load("C"):
    sk[str(oid)].append({"dt": date, "kw": kw, "rank": round(float(avg_rank), 1),
        "best": int(best_rank), "score": round(float(sscore), 4)})
for pid in sk:
    sk[pid].sort(key=lambda x: (x["dt"], x["kw"]))
d["srpKeywords"] = sk

# ---------- 3. inflow (flat per-day array, pid str; D1+D2) ----------
inflow = []
for raw in ("D1", "D2"):
    for date, pid, ch, imp, click, click_uv in load(raw):
        inflow.append({"dt": date, "pid": str(pid), "inflow": ch,
            "imp": int(imp), "click": int(click), "click_uv": int(click_uv)})
d["inflow"] = inflow

# ---------- 4. srpMatrix (flat, F) ----------
matrix = []
for oid, kw, avg_rank, best_rank, score, ctr, imp in load("F"):
    matrix.append({"pid": str(oid), "kw": kw, "rank": round(float(avg_rank), 1),
        "best": int(best_rank), "score": round(float(score), 3),
        "ctr": round(float(ctr) * 100, 1), "imp": int(imp)})
d["srpMatrix"] = matrix

# ---------- 5. scoreTs ----------
st = {p: [] for p in PIDS}
for date, oid, score in load("scoreTs"):
    st[str(oid)].append({"dt": date, "score": round(float(score), 4)})
d["scoreTs"] = st

# ---------- 6. srpFeatureTs ----------
ft = {p: [] for p in PIDS}
for dt, pid, review, sell28, view28, spv28, wish, card, qc in load("srpFeatureTs"):
    ft[str(pid)].append({"dt": dt, "review": round(float(review), 4),
        "sell28": round(float(sell28), 4), "view28": round(float(view28), 4),
        "spv28": round(float(spv28), 4), "wish": round(float(wish), 4),
        "card": round(float(card), 4), "qc_rank": round(float(qc), 2)})
d["srpFeatureTs"] = ft

# ---------- 7. ages.buyer (flat array, E) ----------
BUCKETS = ["20-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60+"]
agg = {p: {} for p in PIDS}
for pid, grp, cnt in load("E"):
    agg[str(pid)][grp] = int(cnt)
shortName = {p: d["products"][p]["shortName"] for p in PIDS}
buyer = []
for pid in PIDS:
    counts = agg[pid]
    total = sum(counts.values())
    age = {}
    for b in BUCKETS:
        age[b] = round(counts.get(b, 0) / total * 100, 1) if total else 0.0
    row = {"product": f"{shortName[pid]} ({pid})", "sample": total, "age": age}
    if total < 100:
        row["warn"] = True
    buyer.append(row)
d["ages"]["buyer"] = buyer

# ---------- 8. inflowCvr (7d agg by pid+channel, G) ----------
cvr = {p: {} for p in PIDS}
for dt, pid, ch, click_uv, buy_uv in load("G"):
    pid = str(pid)
    c = cvr[pid].setdefault(ch, {"click_uv": 0, "buy_uv": 0})
    c["click_uv"] += int(click_uv); c["buy_uv"] += int(buy_uv)
for pid in cvr:
    for ch, c in cvr[pid].items():
        c["cvr"] = round(c["buy_uv"] / c["click_uv"] * 100, 2) if c["click_uv"] else 0.0
d["inflowCvr"]["period"] = "2026-06-01 ~ 2026-06-07"
d["inflowCvr"]["data"] = cvr

# ---------- 9. categoryBenchmark (H + I) ----------
def ratio(n, dn): return round(n / dn, 2) if dn else 0.0
def pct(n, dn): return round(n / dn * 100, 2) if dn else 0.0

H = load("H")  # [pb_flag, band, prod_cnt, imp_uv, pdp_uv, buy_uv]
bands = {}
for pb_flag, band, prod_cnt, imp_uv, pdp_uv, buy_uv in H:
    imp_uv = int(imp_uv or 0); pdp_uv = int(pdp_uv or 0); buy_uv = int(buy_uv or 0)
    b = bands.setdefault(band, {"PB": {"cnt":0,"imp":0,"pdp":0,"buy":0},
                                "3P": {"cnt":0,"imp":0,"pdp":0,"buy":0}})
    b[pb_flag]["cnt"] += int(prod_cnt or 0); b[pb_flag]["imp"] += imp_uv
    b[pb_flag]["pdp"] += pdp_uv; b[pb_flag]["buy"] += buy_uv

# cate_3p_avg = all 3P bands summed
t3 = {"imp":0,"pdp":0,"buy":0}
for band, b in bands.items():
    t3["imp"] += b["3P"]["imp"]; t3["pdp"] += b["3P"]["pdp"]; t3["buy"] += b["3P"]["buy"]
cate_3p = {"imp_uv": t3["imp"], "pdp_uv": t3["pdp"], "buy_uv": t3["buy"],
           "ctr": pct(t3["pdp"], t3["imp"]), "cvr": pct(t3["buy"], t3["pdp"])}

BAND_LABEL = {"01_<30k":"<30k","02_30-50k":"30-50k","03_50-70k":"50-70k",
              "04_70-100k":"70-100k","05_100k+":"100k+"}
price_bands = []
for band in ["01_<30k","02_30-50k","03_50-70k","04_70-100k","05_100k+"]:
    b = bands.get(band, {"PB":{"cnt":0,"imp":0,"pdp":0,"buy":0},"3P":{"cnt":0,"imp":0,"pdp":0,"buy":0}})
    pbb, p3 = b["PB"], b["3P"]
    pb_ctr = pct(pbb["pdp"], pbb["imp"]); p3_ctr = pct(p3["pdp"], p3["imp"])
    pb_cvr = pct(pbb["buy"], pbb["pdp"]); p3_cvr = pct(p3["buy"], p3["pdp"])
    price_bands.append({"band": band, "label": BAND_LABEL[band],
        "pb_prod_cnt": pbb["cnt"], "p3_prod_cnt": p3["cnt"],
        "pb_imp_uv": pbb["imp"], "pb_pdp_uv": pbb["pdp"], "pb_buy_uv": pbb["buy"],
        "p3_imp_uv": p3["imp"], "p3_pdp_uv": p3["pdp"], "p3_buy_uv": p3["buy"],
        "pb_ctr": pb_ctr, "p3_ctr": p3_ctr, "ctr_ratio": ratio(pb_ctr, p3_ctr),
        "pb_cvr": pb_cvr, "p3_cvr": p3_cvr, "cvr_ratio": ratio(pb_cvr, p3_cvr)})
band_by_key = {pb["band"]: pb for pb in price_bands}

I = {}
for pid, imp_uv, pdp_uv, buy_uv in load("I"):
    I[str(pid)] = (int(imp_uv or 0), int(pdp_uv or 0), int(buy_uv or 0))

# self_products: reuse existing metadata, refresh metrics
old_self = {s["pid"]: s for s in d["categoryBenchmark"]["self_products"]}
self_products = []
for pid in ["3918642", "3640244"]:
    meta = old_self[pid]
    imp_uv, pdp_uv, buy_uv = I[pid]
    ctr = pct(pdp_uv, imp_uv); cvr = pct(buy_uv, pdp_uv)
    band = band_by_key[meta["price_band"]]
    self_products.append({"pid": pid, "name": meta["name"], "line": meta["line"],
        "price": meta["price"], "price_band": meta["price_band"],
        "ctr": ctr, "cvr": cvr, "imp_uv": imp_uv, "pdp_uv": pdp_uv, "buy_uv": buy_uv,
        "cate_p3_ctr": cate_3p["ctr"], "cate_p3_cvr": cate_3p["cvr"],
        "cate_ctr_ratio": ratio(ctr, cate_3p["ctr"]), "cate_cvr_ratio": ratio(cvr, cate_3p["cvr"]),
        "band_p3_ctr": band["p3_ctr"], "band_p3_cvr": band["p3_cvr"],
        "band_ctr_ratio": ratio(ctr, band["p3_ctr"]), "band_cvr_ratio": ratio(cvr, band["p3_cvr"])})

# pb_lineup: reuse metadata, refresh metrics
old_lineup = {s["pid"]: s for s in d["categoryBenchmark"]["pb_lineup"]}
pb_lineup = []
for pid, meta in old_lineup.items():
    imp_uv, pdp_uv, buy_uv = I.get(pid, (0, 0, 0))
    pb_lineup.append({"pid": pid, "name": meta["name"], "line": meta["line"],
        "price": meta["price"], "is_self": meta["is_self"],
        "imp_uv": imp_uv, "pdp_uv": pdp_uv, "buy_uv": buy_uv,
        "ctr": pct(pdp_uv, imp_uv), "cvr": pct(buy_uv, pdp_uv)})

d["categoryBenchmark"] = {"period": "2026-06-01 ~ 2026-06-07", "cate_3p_avg": cate_3p,
    "self_products": self_products, "price_bands": price_bands, "pb_lineup": pb_lineup}

# ---------- meta ----------
d["meta"]["lastUpdate"] = max(d["daily"][p][-1]["dt"] for p in PIDS)

# ---------- validations ----------
assert isinstance(d["srpMatrix"], list) and all(isinstance(x["pid"], str) for x in d["srpMatrix"])
assert isinstance(d["inflow"], list) and all(isinstance(r["pid"], str) for r in d["inflow"])
assert isinstance(d["ages"]["buyer"], list) and len(d["ages"]["buyer"]) == 6
assert isinstance(d["categoryBenchmark"]["self_products"], list)
assert isinstance(d["categoryBenchmark"]["price_bands"], list)
assert isinstance(d["categoryBenchmark"]["pb_lineup"], list)
m3918 = sum(r["gmv"] for r in d["daily"]["3918642"] if r["dt"].startswith("2026-03"))
m3640 = sum(r["gmv"] for r in d["daily"]["3640244"] if r["dt"].startswith("2026-03"))
m767  = sum(r["gmv"] for r in d["daily"]["767440"]  if r["dt"].startswith("2026-03"))
assert m3918 == 11553700, f"소프트워싱 3월 GMV mismatch: {m3918}"
assert m3640 == 45449200, f"알러지케어 3월 GMV mismatch: {m3640}"
# inflow per-day uniqueness check (WoW needs 14 distinct dates)
ndates = len({r["dt"] for r in d["inflow"]})
assert ndates >= 13, f"inflow distinct dates too few: {ndates}"

json.dump(d, io.open(PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

# ---------- report ----------
print("daily appended:", daily_added)
print("lastUpdate:", d["meta"]["lastUpdate"])
print(f"March GMV OK: 소프트워싱 {m3918:,} / 알러지케어 {m3640:,} / 카스테라 {m767:,}")
print("inflow rows:", len(inflow), "distinct dates:", ndates)
print("srpKeywords:", {p: len(v) for p, v in sk.items()})
print("srpMatrix rows:", len(matrix))
print("scoreTs last:", {p: st[p][-1] for p in ["3918642","3640244"]})
print("ages samples:", [(b["product"], b["sample"], b.get("warn")) for b in buyer])
print("cate_3p_avg:", cate_3p)
print("self_products:", [(s["pid"], s["ctr"], s["cvr"]) for s in self_products])
print("06-07 GMV: 소프트워싱", A.get("3918642"), "/ 알러지케어", A.get("3640244"))
