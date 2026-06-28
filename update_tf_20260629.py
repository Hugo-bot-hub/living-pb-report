"""2026-06-29 (Mon) daily update.
Full replace of SRP/inflow/cvr/benchmark/scoreTs/featTs + ages (Monday).
Daily anchored at 2026-06-27 (06-28 UV not yet landed in
commerce_daily_user_count_v3 -> rule: never append a row missing UV).
Reads raw MCP response files (each {"data": [...]}) from tmp dir.
"""
import json
import sys
from pathlib import Path

TMP = Path("tf-data/tmp_2026_06_29")
PIDS = ["3918642", "3640244", "767440", "2636441", "1930788", "442026"]
REFRESH_FROM = "2026-06-24"


def rows(name):
    with open(TMP / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)["data"]


def to_int(x):
    return 0 if x in (None, "") else int(float(x))


a_rows, b_rows, buv_rows = rows("a"), rows("b"), rows("buv")
c_rows, f_rows = rows("c"), rows("f")
d_rows = rows("d_a") + rows("d_b")
h_rows, i_rows = rows("h"), rows("i")
g_rows = rows("g")
score_rows, feat_rows = rows("score"), rows("feat")
e_rows = rows("e")

a_map = {(dt, str(p)): (to_int(g), round(float(gp), 2) if gp not in (None, "") else 0.0, to_int(q)) for dt, p, g, gp, q in a_rows}
b_map = {(dt, str(p)): (to_int(i), to_int(pd), to_int(pu)) for dt, p, i, pd, pu in b_rows}
uv_map = {(dt, str(p)): (to_int(i), to_int(pd), to_int(bu)) for dt, p, i, pd, bu in buv_rows}

# dynamic daily cutoff = latest date present in BOTH PV and UV
b_dates = {dt for (dt, p) in b_map}
uv_dates = {dt for (dt, p) in uv_map}
DAILY_CUTOFF = max(b_dates & uv_dates)

with open("tf-data.json", encoding="utf-8") as f:
    d = json.load(f)

# --- daily: keep history < REFRESH_FROM, rebuild REFRESH_FROM..DAILY_CUTOFF ---
appended = {}
for pid in PIDS:
    kept = [r for r in d["daily"][pid] if r["dt"] < REFRESH_FROM]
    dts = sorted({dt for (dt, p) in (set(b_map) & set(uv_map)) if p == pid and REFRESH_FROM <= dt <= DAILY_CUTOFF})
    new_rows = []
    for dt in dts:
        gmv, gp, qty = a_map.get((dt, pid), (0, 0.0, 0))
        imp, pdp, purchase = b_map[(dt, pid)]
        imp_uv, pdp_uv, buy_uv = uv_map[(dt, pid)]
        new_rows.append({"dt": dt, "gmv": gmv, "gp": gp, "qty": qty,
                         "imp": imp, "pdp": pdp, "purchase": purchase,
                         "imp_uv": imp_uv, "pdp_uv": pdp_uv, "buy_uv": buy_uv})
    d["daily"][pid] = kept + new_rows
    appended[pid] = len(new_rows)

# --- srpKeywords (C) ---
srp_keywords = {"3918642": [], "3640244": []}
for dt, pid, kw, rank, best, score in c_rows:
    if pid in srp_keywords:
        srp_keywords[pid].append({"dt": dt, "kw": kw, "rank": round(float(rank), 1),
                                  "best": int(best), "score": round(float(score), 3)})
for pid in srp_keywords:
    srp_keywords[pid].sort(key=lambda x: (x["dt"], x["kw"]), reverse=True)
d["srpKeywords"] = srp_keywords

# --- srpMatrix (F): flat array, ctr stored as percent ---
d["srpMatrix"] = [{"pid": str(p), "kw": kw, "rank": round(float(r), 2), "best": int(b),
                   "score": round(float(s), 3), "ctr": round(float(ctr) * 100, 2), "imp": int(imp)}
                  for p, kw, r, b, s, ctr, imp in f_rows]

# --- inflow (D, 14-day per-day flat array, pid as str) ---
inflow = [{"dt": dt, "pid": str(p), "inflow": ch, "imp": int(i), "click": int(c), "click_uv": int(cu)}
          for dt, p, ch, i, c, cu in d_rows]
inflow.sort(key=lambda x: (x["dt"], x["pid"], -x["imp"]))
d["inflow"] = inflow
inflow_dates = sorted({r["dt"] for r in inflow})

# --- scoreTs / srpFeatureTs (60d, replace) ---
score_ts = {pid: [] for pid in PIDS}
for dt, pid, s in score_rows:
    if pid in score_ts:
        score_ts[pid].append({"dt": dt, "score": round(float(s), 3)})
for pid in score_ts:
    score_ts[pid].sort(key=lambda x: x["dt"])
d["scoreTs"] = score_ts

feat_ts = {pid: [] for pid in PIDS}
for dt, pid, rv, s28, v28, spv, w, cd, qc in feat_rows:
    if pid in feat_ts:
        feat_ts[pid].append({"dt": dt, "review": round(float(rv), 4), "sell28": round(float(s28), 4),
                             "view28": round(float(v28), 4), "spv28": round(float(spv), 4),
                             "wish": round(float(w), 4), "card": round(float(cd), 4),
                             "qc_rank": round(float(qc), 2)})
for pid in feat_ts:
    feat_ts[pid].sort(key=lambda x: x["dt"])
d["srpFeatureTs"] = feat_ts

# --- ages (E, Monday): flat array + regenerate insight ---
AGE_KEYS = ["20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60+"]
counts = {pid: {k: 0 for k in AGE_KEYS} for pid in PIDS}
for pid, ag, cnt in e_rows:
    pid = str(pid)
    if pid in counts and ag in counts[pid]:
        counts[pid][ag] = int(cnt)
buyer = []
insight_parts = []
for pid in PIDS:
    sample = sum(counts[pid].values())
    age = {k: (round(counts[pid][k] / sample * 100, 1) if sample else 0.0) for k in AGE_KEYS}
    entry = {"pid": pid, "product": d["products"][pid]["label"], "sample": sample, "age": age}
    if sample < 100:
        entry["warn"] = True
    buyer.append(entry)
    pct_25_34 = round((age["25-29"] + age["30-34"]), 1)
    insight_parts.append(f"{d['products'][pid]['shortName']} 25-34세 {pct_25_34}% (샘플 {sample})")
d["ages"]["buyer"] = buyer
d["ages"]["insight"] = " | ".join(insight_parts)

# --- inflowCvr (G): {pid: {channel: {click_uv, buy_uv, cvr}}} summed over window ---
cvr_agg = {}
g_dates = sorted({r[0] for r in g_rows})
for dt, pid, ch, cu, bu in g_rows:
    cell = cvr_agg.setdefault(str(pid), {}).setdefault(ch, {"click_uv": 0, "buy_uv": 0})
    cell["click_uv"] += int(cu)
    cell["buy_uv"] += int(bu)
for pid in cvr_agg:
    for ch, cell in cvr_agg[pid].items():
        cell["cvr"] = round(cell["buy_uv"] / cell["click_uv"] * 100, 2) if cell["click_uv"] else 0.0
d["inflowCvr"]["data"] = cvr_agg
d["inflowCvr"]["period"] = f"{g_dates[0]} ~ {g_dates[-1]}"

# --- categoryBenchmark (H + I): flat arrays only ---
bench = d["categoryBenchmark"]
h_map = {(flag, band): (int(c), to_int(i), to_int(pd), to_int(bu)) for flag, band, c, i, pd, bu in h_rows}
i_map = {str(p): (to_int(i), to_int(pd), to_int(bu)) for p, i, pd, bu in i_rows}

BANDS = [("01_<30k", "<3만원"), ("02_30-50k", "3-5만원"), ("03_50-70k", "5-7만원"),
         ("04_70-100k", "7-10만원"), ("05_100k+", "10만원+")]
p3_tot = [0, 0, 0]
price_bands = []
for band, label in BANDS:
    pb = h_map.get(("PB", band), (0, 0, 0, 0))
    p3 = h_map.get(("3P", band), (0, 0, 0, 0))
    p3_tot = [p3_tot[0] + p3[1], p3_tot[1] + p3[2], p3_tot[2] + p3[3]]
    pb_ctr = round(pb[2] / pb[1] * 100, 2) if pb[1] else 0.0
    p3_ctr = round(p3[2] / p3[1] * 100, 2) if p3[1] else 0.0
    pb_cvr = round(pb[3] / pb[2] * 100, 2) if pb[2] else 0.0
    p3_cvr = round(p3[3] / p3[2] * 100, 2) if p3[2] else 0.0
    price_bands.append({"band": band, "label": label,
                        "pb_prod_cnt": pb[0], "p3_prod_cnt": p3[0],
                        "pb_imp_uv": pb[1], "pb_pdp_uv": pb[2], "pb_buy_uv": pb[3],
                        "p3_imp_uv": p3[1], "p3_pdp_uv": p3[2], "p3_buy_uv": p3[3],
                        "pb_ctr": pb_ctr, "p3_ctr": p3_ctr,
                        "ctr_ratio": round(pb_ctr / p3_ctr, 2) if p3_ctr else 0.0,
                        "pb_cvr": pb_cvr, "p3_cvr": p3_cvr,
                        "cvr_ratio": round(pb_cvr / p3_cvr, 2) if p3_cvr else 0.0})
bench["price_bands"] = price_bands

cate_ctr = round(p3_tot[1] / p3_tot[0] * 100, 2) if p3_tot[0] else 0.0
cate_cvr = round(p3_tot[2] / p3_tot[1] * 100, 2) if p3_tot[1] else 0.0
bench["cate_3p_avg"] = {"imp_uv": p3_tot[0], "pdp_uv": p3_tot[1], "buy_uv": p3_tot[2],
                        "ctr": cate_ctr, "cvr": cate_cvr}

band_lookup = {b["band"]: b for b in price_bands}
SELF = {"3918642": "01_<30k", "3640244": "02_30-50k"}
old_self = {p["pid"]: p for p in bench["self_products"]}
self_products = []
for pid, band in SELF.items():
    iu, pu, bu = i_map.get(pid, (0, 0, 0))
    ctr = round(pu / iu * 100, 2) if iu else 0.0
    cvr = round(bu / pu * 100, 2) if pu else 0.0
    bb = band_lookup[band]
    old = old_self[pid]
    self_products.append({"pid": pid, "name": old["name"], "line": old["line"],
                          "price": old["price"], "price_band": band,
                          "ctr": ctr, "cvr": cvr,
                          "imp_uv": iu, "pdp_uv": pu, "buy_uv": bu,
                          "cate_p3_ctr": cate_ctr, "cate_p3_cvr": cate_cvr,
                          "cate_ctr_ratio": round(ctr / cate_ctr, 2) if cate_ctr else 0.0,
                          "cate_cvr_ratio": round(cvr / cate_cvr, 2) if cate_cvr else 0.0,
                          "band_p3_ctr": bb["p3_ctr"], "band_p3_cvr": bb["p3_cvr"],
                          "band_ctr_ratio": round(ctr / bb["p3_ctr"], 2) if bb["p3_ctr"] else 0.0,
                          "band_cvr_ratio": round(cvr / bb["p3_cvr"], 2) if bb["p3_cvr"] else 0.0})
bench["self_products"] = self_products

pb_lineup = []
for old in bench["pb_lineup"]:
    pid = old["pid"]
    iu, pu, bu = i_map.get(pid, (0, 0, 0))
    pb_lineup.append({"pid": pid, "name": old["name"], "line": old["line"], "price": old["price"],
                      "is_self": old["is_self"], "imp_uv": iu, "pdp_uv": pu, "buy_uv": bu,
                      "ctr": round(pu / iu * 100, 2) if iu else 0.0,
                      "cvr": round(bu / pu * 100, 2) if pu else 0.0})
bench["pb_lineup"] = pb_lineup
bench["period"] = f"{inflow_dates[-7]} ~ {inflow_dates[-1]}"

# --- meta ---
last_dt = max(r["dt"] for pid in PIDS for r in d["daily"][pid])
d["meta"]["lastUpdate"] = last_dt
d["meta"]["srpUpdate"] = max(r["dt"] for r in srp_keywords["3918642"])
d["meta"]["inflowUpdate"] = inflow_dates[-1]
d["meta"]["agesUpdate"] = "2026-06-29"

# sanity: structures that must be flat arrays
assert isinstance(bench["self_products"], list) and isinstance(bench["price_bands"], list)
assert isinstance(bench["pb_lineup"], list) and isinstance(d["srpMatrix"], list)
assert isinstance(d["inflow"], list) and isinstance(d["ages"]["buyer"], list)
assert len(inflow_dates) == 14, f"inflow must span 14 days, got {len(inflow_dates)}"

with open("tf-data.json", "w", encoding="utf-8") as f:
    json.dump(d, f, ensure_ascii=False, indent=2)

# --- March GMV validation (history untouched, but verify) ---
march_targets = {"3918642": 11553700, "3640244": 45449200, "767440": 631833900,
                 "1930788": 80781400, "442026": 75675200}
print(f"=== update {last_dt} (cutoff {DAILY_CUTOFF}) | daily rebuilt: {appended} ===")
print(f"srpKeywords {sum(len(v) for v in srp_keywords.values())} | srpMatrix {len(d['srpMatrix'])} | "
      f"inflow {len(inflow)} rows/{len(inflow_dates)}d | scoreTs..{score_ts['3918642'][-1]['dt']} | "
      f"featTs..{feat_ts['3918642'][-1]['dt']} | cvr {d['inflowCvr']['period']} | bench {bench['period']}")
print("ages:", d["ages"]["insight"])
ok = True
for pid, target in march_targets.items():
    actual = sum(r["gmv"] for r in d["daily"][pid] if r["dt"].startswith("2026-03"))
    status = "OK" if abs(actual - target) < 100 else "MISMATCH"
    if status == "MISMATCH":
        ok = False
    print(f"  {pid}: target={target:,} actual={actual:,} {status}")
for pid in ("3918642", "3640244"):
    r = d["daily"][pid][-1]
    print(f"{pid} {r['dt']} GMV {r['gmv']:,} qty={r['qty']} pdp_uv={r['pdp_uv']} buy_uv={r['buy_uv']}")
sys.exit(0 if ok else 1)
