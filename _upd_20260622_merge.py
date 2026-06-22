# -*- coding: utf-8 -*-
"""2026-06-22 머지: daily 6/20~6/21 append + SRP/inflow/cvr/benchmark/scoreTs/featTs/ages 전체 교체.
월요일이라 ages(E) 포함. 스케줄러 6/22 API 529 5회 실패 → 수동 복구."""
import json, sys
from pathlib import Path

TMP = Path(r"tf-data\tmp_2026_06_22")
PIDS = ["3918642", "3640244", "767440", "2636441", "1930788", "442026"]
REFRESH_FROM = "2026-06-20"
DAILY_CUTOFF = "2026-06-21"  # 마지막 완전(PV+UV) 일자


def load(name):
    payload = json.loads((TMP / f"{name}.json").read_text(encoding="utf-8"))
    if payload.get("status") != "SUCCEEDED":
        raise SystemExit(f"query {name} not SUCCEEDED: {payload.get('status')}")
    return payload["data"]


def to_int(x):
    return 0 if x in (None, "") else int(float(x))


a_rows, b_rows, buv_rows = load("a"), load("b"), load("b_uv")
c_rows, f_rows = load("c"), load("f")
d_rows = load("d_a") + load("d_b")
g_rows, h_rows, i_rows = load("g"), load("h"), load("i")
score_rows, feat_rows, e_rows = load("score"), load("feat"), load("e")

a_map = {(dt, str(p)): (to_int(g), round(float(gp), 2), to_int(q)) for dt, p, g, gp, q in a_rows}
b_map = {(dt, str(p)): (to_int(i), to_int(pd), to_int(pu)) for dt, p, i, pd, pu in b_rows}
uv_map = {(dt, str(p)): (to_int(i), to_int(pd), to_int(bu)) for dt, p, i, pd, bu in buv_rows}

d = json.loads(Path("tf-data.json").read_text(encoding="utf-8"))

# --- daily: keep < REFRESH_FROM, append REFRESH_FROM..DAILY_CUTOFF (PV+UV both required) ---
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

# --- srpMatrix (F): flat array, ctr percent ---
d["srpMatrix"] = [{"pid": str(p), "kw": kw, "rank": round(float(r), 2), "best": int(b),
                   "score": round(float(s), 3), "ctr": round(float(ctr) * 100, 2), "imp": int(imp)}
                  for p, kw, r, b, s, ctr, imp in f_rows]

# --- inflow (D, 14d per-day flat array, pid str) ---
inflow = [{"dt": dt, "pid": str(p), "inflow": ch, "imp": int(i), "click": int(c), "click_uv": int(cu)}
          for dt, p, ch, i, c, cu in d_rows]
inflow.sort(key=lambda x: (x["dt"], x["pid"], -x["imp"]))
d["inflow"] = inflow
inflow_dates = sorted({r["dt"] for r in inflow})

# --- scoreTs / srpFeatureTs (60d replace) ---
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

# --- inflowCvr (G): {pid:{channel:{click_uv,buy_uv,cvr}}} summed over 7d ---
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

# --- categoryBenchmark (H+I): flat arrays only ---
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
bench["cate_3p_avg"] = {"imp_uv": p3_tot[0], "pdp_uv": p3_tot[1], "buy_uv": p3_tot[2], "ctr": cate_ctr, "cvr": cate_cvr}

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
    self_products.append({"pid": pid, "name": old["name"], "line": old["line"], "price": old["price"],
                          "price_band": band, "ctr": ctr, "cvr": cvr, "imp_uv": iu, "pdp_uv": pu, "buy_uv": bu,
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

# --- ages (E, Monday): flat buyer array ---
AGES = ["20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60+"]
agg = {}
for pid, ag, cnt in e_rows:
    agg.setdefault(str(pid), {})[ag] = int(cnt)
products = d["products"]
buyer = []
for pid in PIDS:
    a = agg.get(pid, {})
    total = sum(a.values())
    label = products.get(pid, {}).get("label", pid)
    age_pct = {ag: (round(a.get(ag, 0) / total * 100, 1) if total else 0) for ag in AGES}
    item = {"pid": pid, "product": label, "sample": total, "age": age_pct}
    if total < 100:
        item["warn"] = f"샘플 {total}명 (통계 신뢰 제한)"
    buyer.append(item)
parts = []
for b in buyer:
    short = b["product"].split(",")[0].lstrip("🔴🟢⚔️ ").strip()
    pct = round(b["age"].get("25-29", 0) + b["age"].get("30-34", 0), 1)
    parts.append(f"{short} 25-34세 {pct}% (샘플 {b['sample']})")
d["ages"] = {
    "source": "ba_preserved.commerce_gross_profit_orders × dump_member.verified_users 직접 조인 (2025.01~2026.06 구매자, 본인인증 완료자만 매칭)",
    "buyer": buyer,
    "insight": " | ".join(parts),
}

# --- meta ---
last_dt = max(r["dt"] for pid in PIDS for r in d["daily"][pid])
d["meta"]["lastUpdate"] = last_dt
d["meta"]["srpUpdate"] = max(r["dt"] for r in srp_keywords["3918642"])
d["meta"]["inflowUpdate"] = inflow_dates[-1]
d["meta"]["agesUpdate"] = last_dt

# sanity
assert isinstance(bench["self_products"], list) and isinstance(bench["price_bands"], list)
assert isinstance(bench["pb_lineup"], list) and isinstance(d["srpMatrix"], list)
assert isinstance(d["inflow"], list) and isinstance(d["ages"]["buyer"], list)
assert len(inflow_dates) == 14, f"inflow must span 14 days, got {len(inflow_dates)}"

Path("tf-data.json").write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

# --- validation ---
march_targets = {"3918642": 11553700, "3640244": 45449200, "767440": 631833900, "1930788": 80781400, "442026": 75675200}
print(f"=== update {last_dt} | daily appended: {appended} ===")
print(f"srpKeywords {sum(len(v) for v in srp_keywords.values())} | srpMatrix {len(d['srpMatrix'])} | "
      f"inflow {len(inflow)} rows/{len(inflow_dates)}d | scoreTs..{score_ts['3918642'][-1]['dt']} | "
      f"featTs..{feat_ts['3918642'][-1]['dt']} | cvr {d['inflowCvr']['period']} | bench {bench['period']}")
print(f"ages buyers: " + ", ".join(f"{b['pid']}={b['sample']}" for b in buyer))
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
