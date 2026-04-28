"""v10 daily update: 5 Athena query results → tf-data.json
Inputs: tf-data/tmp_YYYY_MM_DD/query_{a,b,c,d,f}.json
Updates: daily[pid], srpKeywords[pid], srpMatrix, inflow, meta.lastUpdate
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

DT = sys.argv[1] if len(sys.argv) > 1 else "2026_04_28"
TMP = Path(f"tf-data/tmp_{DT}")
PIDS = ["3918642", "3640244", "767440", "2636441", "1930788", "442026"]

with open(TMP / "query_a.json", encoding="utf-8") as f:
    a_rows = json.load(f)
with open(TMP / "query_b.json", encoding="utf-8") as f:
    b_rows = json.load(f)
with open(TMP / "query_c.json", encoding="utf-8") as f:
    c_rows = json.load(f)
with open(TMP / "query_d.json", encoding="utf-8") as f:
    d_rows = json.load(f)
with open(TMP / "query_f.json", encoding="utf-8") as f:
    f_rows = json.load(f)

# === 1. daily[pid]: A + B merge ===
def to_int(x):
    if x is None or x == "":
        return 0
    return int(x)

a_map = {}
for dt, pid, gmv, gp, qty in a_rows:
    a_map[(dt, str(pid))] = (to_int(gmv), to_int(gp), to_int(qty))

b_map = {}
for dt, pid, imp, pdp, purchase in b_rows:
    b_map[(dt, str(pid))] = (to_int(imp), to_int(pdp), to_int(purchase))

all_keys = set(a_map.keys()) | set(b_map.keys())
daily_new = {pid: [] for pid in PIDS}
for dt, pid in sorted(all_keys):
    if pid not in PIDS:
        continue
    gmv, gp, qty = a_map.get((dt, pid), (0, 0, 0))
    imp, pdp, purchase = b_map.get((dt, pid), (0, 0, 0))
    daily_new[pid].append({
        "dt": dt, "gmv": gmv, "gp": gp, "qty": qty,
        "imp": imp, "pdp": pdp, "purchase": purchase
    })

# === 2. srpKeywords[pid] from C ===
srp_keywords = {"3918642": [], "3640244": []}
for dt, pid, kw, avg_rank, best, score in c_rows:
    if pid not in srp_keywords:
        continue
    srp_keywords[pid].append({
        "dt": dt, "kw": kw,
        "rank": round(avg_rank, 1),
        "best": int(best),
    })
for pid in srp_keywords:
    srp_keywords[pid].sort(key=lambda x: (x["dt"], x["kw"]), reverse=True)

# === 3. srpMatrix from F ===
srp_matrix = []
for pid, kw, avg_rank, best, score, ctr, imp in f_rows:
    srp_matrix.append({
        "pid": str(pid), "kw": kw,
        "rank": round(avg_rank, 2), "best": int(best),
        "score": round(score, 2), "ctr": round(ctr, 3),
        "imp": int(imp)
    })

# === 4. inflow from D ===
inflow = []
for dt, pid, inflow_name, imp, click, click_uv in d_rows:
    inflow.append({
        "dt": dt, "pid": str(pid), "inflow": inflow_name,
        "imp": int(imp), "click": int(click), "click_uv": int(click_uv)
    })
inflow.sort(key=lambda x: (x["dt"], x["pid"], -x["imp"]))

# === 5. Load existing & merge ===
with open("tf-data.json", encoding="utf-8") as f:
    d = json.load(f)

# Preserve existing pdp_uv/imp_uv/buy_uv from old daily where dt overlaps
old_daily = d.get("daily", {})
for pid in PIDS:
    old_by_dt = {row["dt"]: row for row in old_daily.get(pid, [])}
    for row in daily_new[pid]:
        old = old_by_dt.get(row["dt"])
        if old:
            for k in ("pdp_uv", "imp_uv", "buy_uv"):
                if k in old:
                    row[k] = old[k]

d["daily"] = daily_new
d["srpKeywords"] = srp_keywords
d["srpMatrix"] = srp_matrix
d["inflow"] = inflow

# meta
last_dt = max(row["dt"] for pid in PIDS for row in daily_new[pid])
d["meta"]["lastUpdate"] = last_dt
d["meta"]["version"] = "v10"

with open("tf-data.json", "w", encoding="utf-8") as f:
    json.dump(d, f, ensure_ascii=False, indent=2)

# === 6. 3월 GMV 정합성 검증 ===
march_targets = {
    "3918642": 11553700,  # 소프트워싱
    "3640244": 45449200,  # 알러지케어
    "767440": 631833900,  # 카스테라
    "1930788": 80781400,  # 돈워리
    "442026": 75675200,   # 가드 M2
}
march_actual = {pid: 0 for pid in march_targets}
for pid in march_targets:
    for row in daily_new[pid]:
        if row["dt"].startswith("2026-03"):
            march_actual[pid] += row["gmv"]

print(f"=== v10 update ({last_dt}) ===")
print(f"Daily rows: {sum(len(daily_new[p]) for p in PIDS)}")
print(f"SRP keywords: {sum(len(v) for v in srp_keywords.values())}")
print(f"SRP matrix: {len(srp_matrix)}")
print(f"Inflow rows: {len(inflow)}")
print()
print("=== 3월 GMV 정합성 ===")
ok = True
for pid, target in march_targets.items():
    actual = march_actual[pid]
    diff = actual - target
    status = "OK" if abs(diff) < 100 else "MISMATCH"
    if abs(diff) >= 100:
        ok = False
    print(f"  {pid}: target={target:,} actual={actual:,} diff={diff:+,} {status}")
print()
# 어제(가장 최근) 자사 GMV
today_gmv = {}
for pid in ("3918642", "3640244"):
    last_row = daily_new[pid][-1]
    today_gmv[pid] = (last_row["dt"], last_row["gmv"])
print(f"소프트워싱 {today_gmv['3918642'][0]} GMV: {today_gmv['3918642'][1]:,}")
print(f"알러지케어 {today_gmv['3640244'][0]} GMV: {today_gmv['3640244'][1]:,}")
sys.exit(0 if ok else 1)
