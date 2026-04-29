"""모든 daily 엔트리에 UV 필드 복구 (commerce_daily_user_count_v3 전체 재쿼리)"""
import json
from pathlib import Path

ROOT = Path(r"C:\Users\yys\living-pb-report")
TF = ROOT / "tf-data.json"
RAW = ROOT / "tf-data" / "uv_full.json"

uv_rows = json.loads(RAW.read_text(encoding="utf-8"))
# rows: [dt, pid, imp_uv, pdp_uv, buy_uv]
UV_BY = {(r[0], r[1]): (
    int(r[2]) if r[2] is not None else 0,
    int(r[3]) if r[3] is not None else 0,
    int(r[4]) if r[4] is not None else 0,
) for r in uv_rows}

data = json.loads(TF.read_text(encoding="utf-8"))

filled = 0
skipped = 0
for pid, arr in data["daily"].items():
    for r in arr:
        key = (r["dt"], pid)
        if key in UV_BY:
            imp_uv, pdp_uv, buy_uv = UV_BY[key]
            r["imp_uv"] = imp_uv
            r["pdp_uv"] = pdp_uv
            r["buy_uv"] = buy_uv
            filled += 1
        else:
            skipped += 1

print(f"UV 채움: {filled}건 / 스킵: {skipped}건")

# Spot check
for pid in ["3918642", "3640244", "767440"]:
    print(f"\n{pid} 마지막 5개:")
    for r in data["daily"][pid][-5:]:
        print(f"  {r['dt']}: pdp_uv={r.get('pdp_uv')} buy_uv={r.get('buy_uv')}")

# Validation
TARGETS = {"3918642": 11553700, "3640244": 45449200, "767440": 631833900,
           "1930788": 80781400, "442026": 75675200}
print("\n3월 GMV 검증:")
ok = True
for pid, target in TARGETS.items():
    s = sum(r["gmv"] for r in data["daily"].get(pid, []) if r["dt"].startswith("2026-03"))
    flag = "OK" if s == target else "MISMATCH"
    print(f"  {pid}: {s:,} vs {target:,} {flag}")
    if s != target: ok = False
if not ok:
    raise SystemExit("검증 실패")

TF.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print("\n저장 완료")
