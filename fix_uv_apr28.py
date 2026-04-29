"""4/28 entry에 누락된 UV 필드 보완"""
import json
from pathlib import Path

TF = Path(r"C:\Users\yys\living-pb-report\tf-data.json")

UV_428 = {
    "1930788": (19282, 961, 19),
    "2636441": (34171, 1664, 55),
    "3640244": (42874, 710, 8),
    "3918642": (14293, 412, 5),
    "442026":  (11195, 499, 5),
    "767440":  (58065, 3030, 191),
}

data = json.loads(TF.read_text(encoding="utf-8"))
for pid, (imp_uv, pdp_uv, buy_uv) in UV_428.items():
    arr = data["daily"].get(pid, [])
    for r in arr:
        if r["dt"] == "2026-04-28":
            r["imp_uv"] = imp_uv
            r["pdp_uv"] = pdp_uv
            r["buy_uv"] = buy_uv
            print(f"  {pid}: imp_uv={imp_uv} pdp_uv={pdp_uv} buy_uv={buy_uv}")
            break

TF.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
print("저장 완료")
