#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_data.py - Build data.json for living-pb-report dashboard
Reads Athena query results (daily, weekly) + existing data.json (monthly, priceBandData, orgMapping)
and assembles the complete data.json.
"""

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────
BASE = Path(r"C:/Users/yys/living-pb-report")
DAILY_FILE = Path(r"C:/Users/yys/.claude/projects/C--Users-yys/6edb39e8-511c-4dbe-a34f-b86f7e3f67d6/tool-results/mcp-ohouse-athena-mcp-execute_athena_query-1775011686997.txt")
WEEKLY_FILE = Path(r"C:/Users/yys/.claude/projects/C--Users-yys/6edb39e8-511c-4dbe-a34f-b86f7e3f67d6/tool-results/mcp-ohouse-athena-mcp-execute_athena_query-1775011691326.txt")
EXISTING_JSON = BASE / "data.json"
OUTPUT_JSON = BASE / "data.json"


def safe_float(v, default=0.0):
    """Convert value to float, handling None/NaN/Infinity."""
    if v is None:
        return default
    if isinstance(v, str):
        v = v.strip()
        if v in ("", "null", "None", "NaN", "nan"):
            return default
        if v in ("Infinity", "inf", "-Infinity", "-inf"):
            return default
        try:
            f = float(v)
        except (ValueError, TypeError):
            return default
    else:
        try:
            f = float(v)
        except (ValueError, TypeError):
            return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def safe_int(v, default=0):
    return int(safe_float(v, default))


def safe_float_or_null(v):
    """Return float or None (for JSON null)."""
    f = safe_float(v, None)
    if f is None:
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def load_athena_result(path):
    """Load Athena JSON result file, return list of dicts."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    columns = [c["name"] for c in raw["columns"]]
    rows = []
    for row in raw["data"]:
        d = {}
        for i, col in enumerate(columns):
            d[col] = row[i]
        rows.append(d)
    return rows


def org_for_cate1(cate1):
    """Map cate_d1 to org."""
    if cate1 == "가구":
        return "가구PB"
    elif cate1 in ("생활용품", "패브릭"):
        return "리빙PB"
    return None


def default_bench_cvr(cate1):
    """Default benchmark CVR by category (ratio, not percentage).
    Based on actual 3P category averages from Athena (March 2026).
    가구: ~0.5-1.0%, 생활용품: ~1.8-2.8%, 패브릭: ~0.9-1.4%
    """
    if cate1 == "가구":
        return 0.005  # 0.5%
    elif cate1 == "생활용품":
        return 0.020  # 2.0%
    return 0.011  # 1.1% (패브릭 등)


def default_bench_ctr(cate1):
    """Default benchmark CTR by category (ratio, not percentage).
    Based on actual 3P category averages from Athena (March 2026).
    All categories: ~2.7-3.4%
    """
    if cate1 == "생활용품":
        return 0.030  # 3.0%
    return 0.029  # 2.9%


DEFAULT_BENCH3P = 0.005  # 0.5% CVR as fallback


# ── Load data ──────────────────────────────────────────────────────
print("Loading source files...")
daily_rows = load_athena_result(DAILY_FILE)
weekly_rows = load_athena_result(WEEKLY_FILE)

print(f"  Daily rows: {len(daily_rows)}")
print(f"  Weekly rows: {len(weekly_rows)}")

# Load existing data.json for orgMapping, priceBandData, monthly, and other preserved fields
with open(EXISTING_JSON, "r", encoding="utf-8") as f:
    existing = json.load(f)

org_mapping = existing.get("orgMapping", {"가구PB": ["가구"], "리빙PB": ["생활용품", "패브릭"]})
price_band_data = existing.get("priceBandData", {})
existing_monthly = existing.get("monthly", {})
existing_weekly = existing.get("weekly", {})

# ── Preserve fields from existing weekly that we can't reconstruct ──
preserved_weekly_fields = {}
for key in ["promoMap", "promoDetails", "productDealWeeks", "productDealDates", "productDailyGmv", "tp3WeeklyGmv"]:
    preserved_weekly_fields[key] = existing_weekly.get(key, {})

# ── Build DAILY data ──────────────────────────────────────────────
print("Building daily data...")

# Group daily rows by date
daily_by_date = defaultdict(list)
for row in daily_rows:
    daily_by_date[row["date"]].append(row)

all_daily_dates = sorted(daily_by_date.keys())
# Last 7 days
daily_dates = all_daily_dates[-7:] if len(all_daily_dates) >= 7 else all_daily_dates

# We need prev day data too, so include one extra day before daily_dates[0] if available
all_dates_needed = set(daily_dates)
if daily_dates:
    # Find prev of first date
    first_idx = all_daily_dates.index(daily_dates[0])
    if first_idx > 0:
        all_dates_needed.add(all_daily_dates[first_idx - 1])

# Build dailyBench from existing (preserve it)
existing_daily_bench = {}
if "dailyAll" in existing:
    for dt, dobj in existing["dailyAll"].items():
        if "dailyBench" in dobj:
            existing_daily_bench[dt] = dobj["dailyBench"]

daily_all = {}
for i, date in enumerate(daily_dates):
    rows = daily_by_date[date]
    # Find prev date
    date_idx = all_daily_dates.index(date)
    prev_date = all_daily_dates[date_idx - 1] if date_idx > 0 else None
    prev_rows = daily_by_date.get(prev_date, []) if prev_date else []

    # Build prev lookup: product_id -> row
    prev_lookup = {}
    for r in prev_rows:
        prev_lookup[str(r["product_id"])] = r

    # Total GMV (백만원)
    total_gmv = round(sum(safe_float(r["gmv"]) for r in rows) / 1_000_000, 1)
    total_gmv_prev = round(sum(safe_float(r["gmv"]) for r in prev_rows) / 1_000_000, 1) if prev_rows else None

    # Summary by org
    summary = {}
    org_gmv = defaultdict(float)
    org_gmv_prev = defaultdict(float)
    org_imp = defaultdict(int)
    org_pdp = defaultdict(int)
    org_purchase = defaultdict(int)

    for r in rows:
        org = org_for_cate1(r["cate_d1"])
        if org:
            org_gmv[org] += safe_float(r["gmv"])
            org_imp[org] += safe_int(r["imp"])
            org_pdp[org] += safe_int(r["pdp"])
            org_purchase[org] += safe_int(r["purchase"])

    for r in prev_rows:
        org = org_for_cate1(r["cate_d1"])
        if org:
            org_gmv_prev[org] += safe_float(r["gmv"])

    for org in ["가구PB", "리빙PB"]:
        summary[org] = {
            "gmv": round(org_gmv[org] / 1_000_000, 1),
            "gmvPrev": round(org_gmv_prev[org] / 1_000_000, 1) if prev_rows else None,
            "imp": org_imp[org],
            "pdp": org_pdp[org],
            "purchase": org_purchase[org],
        }

    # dailyBench: try to get from existing, build keys from current data
    daily_bench = {}
    if date in existing_daily_bench:
        daily_bench = existing_daily_bench[date]
    else:
        # Build bench from data's cate1|cate2 combinations
        # We don't have 3P bench data from Athena, so use defaults
        cate_combos = set()
        for r in rows:
            cate_combos.add((r["cate_d1"], r["cate_d2"]))
        for c1, c2 in cate_combos:
            key = f"{c1}|{c2}"
            daily_bench[key] = {
                date: {
                    "cvr": round(default_bench_cvr(c1) * 100, 2),
                    "ctr": round(default_bench_ctr(c1) * 100, 2),
                }
            }

    # Products
    products = []
    for r in rows:
        pid = str(r["product_id"])
        gmv_raw = safe_float(r["gmv"])
        gmv_m = round(gmv_raw / 1_000_000, 1)
        imp = safe_int(r["imp"])
        pdp = safe_int(r["pdp"])
        purchase = safe_int(r["purchase"])

        # CTR and CVR: raw values are ratios (0.038 = 3.8%)
        ctr_raw = safe_float(r.get("ctr"))
        cvr_raw = safe_float(r.get("cvr"))
        ctr_pct = round(ctr_raw * 100, 2) if ctr_raw else 0
        cvr_pct = round(cvr_raw * 100, 2) if cvr_raw else 0

        # Prev day GMV
        prev_r = prev_lookup.get(pid)
        gmv_prev = round(safe_float(prev_r["gmv"]) / 1_000_000, 1) if prev_r else None

        # Benchmark: try from dailyBench
        bench_key = f"{r['cate_d1']}|{r['cate_d2']}"
        bench_entry = daily_bench.get(bench_key, {}).get(date, {})
        bench_cvr = bench_entry.get("cvr")
        bench_ctr = bench_entry.get("ctr")

        # Fallback defaults
        if bench_cvr is None or (isinstance(bench_cvr, float) and (math.isnan(bench_cvr) or math.isinf(bench_cvr))):
            bench_cvr = round(default_bench_cvr(r["cate_d1"]) * 100, 2)
        if bench_ctr is None or (isinstance(bench_ctr, float) and (math.isnan(bench_ctr) or math.isinf(bench_ctr))):
            bench_ctr = round(default_bench_ctr(r["cate_d1"]) * 100, 2)

        products.append({
            "id": int(r["product_id"]),
            "name": r["product_name"],
            "cate1": r["cate_d1"],
            "cate2": r["cate_d2"],
            "gmv": gmv_m,
            "imp": imp,
            "pdp": pdp,
            "purchase": purchase,
            "ctr": ctr_pct,
            "cvr": cvr_pct,
            "benchCvr": round(bench_cvr, 4) if bench_cvr is not None else None,
            "benchCtr": round(bench_ctr, 4) if bench_ctr is not None else None,
            "gmvPrev": gmv_prev,
        })

    # Sort products by GMV descending
    products.sort(key=lambda p: p["gmv"] or 0, reverse=True)

    daily_all[date] = {
        "date": date,
        "datePrev": prev_date,
        "totalGmv": total_gmv,
        "totalGmvPrev": total_gmv_prev,
        "summary": summary,
        "dailyBench": daily_bench,
        "products": products,
    }

# ── Build WEEKLY data ──────────────────────────────────────────────
print("Building weekly data...")

# Group weekly rows by week
weekly_by_week = defaultdict(list)
for row in weekly_rows:
    weekly_by_week[row["week"]].append(row)

weeks = sorted(weekly_by_week.keys())

# Build product registry from weekly data
product_registry = {}  # product_id -> {name, cate1, cate2}
for row in weekly_rows:
    pid = int(row["product_id"])
    if pid not in product_registry:
        product_registry[pid] = {
            "name": row["product_name"],
            "cate1": row["cate_d1"],
            "cate2": row["cate_d2"],
        }

# Weekly summary
weekly_summary = {}
for week in weeks:
    rows = weekly_by_week[week]
    total = 0
    by_cate1 = defaultdict(float)
    by_org = defaultdict(float)

    for r in rows:
        gmv = safe_float(r["gmv"])
        total += gmv
        by_cate1[r["cate_d1"]] += gmv
        org = org_for_cate1(r["cate_d1"])
        if org:
            by_org[org] += gmv

    # GMV in 만원
    weekly_summary[week] = {
        "가구": round(by_cate1.get("가구", 0) / 10000, 1),
        "total": round(total / 10000, 1),
        "패브릭": round(by_cate1.get("패브릭", 0) / 10000, 1),
        "생활용품": round(by_cate1.get("생활용품", 0) / 10000, 1),
        "가구PB": round(by_org.get("가구PB", 0) / 10000, 1),
        "리빙PB": round(by_org.get("리빙PB", 0) / 10000, 1),
    }

# Weekly products
weekly_products = []
all_pids = set()
for row in weekly_rows:
    all_pids.add(int(row["product_id"]))

# Build lookup: (week, pid) -> row
weekly_lookup = {}
for row in weekly_rows:
    weekly_lookup[(row["week"], int(row["product_id"]))] = row

# bp3Cvr from existing (3P benchmark by cate per week)
bp3_cvr = existing_weekly.get("bp3Cvr", {})
# Extend bp3Cvr for new weeks with defaults
new_weeks_set = set(weeks)
existing_weeks_set = set()
for cate_key, week_data in bp3_cvr.items():
    existing_weeks_set.update(week_data.keys())

for week in weeks:
    if week not in existing_weeks_set:
        # Add default benchmarks for new weeks
        cate_combos = set()
        for r in weekly_by_week[week]:
            cate_combos.add(f"{r['cate_d1']}|{r['cate_d2']}")
        for key in cate_combos:
            if key not in bp3_cvr:
                bp3_cvr[key] = {}
            if week not in bp3_cvr[key]:
                c1 = key.split("|")[0]
                bp3_cvr[key][week] = round(default_bench_cvr(c1) * 100, 2)

for pid in sorted(all_pids):
    info = product_registry[pid]
    weekly_data = {}

    for week in weeks:
        row = weekly_lookup.get((week, pid))
        if row is None:
            continue

        gmv = safe_float(row["gmv"])
        imp = safe_int(row["imp"])
        pdp = safe_int(row["pdp"])
        purchase = safe_int(row["purchase"])
        cvr_raw = safe_float(row.get("cvr"))
        cvr_pct = round(cvr_raw * 100, 2) if cvr_raw else 0

        # bench3p from bp3Cvr
        bench_key = f"{info['cate1']}|{info['cate2']}"
        bench3p_val = None
        if bench_key in bp3_cvr and week in bp3_cvr[bench_key]:
            b = bp3_cvr[bench_key][week]
            if b is not None and not (isinstance(b, float) and (math.isnan(b) or math.isinf(b))):
                bench3p_val = round(b, 3)
        if bench3p_val is None:
            bench3p_val = DEFAULT_BENCH3P

        weekly_data[week] = {
            "gmv": round(gmv / 10000, 1),
            "imp": imp,
            "pdpUv": pdp,
            "buyUv": purchase,
            "buyCnt": purchase,
            "cvr": cvr_pct,
            "pdp": pdp,
            "purchase": purchase,
            "bench3p": bench3p_val,
        }

    # Also bring forward any older weeks from existing data
    existing_prod = None
    for ep in existing_weekly.get("products", []):
        if ep["id"] == pid:
            existing_prod = ep
            break
    if existing_prod and "weekly" in existing_prod:
        for old_week, old_data in existing_prod["weekly"].items():
            if old_week not in weekly_data and old_week not in weeks:
                weekly_data[old_week] = old_data

    weekly_products.append({
        "id": pid,
        "name": info["name"],
        "cate1": info["cate1"],
        "cate2": info["cate2"],
        "weekly": weekly_data,
    })

# ── Build MONTHLY data ──────────────────────────────────────────────
print("Building monthly data...")

# Reuse existing monthly data since we don't have raw monthly Athena results
# Update monthly from weekly data where possible (March data can be aggregated)
monthly = existing_monthly

# If monthly is empty, create minimal structure
if not monthly:
    monthly = {
        "months": [],
        "summary": {},
        "products": [],
        "bp3CvrMonthly": {},
    }

# Update March monthly from weekly data (weeks 2026-03-02 through 2026-03-30)
march_weeks = [w for w in weeks if w.startswith("2026-03")]
if march_weeks and "2026-03-01" in monthly.get("months", []):
    print(f"  Updating March monthly from {len(march_weeks)} weeks...")
    # Aggregate weekly data into March
    march_by_pid = defaultdict(lambda: {"gmv": 0, "imp": 0, "pdp": 0, "purchase": 0})
    for week in march_weeks:
        for row in weekly_by_week.get(week, []):
            pid = int(row["product_id"])
            march_by_pid[pid]["gmv"] += safe_float(row["gmv"])
            march_by_pid[pid]["imp"] += safe_int(row["imp"])
            march_by_pid[pid]["pdp"] += safe_int(row["pdp"])
            march_by_pid[pid]["purchase"] += safe_int(row["purchase"])

    # Update monthly products for March
    for mp in monthly.get("products", []):
        pid = mp["id"]
        if pid in march_by_pid:
            md = march_by_pid[pid]
            cvr = round(md["purchase"] / md["pdp"] * 100, 2) if md["pdp"] > 0 else 0
            mp["monthly"]["2026-03-01"] = {
                "gmv": round(md["gmv"] / 10000, 1),
                "imp": md["imp"],
                "pdpUv": md["pdp"],
                "buyUv": md["purchase"],
                "buyCnt": md["purchase"],
                "cvr": cvr,
                "pdp": md["pdp"],
                "purchase": md["purchase"],
                "bench3p": None,
            }

    # Update monthly summary for March
    total_march = 0
    by_cate1_march = defaultdict(float)
    by_org_march = defaultdict(float)
    for pid, md in march_by_pid.items():
        info = product_registry.get(pid)
        if info:
            total_march += md["gmv"]
            by_cate1_march[info["cate1"]] += md["gmv"]
            org = org_for_cate1(info["cate1"])
            if org:
                by_org_march[org] += md["gmv"]

    if total_march > 0:
        monthly["summary"]["2026-03-01"] = {
            "가구": round(by_cate1_march.get("가구", 0) / 10000, 1),
            "total": round(total_march / 10000, 1),
            "패브릭": round(by_cate1_march.get("패브릭", 0) / 10000, 1),
            "생활용품": round(by_cate1_march.get("생활용품", 0) / 10000, 1),
            "가구PB": round(by_org_march.get("가구PB", 0) / 10000, 1),
            "리빙PB": round(by_org_march.get("리빙PB", 0) / 10000, 1),
        }

# ── Handle new products that exist in weekly but not in monthly ──
monthly_pids = {mp["id"] for mp in monthly.get("products", [])}
for pid in sorted(all_pids):
    if pid not in monthly_pids:
        info = product_registry[pid]
        mp = {
            "id": pid,
            "name": info["name"],
            "cate1": info["cate1"],
            "cate2": info["cate2"],
            "monthly": {},
        }
        # Add March data if available
        if pid in march_by_pid:
            md = march_by_pid[pid]
            cvr = round(md["purchase"] / md["pdp"] * 100, 2) if md["pdp"] > 0 else 0
            mp["monthly"]["2026-03-01"] = {
                "gmv": round(md["gmv"] / 10000, 1),
                "imp": md["imp"],
                "pdpUv": md["pdp"],
                "buyUv": md["purchase"],
                "buyCnt": md["purchase"],
                "cvr": cvr,
                "pdp": md["pdp"],
                "purchase": md["purchase"],
                "bench3p": None,
            }
        monthly["products"].append(mp)

# ── Assemble final JSON ──────────────────────────────────────────────
print("Assembling final data.json...")

# Build the daily object (latest day only, for backward compat)
latest_date = daily_dates[-1] if daily_dates else None
daily_obj = daily_all.get(latest_date, {}) if latest_date else {}

output = {
    "daily": daily_obj,
    "dailyAll": daily_all,
    "dailyDates": daily_dates,
    "weekly": {
        "weeks": weeks,
        "summary": weekly_summary,
        "products": weekly_products,
        "bp3Cvr": bp3_cvr,
        "promoMap": preserved_weekly_fields.get("promoMap", {}),
        "promoDetails": preserved_weekly_fields.get("promoDetails", {}),
        "productDealWeeks": preserved_weekly_fields.get("productDealWeeks", {}),
        "productDealDates": preserved_weekly_fields.get("productDealDates", {}),
        "productDailyGmv": preserved_weekly_fields.get("productDailyGmv", {}),
        "tp3WeeklyGmv": preserved_weekly_fields.get("tp3WeeklyGmv", {}),
    },
    "monthly": monthly,
    "orgMapping": org_mapping,
    "priceBandData": price_band_data,
}

# ── Write output ──────────────────────────────────────────────────────
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

file_size = OUTPUT_JSON.stat().st_size
print(f"Done! Written to {OUTPUT_JSON} ({file_size:,} bytes)")

# ── Validation ──────────────────────────────────────────────────────
print("\n=== Validation ===")
print(f"dailyDates: {output['dailyDates']}")
print(f"dailyAll dates: {list(output['dailyAll'].keys())}")
for dt in output["dailyDates"][-2:]:
    d = output["dailyAll"][dt]
    print(f"  {dt}: totalGmv={d['totalGmv']}백만원, products={len(d['products'])}")
print(f"weekly weeks: {output['weekly']['weeks']}")
print(f"weekly products: {len(output['weekly']['products'])}")
print(f"monthly months: {output['monthly']['months']}")
print(f"monthly products: {len(output['monthly']['products'])}")
print(f"orgMapping: {output['orgMapping']}")
print(f"priceBandData keys: {list(output['priceBandData'].keys())}")
