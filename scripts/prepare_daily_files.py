"""Build the daily landing files for Assignment 2.3 from the raw extracts.

Usage:  python prepare_daily_files.py --raw <path to raw folder> --out <output folder>

It writes two sets of the same four files for each of the five days:
  clean/        simple data first: known-bad rows already removed
  raw_defects/  the same rows as in the source, defects left in

File names: item_master_YYYYMMDD.csv, store_master_YYYYMMDD.csv,
            pos_sales_YYYYMMDD.csv, inventory_snapshot_YYYYMMDD.csv
"""
import argparse
import csv
import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

DAYS = ["20260916", "20260917", "20260918", "20260919", "20260920"]
STORE_SNAPSHOTS = ["20260913", "20260920"]   # weekly store master (Sundays)
SOH_SNAPSHOTS = ["20260913", "20260920"]     # weekly stock snapshots (Sundays)


def read_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def latest_on_or_before(day, snapshots):
    return max(s for s in snapshots if s <= day)


def is_number(text):
    try:
        Decimal(text)
        return True
    except (InvalidOperation, TypeError):
        return False


def amounts_ok(r):
    """Quantity is not a sentinel, and the money columns agree with each other."""
    q, price = Decimal(r["QTY"]), Decimal(r["UNIT_PRICE"])
    ext, disc, net = Decimal(r["EXTENDED_AMT"]), Decimal(r["DISCOUNT_AMT"]), Decimal(r["NET_AMT"])
    cent = Decimal("0.01")
    return (abs(q) < 1000
            and abs(ext - q * price) <= cent
            and abs(ext - disc - net) <= cent
            and abs(disc) <= abs(ext)
            and disc * ext >= 0)


def build_item_master(raw):
    items = read_csv(raw / "rms/ITEM_MASTER.csv")
    dept = {r["DEPT"]: r["DEPT_NAME"] for r in read_csv(raw / "rms/DEPS.csv")}
    sub = {(r["DEPT"], r["CLASS"], r["SUBCLASS"]): r["SUB_NAME"]
           for r in read_csv(raw / "rms/SUBCLASS.csv")}
    out = []
    for r in items:
        r = dict(r)
        r["DEPT_NAME"] = dept.get(r["DEPT"], "")
        r["SUB_NAME"] = sub.get((r["DEPT"], r["CLASS"], r["SUBCLASS"]), "")
        out.append(r)
    fields = list(items[0].keys()) + ["DEPT_NAME", "SUB_NAME"]
    return out, fields


def build_store_master(raw, snapshot):
    stores = read_csv(raw / "rms/STORE.csv")
    district = {r["DISTRICT"]: r for r in read_csv(raw / "rms/DISTRICT.csv")}
    region = {r["REGION"]: r["REGION_NAME"] for r in read_csv(raw / "rms/REGION.csv")}
    moves = read_csv(raw / "rms/STORE_DISTRICT_MOVE.csv")
    out = []
    for r in stores:
        r = dict(r)
        for m in moves:  # before the move date the store still sits in the old district
            if r["STORE"] == m["STORE"] and snapshot < m["EFFECTIVE_DATE"]:
                r["DISTRICT"] = m["FROM_DISTRICT"]
        d = district.get(r["DISTRICT"], {})
        r["DISTRICT_NAME"] = d.get("DISTRICT_NAME", "")
        r["REGION"] = d.get("REGION", "")
        r["REGION_NAME"] = region.get(r["REGION"], "")
        r["SNAPSHOT_DT"] = snapshot
        out.append(r)
    fields = list(stores[0].keys()) + ["DISTRICT_NAME", "REGION", "REGION_NAME", "SNAPSHOT_DT"]
    return out, fields


def read_xstore(raw):
    path = raw / "pos_xstore/SA_TRAN_ITEM_20260801_20261103.psv"
    header, rows = None, []
    with open(path, encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("|")
            if p[0] in ("H", "T"):
                continue
            if p[0] == "RTL_LOC_ID":
                header = p
                continue
            if len(p) == len(header):
                rows.append(dict(zip(header, p)))
    return rows, header


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    raw, out = Path(a.raw), Path(a.out)

    items, item_fields = build_item_master(raw)
    item_ids = {r["ITEM"] for r in items}
    sales, sales_fields = read_xstore(raw)
    summary = {}

    for day in DAYS:
        iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
        store_snap = latest_on_or_before(day, STORE_SNAPSHOTS)
        stores, store_fields = build_store_master(raw, store_snap)
        store_ids = {r["STORE"] for r in stores}
        soh_snap = latest_on_or_before(day, SOH_SNAPSHOTS)
        soh = read_csv(raw / f"rms/soh_full/ITEM_LOC_SOH_FULL_{soh_snap}.csv")
        soh_fields = list(soh[0].keys())
        day_sales = [r for r in sales if r["BUSINESS_DATE"] == iso]

        # simple data first: keep only rows that are valid in every way
        seen, clean_sales = set(), []
        for r in day_sales:
            key = (r["RTL_LOC_ID"], r["WKSTN_ID"], r["TRANS_SEQ"], r["RTRANS_LINEITM_SEQ"])
            if key in seen:
                continue
            ok = (r["VOID_FLAG"] == "N" and r["POST_VOID_FLAG"] == "N"
                  and r["TRANS_TYPCODE"] in ("SALE", "RETURN")
                  and r["CURRENCY"] == "USD"
                  and r["ITEM_ID"] in item_ids and r["RTL_LOC_ID"] in store_ids
                  and all(is_number(r[c]) for c in ("QTY", "UNIT_PRICE", "EXTENDED_AMT", "DISCOUNT_AMT", "NET_AMT"))
                  and amounts_ok(r)
                  and ((r["TRANS_TYPCODE"] == "SALE") == (Decimal(r["QTY"]) > 0)))
            if ok:
                seen.add(key)
                clean_sales.append(r)
        clean_soh = [r for r in soh if r["LOC"] in store_ids and r["ITEM"] in item_ids
                     and is_number(r["STOCK_ON_HAND"]) and Decimal(r["STOCK_ON_HAND"]) >= 0]

        for folder, s_rows, i_rows in (("clean", clean_sales, clean_soh),
                                       ("raw_defects", day_sales, soh)):
            base = out / folder / day
            write_csv(base / f"item_master_{day}.csv", items, item_fields)
            write_csv(base / f"store_master_{day}.csv", stores, store_fields)
            write_csv(base / f"pos_sales_{day}.csv", s_rows, sales_fields)
            write_csv(base / f"inventory_snapshot_{day}.csv", i_rows, soh_fields)

        summary[day] = {
            "store_snapshot_used": store_snap, "stock_snapshot_used": soh_snap,
            "clean": {"pos_sales_rows": len(clean_sales),
                      "pos_sales_net_amt": str(sum(Decimal(r["NET_AMT"]) for r in clean_sales)),
                      "inventory_rows": len(clean_soh), "item_rows": len(items), "store_rows": len(stores)},
            "raw_defects": {"pos_sales_rows": len(day_sales), "inventory_rows": len(soh),
                            "null_item": sum(r["ITEM_ID"] in ("", "NULL") for r in day_sales),
                            "unknown_item": sum(r["ITEM_ID"] not in item_ids and r["ITEM_ID"] not in ("", "NULL") for r in day_sales),
                            "voids": sum(r["VOID_FLAG"] == "Y" or r["TRANS_TYPCODE"] == "VOID" for r in day_sales),
                            "non_usd": sum(r["CURRENCY"] != "USD" for r in day_sales),
                            "sentinel_qty_99999": sum(r["QTY"] in ("99999", "-99999") for r in day_sales),
                            "negative_sale_qty": sum(r["TRANS_TYPCODE"] == "SALE" and Decimal(r["QTY"]) < 0 for r in day_sales),
                            "negative_stock": sum(Decimal(r["STOCK_ON_HAND"]) < 0 for r in soh),
                            "non_store_stock_rows": sum(r["LOC"] not in store_ids for r in soh)},
        }
    (out / "landing_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
