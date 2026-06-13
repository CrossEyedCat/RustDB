#!/usr/bin/env python3
"""Compare TPC-C CI artifacts across runs."""
import csv
import json
import re
from pathlib import Path

RUNS = {
    "main_baseline (26485428155)": "ci-26485428155",
    "pr93_regress (26835987819)": "ci-26835987819",
    "pr93_best (26845065968)": "ci-26845065968",
    "main_post93 (26850594405)": "ci-26850594405",
    "pr94_shard5 (26850665870)": "ci-26850665870",
}
BASE = Path(__file__).resolve().parent.parent / "tpcc-out"


def phase_p50_p95(md: Path, field: str):
    if not md.is_file():
        return None, None
    for line in md.read_text(encoding="utf-8").splitlines():
        if f"`{field}`" in line and line.strip().startswith("|"):
            parts = [x.strip() for x in line.split("|")]
            if len(parts) > 4 and parts[3].replace(".", "").replace("-", "").isdigit():
                return parts[3], parts[4]
    return None, None


def load(label: str, dname: str):
    d = BASE / dname
    tpcc = (d / "tpcc.txt").read_text(encoding="utf-8")
    pg = (d / "postgres_tpcc.txt").read_text(encoding="utf-8")
    rust_tps = float(re.search(r"txns_per_s \(successful only\): ([\d.]+)", tpcc).group(1))
    pg_tps = float(re.search(r"txns_per_s \(successful only\): ([\d.]+)", pg).group(1))
    tpmc = float(re.search(r"tpmC: ([\d.]+)", tpcc).group(1))
    val_path = d / "validation.json"
    val = json.loads(val_path.read_text(encoding="utf-8")) if val_path.is_file() else {}
    md = d / "phases_native_new_order.md"
    ol_lock = phase_p50_p95(md, "insert_order_line_pm_lock_wait_us")
    ol_ins = phase_p50_p95(md, "insert_order_line_us")
    oorder = phase_p50_p95(md, "insert_oorder_us")
    dist = phase_p50_p95(md, "district_us")
    by = {}
    with (d / "tpcc_txn.log").open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by.setdefault(row["kind"], []).append(int(row["elapsed_us"]))

    def p50(kind):
        v = sorted(by.get(kind, []))
        return v[len(v) // 2] / 1000 if v else None

    return {
        "label": label,
        "rust_tps": rust_tps,
        "pg_tps": pg_tps,
        "ratio": 100 * rust_tps / pg_tps,
        "tpmc": tpmc,
        "ol_lock_p50": ol_lock[0],
        "ol_ins_p50": ol_ins[0],
        "oorder_p50": oorder[0],
        "dist_p95": dist[1],
        "no_client_p50": p50("new_order"),
        "pay_client_p50": p50("payment"),
        "claim_pg": val.get("claim_faster_than_pg"),
        "new_orders": int(re.search(r"new_orders \(successful only\): (\d+)", tpcc).group(1)),
    }


def main():
    rows = [load(l, d) for l, d in RUNS.items() if (BASE / d / "tpcc.txt").is_file()]
    pg_vals = [r["pg_tps"] for r in rows]
    pg_mean = sum(pg_vals) / len(pg_vals)
    pg_min, pg_max = min(pg_vals), max(pg_vals)

    print("=== Multi-run table (same CI job: c=64, 300s, native bench) ===\n")
    hdr = (
        f"{'run':28} {'rust':>6} {'pg':>6} {'ratio':>6} {'tpmC':>6} "
        f"{'ol_lock':>8} {'ol_ins':>7} {'oorder':>7} {'no_p50':>7} {'claim':>5}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(
            f"{r['label'][:28]:28} {r['rust_tps']:6.0f} {r['pg_tps']:6.0f} "
            f"{r['ratio']:5.1f}% {r['tpmc']:6.0f} "
            f"{str(r['ol_lock_p50'] or '-'):>8} {str(r['ol_ins_p50'] or '-'):>7} "
            f"{str(r['oorder_p50'] or '-'):>7} {r['no_client_p50'] or 0:7.1f} "
            f"{str(r['claim_pg']):>5}"
        )

    print(f"\nPG TPS spread across runs: min={pg_min:.0f} max={pg_max:.0f} mean={pg_mean:.0f} "
          f"(±{100*(pg_max-pg_min)/pg_mean:.0f}% range)")

    a = next(r for r in rows if "main_post93" in r["label"])
    b = next(r for r in rows if "pr94" in r["label"])
    print("\n=== Fair A/B: main after #93 merge vs PR #94 (only change: d_id sharding) ===")
    print(f"RustDB TPS:  {a['rust_tps']:.0f} -> {b['rust_tps']:.0f}  "
          f"({100*(b['rust_tps']/a['rust_tps']-1):+.1f}%)")
    print(f"tpmC:        {a['tpmc']:.0f} -> {b['tpmc']:.0f}  "
          f"({100*(b['tpmc']/a['tpmc']-1):+.1f}%)")
    print(f"PG TPS:      {a['pg_tps']:.0f} -> {b['pg_tps']:.0f}  "
          f"({100*(b['pg_tps']/a['pg_tps']-1):+.1f}%)  <-- runner variance")
    print(f"Ratio:       {a['ratio']:.1f}% -> {b['ratio']:.1f}%")
    print(f"ol_lock p50: {a['ol_lock_p50']} ms -> {b['ol_lock_p50']} ms")
    print(f"ol_ins p50:  {a['ol_ins_p50']} ms -> {b['ol_ins_p50']} ms")
    print(f"oorder p50:  {a['oorder_p50']} ms -> {b['oorder_p50']} ms")
    print(f"new_orders:  {a['new_orders']} -> {b['new_orders']} (more txns if faster)")

    print("\n=== PG-normalized ratio (rust_tps / mean PG across all runs) ===")
    for r in rows:
        print(f"  {r['label'][:28]:28} {100*r['rust_tps']/pg_mean:5.1f}%")

    print("\n=== Cherry-pick check ===")
    worst_rust = min(rows, key=lambda r: r["rust_tps"])
    best_rust = max(rows, key=lambda r: r["rust_tps"])
    print(f"Worst RustDB run: {worst_rust['label']} @ {worst_rust['rust_tps']:.0f} TPS")
    print(f"Best RustDB run:  {best_rust['label']} @ {best_rust['rust_tps']:.0f} TPS")
    print(
        f"PR94 vs worst (pr93_regress): {100*(b['rust_tps']/worst_rust['rust_tps']-1):+.1f}% "
        f"(includes row-lock + fmt fixes, not just sharding)"
    )
    print(
        f"PR94 vs fair baseline (main_post93): {100*(b['rust_tps']/a['rust_tps']-1):+.1f}% "
        f"(sharding-only delta)"
    )


if __name__ == "__main__":
    main()
