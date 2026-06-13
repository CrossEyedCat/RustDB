#!/usr/bin/env python3
"""Compare PostgreSQL baseline metrics across CI benchmark job logs."""
from __future__ import annotations

import json
import re
import subprocess
import sys

RUNS = [
    ("main post #93", 26850594405),
    ("PR #94 merge CI", 26852962238),
    ("PR #95 CI", 26853935022),
    ("main post #95 merge", 26855622083),
]


def job_log(run_id: int) -> str:
    jobs = json.loads(
        subprocess.check_output(
            ["gh", "api", f"repos/CrossEyedCat/RustDB/actions/runs/{run_id}/jobs"],
            text=True,
        )
    )["jobs"]
    job = next(j for j in jobs if "Benchmark TPC-C" in j["name"])
    return subprocess.check_output(
        ["gh", "api", f"repos/CrossEyedCat/RustDB/actions/jobs/{job['id']}/logs"],
        text=True,
        errors="replace",
    )


def strip_ts(line: str) -> str:
    return re.sub(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s*", "", line.strip())


def clean(line: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", strip_ts(line))


def extract_pg_block(log: str) -> str:
    """Text between first postgres throughput header and RustDB section."""
    markers = [
        "== postgres_tpcc throughput ==",
        "==> Run TPC-C throughput benchmark",
        "==> RustDB",
        "== tpcc throughput ==",
    ]
    start = log.find("== postgres_tpcc throughput ==")
    if start < 0:
        return ""
    end = len(log)
    for m in markers[1:]:
        i = log.find(m, start + 1)
        if i > start:
            end = min(end, i)
    chunk = log[start:end]
    lines = []
    for raw in chunk.splitlines():
        s = clean(raw)
        if s and not s.startswith("##["):
            lines.append(s)
    return "\n".join(lines[:25])


def extract_pg_analyze(log: str) -> list[str]:
    rows = []
    in_pg = False
    for raw in log.splitlines():
        s = clean(raw)
        if "--- PostgreSQL (postgres_tpcc) ---" in s:
            in_pg = True
            continue
        if in_pg and ("--- RustDB" in s or "==> order_status micro" in s):
            break
        if in_pg and re.match(
            r"^(new_order|payment|order_status|delivery|stock_level)\s+\d+\s+[\d.]",
            s,
        ):
            rows.append(s)
    return rows


def parse_latency_line(fields: dict[str, str]) -> None:
    """Split combined p50/p95/p99 line from postgres_tpcc.txt."""
    for k in list(fields.keys()):
        if k.startswith("p50_ms") and "p95_ms" in fields[k]:
            blob = f"{k}: {fields[k]}"
            for part in ("p50_ms", "p95_ms", "p99_ms"):
                m = re.search(rf"{part}:\s*([\d.]+)", blob)
                if m:
                    fields[part] = m.group(1)
            if "p50_ms" not in fields or fields["p50_ms"].startswith("p95"):
                m = re.search(r"p50_ms:\s*([\d.]+)", blob)
                if m:
                    fields["p50_ms"] = m.group(1)


def extract_validation_pg(log: str) -> dict:
    out: dict = {}
    for raw in log.splitlines():
        s = clean(raw)
        if not s.startswith('"'):
            continue
        for key in (
            "postgres_txns_per_s",
            "pg_payment_p95_ms",
            "pg_txns_per_s_min",
        ):
            if f'"{key}"' in s:
                m = re.search(rf'"{key}":\s*([\d.]+)', s)
                if m:
                    out[key] = float(m.group(1))
    return out


def parse_fields(block: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in block.splitlines():
        if ":" not in line:
            continue
        k, _, v = line.partition(":")
        out[k.strip()] = v.strip()
    return out


def kind_p95(rows: list[str], kind: str) -> float | None:
    for row in rows:
        if row.startswith(kind + " "):
            parts = row.split()
            if len(parts) >= 4:
                return float(parts[3])
    return None


def main() -> None:
    ids = [int(x) for x in sys.argv[1:]] if len(sys.argv) > 1 else [r[1] for r in RUNS]
    label_map = {rid: lbl for lbl, rid in RUNS}

    print("PostgreSQL baseline comparison (first block in CI job, before RustDB)\n")
    hdr = f"{'run':26} {'TPS':>6} {'tpmC':>7} {'elapsed':>8} {'succ%':>6} {'p50':>6} {'p95':>7} {'pay_p95':>8} {'new_ord':>8}"
    print(hdr)
    print("-" * len(hdr))

    details: list[tuple[str, dict, list[str], dict]] = []
    for rid in ids:
        log = job_log(rid)
        block = extract_pg_block(log)
        fields = parse_fields(block)
        parse_latency_line(fields)
        analyze = extract_pg_analyze(log)
        val = extract_validation_pg(log)
        label = label_map.get(rid, str(rid))[:26]
        tps = fields.get("txns_per_s (successful only)", "?")
        pay_p95 = kind_p95(analyze, "payment")
        if pay_p95 is None:
            pay_p95 = val.get("pg_payment_p95_ms")
        print(
            f"{label:26} {tps:>6} {fields.get('tpmC', '?'):>7} "
            f"{fields.get('elapsed_s', '?'):>8} {fields.get('success_rate_pct', '?'):>6} "
            f"{fields.get('p50_ms', '?'):>6} {fields.get('p95_ms', '?'):>7} "
            f"{str(pay_p95 or '?'):>8} {fields.get('new_orders (successful only)', '?'):>8}"
        )
        details.append((label, fields, analyze, val))

    print("\nPer-kind p50 (ms) from analyze_tpcc_txn_log (PostgreSQL full mix):\n")
    kinds = ["new_order", "payment", "order_status", "delivery", "stock_level"]
    print(f"{'kind':14} " + " ".join(f"{d[0][:12]:>12}" for d in details))
    for kind in kinds:
        cells = []
        for _, _, analyze, _ in details:
            v = None
            for row in analyze:
                if row.startswith(kind + " "):
                    v = row.split()[2]
            cells.append(f"{v or '-':>12}")
        print(f"{kind:14} " + " ".join(cells))

    print("\norder_status micro (60s, PG only):\n")
    for rid in ids:
        log = job_log(rid)
        label = label_map.get(rid, str(rid))[:26]
        micro_tps = re.findall(
            r"txns_per_s \(successful only\): ([\d.]+)", log
        )
        # full mix = first two; micro adds 3rd and 4th if present
        if len(micro_tps) >= 4:
            pg_micro = float(micro_tps[2])
            rd_micro = float(micro_tps[3])
            print(
                f"  {label}: PG micro TPS={pg_micro:.0f}  "
                f"(full mix PG={micro_tps[0]})"
            )
        m = re.search(
            r"order_status p50_ms: postgres=([\d.]+) rustdb=([\d.]+) ratio=([\d.]+)%",
            log,
        )
        if m:
            print(
                f"    order_status p50: PG={m.group(1)}ms RustDB={m.group(2)}ms "
                f"(ratio {m.group(3)}%)"
            )

    for label, fields, analyze, val in details:
        if "post #95 merge" in label or "PR #95 CI" in label:
            print(f"--- {label} ---")
            for k in [
                "txn_attempts",
                "txn_successes",
                "success_rate_pct",
                "elapsed_s",
                "txns_per_s (successful only)",
                "attempts_per_s (all tries)",
                "new_orders (successful only)",
                "tpmC",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "failed_attempts",
            ]:
                if k in fields:
                    print(f"  {k}: {fields[k]}")
            if val:
                print(f"  validation pg_payment_p95_ms: {val.get('pg_payment_p95_ms')}")
            print()


if __name__ == "__main__":
    main()
