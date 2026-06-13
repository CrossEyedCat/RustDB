#!/usr/bin/env python3
import json
import re
import subprocess
import sys

PG_REF = 1380.0


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


def parse_run(run_id: int) -> dict:
    log = job_log(run_id)
    tps = re.findall(r"txns_per_s \(successful only\): ([\d.]+)", log)[:2]
    tpmc = re.findall(r"tpmC: ([\d.]+)", log)[:2]
    pg_tps, rd_tps = float(tps[0]), float(tps[1])
    pg_tpmc, rd_tpmc = float(tpmc[0]), float(tpmc[1])

    val: dict = {}
    for raw in log.splitlines():
        s = re.sub(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s*", "", raw).strip()
        s = re.sub(r"\x1b\[[0-9;]*m", "", s)
        if not s.startswith('"'):
            continue
        m = re.match(r'"([^"]+)":\s*(.+?),?\s*$', s)
        if not m:
            continue
        k, v = m.group(1), m.group(2).strip().rstrip(",")
        if v in ("true", "false"):
            val[k] = v == "true"
        else:
            try:
                val[k] = float(v)
            except ValueError:
                val[k] = v.strip('"')

    micro = re.search(
        r"order_status p50_ms: postgres=([\d.]+) rustdb=([\d.]+) ratio=([\d.]+)%",
        log,
    )
    warns = [
        re.sub(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s*", "", x).strip()
        for x in log.splitlines()
        if "::warning::" in x and "${" not in x and "echo " not in x
    ]

    raw = 100 * rd_tps / pg_tps
    ref = val.get("ratio_percent_vs_pg_reference") or 100 * rd_tps / max(pg_tps, PG_REF)
    return {
        "run_id": run_id,
        "pg_tps": pg_tps,
        "rd_tps": rd_tps,
        "pg_tpmc": pg_tpmc,
        "rd_tpmc": rd_tpmc,
        "ratio_raw": raw,
        "ratio_ref": ref,
        "valid": val.get("valid"),
        "claim": val.get("claim_faster_than_pg"),
        "micro": micro.groups() if micro else None,
        "warns": warns,
    }


def main() -> None:
    ids = [int(x) for x in sys.argv[1:]] or [26855622083]
    labels = {
        26853935022: "PR #95 CI",
        26855622083: "main post #95 merge",
        26852962238: "PR #94 merge CI",
    }
    for rid in ids:
        d = parse_run(rid)
        label = labels.get(rid, str(rid))
        print(f"=== {label} ({rid}) ===")
        print(
            f"RustDB TPS={d['rd_tps']:.0f}  PG TPS={d['pg_tps']:.0f}  "
            f"raw={d['ratio_raw']:.1f}%  vs_ref={d['ratio_ref']:.1f}%"
        )
        print(f"tpmC RustDB={d['rd_tpmc']:.0f}  PG={d['pg_tpmc']:.0f}")
        print(f"valid={d['valid']}  claim_faster_than_pg={d['claim']}")
        if d["micro"]:
            print(
                f"order_status micro: PG p50={d['micro'][0]}ms  "
                f"RustDB p50={d['micro'][1]}ms  ratio={d['micro'][2]}%"
            )
        for w in d["warns"]:
            print(f"WARN: {w[:200]}")
        print()


if __name__ == "__main__":
    main()
