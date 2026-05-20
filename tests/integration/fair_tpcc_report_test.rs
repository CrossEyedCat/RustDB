//! Schema check for aggregate fair_compare/report.json (fixture only; no 300s bench).

use std::path::PathBuf;

fn fixture_report_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fair_compare/report.json")
}

#[test]
fn fair_tpcc_report_fixture_schema() {
    let path = fixture_report_path();
    assert!(
        path.is_file(),
        "missing fixture {}",
        path.display()
    );
    let raw = std::fs::read_to_string(&path).expect("read report.json");
    let v: serde_json::Value = serde_json::from_str(&raw).expect("parse report.json");

    for key in ["runs", "bench", "strict", "interpretation"] {
        assert!(v.get(key).is_some(), "missing top-level key {key}");
    }

    let runs = v["runs"].as_u64().expect("runs number");
    assert!(runs >= 1, "runs must be >= 1");

    for mode in ["bench", "strict"] {
        let m = &v[mode];
        for key in [
            "valid_runs",
            "runs_attempted",
            "ratio_median_pct",
            "claim_faster_than_pg",
        ] {
            assert!(m.get(key).is_some(), "{mode} missing {key}");
        }
        assert!(m["valid_runs"].is_u64());
        assert!(m["claim_faster_than_pg"].is_boolean());
    }

    let interp = &v["interpretation"];
    assert!(interp.get("bench_win").and_then(|x| x.as_str()).is_some());
    assert!(interp.get("strict_win").and_then(|x| x.as_str()).is_some());
}
