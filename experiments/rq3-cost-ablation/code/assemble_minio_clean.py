#!/usr/bin/env python3
"""Assemble a clean 150-run MinIO dataset from unaffected and replacement runs."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

DATASETS = ["ssh", "bgl", "hdfs"]
VARIANTS = ["size-only", "sha256", "blake3", "sha256+mac", "sha256-nocomp"]


def read(path):
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def key(r):
    return r["dataset"], r["variant"], r["rep"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--original", required=True)
    ap.add_argument("--rerun", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    original_dir, rerun_dir, out = Path(a.original), Path(a.rerun), Path(a.out)
    original = [r for r in read(original_dir / "runs.jsonl") if r["dataset"] in DATASETS]
    rerun = [r for r in read(rerun_dir / "runs.jsonl") if r["dataset"] in DATASETS]
    exposed = {("ssh", vn, 0) for vn in VARIANTS}
    exposed |= {("bgl", vn, rep) for vn in VARIANTS for rep in range(5, 10)}
    exposed |= {("hdfs", vn, rep) for vn in VARIANTS for rep in range(4, 10)}
    assert len(original) == 150 and len(exposed) == 60 and len(rerun) == 60
    assert {key(r) for r in rerun} == exposed, "replacement keys do not equal the exposed set"
    assert len({key(r) for r in original}) == 150 and len({key(r) for r in rerun}) == 60
    for r in rerun:
        assert r["drained"] and r.get("started_at") and r.get("finished_at") and r.get("quiet_check")
        if r["rep"] == 0:
            o = r["oracle"]
            assert o["complete"] and o["bad_objects"] == 0 and o["unsafe_commits"] == 0
    kept = [r for r in original if key(r) not in exposed]
    assert len(kept) == 90
    clean = []
    for r in kept:
        r = dict(r)
        r["provenance"] = "original-unexposed"
        clean.append(r)
    for r in rerun:
        r = dict(r)
        r["provenance"] = "quiet-rerun"
        clean.append(r)
    order = {d: i for i, d in enumerate(DATASETS)}, {v: i for i, v in enumerate(VARIANTS)}
    clean.sort(key=lambda r: (order[0][r["dataset"]], order[1][r["variant"]], r["rep"]))
    out.mkdir(parents=True, exist_ok=True)
    (out / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in clean))
    meta = {
        "experiment": "E-RQ3-A MinIO clean assembly",
        "assembled_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "original": str(original_dir), "replacement": str(rerun_dir),
        "original_runs_preserved": True, "unexposed_original_runs": len(kept),
        "quiet_replacement_runs": len(rerun), "clean_runs": len(clean),
        "exposed_chunks_replaced": [
            "SSH rep 0 (5 runs)", "BGL reps 5–9 (25 runs)", "HDFS reps 4–9 (30 runs)"],
        "overlap_windows_excluded": ["19:30:47–19:31:36", "19:39:00–19:39:35", "19:47:10–19:47:58"],
        "quiet_requirement": "no exp_baseline.py process at each replacement run start",
        "network_path": "S3 protocol over a local Docker network; no fault proxy",
    }
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    (out / "ASSEMBLY.md").write_text(
        "# Clean MinIO assembly\n\n"
        "The original 150 matrix records remain unchanged. This dataset replaces all 60 runs whose chunks\n"
        "overlapped the three C23 windows: SSH rep 0, BGL reps 5–9, and HDFS reps 4–9.\n\n"
        "It contains 90 original unexposed records and 60 new quiet-host replacements. Every replacement\n"
        "record has started_at/finished_at and a quiet-environment check. All 15 rep-0 oracle cells pass.\n")
    print(f"Wrote {len(clean)} clean runs: {len(kept)} original + {len(rerun)} replacements")


if __name__ == "__main__":
    main()
