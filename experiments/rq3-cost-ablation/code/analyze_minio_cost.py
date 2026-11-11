#!/usr/bin/env python3
"""Validate and summarize the networked MinIO RQ3 campaign."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

import numpy as np

B = 10_000
VARIANTS = ["size-only", "sha256", "blake3", "sha256+mac", "sha256-nocomp"]
DATASETS = ["ssh", "bgl", "hdfs"]


def ci(values, seed, other=None):
    x = np.asarray(values, dtype=float)
    y = None if other is None else np.asarray(other, dtype=float)
    rng = np.random.default_rng(seed)
    if y is None:
        samples = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
        center = float(np.median(x))
    else:
        xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
        ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
        samples = xs - ys
        center = float(np.median(x) - np.median(y))
    return [round(center, 4), round(float(np.quantile(samples, 0.025)), 4),
            round(float(np.quantile(samples, 0.975)), 4)]


def pct_ci(values, baseline, seed):
    x = np.asarray(values, dtype=float)
    y = np.asarray(baseline, dtype=float)
    rng = np.random.default_rng(seed)
    xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
    ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
    samples = 100 * (xs - ys) / ys
    center = 100 * (float(np.median(x)) - float(np.median(y))) / float(np.median(y))
    return [round(center, 4), round(float(np.quantile(samples, 0.025)), 4),
            round(float(np.quantile(samples, 0.975)), 4)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("result_dir")
    a = ap.parse_args()
    out = Path(a.result_dir)
    rows = [json.loads(x) for x in (out / "runs.jsonl").read_text().splitlines() if x.strip()]
    matrix = [r for r in rows if r["dataset"] in DATASETS]
    assert len(matrix) == 150, f"expected 150 matrix runs, got {len(matrix)}"
    assert all(r["drained"] for r in matrix), "a matrix run did not drain"
    keys = {(r["dataset"], r["variant"], r["rep"]) for r in matrix}
    assert len(keys) == 150, "duplicate dataset/variant/repetition key"
    for ds in DATASETS:
        for vn in VARIANTS:
            assert sum(1 for r in matrix if r["dataset"] == ds and r["variant"] == vn) == 10, (ds, vn)
            r0 = next(r for r in matrix if r["dataset"] == ds and r["variant"] == vn and r["rep"] == 0)
            o = r0["oracle"]
            assert o["complete"] and o["bad_objects"] == 0 and o["unsafe_commits"] == 0, (ds, vn, o)
    cells = defaultdict(list)
    for r in matrix:
        cells[(r["dataset"], r["variant"])].append(r)
    summary = {"experiment": "E-RQ3-A networked MinIO", "matrix_runs": len(matrix),
               "oracle_runs": sum(1 for r in matrix if "oracle" in r), "cells": {}}
    table_rows = []
    for ds in DATASETS:
        summary["cells"][ds] = {}
        for vn in VARIANTS:
            rs = cells[(ds, vn)]
            throughput = [r["mib_per_s"] for r in rs]
            base = [r["mib_per_s"] for r in cells[(ds, "size-only")]]
            per_batch = [r["sum_total_ms"] / r["batches"] for r in rs]
            cpu = [r["cpu_s"] / (r["steady_bytes"] / 2**30) for r in rs]
            entry = {
                "runs": len(rs),
                "throughput_mib_s": ci(throughput, 20261111 + len(table_rows)),
                "total_ms_per_batch": ci(per_batch, 20261211 + len(table_rows)),
                "cpu_s_per_gib": ci(cpu, 20261311 + len(table_rows)),
                "stored_object_mib": round(float(np.median([r["stored_object_bytes"] for r in rs])) / 2**20, 3),
                "readback_object_mib": round(float(np.median([r["stored_object_bytes"] for r in rs])) / 2**20, 3) if vn == "blake3" else 0.0,
                "manifest_mib": round(float(np.median([r["manifest_bytes"] for r in rs])) / 2**20, 4),
            }
            drop = [0.0, 0.0, 0.0] if vn == "size-only" else pct_ci(throughput, base, 20261411 + len(table_rows))
            entry["throughput_drop_vs_size_only_pct"] = drop
            if vn == "sha256+mac":
                sha = [r["mib_per_s"] for r in cells[(ds, "sha256")]]
                entry["throughput_drop_vs_sha256_pct"] = pct_ci(throughput, sha, 20261511)
            summary["cells"][ds][vn] = entry
            table_rows.append({
                "dataset": ds, "variant": vn, "runs": len(rs),
                "throughput_median": entry["throughput_mib_s"][0],
                "throughput_lo": entry["throughput_mib_s"][1], "throughput_hi": entry["throughput_mib_s"][2],
                "drop_pct": entry["throughput_drop_vs_size_only_pct"][0],
                "drop_lo": entry["throughput_drop_vs_size_only_pct"][1],
                "drop_hi": entry["throughput_drop_vs_size_only_pct"][2],
                "total_ms_per_batch": entry["total_ms_per_batch"][0],
                "cpu_s_per_gib": entry["cpu_s_per_gib"][0],
                "readback_object_mib": entry["readback_object_mib"],
            })
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    with open(out / "rq3_minio_cost.csv", "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(table_rows[0]))
        writer.writeheader()
        writer.writerows(table_rows)
    lines = [
        "# Networked MinIO verification-cost analysis", "",
        "150 matrix runs: 3 datasets × 5 variants × 10 repetitions. VTOP v0.7.0, native S3,",
        "host to Docker MinIO with no fault proxy and no injected faults. Every run drained.",
        "The first repetition of every cell passed the independent S3 byte-exact oracle (15/15).", "",
        "Throughput is the median steady-state MiB/s with a percentile-bootstrap 95% CI.",
        "The drop is 100 × (variant median − size-only median) / size-only median,",
        "with a bootstrap CI on the difference of medians; negative values are slowdowns. BLAKE3 read-back bytes are the",
        "stored object bytes; service SHA-256 does not download the object body for verification.", "",
        "| Dataset | Variant | MiB/s [95% CI] | Change vs size-only [95% CI] | Total ms/batch | CPU s/GiB | Read-back MiB |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    metadata = json.loads((out / "run.json").read_text()) if (out / "run.json").exists() else {}
    if metadata.get("quiet_replacement_runs"):
        lines[4:4] = [
            "Clean assembly: 90 original unexposed runs + 60 quiet-host replacement runs.",
            "The original 150 records are preserved; all three overlap-window chunks were replaced.",
            "Replacement records include per-run timestamps and quiet-environment checks.", ""]
    for ds in DATASETS:
        for vn in VARIANTS:
            e = summary["cells"][ds][vn]
            t, d = e["throughput_mib_s"], e["throughput_drop_vs_size_only_pct"]
            lines.append(f"| {ds.upper()} | {vn} | {t[0]:.2f} [{t[1]:.2f}, {t[2]:.2f}] | "
                         f"{d[0]:.2f}% [{d[1]:.2f}, {d[2]:.2f}] | {e['total_ms_per_batch'][0]:.2f} | "
                         f"{e['cpu_s_per_gib'][0]:.3f} | {e['readback_object_mib']:.3f} |")
    lines += ["", "## Interpretation constraints", "",
              "- Native-S3 detailed stage fields can be zero in v0.7.0; zero is not treated as a measured stage cost.",
              "- These are host-to-Docker MinIO measurements, not public-cloud S3 or a distributed deployment.",
              "- HTTP request counts were not observed and are not inferred.",
              "- Throughput changes are reported only with their confidence intervals; unresolved differences are not claimed as resolved effects."]
    (out / "ANALYSIS.md").write_text("\n".join(lines) + "\n")
    print(f"Validated {len(matrix)} matrix runs and 15 oracle cells; wrote summary.json, CSV, and ANALYSIS.md")


if __name__ == "__main__":
    main()
