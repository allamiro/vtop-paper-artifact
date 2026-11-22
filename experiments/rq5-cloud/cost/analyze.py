#!/usr/bin/env python3
"""Validate and summarize an E-RQ5-COST result directory (H3.5).

Per dataset: median steady throughput per mode with a percentile-bootstrap 95% CI, and the difference of
medians against size-only (MiB/s and %) with a bootstrap CI. The CI functions are imported from
research/experiments/rq3-cost-ablation/code/analyze_minio_cost.py, so the method is the one used for the
local MinIO campaign (B = 10,000 resamples of each group, numpy default_rng, fixed seeds).

  python analyze.py results/<run_id> [--expect-reps 10]

Writes summary.json, cost.csv and ANALYSIS.md into the result directory. Refuses (assertion) if the matrix
is incomplete, a run did not drain, or any run failed the oracle or the evidence checks, unless
--allow-failures (then failed runs are listed and excluded from the statistics).
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "rq3-cost-ablation" / "code"))
from analyze_minio_cost import B, ci, pct_ci  # noqa: E402  same bootstrap as the MinIO campaign

DATASETS = ["ssh", "bgl", "hdfs"]
MODES = ["size-only", "sha256-store", "blake3-readback"]
MiB, GiB = 2**20, 2**30


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(float(statistics.median(xs)), 4) if xs else None


def maxof(rs, key):
    xs = [r.get("sys", {}).get(key) for r in rs]
    xs = [x for x in xs if x is not None]
    return max(xs) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("result_dir")
    ap.add_argument("--expect-reps", type=int, default=10)
    ap.add_argument("--allow-failures", action="store_true")
    a = ap.parse_args()
    out = Path(a.result_dir)
    info = json.loads((out / "run.json").read_text())
    rows = [json.loads(x) for x in (out / "runs.jsonl").read_text().splitlines() if x.strip()]
    datasets = [d for d in DATASETS if d in info["matrix"]["datasets"]]
    modes = [m for m in MODES if m in info["matrix"]["modes"]]
    keys = [(r["dataset"], r["mode"], r["rep"]) for r in rows]
    assert len(keys) == len(set(keys)), "duplicate dataset/mode/rep"
    failed = [r for r in rows if not r.get("pass")]
    if failed and not a.allow_failures:
        raise SystemExit("failed runs (use --allow-failures to summarize the rest): "
                         + "; ".join(f"{r['cell']}: {r.get('checks') or r.get('error')}" for r in failed))
    good = [r for r in rows if r.get("pass")]
    cells = defaultdict(list)
    for r in good:
        cells[(r["dataset"], r["mode"])].append(r)
    for ds in datasets:
        for m in modes:
            n = len(cells[(ds, m)])
            if not a.allow_failures:
                assert n == a.expect_reps, f"{ds}/{m}: {n} passing runs, expected {a.expect_reps}"

    summary = {"experiment": info.get("experiment"), "run_id": info.get("run_id"),
               "instance_type": info.get("instance_type"), "availability_zone": info.get("availability_zone"),
               "vtop": info.get("vtop"), "aws_sdk_env": info.get("aws_sdk_env"),
               "runs": len(rows), "passing_runs": len(good), "failed_runs": [r["cell"] for r in failed],
               "bootstrap_resamples": B, "cells": {}}
    table, k = [], 0
    for ds in datasets:
        summary["cells"][ds] = {}
        base = [r["mib_per_s"] for r in cells[(ds, "size-only")]]
        base_e2e = [r["mib_per_s_end_to_end"] for r in cells[(ds, "size-only")]]
        for m in modes:
            rs = cells[(ds, m)]
            if not rs:
                continue
            thr = [r["mib_per_s"] for r in rs]
            e2e = [r["mib_per_s_end_to_end"] for r in rs]
            led = [r["ledger"] for r in rs]
            stage = defaultdict(list)
            for r in rs:
                nb = max(r["ledger"]["committed_batches"], 1)
                for key, v in r["engine_log"]["stage_ms_sums"].items():
                    stage[key].append(v / nb)
            e = {
                "runs": len(rs),
                "throughput_mib_s": ci(thr, 20261121 + k),
                "end_to_end_mib_s": ci(e2e, 20261221 + k),
                "cpu_s_per_gib": ci([r["cpu_s"] / (r["bytes"] / GiB) for r in rs], 20261321 + k),
                "max_rss_mib_median": med([r["max_rss_mib"] for r in rs]),
                "wall_s_median": med([r["wall_s"] for r in rs]),
                "batches_median": med([x["committed_batches"] for x in led]),
                "batch_uncompressed_bytes": {"median": med([x["batch_uncompressed_median"] for x in led]),
                                             "max": max(x["batch_uncompressed_max"] for x in led),
                                             "runs_with_batch_over_max_bytes":
                                                 sum(1 for x in led if x["batches_over_max_bytes"])},
                "object_bytes_max": max(x["object_bytes_max"] for x in led),
                "stored_object_mib_median": round(med([x["object_bytes_total"] for x in led]) / MiB, 3),
                "readback_mib_median": round(med([x["object_bytes_total"] for x in led]) / MiB, 3)
                if m == "blake3-readback" else 0.0,
                "multipart_objects_total": sum(r["oracle"]["multipart_objects"] or 0 for r in rs),
                "methods_seen": sorted({x for r in rs for x in r["oracle"]["receipt_methods"] or []}),
                "per_batch_ms_median": {key: med(v) for key, v in sorted(stage.items())},
                # Linux counters (cost.py counters()); None = not measured on this host, not zero
                "cpu_steal_pct_max": maxof(rs, "cpu_steal_pct"),
                "engine_disk_read_mib_max": None if maxof(rs, "disk_read_bytes") is None
                else round(maxof(rs, "disk_read_bytes") / MiB, 1),
                "ena_allowance_exceeded": None if not any(kk.endswith("allowance_exceeded")
                                                          for r in rs for kk in r.get("sys", {}))
                else sum(v for r in rs for kk, v in r["sys"].items() if kk.endswith("allowance_exceeded")),
                "drained_by_shutdown_flush": sum(1 for r in rs if r.get("drained_after_stop") and not r["drained"]),
            }
            if m == "size-only":
                e["diff_vs_size_only_mib_s"] = e["change_vs_size_only_pct"] = [0.0, 0.0, 0.0]
                e["e2e_change_vs_size_only_pct"] = [0.0, 0.0, 0.0]
            else:
                e["diff_vs_size_only_mib_s"] = ci(thr, 20261421 + k, other=base)
                e["change_vs_size_only_pct"] = pct_ci(thr, base, 20261521 + k)
                e["e2e_change_vs_size_only_pct"] = pct_ci(e2e, base_e2e, 20261621 + k)
                lo, hi = e["diff_vs_size_only_mib_s"][1:]
                e["ci_resolution"] = ("includes 0 (no resolved difference)" if lo <= 0 <= hi
                                      else "below 0 (resolved slowdown)" if hi < 0 else "above 0 (resolved speed-up)")
            summary["cells"][ds][m] = e
            t, d, p = e["throughput_mib_s"], e["diff_vs_size_only_mib_s"], e["change_vs_size_only_pct"]
            table.append({"dataset": ds, "mode": m, "runs": len(rs),
                          "mib_s": t[0], "mib_s_lo": t[1], "mib_s_hi": t[2],
                          "diff_mib_s": d[0], "diff_lo": d[1], "diff_hi": d[2],
                          "change_pct": p[0], "change_lo": p[1], "change_hi": p[2],
                          "cpu_s_per_gib": e["cpu_s_per_gib"][0], "batches": e["batches_median"],
                          "batch_mib_median": round(e["batch_uncompressed_bytes"]["median"] / MiB, 3),
                          "readback_mib": e["readback_mib_median"]})
            k += 1
    # H3.5's prediction, stated per dataset as CI facts (the answer is written in hypotheses.md by the author)
    pred = {}
    for ds in datasets:
        c = summary["cells"][ds]
        s, b = c.get("sha256-store", {}).get("diff_vs_size_only_mib_s"), c.get("blake3-readback", {}).get(
            "diff_vs_size_only_mib_s")
        pred[ds] = {"store_ci_includes_0": bool(s and s[1] <= 0 <= s[2]),
                    "blake3_ci_below_0": bool(b and b[2] < 0)}
    summary["h35_prediction_by_dataset"] = pred
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    with open(out / "cost.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(table[0]))
        w.writeheader()
        w.writerows(table)
    v = info.get("vtop", {})
    lines = [
        "# E-RQ5-COST: verification cost on EC2 + Amazon S3", "",
        f"Run `{info.get('run_id')}`: {info.get('instance_type')} in {info.get('availability_zone')}, AMI "
        f"`{info.get('ami')}`; vtop-engine {v.get('version')} ({v.get('commit')}), archive SHA-256 "
        f"`{v.get('tarball_sha256')}`. AWS_REQUEST_CHECKSUM_CALCULATION="
        f"`{(info.get('aws_sdk_env') or {}).get('AWS_REQUEST_CHECKSUM_CALCULATION')}`.",
        f"{len(rows)} runs ({len(good)} passing: drained, oracle byte-exact, log/receipt/ledger/HEAD agree, "
        f"single-PUT); order shuffled with seed {info['matrix']['seed']}.", "",
        "Throughput is the median steady-state MiB/s (bytes at the second-to-last commit over its time) with a",
        f"percentile-bootstrap 95% CI (B = {B}). The difference is the variant median minus the size-only median,",
        "with a bootstrap CI on the difference of medians; negative values are slowdowns. Batch sizes are measured",
        "from the ledger (uncompressed bytes per committed batch).", "",
        "| Dataset | Mode | MiB/s [95% CI] | Diff vs size-only, MiB/s [95% CI] | Change % [95% CI] | CPU s/GiB | "
        "Batches | Median batch MiB | Read-back MiB |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for t in table:
        lines.append(f"| {t['dataset'].upper()} | {t['mode']} | {t['mib_s']:.2f} [{t['mib_s_lo']:.2f}, "
                     f"{t['mib_s_hi']:.2f}] | {t['diff_mib_s']:.2f} [{t['diff_lo']:.2f}, {t['diff_hi']:.2f}] | "
                     f"{t['change_pct']:.2f}% [{t['change_lo']:.2f}, {t['change_hi']:.2f}] | "
                     f"{t['cpu_s_per_gib']:.3f} | {t['batches']:g} | {t['batch_mib_median']:.2f} | "
                     f"{t['readback_mib']:.1f} |")
    lines += ["", "## H3.5 prediction, per dataset (CI facts only)", ""]
    for ds, p in pred.items():
        lines.append(f"- {ds.upper()}: store SHA-256 CI includes 0: {p['store_ci_includes_0']}; "
                     f"BLAKE3 read-back CI below 0: {p['blake3_ci_below_0']}")
    lines += ["", "## Environment checks", ""]
    for ds in datasets:
        for m in modes:
            e = summary["cells"][ds].get(m)
            if e:
                nm = lambda x, unit="": "not measured" if x is None else f"{x}{unit}"  # noqa: E731
                bb = e["batch_uncompressed_bytes"]
                lines.append(f"- {ds}/{m}: max CPU steal {nm(e['cpu_steal_pct_max'], '%')}, max engine disk read "
                             f"{nm(e['engine_disk_read_mib_max'], ' MiB')}, ENA allowance-exceeded events "
                             f"{nm(e['ena_allowance_exceeded'])}, max batch {bb['max']} B "
                             f"({bb['runs_with_batch_over_max_bytes']} runs over max_bytes), per-batch stage ms "
                             f"{e['per_batch_ms_median'] or 'not logged'}")
    lines += ["", "## Interpretation constraints", "",
              "- One instance, one AZ, one time window; not a distributed deployment.",
              "- Stage timings are reported only if the engine logged them (v0.10.2 #843); absent is not zero.",
              "- Unresolved differences (CI includes 0) are not claimed as effects."]
    if failed:
        lines += ["", "## Failed runs (excluded)", ""] + [f"- {r['cell']}: {r.get('checks') or r.get('error')}"
                                                         for r in failed]
    (out / "ANALYSIS.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
