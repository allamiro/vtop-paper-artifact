#!/usr/bin/env python3
"""Validate and summarize E-RQ5-WAN result directories (one per region; run locally).

  python analyze.py results/<run_eu> results/<run_syd> ... [--out DIR]
                    [--baseline-runs ../cost/results/<run_id>/runs.jsonl] [--allow-failures]

Safety first: any run with an unsafe commit (oracle) makes the script fail, always. A run that failed another
check makes it fail unless --allow-failures (then it is listed and left out of the statistics). Tables:
link (measured RTT), native link per mode (median end-to-end MiB/s with a percentile-bootstrap 95% CI, change vs
size-only, and with --baseline-runs the change vs the us-east-1 E-RQ5-COST SSH runs = the distance effect), kernel
shaping vs VTOP's egress cap, loss/jitter, partition recovery, SIGKILL restart, policed uplink and its control.
The CI functions are imported from rq3-cost-ablation/code/analyze_minio_cost.py (B = 10,000), as cost/analyze.py.
Writes summary.json, wan.csv and ANALYSIS.md to --out (default: the first result directory).
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "rq3-cost-ablation" / "code"))
from analyze_minio_cost import B, ci, pct_ci  # noqa: E402

MODES = ["size-only", "sha256-store", "blake3-readback"]
BW = ["tc10", "cap10", "tc2", "cap2"]
LOSS = ["loss1", "loss5", "jitter"]
MBIT = {"tc10": 10, "cap10": 10, "tc2": 2, "cap2": 2}


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(float(statistics.median(xs)), 3) if xs else None


def fmt(x, nd=2):
    return "–" if x is None else (f"{x:.{nd}f}" if isinstance(x, float) else str(x))


def fci(c, nd=2):
    return "–" if not c else f"{c[0]:.{nd}f} [{c[1]:.{nd}f}, {c[2]:.{nd}f}]"


def load(d: Path):
    info = json.loads((d / "run.json").read_text())
    rows = [json.loads(x) for x in (d / "runs.jsonl").read_text().splitlines() if x.strip()]
    return info, rows


def retries(r):
    w = r.get("engine_log_wan", {})
    k = w.get("key_messages", {})
    return {"sdk_retry_attempts": w.get("sdk_retry_attempts"), "upload_throttled": w.get("upload_throttled"),
            "batch_failed": k.get("batch failed", 0),  # one per failed batch ("did not reach VERIFIED" follows it)
            "replay_required": k.get("recovered: REPLAY_REQUIRED", 0)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("result_dirs", nargs="+")
    ap.add_argument("--out", default=None)
    ap.add_argument("--baseline-runs", default=None, help="E-RQ5-COST runs.jsonl (us-east-1) for the SSH baseline")
    ap.add_argument("--allow-failures", action="store_true")
    a = ap.parse_args()
    out = Path(a.out or a.result_dirs[0])
    out.mkdir(parents=True, exist_ok=True)
    regions, allrows, unsafe, failed = {}, [], [], []
    for d in map(Path, a.result_dirs):
        info, rows = load(d)
        reg = info.get("region") or d.name
        regions[reg] = info
        for r in rows:
            r["_region"] = reg
            allrows.append(r)
            if not r.get("checks", {}).get("no_unsafe_commit", True) or (r.get("oracle") or {}).get("unsafe_commits"):
                unsafe.append(f"{reg}/{r['cell']}")
            if not r.get("pass"):
                failed.append(f"{reg}/{r['cell']}: {r.get('error') or [k for k, v in r.get('checks', {}).items() if not v]}")
    if unsafe:
        raise SystemExit("UNSAFE COMMIT(S): " + "; ".join(unsafe))
    if failed and not a.allow_failures:
        raise SystemExit("failed runs (use --allow-failures to summarize the rest): " + "; ".join(failed))
    good = [r for r in allrows if r.get("pass")]
    by = defaultdict(list)
    for r in good:
        by[(r["_region"], r["condition"], r["mode"])].append(r)
    base = {}
    if a.baseline_runs:
        for x in Path(a.baseline_runs).read_text().splitlines():
            if x.strip():
                b = json.loads(x)
                if b.get("dataset") == "ssh" and b.get("pass"):
                    base.setdefault(b["mode"], []).append(b["mib_per_s_end_to_end"])

    S = {"regions": {}, "bootstrap_resamples": B, "failed_runs": failed, "baseline_runs": a.baseline_runs}
    L = ["# E-RQ5-WAN: VTOP over real long-distance links to Amazon S3 (us-east-1)", ""]
    k = 0
    # ---- link
    L += ["## Link (measured from the engine VM)", "",
          "| Region | Instance, AZ | TCP connect RTT to the engine's S3 endpoint, median ms (start / end) | "
          "HeadBucket median ms | Native PUT / GET Mbit/s (2 MiB, one stream) | vtop-engine |",
          "|---|---|---:|---:|---:|---|"]
    for reg, info in regions.items():
        rs, re_ = info.get("rtt_start", {}), info.get("rtt_end", {})
        t0 = (rs.get("tcp_connect") or [{}])[0].get("median_ms")
        t1 = (re_.get("tcp_connect") or [{}])[0].get("median_ms")
        cal = (info.get("calibration") or {}).get("native", {})
        v = info.get("vtop", {})
        per_run = [r.get("rtt_before", {}).get("median_ms") for r in allrows if r["_region"] == reg]
        S["regions"][reg] = {"instance_type": info.get("instance_type"), "az": info.get("availability_zone"),
                             "rtt_ms_start": t0, "rtt_ms_end": t1, "rtt_ms_per_run_median": med(per_run),
                             "head_bucket_ms": (rs.get("head_bucket") or {}).get("median_ms"),
                             "native_put_get_mbit_s": [cal.get("put_mbit_s"), cal.get("get_mbit_s")],
                             "preflight": (info.get("network", {}).get("preflight") or {}).get("caps"),
                             "vtop": v.get("version"), "commit": v.get("commit")}
        L.append(f"| {reg} | {info.get('instance_type')}, {info.get('availability_zone')} | {fmt(t0)} / {fmt(t1)} "
                 f"(per run {fmt(med(per_run))}) | {fmt((rs.get('head_bucket') or {}).get('median_ms'))} | "
                 f"{fmt(cal.get('put_mbit_s'))} / {fmt(cal.get('get_mbit_s'))} | {v.get('version')} "
                 f"({str(v.get('commit'))[:10]}) |")
    # ---- native
    L += ["", "## Native link, SSH (73.4 MB), 16 MiB batches", "",
          "End-to-end MiB/s = input bytes / time until the ledger shows the last byte committed (median, "
          f"bootstrap 95% CI, B = {B}). Change vs size-only: difference of medians in % with its CI.", "",
          "| Region | Mode | Runs | Drain s | MiB/s [95% CI] | vs size-only % [CI] | vs us-east-1 % [CI] | "
          "CPU s | Wire tx Mbit/s | SDK retries |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    table = []
    for reg in regions:
        S["regions"][reg]["native"] = {}
        b0 = [r["mib_per_s_end_to_end"] for r in by[(reg, "native", "size-only")]]
        for m in MODES:
            rs = by[(reg, "native", m)]
            if not rs:
                continue
            e2e = [r["mib_per_s_end_to_end"] for r in rs]
            e = {"runs": len(rs), "drain_s": med([r["drain_s"] for r in rs]), "mib_s": ci(e2e, 20261122 + k),
                 "cpu_s": med([r["cpu_s"] for r in rs]), "tx_mbit_s": med([r.get("wire_tx_mbit_s") for r in rs]),
                 "sdk_retries": sum(retries(r)["sdk_retry_attempts"] or 0 for r in rs)}
            if m != "size-only" and b0:
                e["vs_size_only_pct"] = pct_ci(e2e, b0, 20261222 + k)
            if base.get(m):
                e["vs_us_east_1_pct"] = pct_ci(e2e, base[m], 20261322 + k)
            k += 1
            S["regions"][reg]["native"][m] = e
            L.append(f"| {reg} | {m} | {e['runs']} | {fmt(e['drain_s'])} | {fci(e['mib_s'])} | "
                     f"{fci(e.get('vs_size_only_pct'), 1)} | {fci(e.get('vs_us_east_1_pct'), 1)} | {fmt(e['cpu_s'])} | "
                     f"{fmt(e['tx_mbit_s'])} | {e['sdk_retries']} |")
    # ---- bandwidth
    L += ["", "## Low bandwidth: kernel shaping (tc, both directions) vs VTOP's egress cap (uploads only)", "",
          "Upload body rate = object bytes / drain time; of cap = that rate in Mbit/s over the cap. Calibration = "
          "timed 2 MiB PUT/GET through the same tc shaping.", "",
          "| Region | Condition | Mode | Runs | Drain s | MiB/s | Upload body Mbit/s (of cap) | Wire tx / rx Mbit/s | "
          "Calibration PUT / GET Mbit/s | SDK retries |", "|---|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for reg, info in regions.items():
        S["regions"][reg]["bandwidth"] = {}
        for c in BW:
            cal = (info.get("calibration") or {}).get(c, {})
            for m in ("sha256-store", "blake3-readback"):
                rs = by[(reg, c, m)]
                if not rs:
                    continue
                body = [(r["ledger"].get("object_bytes_total") or 0) * 8 / 1e6 / r["drain_s"] for r in rs]
                e = {"runs": len(rs), "drain_s": med([r["drain_s"] for r in rs]),
                     "mib_s": med([r["mib_per_s_end_to_end"] for r in rs]), "upload_body_mbit_s": med(body),
                     "tx_mbit_s": med([r.get("wire_tx_mbit_s") for r in rs]),
                     "rx_mbit_s": med([r.get("wire_rx_mbit_s") for r in rs]),
                     "calibration": [cal.get("put_mbit_s"), cal.get("get_mbit_s")],
                     "ingress_method": (rs[0].get("shaping") or {}).get("ingress"),
                     "sdk_retries": sum(retries(r)["sdk_retry_attempts"] or 0 for r in rs)}
                e["of_cap"] = round(e["upload_body_mbit_s"] / MBIT[c], 3) if e["upload_body_mbit_s"] else None
                S["regions"][reg]["bandwidth"][f"{c}/{m}"] = e
                L.append(f"| {reg} | {c} | {m} | {e['runs']} | {fmt(e['drain_s'])} | {fmt(e['mib_s'])} | "
                         f"{fmt(e['upload_body_mbit_s'])} ({fmt(e['of_cap'])}) | {fmt(e['tx_mbit_s'])} / "
                         f"{fmt(e['rx_mbit_s'])} | {fmt(cal.get('put_mbit_s'))} / {fmt(cal.get('get_mbit_s'))} | "
                         f"{e['sdk_retries']} |")
    # ---- loss / jitter
    L += ["", "## Loss and jitter (netem on egress), store SHA-256", "",
          "| Region | Condition | Runs | Drain s | × native drain | MiB/s | SDK retries | Failed batches |",
          "|---|---|---:|---:|---:|---:|---:|---:|"]
    for reg in regions:
        S["regions"][reg]["loss"] = {}
        nat = med([r["drain_s"] for r in by[(reg, "native", "sha256-store")]])
        for c in LOSS:
            rs = by[(reg, c, "sha256-store")]
            if not rs:
                continue
            e = {"runs": len(rs), "drain_s": med([r["drain_s"] for r in rs]),
                 "mib_s": med([r["mib_per_s_end_to_end"] for r in rs]),
                 "sdk_retries": sum(retries(r)["sdk_retry_attempts"] or 0 for r in rs),
                 "failed_batches": sum(retries(r)["batch_failed"] for r in rs)}
            e["x_native"] = round(e["drain_s"] / nat, 2) if nat and e["drain_s"] else None
            S["regions"][reg]["loss"][c] = e
            L.append(f"| {reg} | {c} | {e['runs']} | {fmt(e['drain_s'])} | {fmt(e['x_native'])} | {fmt(e['mib_s'])} | "
                     f"{e['sdk_retries']} | {e['failed_batches']} |")
    # ---- faults
    L += ["", "## Partition (all egress but link-local and the VPC dropped for 60 s, mid-upload)", "",
          "| Region | Committed at cut / size | Commits during cut | First commit after restore s | Tx resumed s | "
          "Drain after restore s | SDK retries | Failed batches | Complete, unsafe |",
          "|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for reg in regions:
        for r in by[(reg, "partition", "sha256-store")]:
            p, rt = r.get("partition", {}), retries(r)
            S["regions"][reg]["partition"] = {**p, **rt, "committed_at_cut": r["events"].get("committed_at_trigger")}
            L.append(f"| {reg} | {r['events'].get('committed_at_trigger')} / {r['bytes']} | {p.get('commits_during')} | "
                     f"{fmt(p.get('first_commit_after_restore_s'))} | {fmt(p.get('tx_resumed_after_restore_s'))} | "
                     f"{fmt(p.get('drain_after_restore_s'))} | {rt['sdk_retry_attempts']} | {rt['batch_failed']} | "
                     f"{r['oracle'].get('complete')}, {r['oracle'].get('unsafe_commits')} |")
    L += ["", "## SIGKILL during upload, then restart", "",
          "| Region | Mode | Committed at kill / size | Restart to drain s | Duplicate bytes | Unreferenced objects | "
          "Committed rows sharing an object | Replays | Complete, unsafe |", "|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for reg in regions:
        S["regions"][reg]["sigkill"] = {}
        for m in ("sha256-store", "blake3-readback"):
            for r in by[(reg, "sigkill", m)]:
                o = r["oracle"]
                e = {"committed_at_kill": r["events"].get("committed_at_trigger"), **r.get("kill", {}),
                     "duplicate_bytes": o.get("duplicate_bytes"), "unreferenced_objects": o.get("unreferenced_data_objects"),
                     "rows_sharing_object": r.get("ledger_times", {}).get("committed_rows_sharing_an_object"),
                     "replays": retries(r)["replay_required"]}
                S["regions"][reg]["sigkill"][m] = e
                L.append(f"| {reg} | {m} | {e['committed_at_kill']} / {r['bytes']} | {fmt(e.get('restart_drain_s'))} | "
                         f"{e['duplicate_bytes']} | {e['unreferenced_objects']} | {e['rows_sharing_object']} | "
                         f"{e['replays']} | {o.get('complete')}, {o.get('unsafe_commits')} |")
    L += ["", "## Policed uplink (policer at the rate shown, 5,000 B bucket; 32 MiB SSH slice, uncompressed, multipart)", "",
          "Treatment = shipped deadline (300 s + body / 4,096 B/s per attempt, #663); control = fixed 120 s "
          "(request_min_bytes_per_second 10^9), 480 s window.", "",
          "| Region | Arm | Committed / size | Drain s | Longest seal-to-commit s | Wire tx Mbit/s | SDK retries | "
          "Failed batches | Unsafe |", "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for reg in regions:
        S["regions"][reg]["policed"] = {}
        for c in ("policed", "policed-fixed"):
            for r in by[(reg, c, "sha256-raw-mp")]:
                rt = retries(r)
                e = {"committed": r.get("committed_bytes_final"), "bytes": r["bytes"], "drain_s": r.get("drain_s"),
                     "seal_to_commit_max_s": r.get("ledger_times", {}).get("seal_to_commit_max_s"),
                     "tx_mbit_s": r.get("wire_tx_mbit_s"), **rt, "unsafe": r["oracle"].get("unsafe_commits"),
                     "old_deadline_exceeded": r.get("old_deadline_exceeded")}
                S["regions"][reg]["policed"][c] = e
                e["police_mbit"] = (r.get("condition_spec") or {}).get("police")
                L.append(f"| {reg} | {c} ({e['police_mbit']} Mbit/s) | {e['committed']} / {e['bytes']} | {fmt(e['drain_s'])} | "
                         f"{fmt(e['seal_to_commit_max_s'])} | {fmt(e['tx_mbit_s'])} | {rt['sdk_retry_attempts']} | "
                         f"{rt['batch_failed']} | {e['unsafe']} |")
                if c == "policed" and not e["old_deadline_exceeded"]:
                    L.append(f"| {reg} | note | no batch took > 300 s from seal to commit: here the treatment is "
                             "NOT a #663 regression check (the old fixed deadline would have passed too) | | | | | | |")
    for r in allrows:
        table.append({"region": r["_region"], "cell": r["cell"], "condition": r.get("condition"), "mode": r.get("mode"),
                      "pass": r.get("pass"), "drain_s": r.get("drain_s"), "mib_s": r.get("mib_per_s_end_to_end"),
                      "tx_mbit_s": r.get("wire_tx_mbit_s"), "rx_mbit_s": r.get("wire_rx_mbit_s"),
                      "cpu_s": r.get("cpu_s"), "rtt_ms": r.get("rtt_before", {}).get("median_ms"),
                      **retries(r), "unsafe": (r.get("oracle") or {}).get("unsafe_commits"),
                      "complete": (r.get("oracle") or {}).get("complete")})
    L += ["", "## Interpretation constraints", "",
          "- One VM per region, one time window; the path to S3 is the public AWS network between regions at that time.",
          "- Bandwidth limits and loss are emulated on the VM; the distance (RTT) is real.",
          "- The engine cap limits upload bodies only; tc shapes both directions (read-back downloads too).",
          "- Faults ran once per region (4 trials each across regions): they show behaviour, not rates.",
          "- Unresolved differences (CI includes 0) are not claimed as effects."]
    if failed:
        L += ["", "## Failed runs (excluded)", ""] + [f"- {f}" for f in failed]
    (out / "summary.json").write_text(json.dumps(S, indent=1, default=str))
    if table:
        with open(out / "wan.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(table[0]))
            w.writeheader()
            w.writerows(table)
    (out / "ANALYSIS.md").write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
