#!/usr/bin/env python3
"""Validate and summarize E-RQ5-PERF result directories (H4.2); one directory per leg (broker `results/` folder).

  python analyze.py <leg-dir> [<leg-dir> ...] --out <dir> [--request-profile request-profile.json]
         [--expect-reps tput=5,live=3,kill=3] [--allow-failures]
  python analyze.py --make-request-profile <requests.jsonl> <lab broker results dir> --out request-profile.json

Per leg and config: medians with percentile-bootstrap 95% CIs (ci/pct_ci imported from
rq3-cost-ablation/code/analyze_minio_cost.py, B = 10,000, as cost/analyze.py and wan/analyze.py) of steady committed
and archived throughput, CPU s/GiB and cores, peak memory, objects and S3 requests per GiB, request cost per GiB,
compute cost per GiB at the measured rate, inter-region transfer per GiB (Sydney leg); latency percentiles of the
live runs; recovery times of the kill runs; correctness per run (oracle). Differences: every config against
vtop-sha256-store (VTOP's strong mode on S3) and VTOP's modes against vtop-size-only; with two legs, Sydney against
us-east-1 per config (the distance effect). Warm-up runs are excluded. Writes summary.json, perf.csv, ANALYSIS.md.
Refuses when any run is unsafe (always) or failed (unless --allow-failures), or when reps are missing.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "rq3-cost-ablation" / "code"))
from analyze_minio_cost import B, ci, pct_ci  # noqa: E402

MiB, GiB, GB = 2**20, 2**30, 1e9
ORDER = ["vtop-size-only", "vtop-sha256-store", "vtop-blake3-readback", "vtop-sha256-gzip6", "confluent", "vector",
         "fluentbit"]
REF = "vtop-sha256-store"
# Prices (README "Time and cost", sources there): S3 us-east-1 requests; EC2 on demand per leg; inter-region
PUT_CLASS, GET_CLASS = 0.005 / 1000, 0.0004 / 1000          # USD per request (Tier1 / Tier2)
EC2_H = {"us-east-1": 0.0672, "ap-southeast-2": 0.0848}      # t4g.large USD/h
XFER_UP = {"us-east-1": 0.0, "ap-southeast-2": 0.098}        # USD/GB, EC2 region -> us-east-1
XFER_DOWN = {"us-east-1": 0.0, "ap-southeast-2": 0.02}       # USD/GB, us-east-1 -> region
TIER1 = {"PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload", "ListObjects", "POST",
         "PutBucket", "ListParts", "CopyObject"}


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(float(statistics.median(xs)), 4) if xs else None


def cin(xs, seed):
    xs = [x for x in xs if x is not None]
    return ci(xs, seed) if xs else None


def g(d, *ks):
    for k in ks:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


# ------------------------------------------------------------------ request profile (lab, counting proxy)
def make_profile(req_file: Path, lab_dir: Path, out: Path):
    reqs = [json.loads(x) for x in req_file.read_text().splitlines() if x.strip()]
    runs = [json.loads(x) for x in (lab_dir / "runs.jsonl").read_text().splitlines() if x.strip()]
    prof = defaultdict(lambda: {"data_objects": 0, "ops": Counter(), "runs": 0})
    starts = sorted(g(r, "start", "t_start") for r in runs if g(r, "start", "t_start"))
    for r in runs:
        if not r.get("pass") or r.get("type") == "kill":
            continue
        t0, t1 = g(r, "start", "t_start"), r.get("t_end")
        if not t0 or not t1:
            continue
        nxt = min([x for x in starts if x > t0] or [float("inf")])
        ops = Counter(q["op"] for q in reqs if t0 - 0.5 <= q["t"] < min(t1 + 5, nxt - 0.5))
        p = prof[r["config"]]
        p["data_objects"] += g(r, "s3", "objects_by_kind", "data") or 0
        p["ops"].update(ops)
        p["runs"] += 1
    res = {}
    for c, p in prof.items():
        n = max(1, p["data_objects"])
        res[c] = {"runs": p["runs"], "data_objects": p["data_objects"], "ops_total": dict(p["ops"]),
                  "per_data_object": {k: round(v / n, 3) for k, v in sorted(p["ops"].items())},
                  "tier1_per_object": round(sum(v for k, v in p["ops"].items() if k in TIER1) / n, 3),
                  "tier2_per_object": round(sum(v for k, v in p["ops"].items() if k not in TIER1) / n, 3)}
    out.write_text(json.dumps({"source": str(req_file), "lab_runs": str(lab_dir),
                               "note": "measured against MinIO through reqcount_proxy.py (dry run); applied to the "
                                       "cloud object counts, so it is an estimate for Amazon S3", "configs": res},
                              indent=1))
    print(json.dumps(res, indent=1))


# ------------------------------------------------------------------ summary
def leg_summary(d: Path, a, profile, k0: int) -> tuple[dict, list[dict]]:
    info = json.loads((d / "run.json").read_text())
    rows = [json.loads(x) for x in (d / "runs.jsonl").read_text().splitlines() if x.strip()]
    # Offline re-judgements (rejudge.py, from the run's kept S3 objects) replace the on-VM oracle of that run.
    # First use: 106-tput-vector-r4 of 20261122T041404Z-use1, failed on the old locate() tie-break (LESSONS.md).
    rejudged = []
    for f in sorted(list(d.glob("rejudge-*.json")) + list(d.parent.glob("rejudge-*.json"))):
        rj = json.loads(f.read_text())
        for r in rows:
            if r["run"] == rj["run"]:
                o = {k: v for k, v in rj["rejudged_oracle"].items() if k != "ambiguous_placements"}
                r["oracle_on_vm"], r["oracle"] = r["oracle"], o
                ch = dict(r.get("checks") or {})
                ch.update(complete=o["complete"], no_unsafe_commit=not o["unsafe_final"],
                          no_lost_committed=not o["lost_committed_records"])
                r["checks"], r["pass"] = ch, all(ch.values())
                rejudged.append({"run": r["run"], "file": f.name, "pass": r["pass"]})
    leg = info.get("leg") or "lab"
    FAULTS = ("corrupt", "lost")   # the fault phase (cloud RQ4): unsafe outcomes there are results
    unsafe = [r["run"] for r in rows if r.get("type") not in FAULTS
              and (g(r, "oracle", "unsafe_final") or g(r, "oracle", "lost_committed_records"))]
    if unsafe:
        raise SystemExit(f"{d}: unsafe commit or lost committed records in {unsafe} — inspect before any summary")
    meas = [r for r in rows if not r.get("warmup")]
    failed = [r for r in meas if not r.get("pass")]
    if failed and not a.allow_failures:
        raise SystemExit(f"{d}: failed runs (use --allow-failures): "
                         + "; ".join(f"{r['run']}: {r.get('checks') or r.get('error')}" for r in failed))
    good = [r for r in meas if r.get("pass")]
    cells = defaultdict(list)
    for r in good:
        cells[(r["type"], r["config"])].append(r)
    if a.expect_reps:
        for t, n in (x.split("=") for x in a.expect_reps.split(",")):
            for (tt, c), rs in cells.items():
                if tt == t and len(rs) != int(n) and not a.allow_failures:
                    raise SystemExit(f"{d}: {t}/{c}: {len(rs)} passing runs, expected {n}")
    s = {"leg": leg, "session": info.get("session"), "runs": len(rows), "measured": len(meas), "passing": len(good),
         "failed": [r["run"] for r in failed], **({"rejudged": rejudged} if rejudged else {}), "agent": {k: g(info, "agent", k) for k in ("platform", "vtop", "images")},
         "rtt": {k: info.get(k) for k in ("rtt_start_broker", "rtt_start_sink", "rtt_end_broker", "rtt_end_sink")},
         "tput_topic": info.get("tput_topic"), "fault_topic": info.get("fault_topic"),
         "tput": {}, "live": {}, "kill": {}, "faults": {}, "correctness": {}}
    table, k = [], k0
    ec2 = EC2_H.get(leg)
    for c in ORDER:
        rs = cells.get(("tput", c), [])
        if not rs:
            continue
        thr = [g(r, "throughput", "committed", "mib_s") for r in rs]
        arc = [g(r, "throughput", "archived", "mib_s") for r in rs]
        done = [g(r, "throughput", "done", "mib_s") for r in rs]
        gib = [g(r, "throughput", "total_payload_bytes") / GiB for r in rs]
        objs = [g(r, "s3", "objects_by_kind", "data") for r in rs]
        e = {"runs": len(rs),
             "done_mib_s": cin(done, 500 + k), "committed_mib_s": cin(thr, 1000 + k), "archived_mib_s": cin(arc, 2000 + k),
             "records_s_median": med([g(r, "throughput", "committed", "records_s_approx") for r in rs]),
             "end_to_end_mib_s_median": med([g(r, "throughput", "end_to_end_committed_mib_s") for r in rs]),
             "cpu_s_per_gib": cin([g(r, "throughput", "cpu_s_per_gib") for r in rs], 3000 + k),
             "cpu_cores_median": med([g(r, "throughput", "cpu_cores_window") for r in rs]),
             "peak_anon_mib_median": med([(g(r, "sink", "summary", "peak_anon_bytes") or 0) / MiB or None for r in rs]),
             "peak_cgroup_mib_median": med([(g(r, "sink", "summary", "peak_cgroup_memory_bytes") or 0) / MiB or None
                                            for r in rs]),
             "vtop_maxrss_mib_median": med([(g(r, "sink", "summary", "maxrss_bytes_rusage") or 0) / MiB or None
                                            for r in rs]),
             "data_objects_per_gib": med([o / x for o, x in zip(objs, gib) if o]),
             "raw_mib_per_object_median": med([(g(r, "s3", "raw_bytes_per_object_median") or 0) / MiB for r in rs]),
             "records_per_object_median": med([g(r, "s3", "records_per_object_median") for r in rs]),
             "compression_ratio_median": med([g(r, "s3", "data_bytes_raw") / max(1, g(r, "s3", "data_bytes_stored"))
                                              for r in rs]),
             "multipart_parts_per_object": med([g(r, "s3", "data_multipart_parts") / max(1, o) for r, o in zip(rs, objs)]),
             "nondata_objects_per_gib": med([(sum((g(r, "s3", "objects_by_kind") or {}).values()) - (o or 0)) / x
                                             for r, o, x in zip(rs, objs, gib)]),
             "sink_public_tx_mib_per_gib": med([(g(r, "sink", "summary", "counters_delta", "public_out_bytes") or 0)
                                                / MiB / x or None for r, x in zip(rs, gib)]),
             "sink_public_rx_mib_per_gib": med([(g(r, "sink", "summary", "counters_delta", "public_in_bytes") or 0)
                                                / MiB / x or None for r, x in zip(rs, gib)]),
             "sink_disk_write_mib_per_gib": med([(g(r, "sink", "summary", "counters_delta", "disk_write_bytes") or 0)
                                                 / MiB / x or None for r, x in zip(rs, gib)]),
             "ena_allowance_exceeded_sink": sum(sum(v for kk, v in (g(r, "sink", "summary", "counters_delta") or {}).items()
                                                    if kk.endswith("allowance_exceeded")) for r in rs),
             "ena_allowance_exceeded_broker": sum(sum(v for kk, v in (r.get("broker_host") or {}).items()
                                                      if kk.endswith("allowance_exceeded")) for r in rs),
             "cpu_steal_pct_max_broker": max((g(r, "broker_host", "cpu_steal_pct") or 0 for r in rs), default=None)}
        if profile and c in profile:
            p = profile[c]
            dpg = e["data_objects_per_gib"] or 0
            e["requests_per_gib_est"] = {"tier1": round(p["tier1_per_object"] * dpg, 2),
                                         "tier2": round(p["tier2_per_object"] * dpg, 2)}
            e["request_usd_per_gib_est"] = round(p["tier1_per_object"] * dpg * PUT_CLASS
                                                 + p["tier2_per_object"] * dpg * GET_CLASS, 6)
        if ec2 and e["done_mib_s"]:
            e["sink_ec2_usd_per_gib"] = round(ec2 / (e["done_mib_s"][0] * 3600 / 1024), 6)
        if leg in XFER_UP and e["sink_public_tx_mib_per_gib"] is not None:
            e["transfer_usd_per_gib"] = round(e["sink_public_tx_mib_per_gib"] * MiB / GB * XFER_UP[leg]
                                              + (e["sink_public_rx_mib_per_gib"] or 0) * MiB / GB * XFER_DOWN[leg], 6)
        s["tput"][c] = e
        k += 1
    # differences of medians (committed MiB/s, CPU s/GiB): every config vs REF; VTOP modes vs size-only
    ref = [g(r, "throughput", "done", "mib_s") for r in cells.get(("tput", REF), [])]
    so = [g(r, "throughput", "done", "mib_s") for r in cells.get(("tput", "vtop-size-only"), [])]
    ref_cpu = [g(r, "throughput", "cpu_s_per_gib") for r in cells.get(("tput", REF), [])]
    for c, e in s["tput"].items():
        mine = [g(r, "throughput", "done", "mib_s") for r in cells[("tput", c)]]
        cpu = [g(r, "throughput", "cpu_s_per_gib") for r in cells[("tput", c)]]
        if c != REF and len(ref) and len(mine) and None not in ref + mine:
            e["done_vs_ref_pct"] = pct_ci(mine, ref, 4000 + k)
            k += 1
        if c != REF and len(ref_cpu) and len(cpu) and None not in ref_cpu + cpu:
            e["cpu_per_gib_vs_ref_pct"] = pct_ci(cpu, ref_cpu, 4500 + k)
            k += 1
        if c.startswith("vtop-") and c != "vtop-size-only" and so and None not in so + mine:
            e["done_vs_size_only_pct"] = pct_ci(mine, so, 5000 + k)
            k += 1
        table.append({"leg": leg, "type": "tput", "config": c, "runs": e["runs"],
                      "done_mib_s": g(e, "done_mib_s", 0), "lo": g(e, "done_mib_s", 1), "hi": g(e, "done_mib_s", 2),
                      "committed_mib_s": g(e, "committed_mib_s", 0), "vs_ref_pct": g(e, "done_vs_ref_pct", 0),
                      "cpu_s_per_gib": g(e, "cpu_s_per_gib", 0), "peak_anon_mib": e["peak_anon_mib_median"],
                      "objects_per_gib": e["data_objects_per_gib"], "request_usd_per_gib": e.get("request_usd_per_gib_est"),
                      "ec2_usd_per_gib": e.get("sink_ec2_usd_per_gib"), "transfer_usd_per_gib": e.get("transfer_usd_per_gib")})
    for c in ORDER:
        rs = cells.get(("live", c), [])
        if not rs:
            continue
        e = {"runs": len(rs)}
        for comp in ("end_to_end_s", "object_visible_s", "commit_s"):
            for q in ("p50", "p90", "p99", "p100"):
                e[f"{comp}_{q}"] = cin([g(r, "latency", comp, q) for r in rs], 6000 + k)
                k += 1
        e["missing_records"] = sum(g(r, "latency", "end_to_end_s", "missing") or 0 for r in rs)
        e["commit_before_object_records_median"] = med([g(r, "latency", "commit_before_object_records") for r in rs])
        e["achieved_rate_median"] = med([g(r, "latency", "producer", "achieved_rate") for r in rs])
        e["raw_mib_per_object_median"] = med([(g(r, "s3", "raw_bytes_per_object_median") or 0) / MiB for r in rs])
        s["live"][c] = e
        table.append({"leg": leg, "type": "live", "config": c, "runs": len(rs),
                      "p50_s": g(e, "end_to_end_s_p50", 0), "p99_s": g(e, "end_to_end_s_p99", 0),
                      "max_s": g(e, "end_to_end_s_p100", 0)})
    for c in ORDER:
        rs = cells.get(("kill", c), [])
        if not rs:
            continue
        e = {"runs": len(rs)}
        for m in ("ready_s", "first_commit_after_s", "first_object_after_s", "resteady_s", "pre_kill_mib_s"):
            e[m] = cin([g(r, "recovery", m) for r in rs], 7000 + k)
            k += 1
        e["resteady_not_reached"] = sum(1 for r in rs if g(r, "recovery", "resteady_s") is None)
        e["duplicate_records"] = [g(r, "oracle", "duplicate_records") for r in rs]
        e["complete"] = sum(1 for r in rs if g(r, "oracle", "complete"))
        s["kill"][c] = e
        table.append({"leg": leg, "type": "kill", "config": c, "runs": len(rs),
                      "first_commit_after_s": g(e, "first_commit_after_s", 0), "resteady_s": g(e, "resteady_s", 0),
                      "dup_records_median": med(e["duplicate_records"])})
    for fk in FAULTS:   # cloud counterpart of RQ4 (H4.1): native S3 overwrite/delete after the write is listed
        for c in ORDER:
            rs = cells.get((fk, c), [])
            if not rs:
                continue
            cls = Counter(g(r, "fault", "damage_vs_commit") for r in rs)
            pre = [r for r in rs if g(r, "fault", "damage_vs_commit") in ("damage_before_commit",
                                                                         "commit_before_object_visible")]
            e = {"trials": len(rs), "classes": dict(cls),
                 "unsafe_trials": sum(1 for r in rs if g(r, "oracle", "unsafe_final")),
                 "unsafe_trials_rq4_comparable": sum(1 for r in pre if g(r, "oracle", "unsafe_final")),
                 "rq4_comparable_trials": len(pre),
                 "lost_committed_records": [g(r, "oracle", "lost_committed_records") for r in rs],
                 "complete": sum(1 for r in rs if g(r, "oracle", "complete")),
                 "nth": [r.get("nth_faulted_write") for r in rs],
                 "visible_to_damage_s_median": med([g(r, "fault", "visible_to_damage_s") for r in rs]),
                 "ends": dict(Counter(str(r.get("end")) for r in rs))}
            if c.startswith("vtop-"):
                e["vtop_vs_verification"] = dict(Counter(g(r, "fault", "damage_vs_vtop_verification") for r in rs))
                e["vtop_post_scrub_rc"] = [g(r, "fault", "vtop_post_scrub_rc") for r in rs]
                e["vtop_post_audit_coverage_rc"] = [g(r, "fault", "vtop_post_audit_coverage_rc") for r in rs]
            s["faults"][f"{fk}/{c}"] = e
            table.append({"leg": leg, "type": fk, "config": c, "runs": len(rs),
                          "unsafe_trials": e["unsafe_trials"], "rq4_comparable": len(pre),
                          "unsafe_rq4_comparable": e["unsafe_trials_rq4_comparable"]})
    for (t, c), rs in sorted(cells.items()):
        s["correctness"][f"{t}/{c}"] = {
            "runs": len(rs), "complete": sum(1 for r in rs if g(r, "oracle", "complete")),
            "unsafe": sum(g(r, "oracle", "unsafe_final") or 0 for r in rs),
            "lost_committed_records": sum(g(r, "oracle", "lost_committed_records") or 0 for r in rs),
            "duplicate_records": sum(g(r, "oracle", "duplicate_records") or 0 for r in rs),
            "bad_objects": sum(g(r, "oracle", "s3_data_objects_bad") or 0 for r in rs),
            "ambiguous_lines": sum(g(r, "oracle", "ambiguous_lines") or 0 for r in rs)}
    s["_cells"] = cells
    return s, table


def fmt(x, nd=2):
    if x is None:
        return "–"
    if isinstance(x, list):
        return f"{x[0]:.{nd}f} [{x[1]:.{nd}f}, {x[2]:.{nd}f}]"
    return f"{x:.{nd}f}" if isinstance(x, float) else str(x)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="*")
    ap.add_argument("--out", required=True)
    ap.add_argument("--request-profile", default=None)
    ap.add_argument("--expect-reps", default="")
    ap.add_argument("--allow-failures", action="store_true")
    ap.add_argument("--make-request-profile", nargs=2, metavar=("REQUESTS_JSONL", "LAB_DIR"))
    a = ap.parse_args()
    if a.make_request_profile:
        return make_profile(Path(a.make_request_profile[0]), Path(a.make_request_profile[1]), Path(a.out))
    profile = json.loads(Path(a.request_profile).read_text())["configs"] if a.request_profile else None
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    legs, table = [], []
    for i, d in enumerate(a.dirs):
        s, t = leg_summary(Path(d), a, profile, 100000 * (i + 1))
        legs.append(s)
        table += t
    cross = {}
    if len(legs) == 2:   # distance effect per config: far leg vs near leg, committed MiB/s and live p50
        near, far = sorted(legs, key=lambda s: s["leg"] != "us-east-1")
        for c in ORDER:
            x = [g(r, "throughput", "done", "mib_s") for r in far["_cells"].get(("tput", c), [])]
            y = [g(r, "throughput", "done", "mib_s") for r in near["_cells"].get(("tput", c), [])]
            lx = [g(r, "latency", "end_to_end_s", "p50") for r in far["_cells"].get(("live", c), [])]
            ly = [g(r, "latency", "end_to_end_s", "p50") for r in near["_cells"].get(("live", c), [])]
            e = {}
            if x and y and None not in x + y:
                e["done_far_vs_near_pct"] = pct_ci(x, y, 9000 + len(cross))
            if lx and ly and None not in lx + ly:
                e["live_p50_far_minus_near_s"] = ci(lx, 9500 + len(cross), other=ly)
            if e:
                cross[c] = e
    summary = {"experiment": "E-RQ5-PERF (H4.2)", "bootstrap_resamples": B, "reference": REF,
               "legs": [{k: v for k, v in s.items() if k != "_cells"} for s in legs], "distance_effect": cross,
               "request_profile": a.request_profile}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
    keys = sorted({k for r in table for k in r}, key=lambda k: list(table[0]).index(k) if k in table[0] else 99)
    with open(out / "perf.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(table)
    L = ["# E-RQ5-PERF: VTOP v0.10.2 vs Confluent S3 Sink 12.1.15, Vector 0.61.0, Fluent Bit 5.2.0", "",
         f"Medians with percentile-bootstrap 95% CIs (B = {B}); differences are of medians, in % of the reference "
         f"(`{REF}`). Unresolved differences (CI includes 0) are not claimed. Warm-up runs excluded.", ""]
    for s in legs:
        L += [f"## Leg {s['leg']} (session {s['session']}): {s['measured']} measured runs, {s['passing']} passing", ""]
        rt = s["rtt"].get("rtt_start_sink") or {}
        if rt and "median_ms" in rt:
            L.append(f"RTT sink → S3 (TCP handshake, median of {rt.get('ok')}): {rt['median_ms']} ms; "
                     f"broker probe: {g(s['rtt'], 'rtt_start_broker', 'tcp_connect')}")
            L.append("")
        L += ["### Throughput (backlog drain, steady 10–90%)", "",
              "Done = archived intact AND source-committed (the latency's end point); committed and archived alone "
              "are also given.", "",
              "| Config | Runs | Done MiB/s | vs ref % | Committed MiB/s | Archived MiB/s | CPU s/GiB | Cores | "
              "Peak anon MiB | Objects/GiB | Raw MiB/object | Req USD/GiB (est.) | EC2 USD/GiB | Transfer USD/GiB |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for c, e in s["tput"].items():
            L.append(f"| {c} | {e['runs']} | {fmt(e['done_mib_s'])} | {fmt(e.get('done_vs_ref_pct'), 1)} | "
                     f"{fmt(e['committed_mib_s'])} | {fmt(e['archived_mib_s'])} | {fmt(e['cpu_s_per_gib'])} | {fmt(e['cpu_cores_median'])} | "
                     f"{fmt(e['peak_anon_mib_median'], 0)} | {fmt(e['data_objects_per_gib'], 1)} | "
                     f"{fmt(e['raw_mib_per_object_median'])} | {fmt(e.get('request_usd_per_gib_est'), 5)} | "
                     f"{fmt(e.get('sink_ec2_usd_per_gib'), 5)} | {fmt(e.get('transfer_usd_per_gib'), 5)} |")
        L += ["", "### Latency (live, fixed rate; produce → durably in S3 and offset committed)", "",
              "| Config | Runs | p50 s | p90 s | p99 s | max s | object-visible p50 | commit p50 | missing |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for c, e in s["live"].items():
            L.append(f"| {c} | {e['runs']} | {fmt(e['end_to_end_s_p50'])} | {fmt(e['end_to_end_s_p90'])} | "
                     f"{fmt(e['end_to_end_s_p99'])} | {fmt(e['end_to_end_s_p100'])} | {fmt(e['object_visible_s_p50'])} | "
                     f"{fmt(e['commit_s_p50'])} | {e['missing_records']} |")
        L += ["", "### Recovery after one SIGKILL (times from the kill)", "",
              "| Config | Runs | ready s | first commit s | first object s | re-steady s (not reached) | duplicates | complete |",
              "|---|---:|---:|---:|---:|---:|---|---:|"]
        for c, e in s["kill"].items():
            L.append(f"| {c} | {e['runs']} | {fmt(e['ready_s'])} | {fmt(e['first_commit_after_s'])} | "
                     f"{fmt(e['first_object_after_s'])} | {fmt(e['resteady_s'])} ({e['resteady_not_reached']}) | "
                     f"{e['duplicate_records']} | {e['complete']}/{e['runs']} |")
        if s["faults"]:
            L += ["", "### Storage faults on S3 (cloud counterpart of RQ4; native overwrite/delete, no proxy)", "",
                  "Class = when the damage landed relative to the sink's first commit into the damaged object's "
                  "records (`damage_before_commit` and `commit_before_object_visible` are the RQ4-comparable cases).", "",
                  "| Fault/config | Trials | Classes | Unsafe (all) | Unsafe (RQ4-comparable / n) | Lost committed records | "
                  "Complete | VTOP vs its verification | VTOP scrub/audit rc |", "|---|---:|---|---:|---:|---|---:|---|---|"]
            for c, e in s["faults"].items():
                L.append(f"| {c} | {e['trials']} | {e['classes']} | {e['unsafe_trials']} | "
                         f"{e['unsafe_trials_rq4_comparable']} / {e['rq4_comparable_trials']} | "
                         f"{e['lost_committed_records']} | {e['complete']} | {e.get('vtop_vs_verification', '–')} | "
                         f"{e.get('vtop_post_scrub_rc', '–')} / {e.get('vtop_post_audit_coverage_rc', '–')} |")
        L += ["", "### Correctness (oracle, every run)", "", "| Cell | Runs | Complete | Unsafe | Lost committed | "
              "Duplicate records | Bad objects |", "|---|---:|---:|---:|---:|---:|---:|"]
        for c, e in s["correctness"].items():
            L.append(f"| {c} | {e['runs']} | {e['complete']} | {e['unsafe']} | {e['lost_committed_records']} | "
                     f"{e['duplicate_records']} | {e['bad_objects']} |")
        if s["failed"]:
            L += ["", "Failed runs (excluded): " + ", ".join(s["failed"])]
        L.append("")
    if cross:
        L += ["## Distance effect (far leg vs us-east-1)", "", "| Config | Done MiB/s change % | Live p50 change s |",
              "|---|---:|---:|"]
        for c, e in cross.items():
            L.append(f"| {c} | {fmt(e.get('done_far_vs_near_pct'), 1)} | {fmt(e.get('live_p50_far_minus_near_s'))} |")
        L.append("")
    L += ["## Interpretation constraints", "",
          "- One instance pair per leg, one time window, t4g.large (burstable, unlimited credits); not a cluster.",
          "- S3 request counts on AWS are estimates: measured per object against MinIO (dry run) × cloud object counts.",
          "- Consumed (fetched) rate is not observable without instrumenting the systems; committed and archived "
          "rates are reported, plus the sink NIC receive rate.",
          "- Latency resolution: LIST poll and commit poll intervals (run.json `measurement`)."]
    (out / "ANALYSIS.md").write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
