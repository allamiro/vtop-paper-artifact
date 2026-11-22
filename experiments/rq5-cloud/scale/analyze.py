#!/usr/bin/env python3
"""Validate and summarize an E-RQ5-SCALE session (proposed H3.7), run locally by the operator.

  python analyze.py results/<session> [--allow-failures]

Input: the session folder as downloaded by launch.sh: schedule.json and one h<slot>-<instance>/ folder per host
(host.json, engines.jsonl, steps.jsonl, ledger/, logs/, oracle/). Output in the same folder: summary.json,
steps.csv, engines.csv, ANALYSIS.md.

Definitions (per step; the warm-up step is excluded from every statistic):
  per-engine MiB/s   steady rate as cost.py: bytes committed at the second-to-last commit / its time since spawn
  aggregate MiB/s    sum of the step's per-engine steady rates
  window MiB/s       bytes all engines committed inside the common window [last spawn, first engine's
                     second-to-last commit] / its length: a check that the engines really ran together
  efficiency E(N)    median aggregate at N / (N x median aggregate at N = 1), percentile-bootstrap 95% CI
                     (B = 10,000, steps resampled within each N)
  S3 requests        every request attempt the engines' own AWS SDK started (engine log, RUST_LOG debug), per
                     operation; rate = attempts / (last stop - first spawn); peak = busiest second summed over
                     the step's engines; retries and the SDK's throttling verdicts (503 SlowDown) counted
Medians and differences use ci()/pct_ci() imported from rq3-cost-ablation/code/analyze_minio_cost.py, as COST.
Refuses (exit 2) if any engine-run failed the oracle or the evidence checks unless --allow-failures; an unsafe
commit is always reported first.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "rq3-cost-ablation" / "code"))
from analyze_minio_cost import B, ci, pct_ci  # noqa: E402

MiB = 2**20
ENA = ("bw_in_allowance_exceeded", "bw_out_allowance_exceeded", "pps_allowance_exceeded",
       "conntrack_allowance_exceeded", "linklocal_allowance_exceeded")


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(float(statistics.median(xs)), 4) if xs else None


def committed_at(series, t):
    c = 0
    for ts, b in series:
        if ts <= t:
            c = b
        else:
            break
    return c


def ratio_ci(num, den, scale, seed):
    """median(num) / (scale x median(den)) with a percentile bootstrap, steps resampled within each group."""
    x, y = np.asarray(num, float), np.asarray(den, float)
    rng = np.random.default_rng(seed)
    xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
    ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
    s = xs / (scale * ys)
    c = float(np.median(x)) / (scale * float(np.median(y)))
    return [round(c, 4), round(float(np.quantile(s, 0.025)), 4), round(float(np.quantile(s, 0.975)), 4)]


def step_summary(step, recs, hosts_steps):
    spawn = [r["t_spawn"] for r in recs]
    stop = [r["t_stop"] for r in recs]
    steady_end = [r["t_spawn"] + r["steady_s"] for r in recs]
    t_a, t_b = max(spawn), min(steady_end)
    win = None
    if t_b > t_a:
        got = sum(committed_at(r["commit_series"], t_b) - committed_at(r["commit_series"], t_a) for r in recs)
        win = round(got / MiB / (t_b - t_a), 3)
    span = max(stop) - min(spawn)
    per_s = Counter()
    ops = defaultdict(lambda: Counter())
    for r in recs:
        q = r.get("requests") or {}
        for k, v in (q.get("attempts_per_s") or {}).items():
            per_s[int(k)] += v
        for m, d in (q.get("ops") or {}).items():
            for k in ("requests", "attempts", "retries", "throttle_verdicts", "retry_verdicts"):
                ops[m][k] += d.get(k, 0)
    attempts = sum(c["attempts"] for c in ops.values())
    ena = Counter()
    busy, steal = [], []
    for hs in hosts_steps:
        s = hs.get("sys") or {}
        for k in ENA:
            if k in s:
                ena[k] += s[k]
        if s.get("cpu_busy_pct") is not None:
            busy.append(s["cpu_busy_pct"])
            steal.append(s.get("cpu_steal_pct"))
    return {
        "step": step["id"], "kind": step["kind"], "n": step["n"], "per_host": step["per_host"],
        "mode": step["mode"], "round": step["round"], "engines": len(recs),
        "agg_mib_s": round(sum(r["mib_per_s"] for r in recs), 3),
        "agg_records_s": round(sum(r["records_per_s"] or 0 for r in recs), 1),
        "window_mib_s": win, "window_s": round(t_b - t_a, 3),
        "window_overlap_fraction": round((t_b - t_a) / med([r["steady_s"] for r in recs]), 4) if win else 0.0,
        "start_skew_s": round(max(spawn) - min(spawn), 4), "max_late_s": max(r["late_s"] for r in recs),
        "makespan_s": round(span, 3), "makespan_mib_s": round(sum(r["bytes"] for r in recs) / MiB / span, 3),
        "per_engine_mib_s_median": med([r["mib_per_s"] for r in recs]),
        "per_engine_mib_s_min": min(r["mib_per_s"] for r in recs),
        "per_engine_cores_median": med([r["cpu_s"] / r["wall_s"] for r in recs if r.get("cpu_s")]),
        "host_cpu_busy_pct_median": med(busy), "host_cpu_steal_pct_max": max([x for x in steal if x is not None],
                                                                             default=None),
        "s3_attempts": attempts, "s3_attempts_per_s": round(attempts / span, 2),
        "s3_peak_attempts_per_s": max(per_s.values(), default=0),
        "s3_ops": {m: dict(c) for m, c in sorted(ops.items())},
        "s3_retries": sum(c["retries"] for c in ops.values()),
        "s3_throttle_verdicts": sum(c["throttle_verdicts"] for c in ops.values()),
        "engine_throttle_events": sum((r["engine_log"]["events"].get("upload_throttled", 0)
                                       + r["engine_log"]["events"].get("object_key_probe_throttled", 0)) for r in recs),
        "ena_allowance_exceeded": dict(ena) if ena else None,
        "batches": sum(r["ledger"].get("committed_batches", 0) for r in recs),
        "batches_over_max_bytes": sum(r["ledger"].get("batches_over_max_bytes", 0) for r in recs),
        "latency_ms_p50_median": {m: med([(r["requests"]["ops"].get(m) or {}).get("latency_ms", {}).get("p50")
                                          for r in recs]) for m in ops},
        "latency_ms_p99_max": {m: max([(r["requests"]["ops"].get(m) or {}).get("latency_ms", {}).get("p99") or 0
                                       for r in recs]) for m in ops},
        "pass": all(r["pass"] for r in recs),
    }


def windows(series, t0, t1, w):
    """Committed MiB/s per window of w seconds in [t0, t1)."""
    out, t = [], t0
    while t + w <= t1:
        out.append(round((committed_at(series, t + w) - committed_at(series, t)) / MiB / w, 3))
        t += w
    return out


def sustain_summary(step, recs):
    w = 60 if step.get("sustain_s", 0) >= 300 else 5  # dry runs are short
    per = []
    for r in recs:
        t0, t1 = r["t_spawn"], r["t_spawn"] + step["sustain_s"]
        ws = windows(r["commit_series"], t0, t1, w)
        slope = None
        if len(ws) >= 3:
            x = np.arange(len(ws)) * w
            slope = round(float(np.polyfit(x, ws, 1)[0]) * 600 / max(float(np.mean(ws)), 1e-9) * 100, 3)
        rss = [k for _, k in r.get("rss_series_kib", []) if k]
        per.append({"engine": r["engine"], "files": len(r["files"]), "bytes": r["bytes"], "window_s": w,
                    "rates_mib_s": ws, "first": ws[0] if ws else None, "last": ws[-1] if ws else None,
                    "trend_pct_per_10min": slope, "rss_mib_first_last": [round(rss[0] / 1024, 1),
                                                                        round(rss[-1] / 1024, 1)] if rss else None,
                    "peak_rss_mib": r.get("peak_rss_mib"), "pass": r["pass"],
                    "attempts": r["requests"]["attempts_total"], "retries": r["requests"]["retries_total"],
                    "throttle_verdicts": r["requests"]["throttle_verdicts_total"]})
    n = min(len(p["rates_mib_s"]) for p in per)
    agg = [round(sum(p["rates_mib_s"][i] for p in per), 3) for i in range(n)]
    return {"step": step["id"], "sustain_s": step["sustain_s"], "engines": len(recs),
            "total_gib": round(sum(r["bytes"] for r in recs) / 2**30, 3), "aggregate_rates_mib_s": agg,
            "aggregate_median_mib_s": med(agg), "per_engine": per, "pass": all(r["pass"] for r in recs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("session_dir")
    ap.add_argument("--allow-failures", action="store_true")
    a = ap.parse_args()
    d = Path(a.session_dir)
    sched = json.loads((d / "schedule.json").read_text())
    hosts = sorted(p for p in d.iterdir() if p.is_dir() and p.name.startswith("h"))
    recs, hsteps, hinfo = [], defaultdict(list), {}
    for h in hosts:
        info = json.loads((h / "host.json").read_text())
        hinfo[info["slot"]] = info
        if (h / "engines.jsonl").exists():
            recs += [json.loads(x) for x in (h / "engines.jsonl").read_text().splitlines() if x.strip()]
        if (h / "steps.jsonl").exists():
            for x in (h / "steps.jsonl").read_text().splitlines():
                if x.strip():
                    s = json.loads(x)
                    hsteps[s["step"]].append(s)
    by_step = defaultdict(list)
    for r in recs:
        by_step[r["step"]].append(r)
    keys = [(r["step"], r["engine"]) for r in recs]
    assert len(keys) == len(set(keys)), "duplicate step/engine record"
    unsafe = [f"{r['step']}/{r['engine']}" for r in recs if (r.get("oracle") or {}).get("unsafe_commits")]
    failed = [r for r in recs if not r.get("pass")]
    executed, missing = [], []
    for s in sched["steps"]:
        want = {e["engine"] for e in s["engines"]}
        have = {r["engine"] for r in by_step.get(s["id"], [])}
        if have == want:
            executed.append(s)
        elif have:
            missing.append(f"{s['id']}: {sorted(want - have)} missing")
        else:
            missing.append(f"{s['id']}: not run")
    stops = sorted({h.get("stopped") for h in hinfo.values() if h.get("stopped")})
    if unsafe:
        print(f"UNSAFE COMMITS: {unsafe}", file=sys.stderr)
    if failed and not a.allow_failures:
        print("failed engine-runs (use --allow-failures to summarize the rest): "
              + "; ".join(f"{r['step']}/{r['engine']}: {r.get('checks')}" for r in failed), file=sys.stderr)
        return 2

    good_steps = [s for s in executed if all(r["pass"] for r in by_step[s["id"]]) and s["kind"] != "warmup"]
    st = {s["id"]: step_summary(s, by_step[s["id"]], [x for x in hsteps[s["id"]] if not x.get("idle")])
          for s in good_steps if s["kind"] != "sustain"}
    summary = {"session": sched["session"], "hosts": sched["hosts"], "steps_planned": len(sched["steps"]),
               "steps_executed": len(executed), "steps_not_executed": missing, "host_stops": stops,
               "engine_runs": len(recs), "engine_runs_passing": len(recs) - len(failed),
               "failed_engine_runs": [f"{r['step']}/{r['engine']}" for r in failed], "unsafe": unsafe,
               "instance_types": sorted({str(h.get("instance_type")) for h in hinfo.values()}),
               "availability_zones": sorted({str(h.get("availability_zone")) for h in hinfo.values()}),
               "vtop": sorted({json.dumps(h.get("vtop"), sort_keys=True) for h in hinfo.values()}),
               "bootstrap_resamples": B, "per_n": {}, "steps": st}
    scale = defaultdict(list)
    for s in good_steps:
        if s["kind"] == "scale":
            scale[s["n"]].append(s["id"])
    eng = lambda ids: [r["mib_per_s"] for i in ids for r in by_step[i]]  # noqa: E731
    base_ids = scale.get(1, [])
    base_agg = [st[i]["agg_mib_s"] for i in base_ids]
    k = 0
    for n in sorted(scale):
        ids = scale[n]
        agg = [st[i]["agg_mib_s"] for i in ids]
        e = {"steps": len(ids), "engine_runs": sum(len(by_step[i]) for i in ids),
             "aggregate_mib_s": ci(agg, 20261124 + k),
             "aggregate_records_s_median": med([st[i]["agg_records_s"] for i in ids]),
             "window_mib_s_median": med([st[i]["window_mib_s"] for i in ids]),
             "window_overlap_fraction_min": min(st[i]["window_overlap_fraction"] for i in ids),
             "start_skew_s_max": max(st[i]["start_skew_s"] for i in ids),
             "per_engine_mib_s": ci(eng(ids), 20261224 + k),
             "per_engine_cores_median": med([st[i]["per_engine_cores_median"] for i in ids]),
             "per_engine_peak_rss_mib_median": med([r.get("peak_rss_mib") for i in ids for r in by_step[i]]),
             "host_cpu_busy_pct_median": med([st[i]["host_cpu_busy_pct_median"] for i in ids]),
             "s3_attempts_per_s_median": med([st[i]["s3_attempts_per_s"] for i in ids]),
             "s3_peak_attempts_per_s_max": max(st[i]["s3_peak_attempts_per_s"] for i in ids),
             "s3_retries": sum(st[i]["s3_retries"] for i in ids),
             "s3_throttle_verdicts": sum(st[i]["s3_throttle_verdicts"] for i in ids),
             "engine_throttle_events": sum(st[i]["engine_throttle_events"] for i in ids),
             "ena_allowance_exceeded_sum": sum(sum((st[i]["ena_allowance_exceeded"] or {}).values()) for i in ids)
             if any(st[i]["ena_allowance_exceeded"] is not None for i in ids) else None,
             "batches_over_max_bytes": sum(st[i]["batches_over_max_bytes"] for i in ids),
             "put_latency_ms_p50_median": med([st[i]["latency_ms_p50_median"].get("PutObject") for i in ids]),
             "head_latency_ms_p50_median": med([st[i]["latency_ms_p50_median"].get("HeadObject") for i in ids])}
        if base_agg and n != 1:
            e["efficiency"] = ratio_ci(agg, base_agg, n, 20261324 + k)
            e["per_engine_change_vs_n1_pct"] = pct_ci(eng(ids), eng(base_ids), 20261424 + k)
        summary["per_n"][n] = e
        k += 1
    ctl = [s["id"] for s in good_steps if s["kind"] == "control"]
    top = scale.get(sched["hosts"], [])
    if ctl and top:
        summary["control_size_only"] = {
            "n": sched["hosts"], "steps": len(ctl),
            "per_engine_mib_s": ci(eng(ctl), 20261524),
            "sha256_vs_size_only_per_engine_pct": pct_ci(eng(top), eng(ctl), 20261624),
            "sha256_vs_size_only_aggregate_pct": pct_ci([st[i]["agg_mib_s"] for i in top],
                                                        [st[i]["agg_mib_s"] for i in ctl], 20261724),
            "s3_attempts_per_engine_run_median": {
                "size-only": med([r["requests"]["attempts_total"] for i in ctl for r in by_step[i]]),
                "sha256-store": med([r["requests"]["attempts_total"] for i in top for r in by_step[i]])}}
    dense = [s["id"] for s in good_steps if s["kind"] == "dense"]
    if dense:
        agg = [st[i]["agg_mib_s"] for i in dense]
        n = dense and st[dense[0]]["n"]
        summary["dense"] = {"n": n, "per_host": 2, "steps": len(dense), "aggregate_mib_s": ci(agg, 20261824),
                            "per_engine_mib_s": ci(eng(dense), 20261924),
                            "host_cpu_busy_pct_median": med([st[i]["host_cpu_busy_pct_median"] for i in dense]),
                            "s3_throttle_verdicts": sum(st[i]["s3_throttle_verdicts"] for i in dense),
                            "ena_allowance_exceeded_sum": sum(sum((st[i]["ena_allowance_exceeded"] or {}).values())
                                                              for i in dense)}
        if base_agg:
            summary["dense"]["efficiency"] = ratio_ci(agg, base_agg, n, 20262024)
            summary["dense"]["per_engine_change_vs_n1_pct"] = pct_ci(eng(dense), eng(base_ids), 20262124)
    sus = [s for s in good_steps if s["kind"] == "sustain"]
    if sus:
        summary["sustain"] = sustain_summary(sus[0], by_step[sus[0]["id"]])

    (d / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
    cols = ["step", "kind", "n", "per_host", "mode", "round", "engines", "agg_mib_s", "window_mib_s",
            "window_overlap_fraction", "start_skew_s", "per_engine_mib_s_median", "per_engine_mib_s_min",
            "agg_records_s", "s3_attempts_per_s", "s3_peak_attempts_per_s", "s3_retries", "s3_throttle_verdicts",
            "host_cpu_busy_pct_median", "batches", "batches_over_max_bytes"]
    with open(d / "steps.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for s in sorted(st.values(), key=lambda x: x["step"]):
            w.writerow(s)
    ecols = ["step", "kind", "n", "mode", "engine", "slot", "pass", "mib_per_s", "records_per_s", "wall_s", "cpu_s",
             "peak_rss_mib", "late_s", "oracle_s"]
    with open(d / "engines.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(ecols + ["s3_attempts", "s3_retries", "throttle_verdicts", "unsafe", "batches"])
        for r in sorted(recs, key=lambda r: (r["step"], r["engine"])):
            q = r.get("requests") or {}
            w.writerow([r.get(c) for c in ecols] + [q.get("attempts_total"), q.get("retries_total"),
                                                    q.get("throttle_verdicts_total"),
                                                    (r.get("oracle") or {}).get("unsafe_commits"),
                                                    (r.get("ledger") or {}).get("committed_batches")])
    write_md(d, summary, sched)
    print(json.dumps({"engine_runs": len(recs), "failed": len(failed), "unsafe": len(unsafe),
                      "steps_executed": len(executed), "per_n": {n: v["aggregate_mib_s"] for n, v in
                                                                 summary["per_n"].items()}}))
    return 0


def fmt(c, p=2):
    return "—" if c is None else f"{c[0]:.{p}f} [{c[1]:.{p}f}, {c[2]:.{p}f}]"


def write_md(d, s, sched):
    L = [f"# E-RQ5-SCALE: {s['session']}", "",
         f"Hosts: {s['hosts']} ({', '.join(s['instance_types'])}; {', '.join(s['availability_zones'])}). "
         f"Steps executed {s['steps_executed']}/{s['steps_planned']}; engine-runs {s['engine_runs']} "
         f"({s['engine_runs_passing']} passing: drained, oracle byte-exact and complete, 0 unsafe, "
         f"log/receipt/ledger/HEAD agree, single-PUT). Unsafe: {len(s['unsafe'])}.", ""]
    if s["steps_not_executed"]:
        L += [f"Not executed: {'; '.join(s['steps_not_executed'])}. Host stops: {s['host_stops']}", ""]
    L += ["Medians with percentile-bootstrap 95% CI (B = 10000). Per-engine = steady MiB/s of one engine; "
          "aggregate = sum over the step's engines; efficiency = aggregate(N) / (N x aggregate(1)).", "",
          "| N | Steps | Aggregate MiB/s | Efficiency | Per-engine MiB/s | Per-engine vs N=1 % | Window MiB/s | "
          "Records/s | S3 att/s (peak) | Retries | Throttle verdicts | ENA events | Host CPU % |",
          "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for n, e in s["per_n"].items():
        L.append(f"| {n} | {e['steps']} | {fmt(e['aggregate_mib_s'])} | {fmt(e.get('efficiency'), 3)} | "
                 f"{fmt(e['per_engine_mib_s'])} | {fmt(e.get('per_engine_change_vs_n1_pct'))} | "
                 f"{e['window_mib_s_median']} | {e['aggregate_records_s_median']} | {e['s3_attempts_per_s_median']} "
                 f"({e['s3_peak_attempts_per_s_max']}) | {e['s3_retries']} | {e['s3_throttle_verdicts']} | "
                 f"{e['ena_allowance_exceeded_sum']} | {e['host_cpu_busy_pct_median']} |")
    if "control_size_only" in s:
        c = s["control_size_only"]
        L += ["", f"**Size-only control at N = {c['n']}** ({c['steps']} steps): per-engine {fmt(c['per_engine_mib_s'])} "
              f"MiB/s; store SHA-256 vs size-only, per engine {fmt(c['sha256_vs_size_only_per_engine_pct'])} %, "
              f"aggregate {fmt(c['sha256_vs_size_only_aggregate_pct'])} %. S3 attempts per engine-run: "
              f"{c['s3_attempts_per_engine_run_median']}."]
    if "dense" in s:
        c = s["dense"]
        L += ["", f"**Two engines per host, N = {c['n']}** ({c['steps']} steps): aggregate {fmt(c['aggregate_mib_s'])} "
              f"MiB/s, efficiency {fmt(c.get('efficiency'), 3)}, per-engine vs N=1 "
              f"{fmt(c.get('per_engine_change_vs_n1_pct'))} %, host CPU {c['host_cpu_busy_pct_median']} %."]
    if "sustain" in s:
        c = s["sustain"]
        L += ["", f"**Sustained step** {c['step']}: {c['engines']} engines for {c['sustain_s']} s, {c['total_gib']} GiB, "
              f"aggregate median {c['aggregate_median_mib_s']} MiB/s per window; pass {c['pass']}. Per engine "
              "(first/last window MiB/s, trend %/10 min, files): "
              + "; ".join(f"{p['engine']} {p['first']}/{p['last']}, {p['trend_pct_per_10min']}, {p['files']}"
                          for p in c["per_engine"])]
    L += ["", "## Proposed H3.7 prediction (CI facts only)", ""]
    for n, e in s["per_n"].items():
        if "efficiency" in e:
            lo = e["efficiency"][1]
            ch = e["per_engine_change_vs_n1_pct"]
            L.append(f"- N = {n}: efficiency CI lower bound ≥ 0.90: {lo >= 0.90}; per-engine change CI includes 0: "
                     f"{ch[1] <= 0 <= ch[2]}; throttle verdicts {e['s3_throttle_verdicts']}, retries {e['s3_retries']}")
    L.append(f"- every engine-run passed the oracle with 0 unsafe: {not s['failed_engine_runs'] and not s['unsafe']}")
    L += ["", "## Interpretation constraints", "",
          "- One region, one AZ, one time window; t4g instances with burstable CPU (unlimited) and network allowances.",
          "- Engines are independent (one ledger and one source set each); this is horizontal scale-out, "
          "not a shared-ledger cluster.",
          "- S3 request counts are the engines' own SDK attempts; the oracle's and coordination requests are excluded.",
          "- Unresolved differences (CI includes 0) are not claimed as effects."]
    (d / "ANALYSIS.md").write_text("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
