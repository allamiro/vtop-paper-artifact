#!/usr/bin/env python3
"""Validate and summarize the C25 emulated-latency S3 cost campaign.

Each latency level is analysed on its own against size-only at the same latency,
with the same percentile bootstrap as analyze_minio_cost.py (B = 10,000; CI on
the difference of medians). Comparisons with the 0 ms clean set are descriptive
only: different session, and the 0 ms set has no proxy hop.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from analyze_minio_cost import ci, pct_ci  # noqa: E402

VARIANTS = ["size-only", "sha256", "blake3"]
DATASETS = ["ssh", "bgl", "hdfs"]
# (latency_ms, bandwidth_kbps or None, reps per cell)
LEVELS = [(20.0, None, 10), (50.0, None, 5), (50.0, 1250.0, 5)]


def level_key(lat, bw):
    return f"{lat:g}" if bw is None else f"{lat:g}+bw{bw:g}"


def level_label(lat, bw):
    s = f"+{lat:g} ms RTT ({lat / 2:g} ms each way)"
    return s if bw is None else s + f", bandwidth cap {bw:g} KB/s each way (emulated low-bandwidth, high-latency link)"
MIB = 2**20


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("result_dir")
    ap.add_argument("--clean", default=None, help="0 ms clean result dir (descriptive comparison only)")
    a = ap.parse_args()
    out = Path(a.result_dir)
    rows = [json.loads(x) for x in (out / "runs.jsonl").read_text().splitlines() if x.strip()]
    meta = json.loads((out / "run.json").read_text())
    exposed_path = out / "runs-exposed.jsonl"
    exposed = [json.loads(x) for x in exposed_path.read_text().splitlines() if x.strip()] if exposed_path.exists() else []

    summary = {"experiment": "E-RQ3-A S3 over a local Docker network with emulated latency (Toxiproxy)",
               "network": "emulated latency over a local Docker network; not a real WAN or public-cloud S3",
               "toxiproxy_image": meta.get("toxiproxy_image"), "toxiproxy_image_id": meta.get("toxiproxy_image_id"),
               "vtop_commit": meta.get("vtop_commit"), "exposed_runs_replaced": len(exposed), "levels": {}}
    table, lines_by_level = [], {}
    for lat, bw, nrep in LEVELS:
        lk = level_key(lat, bw)
        matrix = [r for r in rows if r.get("latency_ms") == lat and r.get("bandwidth_kbps") == bw]
        n = len(DATASETS) * len(VARIANTS) * nrep
        if not matrix:
            print(f"level {lk}: no runs yet; skipped")
            continue
        assert len(matrix) == n, f"{lk}: expected {n} runs, got {len(matrix)}"
        undrained = [r for r in matrix if not r["drained"]]
        if bw is None:
            assert not undrained, f"{lk}: a run did not drain"
        if bw is not None:
            for r in matrix:
                b = r["bandwidth_checked"]
                assert all(float(b[s]["rate"]) == bw for s in ("upstream", "downstream")), (lk, b)
        over15 = [r for r in matrix if r.get("run_over_15_min")]
        matrix_all, matrix = matrix, [r for r in matrix if r["drained"]]
        assert len({(r["dataset"], r["variant"], r["rep"]) for r in matrix_all}) == n, f"{lk}: duplicate key"
        for r in matrix:
            t = r["toxics_checked"]
            assert all(float(t[s]["latency"]) == lat / 2 and float(t[s]["jitter"]) == 0
                       for s in ("upstream", "downstream")), (lat, t)
        cells = defaultdict(list)
        for r in matrix:
            cells[(r["dataset"], r["variant"])].append(r)
        oracle_cells = 0
        for ds in DATASETS:
            for vn in VARIANTS:
                r0 = next((r for r in cells[(ds, vn)] if r["rep"] == 0), None)
                if r0 is None:
                    continue
                o = r0["oracle"]
                assert o["complete"] and o["bad_objects"] == 0 and o["unsafe_commits"] == 0, (lat, ds, vn, o)
                oracle_cells += 1
        checks = [e["latency_check"] for e in meta.get("appended", [])
                  if e.get("latency_ms") == lat and e.get("bandwidth_kbps") == bw]
        level = {"latency_ms": lat, "per_direction_ms": lat / 2, "jitter_ms": 0, "bandwidth_kbps": bw,
                 "link": "emulated low-bandwidth, high-latency link" if bw is not None else "emulated latency",
                 "reps_per_cell": nrep, "matrix_runs": len(matrix_all), "drained_runs": len(matrix),
                 "undrained_runs": [f"{r['dataset']}/{r['variant']}/r{r['rep']}" for r in undrained],
                 "runs_over_15_min": [f"{r['dataset']}/{r['variant']}/r{r['rep']} ({r['wall_s']:.0f} s)" for r in over15],
                 "max_wall_s": max(r["wall_s"] for r in matrix_all),
                 "oracle_runs": oracle_cells, "all_drained": not undrained,
                 "delay_checks": [{"measured_at": c["measured_at"],
                                   "direct_head_median_ms": c["direct"]["median_ms"],
                                   "proxy_no_toxic_head_median_ms": c["proxy_no_toxic"]["median_ms"],
                                   "proxy_with_toxics_head_median_ms": c["proxy_with_toxics"]["median_ms"],
                                   "added_vs_proxy_no_toxic_median_ms": c["added_vs_proxy_no_toxic_median_ms"],
                                   "added_vs_direct_median_ms": c["added_vs_direct_median_ms"],
                                   **({"transfer": c["transfer"]} if "transfer" in c else {})} for c in checks],
                 "cells": {}}
        k = 0
        for ds in DATASETS:
            level["cells"][ds] = {}
            base = [r["mib_per_s"] for r in cells[(ds, "size-only")]]
            sha = [r["mib_per_s"] for r in cells[(ds, "sha256")]]
            for vn in VARIANTS:
                rs = cells[(ds, vn)]
                thr = [r["mib_per_s"] for r in rs]
                entry = {
                    "runs": len(rs),
                    "throughput_mib_s": ci(thr, 20261111 + k),
                    "total_ms_per_batch": ci([r["sum_total_ms"] / r["batches"] for r in rs], 20261211 + k),
                    "cpu_s_per_gib": ci([r["cpu_s"] / (r["steady_bytes"] / 2**30) for r in rs], 20261311 + k),
                    "stored_object_mib": round(float(np.median([r["stored_object_bytes"] for r in rs])) / MIB, 3),
                    "readback_object_mib": round(float(np.median([r["stored_object_bytes"] for r in rs])) / MIB, 3)
                    if vn == "blake3" else 0.0,
                    "proxy_mib_from_store": round(float(np.median([r["proxy_bytes_from_store"] for r in rs])) / MIB, 3),
                    "proxy_mib_to_store": round(float(np.median([r["proxy_bytes_to_store"] for r in rs])) / MIB, 3),
                    "manifest_mib": round(float(np.median([r["manifest_bytes"] for r in rs])) / MIB, 4),
                    "throughput_drop_vs_size_only_pct": [0.0, 0.0, 0.0] if vn == "size-only"
                    else pct_ci(thr, base, 20261411 + k),
                }
                if vn == "blake3":
                    entry["throughput_change_vs_sha256_pct"] = pct_ci(thr, sha, 20261511 + k)
                if bw is not None:
                    for d in ("upload", "download"):
                        u = [r["link_utilization"][d] for r in rs if r["link_utilization"].get(d) is not None]
                        if u:
                            entry[f"link_utilization_{d}_median_min_max"] = [
                                round(float(np.median(u)), 4), round(min(u), 4), round(max(u), 4)]
                level["cells"][ds][vn] = entry
                d = entry["throughput_drop_vs_size_only_pct"]
                table.append({"latency_ms": lat, "bandwidth_kbps": bw, "dataset": ds, "variant": vn, "runs": len(rs),
                              "throughput_median": entry["throughput_mib_s"][0],
                              "throughput_lo": entry["throughput_mib_s"][1],
                              "throughput_hi": entry["throughput_mib_s"][2],
                              "drop_pct": d[0], "drop_lo": d[1], "drop_hi": d[2],
                              "resolved": vn != "size-only" and (d[2] < 0 or d[1] > 0),
                              "total_ms_per_batch": entry["total_ms_per_batch"][0],
                              "cpu_s_per_gib": entry["cpu_s_per_gib"][0],
                              "readback_object_mib": entry["readback_object_mib"],
                              "proxy_mib_from_store": entry["proxy_mib_from_store"]})
                k += 1
        summary["levels"][lk] = level

    clean = None
    if a.clean:
        clean = json.loads((Path(a.clean) / "summary.json").read_text())
        desc = {}
        for ds in DATASETS:
            desc[ds] = {}
            for vn in VARIANTS:
                desc[ds][vn] = {"0": clean["cells"][ds][vn]["throughput_mib_s"][0]}
                for lk in summary["levels"]:
                    desc[ds][vn][lk] = summary["levels"][lk]["cells"][ds][vn]["throughput_mib_s"][0]
        summary["descriptive_median_mib_s_by_latency"] = {
            "note": ("descriptive only: 0 ms is the K38 clean set (different session, no proxy hop); "
                     "no inferential comparison across latency levels"),
            "source_0ms": str(Path(a.clean).as_posix()).split("research/", 1)[-1], "cells": desc}

    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    with open(out / "rq3_minio_latency.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(table[0]))
        w.writeheader()
        w.writerows(table)

    L = ["# Emulated-latency S3 verification-cost analysis (C25)", "",
         "VTOP v0.7.0 (`6c8f386`), native S3 backend, MinIO behind Toxiproxy "
         f"`{meta.get('toxiproxy_image')}` on one local Docker network. The latency is emulated:",
         "a latency toxic of half the stated round-trip time in each direction, jitter 0, no bandwidth limit.",
         "This is not a real WAN or public-cloud S3. All engine S3 traffic (PUT, HEAD, GET) passes the proxy;",
         "the harness's cleanup, listing and oracle reads use the direct endpoint.", "",
         "Each latency level is analysed on its own against size-only at the same latency. Throughput is the",
         "median steady-state MiB/s with a percentile-bootstrap 95% CI (B = 10,000). The change is",
         "100 × (variant median − size-only median) / size-only median, with a bootstrap CI on the difference",
         "of medians; negative values are slowdowns; *resolved* means the CI excludes zero.",
         "Read-back MiB follows the clean set (stored object bytes for BLAKE3). Proxy MiB from store is",
         "measured: Toxiproxy's downstream byte counter for the run (all responses, including GET bodies).", ""]
    for lat, bw, nrep in LEVELS:
        lk = level_key(lat, bw)
        if lk not in summary["levels"]:
            continue
        lv = summary["levels"][lk]
        drained_txt = ("Every run drained." if lv["all_drained"] else
                       f"Not drained within 1800 s: {', '.join(lv['undrained_runs'])} (excluded from throughput).")
        L += [f"## {level_label(lat, bw)}", "",
              f"{lv['matrix_runs']} runs: 3 datasets × 3 variants × {nrep} repetitions. {drained_txt} "
              f"The first repetition of every cell passed the independent S3 digest-and-byte-exact oracle "
              f"({lv['oracle_runs']}/9). Longest run: {lv['max_wall_s']:.1f} s wall"
              + (f"; runs over 15 min: {', '.join(lv['runs_over_15_min']) or 'none'}." if bw is not None else "."), "",
              "Delay check (median of 50 kept-alive HEAD requests, per chunk): " + "; ".join(
                  f"direct {c['direct_head_median_ms']:.2f} ms, proxy without toxics "
                  f"{c['proxy_no_toxic_head_median_ms']:.2f} ms, with toxics {c['proxy_with_toxics_head_median_ms']:.2f} ms "
                  f"(+{c['added_vs_proxy_no_toxic_median_ms']:.2f})" for c in lv["delay_checks"]) + ".", ""]
        if bw is not None:
            L += ["Bandwidth check (8 MiB object through the proxy, per chunk): " + "; ".join(
                f"PUT {c['transfer']['put_mbit_per_s']:.2f} Mbit/s ({c['transfer']['put_kbytes_per_s']:.0f} KB/s), "
                f"GET {c['transfer']['get_mbit_per_s']:.2f} Mbit/s ({c['transfer']['get_kbytes_per_s']:.0f} KB/s)"
                for c in lv["delay_checks"]) + ".", ""]
        L += [
              "| Dataset | Variant | MiB/s [95% CI] | Change vs size-only [95% CI] | Resolved | Total ms/batch | CPU s/GiB | Read-back MiB | Proxy MiB from store |",
              "|---|---|---:|---:|---|---:|---:|---:|---:|"]
        for ds in DATASETS:
            for vn in VARIANTS:
                e = lv["cells"][ds][vn]
                t, d = e["throughput_mib_s"], e["throughput_drop_vs_size_only_pct"]
                res = "—" if vn == "size-only" else ("yes" if (d[2] < 0 or d[1] > 0) else "no")
                L.append(f"| {ds.upper()} | {vn} | {t[0]:.2f} [{t[1]:.2f}, {t[2]:.2f}] | {d[0]:.2f}% [{d[1]:.2f}, {d[2]:.2f}] | "
                         f"{res} | {e['total_ms_per_batch'][0]:.2f} | {e['cpu_s_per_gib'][0]:.3f} | "
                         f"{e['readback_object_mib']:.3f} | {e['proxy_mib_from_store']:.3f} |")
        if bw is not None:
            L += ["", "Link utilization over the steady interval (median [min, max] across runs): stored payload",
                  "bytes of the batches inside the steady interval / (1,250,000 B/s × steady time), per direction.",
                  "Toxiproxy's byte counters update only when a connection closes, so payload sizes from the",
                  "S3 listing are used; HTTP overhead and in-flight bytes are excluded, so these are lower bounds.", "",
                  "| Dataset | Variant | Upload utilization | Download utilization |", "|---|---|---:|---:|"]
            for ds in DATASETS:
                for vn in VARIANTS:
                    e = lv["cells"][ds][vn]
                    fmt = lambda x: f"{x[0]:.3f} [{x[1]:.3f}, {x[2]:.3f}]" if x else "—"
                    L.append(f"| {ds.upper()} | {vn} | {fmt(e.get('link_utilization_upload_median_min_max'))} | "
                             f"{fmt(e.get('link_utilization_download_median_min_max'))} |")
        L += ["", "BLAKE3 read-back vs store SHA-256 at the same condition:", ""]
        for ds in DATASETS:
            c = lv["cells"][ds]["blake3"]["throughput_change_vs_sha256_pct"]
            L.append(f"- {ds.upper()}: {c[0]:.2f}% [{c[1]:.2f}, {c[2]:.2f}]"
                     f" ({'resolved' if (c[2] < 0 or c[1] > 0) else 'not resolved'})")
        L.append("")
    if clean:
        L += ["## Across latency levels (descriptive only)", "",
              "Median steady-state MiB/s. The 0 ms column is the K38 clean set "
              "(`20261111-v070-minio-cost-clean`): a different session, and without the proxy hop. "
              "No confidence interval or significance is claimed across levels.", "",
              "| Dataset | Variant | 0 ms (clean set) | " + " | ".join(
                  f"+{k.replace('+bw', ' ms, ')}" + (" KB/s" if "bw" in k else " ms") for k in summary["levels"]) + " |",
              "|---|---|---:|" + "---:|" * len(summary["levels"])]
        for ds in DATASETS:
            for vn in VARIANTS:
                m = summary["descriptive_median_mib_s_by_latency"]["cells"][ds][vn]
                L.append(f"| {ds.upper()} | {vn} | {m['0']:.2f} | " + " | ".join(f"{m[k]:.2f}" for k in summary["levels"]) + " |")
        L.append("")
    L += ["## Interpretation constraints", "",
          "- Emulated latency over a local Docker network on one host; not a real WAN, not public-cloud S3.",
          "- No bandwidth limit: the toxic delays data but does not throttle it, so read-back download time is "
          "bounded by local Docker throughput, not by WAN bandwidth. A planned +50 ms, 10 Mbit/s (1250 KB/s each way) "
          "bandwidth-capped level was cancelled by the author for time before it started; no run of it exists."
          if not any(k.endswith("bw1250") for k in summary["levels"]) else
          "- At the latency-only levels there is no bandwidth limit: the toxic delays data but does not throttle it, "
          "so read-back download time is bounded by local Docker throughput. The bandwidth level caps each "
          "direction separately (Toxiproxy bandwidth toxic), so uploads and read-back downloads do not share one cap.",
          "- The engine's TCP handshake to Toxiproxy is local; the latency applies to data in each direction.",
          "- Native-S3 detailed stage fields can be zero in v0.7.0; zero is not treated as a measured stage cost.",
          "- HTTP request counts were not observed and are not inferred; proxy byte counters (per run, counted when "
          "connections close) are observed.",
          "- MinIO image: rebuilt from the same Dockerfile and source commit after the original image was removed "
          "from the host by an outside process (image id differs from the 0 ms set; see ENVIRONMENT-NOTES.md).",
          f"- Runs exposed to foreign CPU load were moved to runs-exposed.jsonl and re-run "
          f"({len(exposed)} runs); see ENVIRONMENT-NOTES.md."]
    (out / "ANALYSIS.md").write_text("\n".join(L) + "\n")
    print(f"Validated {sum(lv['matrix_runs'] for lv in summary['levels'].values())} runs; "
          f"wrote summary.json, rq3_minio_latency.csv, ANALYSIS.md")


if __name__ == "__main__":
    main()
