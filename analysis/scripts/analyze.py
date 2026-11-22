#!/usr/bin/env python3
"""Turn experiment results into statistics, LaTeX tables, and PDF figures, for one build (result set).

  analyze.py [--set v070|v0102|v0104] [--out DIR] [--stamp SHA]

--set v070 (default) reads the v0.7.0 campaign (the v1.0.x paper) and writes, as before,
  analysis/summary.json, analysis/tables/*.tex, analysis/figures/*.pdf.
--set v0102 reads the vtop-engine v0.10.2 campaign (local RQ1-RQ4 re-runs 20261121-v0102-*, the v0.7.0-era
  RQ4 baseline sinks, the cloud campaigns E-RQ5-COST, E-RQ5-WAN, E-RQ5-TAMPER, E-RQ5-SCALE and E-RQ5-PERF, the
  v010 model sweep) and writes
  analysis/v0102/summary.json, analysis/v0102/tables/*.tex, analysis/v0102/figures/*.pdf. It never writes
  the v0.7.0 outputs or paper/.
--set v0104 the same campaign re-run on vtop-engine v0.10.4 (decision D16; results 20261122-v0104-* and the
  2026-11-22 cloud sessions), written to analysis/v0104/.
--out DIR   write summary.json, tables/ and figures/ under DIR instead (e.g. a scratch dir for a reproduction diff).
--stamp SHA the commit recorded as generated_from_commit and in table headers (default: git HEAD).

Inputs per set: SETS below. Every binary-dependent result of a set must come from one VTOP build (asserted).

Statistics: medians with percentile-bootstrap 95% CIs (10,000 resamples, seeded from the data); raw
counts for safety outcomes. Cloud campaigns keep the bootstrap of their own analyzers
(rq3-cost-ablation/code/analyze_minio_cost.py ci/pct_ci, fixed seeds), re-run here from runs.jsonl and
checked against the committed campaign summaries.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("pdf")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

R = Path(__file__).resolve().parents[2]  # research/
EXP = R / "experiments"
RNG = np.random.default_rng(20261110)

# Result sets. Paths are relative to research/experiments/. "extras" switches on the sections that only the
# v0.10.2 set has (power-cut details, batch sizes, cloud, v010 model); the v070 path is byte-for-byte the
# script that produced the submitted paper's summary.json.
SETS = {
    "v070": {
        "label": "v0.7.0", "out": R / "analysis", "run_glob": "*/results/20261110-v070-*/run.json",
        "model": ["rq1-source-safety/results/20261110-model-sweep/sweep.json",
                  "rq1-source-safety/results/20261111-rule-ablation/sweep.json"],
        "crash": "rq1-source-safety/results/20261110-v070-crash",
        "kafka": "rq1-source-safety/results/20261110-v070-kafka",
        "tamper": "rq2-corruption-recovery/results/20261110-v070-tamper",
        "norecheck": "rq2-corruption-recovery/results/20261111-v070-norecheck",
        "cost": "rq3-cost-ablation/results/20261110-v070-cost",
        "cost_run": "rq3-cost-ablation/results/20261110-v070-cost-run",
        "minio": "rq3-cost-ablation/results/20261111-v070-minio-cost-clean",
        "latency": "rq3-cost-ablation/results/20261111-v070-minio-latency",
        "rq4": "rq4-baselines/results/20261111-v070-rq4",
        "rq4_confluent": None,  # v070: the Confluent rows are in the rq4 directory itself
        "rq4_extra": ["rq4-baselines/results/20261111-rq4-vector", "rq4-baselines/results/20261111-rq4-fluentbit"],
        "extras": False,
    },
    "v0102": {
        "label": "v0.10.2", "out": R / "analysis/v0102", "run_glob": "*/results/20261121-v0102-*/run.json",
        "model010": "../state-machine/results/20261120-v010-sweep.json",
        "model010_nonmonotone": "../state-machine/results/20261120-v010-sweep-nonmonotone.json",
        "model010_resume_newest": "../state-machine/results/20261120-v010-sweep-resume-newest.json",
        # Guard-only (v0.6.0 logic) reference rows: model code path unchanged by v010 (sweep reproduced line for line,
        # state-machine/results/20261120-v010-sweep.md); kept as the paper's guard-only contrast.
        "model_reference_v060": "rq1-source-safety/results/20261110-model-sweep/sweep.json",
        "crash": "rq1-source-safety/results/20261121-v0102-crash",
        "kafka": "rq1-source-safety/results/20261121-v0102-kafka",
        "tamper": "rq2-corruption-recovery/results/20261121-v0102-tamper",
        "norecheck": "rq2-corruption-recovery/results/20261121-v0102-norecheck",
        "cost": "rq3-cost-ablation/results/20261121-v0102-cost",
        "cost_run": "rq3-cost-ablation/results/20261121-v0102-cost-run",
        "minio": None, "latency": None,  # no v0.10.2 counterpart (replaced by the cloud campaigns)
        "rq4": "rq4-baselines/results/20261121-v0102-rq4",  # VTOP side only
        # Baseline sinks were not re-run (D4 note in RESULTS.md): their v0.7.0-era trials stand.
        "rq4_confluent": "rq4-baselines/results/20261111-v070-rq4",
        "rq4_extra": ["rq4-baselines/results/20261111-rq4-vector", "rq4-baselines/results/20261111-rq4-fluentbit"],
        "cloud_cost": ["rq5-cloud/cost/results/20261121T002644Z-i-07a97d2f7a612888e",
                       "rq5-cloud/cost/results/20261121T012945Z-i-072d88074e58c7845"],
        # Region order = the order the combined WAN summary was produced in (its bootstrap seeds depend on it).
        "cloud_wan": ["rq5-cloud/wan/results/20261121T013009Z-eu-west-1-i-066226a42ae3e96de",
                      "rq5-cloud/wan/results/20261121T013213Z-ap-southeast-2-i-0bbfd295401175f9f",
                      "rq5-cloud/wan/results/20261121T013213Z-sa-east-1-i-0bc93b4bc564eb2c7",
                      "rq5-cloud/wan/results/20261121T013217Z-ap-northeast-1-i-041c05fa26d6f25c1"],
        "cloud_wan_combined": "rq5-cloud/wan/results/combined/summary.json",
        # E-RQ5-TAMPER (H2.5-H2.7): the Amazon S3 run, and the three local MinIO repetitions it is compared with case by
        # case (lab runs: agreement only, never evidence on their own). The committed AWS summary pairs the AWS run with
        # the first local run; the committed local summary holds the three local runs.
        "cloud_tamper": "rq5-cloud/tamper/results/20261121T143301Z-i-0938589e4d715ff8d",
        "cloud_tamper_local": ["rq5-cloud/tamper/results/20261121-local-minio-v0102",
                               "rq5-cloud/tamper/results/20261121-local-minio-v0102-rep2",
                               "rq5-cloud/tamper/results/20261121-local-al2023-linux-v0102"],
        # E-RQ5-SCALE (H3.7): 1, 2, 4, 8 independent engines (one ledger each) on t4g.large hosts, one bucket.
        "cloud_scale": "rq5-cloud/scale/results/20261121T153823Z-scale",
        # E-RQ5-PERF (H4.2): VTOP (three verification modes + a gzip-6 variant) vs Confluent S3 Sink, Vector and
        # Fluent Bit on 2 x t4g.large per leg; us-east-1 (with the storage-fault phase, D13) and Sydney -> us-east-1.
        # Leg order = the order the combined summary was produced in (its bootstrap seeds depend on it).
        "cloud_perf": ["rq5-cloud/perf/results/20261121T144252Z-use1-broker-i-0d79946597ff9b865/results",
                       "rq5-cloud/perf/results/20261121T150129Z-apse2-broker-i-0f10944cf30cfb95c/results"],
        "cloud_perf_reps": ["tput=5,live=3,kill=3", "tput=3,live=2,kill=2"],
        "cloud_perf_combined": "rq5-cloud/perf/results/combined",
        "extras": True,
    },
    # Decision D16 (2026-11-22): every local and cloud campaign re-run on vtop-engine v0.10.4 (be5b419), same scripts,
    # seeds and plans as v0.10.2. Model results are shared: v0.10.4 rewinds once per unit to the lowest failed start,
    # which is the model's default rule (state-machine/v0104-repin-notes.md). Baseline sinks as in v0102.
    "v0104": {
        "label": "v0.10.4", "out": R / "analysis/v0104", "run_glob": "*/results/20261122-v0104-*/run.json",
        "model010": "../state-machine/results/20261120-v010-sweep.json",
        "model010_nonmonotone": "../state-machine/results/20261120-v010-sweep-nonmonotone.json",
        "model010_resume_newest": "../state-machine/results/20261120-v010-sweep-resume-newest.json",
        # Single-rule ablation of the v010 model (B7, 2026-11-21): no hand-back, no read-order commit. Rows
        # "No hand-back" and "No read-order" of rq1_model.tex and summary rq1_model_ablation (C41; before C41 these two
        # table rows were added by hand and a re-run dropped them).
        "model010_ablation": "../state-machine/results/20261121-v010-rule-ablation.json",
        "model_reference_v060": "rq1-source-safety/results/20261110-model-sweep/sweep.json",
        "crash": "rq1-source-safety/results/20261122-v0104-crash",
        "kafka": "rq1-source-safety/results/20261122-v0104-kafka",
        "tamper": "rq2-corruption-recovery/results/20261122-v0104-tamper",
        "norecheck": "rq2-corruption-recovery/results/20261122-v0104-norecheck",
        "cost": "rq3-cost-ablation/results/20261122-v0104-cost",
        "cost_run": "rq3-cost-ablation/results/20261122-v0104-cost-run",
        "minio": None, "latency": None,
        "rq4": "rq4-baselines/results/20261122-v0104-rq4",  # VTOP side only
        "rq4_confluent": "rq4-baselines/results/20261111-v070-rq4",
        "rq4_extra": ["rq4-baselines/results/20261111-rq4-vector", "rq4-baselines/results/20261111-rq4-fluentbit"],
        "cloud_cost": ["rq5-cloud/cost/results/20261122T021852Z-i-0d197e09de6af68de",
                       "rq5-cloud/cost/results/20261122T041424Z-i-03c2592cd984f1276"],
        "cloud_wan": ["rq5-cloud/wan/results/20261122T021853Z-eu-west-1-i-036821b8632514571",
                      "rq5-cloud/wan/results/20261122T021917Z-ap-southeast-2-i-0819b6cd4872adbb3",
                      "rq5-cloud/wan/results/20261122T021935Z-sa-east-1-i-0093971b23ec6758c",
                      "rq5-cloud/wan/results/20261122T021956Z-ap-northeast-1-i-047cf9c9a10e500d0"],
        "cloud_wan_combined": "rq5-cloud/wan/results/combined-v0104/summary.json",
        "cloud_tamper": "rq5-cloud/tamper/results/20261122T021852Z-i-0e178e36bc660a51f",
        "cloud_tamper_local": ["rq5-cloud/tamper/results/20261122-local-minio-v0104",
                               "rq5-cloud/tamper/results/20261122-local-minio-v0104-rep2",
                               "rq5-cloud/tamper/results/20261122-local-al2023-linux-v0104"],
        "cloud_scale": "rq5-cloud/scale/results/20261122T032729Z-scale",
        "cloud_perf": ["rq5-cloud/perf/results/20261122T041404Z-use1-broker-i-009228b687a1a12c0/results",
                       "rq5-cloud/perf/results/20261122T032729Z-apse2-broker-i-0867590bfc4cd5b2a/results"],
        "cloud_perf_reps": ["tput=5,live=3,kill=3", "tput=3,live=2,kill=2"],
        "cloud_perf_combined": "rq5-cloud/perf/results/combined-v0104",
        # E-RQ5-TAMPER expectations were written for v0.10.2. #882 (v0.10.3, commit 77579228, commands.rs
        # verify_manifest_deep step "1b"): verify-manifest fails a manifest read from another key than the one it names,
        # so the M4 swap, a stated limit on v0.10.2, is now caught by verify-manifest (observed "failed" on AWS and in all
        # three local runs).
        "tamper_engine_changes": {
            ("MAC-SWAP-OFF", "vm_noledger"): "#882 (v0.10.3): verify-manifest binds a manifest to the key it names",
            ("MAC-SWAP-OFF", "vm_ledger"): "#882 (v0.10.3): verify-manifest binds a manifest to the key it names",
            ("MAC-SWAP-ON", "vm_noledger"): "#882 (v0.10.3): verify-manifest binds a manifest to the key it names",
            ("MAC-SWAP-ON", "vm_ledger"): "#882 (v0.10.3): verify-manifest binds a manifest to the key it names",
        },
        "extras": True,
    },
}

# Validated categorical palette (dataviz reference, light mode) + hatches for print.
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
HATCH = ["", "///", "...", "\\\\\\", "xx"]
plt.rcParams.update({
    "font.family": "serif", "font.size": 7.5, "axes.labelsize": 7.5, "legend.fontsize": 6.5,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#52514e", "axes.linewidth": 0.6, "xtick.color": "#52514e", "ytick.color": "#52514e",
    "axes.grid": True, "grid.color": "#e4e3df", "grid.linewidth": 0.5, "axes.axisbelow": True,
    "hatch.linewidth": 0.5, "pdf.fonttype": 42, "figure.dpi": 300,
})
W1 = 3.5  # IEEE single column (in)
summary: dict = {}
# Set by configure(): the result set, its output directories and the commit stamp.
SET, C, OUT, OUT_T, OUT_F, GIT = "v070", SETS["v070"], None, None, None, ""


def configure(set_name: str, out: str | None, stamp: str | None):
    global SET, C, OUT, OUT_T, OUT_F, GIT
    SET, C = set_name, SETS[set_name]
    OUT = Path(out).resolve() if out else C["out"]
    OUT_T, OUT_F = OUT / "tables", OUT / "figures"
    OUT_T.mkdir(parents=True, exist_ok=True)
    OUT_F.mkdir(parents=True, exist_ok=True)
    GIT = stamp or subprocess.run(["git", "-C", str(R), "rev-parse", "--short", "HEAD"], capture_output=True,
                                  text=True).stdout.strip()
    summary["generated_from_commit"] = GIT
    if SET != "v070":
        summary["result_set"] = SET
    # Every binary-dependent result must come from the same VTOP build.
    _commits = {json.loads(p.read_text())["vtop_commit"] for p in EXP.glob(C["run_glob"])}
    assert len(_commits) == 1, f"mixed VTOP builds in results: {_commits}"
    summary["vtop_commit"] = _commits.pop()


def save(fig, name):
    fig.savefig(OUT_F / name, metadata={"CreationDate": None})  # no timestamp: same data, same bytes
    plt.close(fig)


def boot_median(x, n=10000):
    x = np.asarray(x, dtype=float)
    if len(x) == 0:
        return (np.nan, np.nan, np.nan)
    if len(x) == 1:
        return (x[0], x[0], x[0])
    # Seed from the data itself (C7 audit): the same sample always yields the same CI, so a table,
    # summary.json and a figure that bootstrap one cell can never disagree.
    seed = int.from_bytes(hashlib.sha256(np.ascontiguousarray(x).tobytes()).digest()[:8], "little")
    idx = np.random.default_rng([20261110, seed]).integers(0, len(x), size=(n, len(x)))
    meds = np.median(x[idx], axis=1)
    return (float(np.median(x)), float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5)))


def boot_rel_drop(base, x, n=10000):
    """Relative drop of median(x) below median(base), in percent, with a 95% bootstrap CI.

    The two samples are independent runs, so each is resampled on its own. The RNG is seeded from
    both samples, as in boot_median."""
    base, x = np.asarray(base, dtype=float), np.asarray(x, dtype=float)
    pt = (np.median(base) - np.median(x)) / np.median(base) * 100
    seed = int.from_bytes(hashlib.sha256(np.ascontiguousarray(np.concatenate([base, x])).tobytes()).digest()[:8], "little")
    rng = np.random.default_rng([20261111, seed])
    mb = np.median(base[rng.integers(0, len(base), size=(n, len(base)))], axis=1)
    mx = np.median(x[rng.integers(0, len(x), size=(n, len(x)))], axis=1)
    d = (mb - mx) / mb * 100
    return (round(float(pt), 1), round(float(np.percentile(d, 2.5)), 1), round(float(np.percentile(d, 97.5)), 1))


def header(script_note: str) -> str:
    tag = "" if SET == "v070" else f" (--set {SET})"
    return f"% generated by research/analysis/scripts/analyze.py at commit {GIT}{tag}; {script_note}\n"


def jl(path: Path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


# ===================================================================== RQ1-M
def rq1_model():
    p = EXP / C["model"][0]
    if not p.exists():
        return
    rows = json.loads(p.read_text())
    # Single-rule ablation (2026-11-11): no hand-back, no read-order; same record format.
    abl = EXP / C["model"][1]
    if abl.exists():
        rows += json.loads(abl.read_text())
    names = {"v070": "Protocol", "v060": "Guard-only", "nohandback": "No hand-back", "noorder": "No read-order"}
    lines = [header("E-RQ1-M model sweep"),
             r"\begin{tabular}{@{}lccccrl@{}}", r"\toprule",
             r"Design & $N$ & $B$ & Faults$^a$ & States & D1$^b$ & Result \\", r"\midrule"]
    out = []
    for r in rows:
        p_ = r["params"]
        faults = f"{p_['fails']}/{p_['crashes']}/{p_['ledger_loss']}/{p_['mutations']}"
        res = "holds" if r["violation"] is None else f"{r['violation']['property']} violated"
        lines.append(f"{names[p_['variant']]} & {p_['ranges']} & {p_['per_cycle']} & {faults} & "
                     f"{r['states']:,} & {r.get('unrepairable_states', 0):,} & {res} \\\\")
        out.append({**p_, "states": r["states"], "result": res, "D1_states": r.get("unrepairable_states", 0),
                    "D2_states": r.get("damaged_commit_states", 0), "max_duplicates": r["max_duplicates"]})
    lines += [r"\bottomrule", r"\end{tabular}"]
    # Full per-configuration table: artifact/supplement only (C15).
    (OUT_T / "rq1_model_full.tex").write_text("\n".join(lines) + "\n")
    summary["rq1_model"] = out
    # Paper table (C15): one row per (logic, B); results-in-figures/tables-for-claims convention.
    groups = {}
    for o in out:
        groups.setdefault((o["variant"], o["per_cycle"]), []).append(o)
    order = [("v070", 1), ("v060", 1), ("v070", 2), ("nohandback", 1), ("noorder", 1)]
    t = [header("E-RQ1-M model sweep, grouped (full table: rq1_model_full.tex)"),
         r"\begin{tabular}{@{}lcrrrr@{}}", r"\toprule",
         r"Design & $B$ & Configs & States & Violated & Smallest$^a$ \\", r"\midrule"]
    for key in order:
        g = groups.get(key, [])
        if not g:
            continue
        viol = [o for o in g if o["result"] != "holds"]
        smallest = f"{min(o['states'] for o in viol):,}" if viol else "--"
        st = [o["states"] for o in g]
        t.append(f"{names[key[0]]} & {key[1]} & {len(g)} & "
                 f"{min(st):,}--{max(st):,} & {len(viol)} & {smallest} \\\\")
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq1_model.tex").write_text("\n".join(t) + "\n")


# ===================================================================== RQ1-F
def rq1_crash():
    rows = jl(EXP / C["crash"] / "trials.jsonl")
    for r in jl(EXP / C["kafka"] / "trials.jsonl"):
        r["lost_bytes"] = r["lost_records"]  # Kafka: unit is records, not bytes
        for e in r["events"]:
            e.setdefault("first_commit_s", None); e.setdefault("wal_deleted", False)
        rows.append(r)
    if not rows:
        return
    by = defaultdict(list)
    for r in rows:
        by[r["scenario"]].append(r)
    order = [s for s in ["file-mix", "spool-mix", "power-loss", "hdfs", "kafka-ssh-p1", "kafka-hdfs-p4"] if s in by]
    lines = [header("E-RQ1-F crash campaign"), r"\begin{tabular}{@{}lrrrrrr@{}}", r"\toprule",
             r"Scenario & Trials & Kills & Unsafe & Compl.$^c$ & Dup.\ obj.$^a$ & Amp.$^b$ \\", r"\midrule"]
    out = {}
    for s in order:
        rs = by[s]
        kills = sum(r["kills"] for r in rs)
        unsafe = sum(r["unsafe_commits_at_crash"] + r["unsafe_commits_final"] for r in rs)
        # Not loss: data the archive lacks at the end of the final drain while the
        # source has NOT committed past it (it stays readable at the source).
        # Data committed past the archive is what "unsafe" counts.
        unarchived = sum(r["lost_bytes"] for r in rs)
        complete = sum(r["complete"] for r in rs)
        dup = [r["duplicate_objects"] for r in rs]
        amp = [r["replay_amplification"] * 100 for r in rs]
        dm, amp_m = boot_median(dup), boot_median(amp)
        firsts = [e["first_commit_s"] for r in rs for e in r["events"] if e["first_commit_s"] is not None and e["life"] > 0]
        out[s] = {"trials": len(rs), "kills": kills, "unsafe_commits": unsafe, "unarchived_at_end_uncommitted": unarchived,
                  "complete_trials": complete, "bad_objects": sum(r["bad_objects_final"] for r in rs),
                  "dup_objects_median_ci": dm, "dup_objects_max": max(dup),
                  "replay_amp_pct_median_ci": amp_m, "first_commit_after_restart_s_median_ci": boot_median(firsts),
                  "manifest_start_understated": sum(r["manifest_start_understated"] for r in rs),
                  "manifests": sum(r["manifests"] for r in rs),
                  "outages": sum(1 for r in rs for e in r["events"] if e["outage"]),
                  "wal_deleted": sum(1 for r in rs for e in r["events"] if e["wal_deleted"])}
        if C["extras"]:
            # v0.10.x power-loss emulation (vtoplab.power_loss_truncate): the WAL is truncated after the last
            # commit that wrote VERIFIED/SOURCE_COMMITTED (fsynced); later frames are dropped.
            cuts = [e["power_cut"] for r in rs for e in r["events"] if e.get("power_cut")]
            out[s].update({"power_cuts": len(cuts),
                           "power_cuts_dropping_commits": sum(1 for c in cuts if c["dropped_commits"] > 0),
                           "power_cut_dropped_commits_total": sum(c["dropped_commits"] for c in cuts),
                           "power_cuts_empty_wal": sum(1 for c in cuts if c["wal_bytes"] == 0),
                           "duplicate_bytes_total": sum(r.get("duplicate_bytes", 0) for r in rs),
                           "duplicate_objects_total": sum(dup),
                           "final_drain_s_median_ci": boot_median([r["final_drain_s"] for r in rs
                                                                   if r.get("final_drain_s") is not None])})
        lines.append(f"{s} & {len(rs)} & {kills} & {unsafe} & {complete}/{len(rs)} & {dm[0]:.0f} [{dm[1]:.0f},{dm[2]:.0f}] & "
                     f"{amp_m[0]:.1f}\\% \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq1_crash.tex").write_text("\n".join(lines) + "\n")
    summary["rq1_crash"] = out

    # figure: duplicate objects per trial, per scenario (strip + median)
    fig, ax = plt.subplots(figsize=(W1, 1.8))
    for i, s in enumerate(order):
        y = [r["duplicate_objects"] for r in by[s]]
        x = i + RNG.uniform(-0.12, 0.12, len(y))
        ax.scatter(x, y, s=10, color=COLORS[0], edgecolor="white", linewidth=0.4, zorder=3)
        ax.hlines(np.median(y), i - 0.25, i + 0.25, color="#0b0b0b", linewidth=1.2, zorder=4)
    nice = {"file-mix": "Files\n(mix)", "spool-mix": "Spools\n(mix)", "power-loss": "Power\nloss", "hdfs": "HDFS\nfile",
            "kafka-ssh-p1": "Kafka\nSSH", "kafka-hdfs-p4": "Kafka\nHDFS"}
    ax.set_xticks(range(len(order)), [nice.get(o, o) for o in order])
    ax.set_ylabel("Duplicate objects per trial")
    ax.set_ylim(bottom=0)
    fig.tight_layout(pad=0.3)
    save(fig, "rq1-duplicates.pdf")


# ===================================================================== RQ2
def rq2():
    rows = jl(EXP / C["tamper"] / "trials.jsonl")
    if not rows:
        return
    names = {"M0": "None (control)", "M1": "Flip one object byte", "M2": "Truncate object", "M3": "Delete object",
             "M4": "Replace object (other batch)", "M5": "Edit manifest, stale self-hash",
             "M6": "Edit manifest, recompute self-hash", "M7": "Forge object+manifest, recompute", "M8": "Delete manifest"}
    variants = ["strong", "strong+mac", "size-only"]
    cell = defaultdict(lambda: [0, 0, 0])  # detected, trials, complete-after
    for r in rows:
        c = cell[(r["mutation"], r["variant"])]
        c[0] += r["outcome"] == "detected"
        c[1] += 1
        c[2] += r["archive_complete"]
    lines = [header("E-RQ2-R tampering while down"), r"\begin{tabular}{@{}lccc@{}}", r"\toprule",
             r"Mutation & Strong & Strong+MAC & Size-only \\", r"\midrule"]
    out = {}
    for m in sorted(names):
        cols = []
        for v in variants:
            d, n, comp = cell[(m, v)]
            cols.append(f"{d}/{n}" if n else "--")
            out.setdefault(m, {})[v] = {"detected": d, "trials": n, "archive_complete_after": comp}
        lines.append(f"{m} {names[m]} & " + " & ".join(cols) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq2_tamper_full.tex").write_text("\n".join(lines) + "\n")
    # Paper table (C15): mutations grouped by what they change.
    grp = [("M0 none (control)", ["M0"]), ("M1 object byte flip", ["M1"]),
           ("M2--M4 object truncated, deleted, replaced", ["M2", "M3", "M4"]),
           ("M5--M8 manifest edited, forged, deleted", ["M5", "M6", "M7", "M8"])]
    t = [header("E-RQ2-R tampering while down, grouped (full table: rq2_tamper_full.tex)"),
         r"\begin{tabular}{@{}lccc@{}}", r"\toprule",
         r"Mutation & Strong & Strong+MAC & Size-only \\", r"\midrule"]
    for label, ms in grp:
        cols = []
        for v in variants:
            d = sum(cell[(m, v)][0] for m in ms); n = sum(cell[(m, v)][1] for m in ms)
            cols.append(f"{d}/{n}" if n else "--")
        t.append(f"{label} & " + " & ".join(cols) + r" \\")
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq2_tamper.tex").write_text("\n".join(t) + "\n")
    summary["rq2"] = {"cells": out, "trials": len(rows),
                      "selfhash_reimpl_ok": all(r["selfhash_reimpl_ok"] for r in rows),
                      "unsafe_commits": sum(r["unsafe_commit"] for r in rows),
                      "errors_by_mutation": {m: sorted({(r["error"] or "")[:90] for r in rows if r["mutation"] == m and r["error"]}) for m in names}}


# ===================================================================== RQ3
def rq3():
    runs = jl(EXP / C["cost"] / "runs.jsonl")
    bpath = EXP / C["cost"] / "batches.csv"
    if not runs:
        return
    A = [r for r in runs if "batch" not in r["variant"]]
    B = [r for r in runs if "batch" in r["variant"]]
    datasets = [d for d in ["linux", "ssh", "bgl", "hdfs"] if any(r["dataset"] == d for r in A)]
    variants = ["size-only", "sha256", "blake3", "sha256+mac", "sha256-nocomp"]
    tput = defaultdict(list)
    cpu = defaultdict(list)
    # Throughput and CPU come from the continuous engine (vtopctl run) when that
    # series exists; process-once pays a process start per batch.
    runmode = jl(EXP / C["cost_run"] / "runs.jsonl")
    src = [r for r in runmode if r.get("drained")] or A
    summary["rq3_throughput_source"] = "vtopctl run (continuous)" if runmode else "vtopctl process-once"
    for r in src:
        tput[(r["dataset"], r["variant"])].append(r["mib_per_s"])
        cpu[(r["dataset"], r["variant"])].append(r["cpu_s"] / (r["bytes"] / 2**30))  # CPU s per GiB
    rss = defaultdict(list)
    for r in src:
        rss[(r["dataset"], r["variant"])].append(r["max_rss_mib"])
    out = {"throughput_mib_s": {}, "cpu_s_per_gib": {}, "oracle_checks": [r.get("oracle") for r in runs if r.get("oracle")]}
    out["max_rss_mib"] = {}
    for k, v in tput.items():
        out["throughput_mib_s"]["|".join(k)] = boot_median(v)
        out["cpu_s_per_gib"]["|".join(k)] = boot_median(cpu[k])
        out["max_rss_mib"]["|".join(k)] = boot_median(rss[k])

    # Figure: throughput, grouped bars (datasets x variants), medians with CI
    big = [d for d in datasets if d != "linux"]
    fig, ax = plt.subplots(figsize=(W1, 2.0))
    w = 0.16
    for j, v in enumerate(variants):
        meds, lo, hi = [], [], []
        for d in big:
            m, l, h = boot_median(tput[(d, v)])
            meds.append(m); lo.append(m - l); hi.append(h - m)
        x = np.arange(len(big)) + (j - 2) * w
        ax.bar(x, meds, w * 0.92, color=COLORS[j], hatch=HATCH[j], edgecolor="white", linewidth=0.3, label=v)
        ax.errorbar(x, meds, yerr=[lo, hi], fmt="none", ecolor="#0b0b0b", elinewidth=0.6, capsize=1.2)
    ax.set_xticks(range(len(big)), [d.upper() if d != "ssh" else "SSH" for d in big])
    ax.set_ylabel("Throughput (MiB/s)")
    # full scale with a labelled top tick, so every bar's end can be read against the grid
    ax.set_ylim(0, 400)
    ax.set_yticks(range(0, 401, 100))
    ax.yaxis.grid(True, color="#e6e5e0", linewidth=0.5)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.legend(ncol=3, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.30), handlelength=1.2, columnspacing=0.8)
    fig.tight_layout(pad=0.3)
    save(fig, "rq3-throughput.pdf")

    # Stage breakdown from per-batch rows (16 MiB batches, all reps)
    stages = ["compress_ms", "checksum_ms", "object_upload_ms", "manifest_upload_ms", "verify_ms", "commit_ms", "state_write_ms"]
    agg = defaultdict(lambda: defaultdict(list))
    ratio = defaultdict(list)
    with open(bpath) as fh:
        for row in csv.DictReader(fh):
            if row["batch_mib"] != "16" or "batch" in row["variant"]:
                continue
            for s in stages:
                agg[(row["dataset"], row["variant"])][s].append(float(row[s] or 0))
            if row["variant"] != "sha256-nocomp":
                ratio[row["dataset"]].append(int(row["uncompressed_bytes"]) / int(row["compressed_bytes"]))
    # zstd size reduction per 16 MiB batch (median over batches), quoted in Section VII-D
    out["compression_ratio_median"] = {d: round(float(np.median(ratio[d])), 1) for d in big}
    stage_out = {}
    for d in big:
        for v in variants:
            stage_out[f"{d}|{v}"] = {s: float(np.mean(agg[(d, v)][s])) if agg[(d, v)][s] else None for s in stages}
    out["stage_mean_ms_per_batch"] = stage_out
    # figure for HDFS and BGL: stacked horizontal bars per variant
    fig, axes = plt.subplots(1, 2, figsize=(W1, 1.9), sharey=True)
    labels = {"compress_ms": "compress", "checksum_ms": "checksum", "object_upload_ms": "put object",
              "manifest_upload_ms": "put manifest", "verify_ms": "verify", "commit_ms": "commit", "state_write_ms": "ledger"}
    stage_colors = COLORS + ["#6250d6", "#008300"]
    stage_hatch = HATCH + ["--", "oo"]
    for ax, d in zip(axes, [x for x in ["bgl", "hdfs"] if x in big]):
        left = np.zeros(len(variants))
        for k, s in enumerate(stages):
            vals = np.array([stage_out[f"{d}|{v}"][s] or 0 for v in variants])
            ax.barh(range(len(variants)), vals, left=left, color=stage_colors[k], hatch=stage_hatch[k],
                    edgecolor="white", linewidth=0.3, label=labels[s] if d == "bgl" else None)
            left += vals
        ax.set_title(d.upper(), fontsize=7.5)
        ax.set_xlabel("ms per 16 MiB batch")
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(range(len(variants)), variants)
    axes[0].invert_yaxis()
    fig.tight_layout(pad=0.3, rect=(0, 0, 1, 0.78))
    fig.legend(ncol=4, frameon=False, loc="upper center", bbox_to_anchor=(0.55, 1.0), handlelength=1.0, columnspacing=0.6)
    save(fig, "rq3-stages.pdf")

    # Batch-size sweep: throughput from the continuous engine (per dataset) when
    # available; manifest overhead from the process-once sweep (manifest bytes).
    Brun = [r for r in runmode if "batch" in r["variant"] and r.get("drained")]
    if B or Brun:
        sizes = sorted({r["batch_mib"] for r in (Brun or B)})
        mo = [boot_median([r["manifest_bytes"] / r["compressed_bytes"] * 100 for r in B if r["batch_mib"] == s]) for s in sizes] if B else None
        out["batch_sweep"] = {"throughput_source": "vtopctl run" if Brun else "process-once", "datasets": {}}
        fig, axes = plt.subplots(1, 2, figsize=(W1, 1.6))
        dsets = sorted({r["dataset"] for r in Brun}) if Brun else ["bgl"]
        for j, d in enumerate(dsets):
            rows_d = [r for r in (Brun or B) if r["dataset"] == d]
            tp = [boot_median([r["mib_per_s"] for r in rows_d if r["batch_mib"] == s]) for s in sizes]
            out["batch_sweep"]["datasets"][d] = {str(s): t for s, t in zip(sizes, tp)}
            m = [t[0] for t in tp]
            axes[0].plot(sizes, m, color=COLORS[j], linewidth=2, marker="o" if j == 0 else "s", markersize=4,
                         markeredgecolor="white", markeredgewidth=0.6, label=d.upper())
            axes[0].fill_between(sizes, [t[1] for t in tp], [t[2] for t in tp], color=COLORS[j], alpha=0.18, linewidth=0)
        axes[0].set_ylabel("Throughput (MiB/s)")
        if len(dsets) > 1:
            axes[0].legend(frameon=False, loc="lower right", handlelength=1.4)
        if mo:
            out["batch_sweep"]["manifest_overhead_pct"] = {str(s): t for s, t in zip(sizes, mo)}
            axes[1].plot(sizes, [t[0] for t in mo], color=COLORS[2], linewidth=2, marker="o", markersize=4,
                         markeredgecolor="white", markeredgewidth=0.6)
            axes[1].set_yscale("log")
            axes[1].set_ylabel("Manifest bytes\n(% of stored objects)")
        for ax in axes:
            ax.set_xscale("log", base=2)
            ax.set_xticks(sizes, [str(s) for s in sizes])
            ax.set_xlabel("Batch target (MiB)")
        axes[0].set_ylim(bottom=0)
        fig.tight_layout(pad=0.3)
        save(fig, "rq3-batchsize.pdf")

    # Table: throughput + CPU per GiB per dataset/variant
    lines = [header("E-RQ3-A; continuous engine, medians of 10 runs, [95% bootstrap CI]"), r"\begin{tabular}{@{}llrr@{}}", r"\toprule",
             r"Dataset & Variant & MiB/s & CPU s/GiB \\", r"\midrule"]
    for d in big:
        for v in variants:
            m, l, h = boot_median(tput[(d, v)])
            c = boot_median(cpu[(d, v)])[0]
            lines.append(f"{d.upper()} & {v} & {m:.1f} [{l:.1f}, {h:.1f}] & {c:.2f} \\\\")
        lines.append(r"\addlinespace")
    lines[-1] = r"\bottomrule"
    lines.append(r"\end{tabular}")
    (OUT_T / "rq3_cost.tex").write_text("\n".join(lines) + "\n")
    # Relative overheads vs size-only, per dataset
    rel = {}
    for d in big:
        base = boot_median(tput[(d, "size-only")])[0]
        rel[d] = {v: round((base - boot_median(tput[(d, v)])[0]) / base * 100, 1) for v in variants}
    out["throughput_drop_vs_size_only_pct"] = rel
    # The same drop with a 95% bootstrap CI on the difference of medians (C7 audit): overlapping
    # per-variant CIs do not show "no effect", so the claim is tested on the difference itself.
    out["throughput_drop_vs_size_only_ci"] = {
        d: {v: boot_rel_drop(tput[(d, "size-only")], tput[(d, v)]) for v in variants if v != "size-only"}
        for d in big}
    out["mac_drop_vs_sha256_ci"] = {d: boot_rel_drop(tput[(d, "sha256")], tput[(d, "sha256+mac")]) for d in big}
    summary["rq3"] = out


def rq4():
    """E-RQ4-B: VTOP v0.7.0 vs Confluent S3 Sink under storage faults and kills (C13)."""
    d = EXP / C["rq4"]
    runs = jl(d / "trials.jsonl")
    if not runs:
        return
    meta = json.loads((d / "run.json").read_text())
    assert meta["vtop_commit"] == summary["vtop_commit"], "RQ4 used a different VTOP build"
    if C["rq4_confluent"]:
        # VTOP from this build; the Confluent rows from the v0.7.0-era run (sinks not re-run). The VTOP rows of that
        # run are dropped, so no VTOP number below mixes builds.
        assert all(r["system"] == "vtop" for r in runs), "this build's RQ4 directory has non-VTOP rows"
        dc = EXP / C["rq4_confluent"]
        mc = json.loads((dc / "run.json").read_text())
        runs += [r for r in jl(dc / "trials.jsonl") if r["system"] == "confluent"]
        out = {"confluent_s3_sink": mc["confluent_s3_sink"], "images": {**mc["images"], **meta["images"]}, "cells": {},
               "baseline_sources": {"vtop": C["rq4"], "confluent": C["rq4_confluent"], "others": C["rq4_extra"]},
               # Same MinIO release and commit, but the VTOP side ran on a rebuilt image (different digest).
               "minio_image_vtop": meta["images"].get("vtop-minio:RELEASE.2025-09-07T16-13-09Z"),
               "minio_image_baselines": mc["images"].get("vtop-minio:RELEASE.2025-09-07T16-13-09Z")}
    else:
        out = {"confluent_s3_sink": meta["confluent_s3_sink"], "images": meta["images"], "cells": {}}
    # C23 (2026-11-11): Vector and Fluent Bit under the same faults, proxy, topics and oracle.
    for extra in C["rq4_extra"]:
        de = EXP / extra
        if (de / "trials.jsonl").exists():
            runs += jl(de / "trials.jsonl")
            out.setdefault("extra_images", {}).update(json.loads((de / "run.json").read_text())["images"])
    order = [("corrupt", "Ack., corrupted"), ("lost", "Ack., lost"), ("kill", "SIGKILL")]
    lines = [header(f"E-RQ4-B; VTOP {C['label']} vs Confluent S3 Sink, Vector, Fluent Bit; same Kafka topic, proxy and oracle"),
             r"\begin{tabular}{@{}llrrrrr@{}}", r"\toprule",
             r"Fault & System & Trials & Unsafe & Lost rec.$^a$ & Compl. & Dup.\ obj.$^b$ \\", r"\midrule"]
    for f, label in order:
        for sysname, pretty in (("vtop", r"\sysname"), ("confluent", "Confluent"), ("vector", "Vector"),
                                ("fluentbit", "Fluent Bit")):
            every = [r for r in runs if r["fault"] == f and r["system"] == sysname]
            # A storage-fault trial in which the faulted write was never reached (the sink stalled or
            # finished first) tests nothing about that fault; it is counted, not used.
            applied = (lambda r: any("fault" in e for e in r["proxy_events"])) if f != "kill" else (lambda r: r["kills"] > 0)
            rs = [r for r in every if applied(r)]
            if not every:
                continue
            # Unsafe = the committed offset is past the intact archive in the final state. A commit that was
            # ahead of the archive at a kill but covered after restart is counted separately (transient).
            unsafe = sum(r["unsafe_final"] > 0 for r in rs)
            transient = sum(r["unsafe_final"] == 0 and r["unsafe_at_kill"] > 0 for r in rs)
            lost = [r["lost_committed_records"] for r in rs]
            dup = [r["duplicate_objects"] for r in rs]
            transient_events = sum(1 for r in rs if r["unsafe_final"] == 0
                                   for e in r.get("events", []) if e.get("unsafe", 0) > 0)
            cell = {"trials": len(rs), "unsafe_trials": unsafe, "transient_at_kill_trials": transient,
                    "transient_at_kill_events": transient_events,
                    "lost_committed_records_total": int(sum(lost)),
                    "lost_committed_records_median": float(np.median(lost)), "complete": sum(r["complete"] for r in rs),
                    "duplicate_objects_median": float(np.median(dup)), "kills": sum(r["kills"] for r in rs),
                    "elapsed_s_median": float(np.median([r["elapsed_s"] for r in rs])),
                    "faults_applied": sum(1 for r in rs for e in r["proxy_events"] if "fault" in e),
                    "proxy_errors": sum(1 for r in rs for e in r["proxy_events"] if "error" in e),
                    "trials_run": len(every), "fault_not_reached": len(every) - len(rs),
                    "incomplete_all": sum(not r["complete"] for r in every)}
            out["cells"][f"{f}|{sysname}"] = cell
            comp = sum(r["complete"] for r in rs)
            lines.append(f"{label} & {pretty} & {len(rs)} & {unsafe} & {int(sum(lost)):,} & {comp}/{len(rs)} & {np.median(dup):.0f} \\\\".replace(",", "{,}"))
        lines.append(r"\addlinespace")
    lines[-1] = r"\bottomrule"
    lines.append(r"\end{tabular}")
    (OUT_T / "rq4_baselines_full.tex").write_text("\n".join(lines) + "\n")
    # Paper table: one row per system; unsafe trials per fault, then totals (same cells as the full table).
    c = out["cells"]
    t = [header("E-RQ4-B compact (full table: rq4_baselines_full.tex)"),
         r"\begin{tabular}{@{}lcccrcr@{}}", r"\toprule",
         r"& \multicolumn{3}{c}{Unsafe / trials run} & & & \\ \cmidrule(lr){2-4}",
         r"System & Corrupted & Deleted & Killed & Lost rec.$^a$ & Compl. & Dup.$^b$ \\", r"\midrule"]
    for sysname, pretty in (("vtop", r"\sysname"), ("confluent", "Confluent"), ("vector", "Vector"), ("fluentbit", "Fluent Bit")):
        cs = [c.get(f"{f}|{sysname}") for f in ("corrupt", "lost", "kill")]
        if None in cs:
            continue
        lost = cs[0]["lost_committed_records_total"] + cs[1]["lost_committed_records_total"]
        comp = sum(x["complete"] for x in cs); n = sum(x["trials"] for x in cs)
        t.append(f"{pretty} & {cs[0]['unsafe_trials']}/{cs[0]['trials']} & {cs[1]['unsafe_trials']}/{cs[1]['trials']} & "
                 f"{cs[2]['unsafe_trials']}/{cs[2]['trials']} & {lost:,} & {comp}/{n} & {cs[2]['duplicate_objects_median']:.0f} \\\\"
                 .replace(",", "{,}"))
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq4_baselines.tex").write_text("\n".join(t) + "\n")
    v = [r for r in runs if r["system"] == "vtop"]
    out["vtop_trials_run"] = len(v)
    out["vtop_incomplete"] = sum(not r["complete"] for r in v)        # every one hit the drain timeout: a stall
    out["vtop_incomplete_all_timed_out"] = all(r["elapsed_s"] >= 899 for r in v if not r["complete"])
    out["vtop_stalls_before_fault"] = sum(c["fault_not_reached"] for k, c in out["cells"].items() if k.endswith("|vtop"))
    if C["extras"]:
        out["vtop_elapsed_s_max"] = max(r["elapsed_s"] for r in v)
        out["vtop_duplicate_records_total"] = sum(r.get("duplicate_records", 0) for r in v)
    summary["rq4"] = out


def rq3_minio():
    """E-RQ3-A on S3 over a local Docker network (K37 + K38 clean re-run of the 60 overlap-exposed runs)."""
    if not C["minio"]:
        return
    p = EXP / C["minio"] / "summary.json"
    if not p.exists():
        return
    d = json.loads(p.read_text())
    summary["rq3_minio"] = {"matrix_runs": d["matrix_runs"], "oracle_runs": d["oracle_runs"],
                            "change_vs_size_only_pct": {ds: {v: c["throughput_drop_vs_size_only_pct"]
                                                            for v, c in cells.items()}
                                                        for ds, cells in d["cells"].items()}}


def rq3_latency():
    """E-RQ3-A with emulated latency (Toxiproxy +20/+50 ms RTT in front of MinIO; C25)."""
    if not C["latency"]:
        return
    p = EXP / C["latency"] / "summary.json"
    if not p.exists():
        return
    d = json.loads(p.read_text())
    summary["rq3_latency"] = {lvl: {"matrix_runs": L["matrix_runs"], "all_drained": L["all_drained"],
                                    "oracle_runs": L["oracle_runs"],
                                    "change_vs_size_only_pct": {ds: {v: c["throughput_drop_vs_size_only_pct"]
                                                                     for v, c in cells.items()}
                                                                for ds, cells in L["cells"].items()}}
                              for lvl, L in d["levels"].items()}
    rq3_latency_figure(d["levels"])


def rq3_latency_figure(levels):
    """Fig. 4: VTOP slowdown vs size-only at +20/+50 ms, store SHA-256 vs BLAKE3 read-back (median, 95% CI)."""
    V = [("sha256", "Store SHA-256", COLORS[0], HATCH[0]), ("blake3", "BLAKE3 read-back", COLORS[1], HATCH[1])]
    ds = ["ssh", "bgl", "hdfs"]
    fig, axes = plt.subplots(1, 2, figsize=(W1, 1.95), sharey=True)
    w = 0.36
    for ax, lv in zip(axes, ("20", "50")):
        for j, (v, lab, col, h) in enumerate(V):
            cells = [levels[lv]["cells"][d][v]["throughput_drop_vs_size_only_pct"] for d in ds]
            loss = np.array([-m for m, _, _ in cells])  # slowdown = -(change vs size-only): taller = slower
            lo = np.array([-c[2] for c in cells]); hi = np.array([-c[1] for c in cells])
            x = np.arange(len(ds)) + (j - 0.5) * w
            ax.bar(x, loss, w * 0.9, color=col, hatch=h, edgecolor="white", linewidth=0.3, label=lab)
            ax.errorbar(x, loss, yerr=[loss - lo, hi - loss], fmt="none", ecolor="#0b0b0b", elinewidth=0.6,
                        capsize=1.2)
            for xi, yi, top, bot in zip(x, loss, hi, lo):  # value where resolved; ~0 where the CI spans zero
                txt = "\u22480" if bot <= 0 <= top else f"{yi:.0f}%"
                ax.text(xi, top + 1.0, txt, ha="center", va="bottom", fontsize=6.3,
                        color="#6b6a66" if txt == "\u22480" else "#2b2b29")
        ax.axhline(0, color="#52514e", lw=0.6)
        ax.set_xticks(range(len(ds)), [d.upper() for d in ds])
        ax.set_title(f"+{lv} ms round trip", fontsize=7.2, pad=3, color="#2b2b29")
        ax.xaxis.grid(False)
    axes[0].set_ylabel("Slowdown vs size-only (%)")
    axes[0].set_ylim(-4, 35); axes[0].set_yticks(range(0, 31, 10))
    axes[1].tick_params(axis="y", length=0)
    fig.legend(*axes[0].get_legend_handles_labels(), ncol=2, frameon=False, loc="upper center",
               bbox_to_anchor=(0.54, 1.0), handlelength=1.4, columnspacing=1.4)
    fig.tight_layout(pad=0.3, rect=(0, 0, 1, 0.89), w_pad=0.8)
    save(fig, "rq3-latency.pdf")


def rq2_norecheck():
    """E-RQ2-R V-norecheck: v0.7.0 with storage_still_verified() returning true (restart re-check disabled)."""
    d = EXP / C["norecheck"]
    rows = jl(d / "trials.jsonl")
    if not rows:
        return
    meta = json.loads((d / "run.json").read_text())
    assert meta["vtop_commit"] == summary["vtop_commit"], "ablation built from a different VTOP commit"
    mut = [r for r in rows if r["mutation"] != "M0"]
    summary["rq2_norecheck"] = {
        "trials": len(rows), "controls": len(rows) - len(mut), "mutations": len(mut),
        "mutations_accepted": sum(r["outcome"] == "accepted" for r in mut),
        "unsafe_commits": sum(bool(r["unsafe_commit"]) for r in mut),
        "unsafe_by_mutation": {m: sum(bool(r["unsafe_commit"]) for r in mut if r["mutation"] == m)
                               for m in sorted({r["mutation"] for r in mut})},
        "controls_committed": sum(r["outcome"] == "accepted" for r in rows if r["mutation"] == "M0"),
        "patched_source_sha256": meta.get("ablation_engine_sha256"),
    }


# ===================================================================== v0.10.2-only sections
def rq1_model_v010():
    """E-RQ1-M on the v010 model (v0.10.x logic: ordered commit with per-cycle hold-back), with its ablations and the
    two robustness sweeps (committed point not monotone; Kafka resume from the newest committed row)."""
    rows = json.loads((EXP / C["model010"]).read_text())
    names = {"v010": "Protocol", "v010-nohold": "No hold-back", "refusal": "Refused hand-back$^b$",
             "v060": "Guard-only$^c$"}

    def kind(p):
        return "refusal" if p.get("refusals") else p["variant"]

    def rec(r):
        p = r["params"]
        res = "holds" if r["violation"] is None else f"{r['violation']['property']} violated"
        return {**p, "states": r["states"], "result": res, "D1_states": r.get("unrepairable_states", 0),
                "D2_states": r.get("damaged_commit_states", 0), "max_duplicates": r["max_duplicates"],
                "stalled": r.get("stalled", 0)}

    out = [rec(r) for r in rows]
    full = [header("E-RQ1-M v010 model sweep (state-machine/results/20261120-v010-sweep.json)"),
            r"\begin{tabular}{@{}lcccrrrl@{}}", r"\toprule",
            r"Design & $N$ & $B$ & Faults$^a$ & States & D1 & Stalled & Result \\", r"\midrule"]
    for o in out:
        faults = f"{o['fails']}/{o['crashes']}/{o['ledger_loss']}/{o['mutations']}/{o.get('refusals', 0)}"
        full.append(f"{names[kind(o)]} & {o['ranges']} & {o['per_cycle']} & {faults} & {o['states']:,} & "
                    f"{o['D1_states']:,} & {o['stalled']:,} & {o['result']} \\\\")
    full += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq1_model_full.tex").write_text("\n".join(full) + "\n")
    summary["rq1_model"] = out
    ref = [rec(r) for r in json.loads((EXP / C["model_reference_v060"]).read_text()) if r["params"]["variant"] == "v060"]
    summary["rq1_model_reference_v060"] = ref
    base_viol = {json.dumps(o, sort_keys=True) for o in
                 ({k: v for k, v in x.items() if k in ("variant", "ranges", "per_cycle", "fails", "crashes",
                                                        "ledger_loss", "mutations", "refusals")}
                  for x in out if x["result"] != "holds")}
    rob = {}
    for tag in ("nonmonotone", "resume_newest"):
        rr = [rec(r) for r in json.loads((EXP / C[f"model010_{tag}"]).read_text())]
        viol = {json.dumps({k: v for k, v in x.items() if k in ("variant", "ranges", "per_cycle", "fails", "crashes",
                                                                 "ledger_loss", "mutations", "refusals")},
                           sort_keys=True) for x in rr if x["result"] != "holds"}
        prot = [x for x in rr if x["variant"] == "v010" and not x.get("refusals")]
        rob[tag] = {"configs": len(rr), "protocol_configs": len(prot),
                    "protocol_violations": sum(x["result"] != "holds" for x in prot),
                    "protocol_max_states": max(x["states"] for x in prot),
                    "protocol_stalled_max": max(x["stalled"] for x in prot),
                    "protocol_D2_max": max(x["D2_states"] for x in prot),
                    "protocol_max_duplicates": max(x["max_duplicates"] for x in prot),
                    "violating_configs_same_as_base": viol == base_viol}
    summary["rq1_model_robustness"] = rob
    groups = defaultdict(list)
    for o in out:
        groups[(kind(o), o["per_cycle"])].append(o)
    for o in ref:
        groups[("v060", o["per_cycle"])].append(o)
    t = [header("E-RQ1-M v010 model sweep, grouped (full table: rq1_model_full.tex)"),
         r"\begin{tabular}{@{}lcrrrr@{}}", r"\toprule",
         r"Design & $B$ & Configs & States & Violated & Smallest$^a$ \\", r"\midrule"]
    grouped = {}
    for key in [("v010", 1), ("v010", 2), ("v010", 3), ("v010-nohold", 1), ("v010-nohold", 2),
                ("refusal", 1), ("refusal", 2), ("v060", 1)]:
        g = groups.get(key, [])
        if not g:
            continue
        viol = [o for o in g if o["result"] != "holds"]
        st = [o["states"] for o in g]
        smallest = min(o["states"] for o in viol) if viol else None
        grouped[f"{key[0]}|B{key[1]}"] = {"configs": len(g), "states_min": min(st), "states_max": max(st),
                                          "violated": len(viol), "smallest_violation": smallest,
                                          "stalled_max": max(o["stalled"] for o in g),
                                          "D2_max": max(o["D2_states"] for o in g),
                                          "max_duplicates": max(o["max_duplicates"] for o in g)}
        rng = f"{min(st):,}" if min(st) == max(st) else f"{min(st):,}--{max(st):,}"
        t.append(f"{names[key[0]]} & {key[1]} & {len(g)} & {rng} & {len(viol)} & "
                 f"{'--' if smallest is None else f'{smallest:,}'} \\\\")
    if C.get("model010_ablation"):
        # One row per ablated rule over B = 1 and 2 together (the table's B column reads 1--2); per-B groups in summary.
        abl = [rec(r) for r in json.loads((EXP / C["model010_ablation"]).read_text())]
        ablation = {"source": C["model010_ablation"].removeprefix("../"), "groups": {}, "rows": {}}
        t.append("% No hand-back / No read-order: single-rule ablation of the v010 model, "
                 + ablation["source"] + " (summary rq1_model_ablation)")
        for variant, label in (("v010-nohandback", "No hand-back"), ("v010-noorder", "No read-order")):
            g = [o for o in abl if o["variant"] == variant]
            for b in sorted({o["per_cycle"] for o in g}):
                gb = [o for o in g if o["per_cycle"] == b]
                vb = [o for o in gb if o["result"] != "holds"]
                ablation["groups"][f"{variant}|B{b}"] = {
                    "configs": len(gb), "states_min": min(o["states"] for o in gb),
                    "states_max": max(o["states"] for o in gb), "violated": len(vb),
                    "smallest_violation": min(o["states"] for o in vb) if vb else None}
            viol = [o for o in g if o["result"] != "holds"]
            st = [o["states"] for o in g]
            bs = sorted({o["per_cycle"] for o in g})
            smallest = min(o["states"] for o in viol) if viol else None
            ablation["rows"][variant] = {"B": bs, "configs": len(g), "states_min": min(st), "states_max": max(st),
                                         "violated": len(viol), "smallest_violation": smallest,
                                         "max_states_holding": max((o["states"] for o in g if o["result"] == "holds"),
                                                                   default=None)}
            brng = f"{bs[0]}" if len(bs) == 1 else f"{bs[0]}--{bs[-1]}"
            rng = f"{min(st):,}" if min(st) == max(st) else f"{min(st):,}--{max(st):,}"
            t.append(f"{label} & {brng} & {len(g)} & {rng} & {len(viol)} & "
                     f"{'--' if smallest is None else f'{smallest:,}'} \\\\")
        summary["rq1_model_ablation"] = ablation
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq1_model.tex").write_text("\n".join(t) + "\n")
    summary["rq1_model_grouped"] = grouped
    safe = [o["states"] for o in out if o["result"] == "holds" and o["variant"] == "v010"]
    summary["rq1_model_claims"] = {
        "largest_safe_states": max(safe),
        "largest_safe_B1_states": max(o["states"] for o in out if o["variant"] == "v010" and o["per_cycle"] == 1
                                      and o["result"] == "holds"),
        "protocol_violations": sum(1 for o in out if o["variant"] == "v010" and not o.get("refusals")
                                   and o["result"] != "holds"),
        "protocol_stalled_max": max(o["stalled"] for o in out if o["variant"] == "v010"),
        "protocol_D2_max": max(o["D2_states"] for o in out if o["variant"] == "v010"),
        "nohold_B2_smallest_violation": grouped["v010-nohold|B2"]["smallest_violation"],
        "refusal_smallest_violation": {b: grouped[f"refusal|B{b}"]["smallest_violation"] for b in (1, 2)},
        "guard_only_smallest_violation": grouped["v060|B1"]["smallest_violation"],
    }


def log_batch_sizes(result_dir: Path, only_prefix: str | None = None) -> dict:
    """Uncompressed bytes per batch from the engine's batch_metrics log lines (one per batch id), grouped by scenario
    (the trial directory name without its trial suffix), with batching.max_bytes from the trial's config.yaml.
    Needs the local-only raw/ folder; returns {} when it is absent."""
    raw = result_dir / "raw"
    if not raw.is_dir():
        return {}
    groups = defaultdict(lambda: {"max_bytes": set(), "sizes": {}, "trials": 0})
    for tdir in sorted(p for p in raw.iterdir() if p.is_dir()):
        if only_prefix and not tdir.name.startswith(only_prefix):
            continue
        cfg = tdir / "config.yaml"
        m = re.search(r"max_bytes:\s*(\d+)", cfg.read_text()) if cfg.exists() else None
        scen = re.sub(r"-t\d+(-\d+)?$", "", tdir.name)
        g = groups[scen]
        g["trials"] += 1
        if m:
            g["max_bytes"].add(int(m.group(1)))
        for lf in tdir.rglob("*.jsonl"):
            for line in lf.read_text(errors="replace").splitlines():
                if '"batch_metrics"' not in line:
                    continue
                f = json.loads(line)["fields"]
                g["sizes"][f["batch_id"]] = f["uncompressed_bytes"]
    out = {}
    for scen, g in sorted(groups.items()):
        if not g["sizes"]:
            continue
        assert len(g["max_bytes"]) == 1, f"{scen}: several max_bytes settings {g['max_bytes']}"
        mb = g["max_bytes"].pop()
        u = np.array(list(g["sizes"].values()), dtype=float)
        out[scen] = {"trials": g["trials"], "max_bytes": mb, "batches": len(u),
                     "median_mib": round(float(np.median(u)) / 2**20, 3), "max_mib": round(float(u.max()) / 2**20, 3),
                     "max_over_setting": round(float(u.max()) / mb, 3),
                     "batches_over_setting": int((u > mb).sum())}
    return out


def run_mode_batch_sizes(result_dir: Path) -> dict:
    """vtopctl run mode: committed batches per run (ledger states) and mean uncompressed bytes per batch. The final
    batch is partial, so the mean is a lower bound on the size of the full batches."""
    out = {}
    for r in jl(result_dir / "runs.jsonl"):
        k = f"{r['dataset']}|{r['batch_mib']}"
        n = r["states"].get("source_committed", 0)
        e = out.setdefault(k, {"setting_mib": r["batch_mib"], "runs": 0, "committed_batches": set(),
                               "mean_mib": set(), "min_batches_if_all_at_or_below_setting": set()})
        e["runs"] += 1
        e["committed_batches"].add(n)
        e["mean_mib"].add(round(r["bytes"] / n / 2**20, 2))
        e["min_batches_if_all_at_or_below_setting"].add(-(-r["bytes"] // (r["batch_mib"] * 2**20)))
    for e in out.values():
        for k in ("committed_batches", "mean_mib", "min_batches_if_all_at_or_below_setting"):
            e[k] = sorted(e[k])
        e["mean_over_setting"] = round(max(e["mean_mib"]) / e["setting_mib"], 3)
    return dict(sorted(out.items()))


def process_once_batch_sizes(batches_csv: Path) -> dict:
    out = defaultdict(list)
    with open(batches_csv) as fh:
        for row in csv.DictReader(fh):
            out[row["batch_mib"]].append(int(row["uncompressed_bytes"]))
    return {k: {"batches": len(v), "max_mib": round(max(v) / 2**20, 3),
                "over_setting": sum(x > int(k) * 2**20 for x in v)} for k, v in sorted(out.items(), key=lambda kv: int(kv[0]))}


def batch_sizes():
    """X1: measured batch sizes against batching.max_bytes (v0.7.0 ran at about twice the setting in run mode)."""
    bs = {"run_mode_local": run_mode_batch_sizes(EXP / C["cost_run"]),
          "process_once_local": process_once_batch_sizes(EXP / C["cost"] / "batches.csv"),
          "crash": log_batch_sizes(EXP / C["crash"]), "kafka_rq1": log_batch_sizes(EXP / C["kafka"]),
          "kafka_rq4_vtop": log_batch_sizes(EXP / C["rq4"], only_prefix="vtop-")}
    for name, rels in (("cloud_cost", C["cloud_cost"]), ("cloud_wan", C["cloud_wan"])):
        led = [r["ledger"] for rel in rels for r in jl(EXP / rel / "runs.jsonl") if r.get("ledger")]
        led = [x for x in led if x.get("batch_uncompressed_max") is not None]  # runs that committed nothing have none
        bs[name] = {"runs_with_ledger": len(led), "max_batch_uncompressed_bytes": max(x["batch_uncompressed_max"] for x in led),
                    "runs_with_batch_over_max_bytes": sum(1 for x in led if x["batches_over_max_bytes"])}
    summary["batch_sizes"] = bs


def _rerun(script: Path, args: list[str], cwd: Path | None = None):
    r = subprocess.run([sys.executable, str(script), *args], capture_output=True, text=True, cwd=cwd)
    assert r.returncode == 0, f"{script.name} failed: {r.stderr[-2000:]}"


def _allowance(row):
    return sum(v for k, v in row.get("sys", {}).items() if k.endswith("allowance_exceeded"))


def rq5_cost():
    """E-RQ5-COST (H3.5): EC2 + Amazon S3, one host; t4g.medium (3 datasets) and the HDFS re-check on t4g.large (D9).
    The campaign analyzer is re-run on a copy of run.json + runs.jsonl and must reproduce the committed summary."""
    res = {}
    for rel in C["cloud_cost"]:
        d = EXP / rel
        info = json.loads((d / "run.json").read_text())
        assert info["vtop"]["commit"].startswith(summary["vtop_commit"]), f"{rel}: different VTOP build"
        with tempfile.TemporaryDirectory() as tmp:
            for f in ("run.json", "runs.jsonl"):
                shutil.copy(d / f, tmp)
            _rerun(EXP / "rq5-cloud/cost/analyze.py", [tmp])
            regen = json.loads((Path(tmp) / "summary.json").read_text())
        assert regen == json.loads((d / "summary.json").read_text()), f"{rel}: summary.json does not reproduce"
        rows = jl(d / "runs.jsonl")
        cells = {}
        for ds, cm in regen["cells"].items():
            cells[ds] = {}
            for m, e in cm.items():
                rs = [r for r in rows if r["dataset"] == ds and r["mode"] == m]
                cells[ds][m] = {"runs": e["runs"], "throughput_mib_s": e["throughput_mib_s"],
                                "change_vs_size_only_pct": e["change_vs_size_only_pct"],
                                "diff_vs_size_only_mib_s": e["diff_vs_size_only_mib_s"],
                                "ci_resolution": e.get("ci_resolution", "baseline"),
                                "cpu_s_per_gib": e["cpu_s_per_gib"], "batches_median": e["batches_median"],
                                "readback_mib_median": e["readback_mib_median"],
                                "per_batch_total_ms_median": e["per_batch_ms_median"].get("total_ms"),
                                "allowance_exceeded_median": float(np.median([_allowance(r) for r in rs])),
                                "cpu_steal_pct_max": e["cpu_steal_pct_max"]}
        res[info["instance_type"]] = {
            "run_id": info["run_id"], "instance_type": info["instance_type"], "az": info["availability_zone"],
            "vtop": info["vtop"]["version"], "runs": len(rows), "passing_runs": regen["passing_runs"],
            "unsafe_commits": sum(r["oracle"]["unsafe_commits"] for r in rows),
            "complete_runs": sum(bool(r["oracle"]["complete"]) for r in rows),
            "bad_objects": sum(r["oracle"]["bad_objects"] for r in rows),
            "multipart_objects": sum(r["oracle"]["multipart_objects"] or 0 for r in rows),
            "max_batch_uncompressed_bytes": max(r["ledger"]["batch_uncompressed_max"] for r in rows),
            "bootstrap": "analyze_minio_cost ci/pct_ci, B = 10,000, fixed seeds (cost/analyze.py)",
            "cells": cells}
    summary["rq5_cost"] = res
    # Table and figure: slowdown vs size-only per dataset; the HDFS re-check is its own row.
    lines = [header("E-RQ5-COST, EC2 + Amazon S3 (us-east-1), change of median steady MiB/s vs size-only [95% CI]"),
             r"\begin{tabular}{@{}llrrr@{}}", r"\toprule",
             r"Dataset & Instance & Size-only MiB/s & Store SHA-256 & BLAKE3 read-back \\", r"\midrule"]
    bars = []
    for it, ds_list in (("t4g.medium", ["ssh", "bgl", "hdfs"]), ("t4g.large", ["hdfs"])):
        for ds in ds_list:
            c = res[it]["cells"][ds]
            b = c["size-only"]["throughput_mib_s"]
            f = lambda v: f"{v[0]:.1f}\\% [{v[1]:.1f}, {v[2]:.1f}]"  # noqa: E731
            lines.append(f"{ds.upper()} & {it} & {b[0]:.1f} [{b[1]:.1f}, {b[2]:.1f}] & "
                         f"{f(c['sha256-store']['change_vs_size_only_pct'])} & "
                         f"{f(c['blake3-readback']['change_vs_size_only_pct'])} \\\\")
            bars.append((ds.upper() + ("\n(large)" if it == "t4g.large" else ""),
                         c["sha256-store"]["change_vs_size_only_pct"], c["blake3-readback"]["change_vs_size_only_pct"]))
    lines += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_cost.tex").write_text("\n".join(lines) + "\n")
    slowdown_bars(bars, "rq5-cost.pdf", ylim=(-16, 38))


def slowdown_bars(groups, name, ylim):
    """Fig. 4 style: slowdown vs size-only (= -change), store SHA-256 vs BLAKE3 read-back, median with 95% CI."""
    V = [("Store SHA-256", COLORS[0], HATCH[0]), ("BLAKE3 read-back", COLORS[1], HATCH[1])]
    fig, ax = plt.subplots(figsize=(W1, 1.95))
    w = 0.36
    for j, (lab, col, h) in enumerate(V):
        cells = [g[1 + j] for g in groups]
        loss = np.array([-m for m, _, _ in cells])
        lo = np.array([-c[2] for c in cells]); hi = np.array([-c[1] for c in cells])
        x = np.arange(len(groups)) + (j - 0.5) * w
        ax.bar(x, loss, w * 0.9, color=col, hatch=h, edgecolor="white", linewidth=0.3, label=lab)
        ax.errorbar(x, loss, yerr=[loss - lo, hi - loss], fmt="none", ecolor="#0b0b0b", elinewidth=0.6, capsize=1.2)
        for xi, yi, top, bot in zip(x, loss, hi, lo):
            txt = "≈0" if bot <= 0 <= top else f"{yi:.0f}%"
            ax.text(xi, max(top, 0) + 1.0, txt, ha="center", va="bottom", fontsize=6.3,
                    color="#6b6a66" if txt == "≈0" else "#2b2b29")
    ax.axhline(0, color="#52514e", lw=0.6)
    ax.set_xticks(range(len(groups)), [g[0] for g in groups])
    ax.set_ylabel("Slowdown vs size-only (%)")
    ax.set_ylim(*ylim)
    ax.xaxis.grid(False)
    ax.legend(ncol=2, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.18), handlelength=1.4,
              columnspacing=1.4)
    fig.tight_layout(pad=0.3)
    save(fig, name)


def rq5_wan():
    """E-RQ5-WAN (H3.6): engine VMs in four regions writing to the us-east-1 bucket. The WAN analyzer is re-run on the
    region directories (same order and baseline as the committed combined summary) and must reproduce it."""
    combined = json.loads((EXP / C["cloud_wan_combined"]).read_text())
    root = R.parent
    with tempfile.TemporaryDirectory() as tmp:
        _rerun(EXP / "rq5-cloud/wan/analyze.py", [str(EXP / rel) for rel in C["cloud_wan"]] +
               ["--out", tmp, "--baseline-runs", combined["baseline_runs"]], cwd=root)
        regen = json.loads((Path(tmp) / "summary.json").read_text())
    assert regen == combined, "combined WAN summary.json does not reproduce from the region runs"
    rows = []
    for rel in C["cloud_wan"]:
        info = json.loads((EXP / rel / "run.json").read_text())
        assert info["vtop"]["commit"].startswith(summary["vtop_commit"]), f"{rel}: different VTOP build"
        rows += [{**r, "_region": info["region"]} for r in jl(EXP / rel / "runs.jsonl")]
    regs = regen["regions"]
    order = sorted(regs, key=lambda g: regs[g]["rtt_ms_per_run_median"])
    nat = {g: regs[g]["native"] for g in regs}
    rng = lambda xs: [min(xs), max(xs)]  # noqa: E731
    out = {"runs": len(rows), "passing_runs": sum(bool(r["pass"]) for r in rows),
           "unsafe_commits": sum((r.get("oracle") or {}).get("unsafe_commits") or 0 for r in rows),
           "runs_per_region": {g: sum(r["_region"] == g for r in rows) for g in regs},
           "regions_by_rtt": order, "bootstrap": "analyze_minio_cost ci/pct_ci, B = 10,000, fixed seeds (wan/analyze.py)",
           "baseline_runs": combined["baseline_runs"], "regions": regs}
    out["claims"] = {
        "rtt_ms_range": rng([regs[g]["rtt_ms_per_run_median"] for g in regs]),
        "size_only_mib_s_range": rng([nat[g]["size-only"]["mib_s"][0] for g in regs]),
        "size_only_vs_us_east_1_pct_range": rng([nat[g]["size-only"]["vs_us_east_1_pct"][0] for g in regs]),
        "store_change_pct_range": rng([nat[g]["sha256-store"]["vs_size_only_pct"][0] for g in regs]),
        "store_resolved_regions": [g for g in order if nat[g]["sha256-store"]["vs_size_only_pct"][2] < 0],
        "readback_change_pct_range": rng([nat[g]["blake3-readback"]["vs_size_only_pct"][0] for g in regs]),
        "readback_resolved_all": all(nat[g]["blake3-readback"]["vs_size_only_pct"][2] < 0 for g in regs),
        "readback_slowdown_increases_with_rtt": all(
            nat[a]["blake3-readback"]["vs_size_only_pct"][0] > nat[b]["blake3-readback"]["vs_size_only_pct"][0]
            for a, b in zip(order, order[1:])),
        "readback_slower_than_store_all": all(nat[g]["blake3-readback"]["vs_size_only_pct"][0]
                                              < nat[g]["sha256-store"]["vs_size_only_pct"][0] for g in regs),
        "tc2_store_of_cap_range": rng([regs[g]["bandwidth"]["tc2/sha256-store"]["of_cap"] for g in regs]),
        "cap2_store_of_cap_range": rng([regs[g]["bandwidth"]["cap2/sha256-store"]["of_cap"] for g in regs]),
        "tc10_store_of_cap_range": rng([regs[g]["bandwidth"]["tc10/sha256-store"]["of_cap"] for g in regs]),
        "readback_drain_tc2_over_cap2_range": rng([round(regs[g]["bandwidth"]["tc2/blake3-readback"]["drain_s"]
                                                         / regs[g]["bandwidth"]["cap2/blake3-readback"]["drain_s"], 2)
                                                   for g in regs]),
        "loss5_x_native_range": rng([regs[g]["loss"]["loss5"]["x_native"] for g in regs]),
        "loss_failed_batches": sum(e["failed_batches"] for g in regs for e in regs[g]["loss"].values()),
        "partition_commits_during_cut": sum(regs[g]["partition"]["commits_during"] for g in regs),
        "partition_first_commit_after_restore_s_range": rng([regs[g]["partition"]["first_commit_after_restore_s"]
                                                             for g in regs]),
        "sigkill_restart_drain_s_range": rng([e["restart_drain_s"] for g in regs for e in regs[g]["sigkill"].values()]),
        "sigkill_duplicate_bytes": sum(e["duplicate_bytes"] for g in regs for e in regs[g]["sigkill"].values()),
        "sigkill_unreferenced_objects": sum(e["unreferenced_objects"] for g in regs for e in regs[g]["sigkill"].values()),
        "policed_completed_regions": [g for g in order if regs[g]["policed"]["policed"]["committed"]
                                      == regs[g]["policed"]["policed"]["bytes"]],
        "policed_seal_to_commit_over_300s_regions": [g for g in order
                                                     if (regs[g]["policed"]["policed"]["seal_to_commit_max_s"] or 0) > 300],
        "policed_fixed_committed_bytes": sum(regs[g]["policed"]["policed-fixed"]["committed"] for g in regs),
    }
    summary["rq5_wan"] = out
    short = {"eu-west-1": "Ireland", "sa-east-1": "S\\~ao Paulo", "ap-northeast-1": "Tokyo", "ap-southeast-2": "Sydney"}
    f = lambda v: f"{v[0]:.1f}\\% [{v[1]:.1f}, {v[2]:.1f}]"  # noqa: E731
    lines = [header("E-RQ5-WAN, engine in four regions -> us-east-1 bucket; SSH, 10 runs per cell; change of median "
                    "end-to-end MiB/s vs size-only [95% CI]"),
             r"\begin{tabular}{@{}lrrrrr@{}}", r"\toprule",
             r"Region & RTT ms & Size-only MiB/s & Store SHA-256 & BLAKE3 read-back & Partition$^a$ s \\", r"\midrule"]
    for g in order:
        n = nat[g]
        lines.append(f"{short.get(g, g)} & {regs[g]['rtt_ms_per_run_median']:.0f} & {n['size-only']['mib_s'][0]:.2f} & "
                     f"{f(n['sha256-store']['vs_size_only_pct'])} & {f(n['blake3-readback']['vs_size_only_pct'])} & "
                     f"{regs[g]['partition']['first_commit_after_restore_s']:.1f} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_wan.tex").write_text("\n".join(lines) + "\n")
    # Figure: slowdown vs size-only against the measured RTT.
    fig, ax = plt.subplots(figsize=(W1, 2.0))
    x = np.array([regs[g]["rtt_ms_per_run_median"] for g in order])
    for j, (m, lab, mk) in enumerate((("sha256-store", "Store SHA-256", "o"), ("blake3-readback", "BLAKE3 read-back", "s"))):
        c = [nat[g][m]["vs_size_only_pct"] for g in order]
        y = np.array([-v[0] for v in c]); lo = np.array([-v[2] for v in c]); hi = np.array([-v[1] for v in c])
        # markers only: four separate regions, not a fitted curve over RTT
        ax.errorbar(x + (j - 0.5) * 3, y, yerr=[y - lo, hi - y], color=COLORS[j], marker=mk, markersize=4.5,
                    markeredgecolor="white", markeredgewidth=0.6, linestyle="none", elinewidth=0.7, capsize=1.4,
                    label=lab)
    for xi, g in zip(x, order):
        ax.text(xi, -3.2, short.get(g, g).replace("\\~a", "ã"), ha="center", va="top", fontsize=6.2, color="#52514e")
    ax.axhline(0, color="#52514e", lw=0.6)
    ax.set_xlabel("Measured round trip to the bucket (ms)")
    ax.set_ylabel("Slowdown vs size-only (%)")
    ax.set_xlim(40, 220); ax.set_ylim(-8, 42); ax.set_yticks(range(0, 41, 10))
    ax.legend(ncol=2, frameon=False, loc="upper center", bbox_to_anchor=(0.5, 1.18), handlelength=1.2,
              columnspacing=1.4)
    fig.tight_layout(pad=0.3)
    save(fig, "rq5-wan.pdf")


# E-RQ5-TAMPER paper table: one row per attacker capability and tamper; the case ids are the campaign's own. Which
# verifier caught each tamper, and which missed it by design (a stated limit), is read from the cases, not written here.
TAMPER_ROWS = [  # (attacker, short label for the paper table, long label for the supplement, case ids)
    ("Bucket", "Forgery, MAC off", "Forged object and manifest, MAC off", ["MAC-FORGE-OFF-B"]),
    ("Bucket", "Forgery, MAC on", "Forged object and manifest, MAC on", ["MAC-FORGE-ON-B"]),
    ("Bucket", "Manifest swapped in", "Another batch's genuine manifest swapped in, MAC off or on",
     ["MAC-SWAP-OFF", "MAC-SWAP-ON"]),
    ("Bucket", "Object truncated, deleted", "Object truncated or deleted", ["OBJ-TRUNC-ON", "OBJ-DEL-ON"]),
    ("Bucket", "Object byte flipped", "Object byte flipped (same size)", ["OBJ-FLIP-ON"]),
    ("Bucket", "Receipt tampered", "Receipt edited, swapped, or deleted; class edited with the MAC off",
     ["RCPT-EDIT-ON", "RCPT-EDIT-NOHASH-ON", "RCPT-SWAP-ON", "RCPT-DEL-ON", "RCPT-CLASS-OFF"]),
    ("Bucket", "Receipt signer/method, MAC off", "Receipt signer or method edited, MAC off",
     ["RCPT-BY-OFF", "RCPT-METHOD-OFF"]),
    ("+ ledger", "Forgery, MAC off", "Forgery, MAC off, ledger row and receipt rewritten", ["MAC-FORGE-OFF-BL"]),
    ("+ ledger", "Forgery, MAC off, receipt kept", "Forgery, MAC off, ledger row rewritten, receipt left as written",
     ["MAC-FORGE-OFF-BL-RCPT"]),
    ("+ ledger", "Forgery, MAC on", "Forgery, MAC on: stale MAC, MAC stripped, or attacker's own key",
     ["MAC-FORGE-ON-BL", "MAC-STRIP-ON-BL", "MAC-FOREIGN-BL"]),
    ("+ key", "Forgery, leaked key", "Forgery signed with the leaked key, ledger rewritten (before and after retirement)",
     ["MAC-LEAKED-BL", "MAC-RETIRED-BL"]),
    ("Versioned", "Overwrite, delete", "Object or manifest overwritten, delete marker, pinned manifest version deleted",
     ["VER-OVR-MAN", "VER-OVR-OBJ", "VER-DELMARK", "VER-DELPIN"]),
    ("Versioned", "Forgery, MAC off", "Forged object and manifest, MAC off", ["VER-OVR-BOTH-OFF"]),
    ("Key holder", "Manifest rollback", "Rollback to an older valid manifest (versioned and unversioned)",
     ["VER-ROLLBACK", "NOVER-ROLLBACK"]),
    ("Lock", "Delete version, shorten", "Delete a locked version, shorten retention, suspend versioning",
     ["LOCK-DELVER-OBJ", "LOCK-DELVER-MAN", "LOCK-SHORTEN", "VER-SUSPEND-LOCKED"]),
    ("Lock", "Overwrite, delete, bypass", "Overwrite, plain delete (marker), retention cleared with the governance bypass",
     ["LOCK-OVR-OBJ", "LOCK-OVR-MAN", "LOCK-DELMARK-OBJ", "LOCK-BYPASS-CLEAR"]),
    ("Ledger DB", "Audit UPDATE/DELETE", "Audit-trail UPDATE or DELETE with the triggers in place",
     ["AUD-UPDATE-TRIGGER", "AUD-DELETE-TRIGGER"]),
    ("Ledger DB", "Audit or ledger rows edited", "Audit edit, mid delete, swap; keyed chain rewritten without the key; "
     "ledger row deleted mid-range",
     ["AUD-EDIT", "AUD-DELETE-MID", "AUD-SWAP", "AUD-RECOMPUTE-NOKEY", "AUD-REWRITE-ALL-NOKEY", "COV-DELETE-MID"]),
    ("Ledger DB", "Tail cut, chain recomputed", "Audit tail truncated; unkeyed chain recomputed; chain key leaked; "
     "last ledger row deleted", ["AUD-TRUNCATE", "AUD-RECOMPUTE-UNKEYED", "AUD-RECOMPUTE-LEAKED", "COV-DELETE-END"]),
]
# Cases outside the paper table: start-up refusals (stated in the text), controls, and the two local-only cases.
TAMPER_OTHER = {"config": ["VER-REFUSE-UNVERSIONED", "LOCK-REFUSE-PLAIN"],
                "control": ["LOCK-CONTROL", "AUD-CONTROL", "VER-RESTORE"]}
# Verifier -> table code. None: a duplicate view of another check (e.g. scrub.passed beside scrub's verdict).
TAMPER_CODES = {
    "engine_start": "E", "store": "S3", "sql": "D", "scrub": "S", "scrub.passed": None, "scrub.receipt": "C",
    "scrub_require.receipt": "C", "scrub_require.passed": None, "scrub.retention": "T", "scrub_rotated": "S",
    "scrub_rotated.passed": None, "scrub_retired": "S$^r$", "vm_noledger": "V", "vm_noledger_retired": "V$^r$",
    "vm_ledger": "V$_\\ell$", "recovery": "R", "recovery_retired": "R$^r$", "check_storage": "Z",
    "audit_verify": "A", "anchor": "H", "coverage": "K"}
TAMPER_ORDER = ["E", "S3", "D", "S", "S$^r$", "C", "T", "V", "V$^r$", "V$_\\ell$", "R", "R$^r$", "Z", "A", "H", "K"]


def rq5_tamper():
    """E-RQ5-TAMPER (H2.5-H2.7): 55 tamper cases on Amazon S3, compared with three local MinIO runs. The
    campaign analyzer is re-run on the result directories and must reproduce both committed summaries."""
    aws = EXP / C["cloud_tamper"]
    loc = [EXP / rel for rel in C["cloud_tamper_local"]]
    script = EXP / "rq5-cloud/tamper/analyze.py"
    regen = {}
    for name, dirs in (("aws", [aws, loc[0]]), ("local", loc)):
        with tempfile.TemporaryDirectory() as tmp:
            _rerun(script, [str(d) for d in dirs] + ["--out", tmp])
            regen[name] = json.loads((Path(tmp) / "summary.json").read_text())
        assert regen[name] == json.loads((dirs[0] / "summary.json").read_text()), f"{dirs[0].name}: summary.json does not reproduce"
    meta = json.loads((aws / "run.json").read_text())
    assert meta["vtop_commit"].startswith(summary["vtop_commit"]), "E-RQ5-TAMPER: different VTOP build"
    for d in loc:
        assert json.loads((d / "run.json").read_text())["vtop_commit"] == meta["vtop_commit"], f"{d.name}: other build"
    raw = {x["id"]: x for x in jl(aws / "cases.jsonl")}  # a re-run of a case id replaces the earlier line
    cases = regen["aws"][aws.name]["cases"]
    assert set(cases) == set(raw)
    # Engine changes after the driver's expectations were written (set-specific, each with its evidence): a check
    # recorded as DIFFERS because the engine now catches what the expectation called a stated limit counts as caught.
    # The campaign analyzer's own summary.json keeps the raw DIFFERS; outcomes_raw below keeps its counts.
    engine_changes = []
    for (cid, verifier), why in C.get("tamper_engine_changes", {}).items():
        for k in cases[cid]["checks"]:
            if k["verifier"] == verifier and k["label"] == "DIFFERS":
                assert k["observed"] == "failed", f"{cid}/{verifier}: the documented change predicts 'failed'"
                k["label"] = "caught"
                engine_changes.append({"case": cid, "verifier": verifier, "observed": k["observed"], "why": why})
        labels = [k["label"] for k in cases[cid]["checks"]]
        if "DIFFERS" not in labels:
            parts = (["caught"] if "caught" in labels else ["prevented"] if "prevented" in labels else []) \
                + (["NOT CAUGHT (limit)"] if "NOT CAUGHT" in labels else [])
            cases[cid]["outcome"] = " + ".join(parts) or "as expected"
    assert len(engine_changes) == len(C.get("tamper_engine_changes", {})), "every documented engine change observed"
    outcomes = dict(Counter(x["outcome"] for x in cases.values()))
    listed = [c for *_, ids in TAMPER_ROWS for c in ids] + [c for v in TAMPER_OTHER.values() for c in v]
    skipped = sorted(c for c, x in cases.items() if x["outcome"] == "skipped")
    assert len(listed) == len(set(listed)) and set(listed) | set(skipped) == set(cases), "tamper rows must cover every case once"
    # Agreement with each local run: same observed verdict sequence per case (the campaign analyzer's own test).
    loc_cases = {d.name: {x["id"]: x for x in jl(d / "cases.jsonl")} for d in loc}
    agree = {}
    for name, lc in loc_cases.items():
        comp = [c for c in cases if c not in skipped and not lc[c].get("skipped")]
        same = [c for c in comp if [k["observed"] for k in raw[c]["checks"]] == [k["observed"] for k in lc[c]["checks"]]]
        agree[name] = {"comparable": len(comp), "agree": len(same), "differ": sorted(set(comp) - set(same))}
    loc_differs = {name: sorted(c for c, x in regen["local"][name]["cases"].items() if x["outcome"] == "DIFFERS")
                   for name in loc_cases}
    rows = []
    for group, short, label, ids in TAMPER_ROWS:
        caught, missed = set(), set()
        for c in ids:
            for k in cases[c]["checks"]:
                code = TAMPER_CODES[k["verifier"]]
                if code is None:
                    continue
                if k["label"] in ("caught", "prevented"):
                    caught.add(code)
                elif k["label"] == "NOT CAUGHT":
                    missed.add(code)
                assert k["label"] != "DIFFERS", f"{c}: DIFFERS on AWS"
        rows.append({"attacker": group, "tamper": short, "tamper_long": label, "cases": ids,
                     "outcomes": [cases[c]["outcome"] for c in ids],
                     "caught_by": [x for x in TAMPER_ORDER if x in caught],
                     "not_caught_by": [x for x in TAMPER_ORDER if x in missed]})
    by_h = defaultdict(lambda: defaultdict(int))
    for c, x in raw.items():
        by_h[x["hypothesis"]][cases[c]["outcome"]] += 1
    forged = sorted(c for c, x in raw.items()
                    if any(isinstance(v, dict) and v.get("forged_content_committed") for v in x["verdicts"].values()))
    store_err = {c: sorted({k.get("error", "")[:60] for k in x["verdicts"].get("store", {}).get("calls", []) if k.get("error")})
                 for c, x in raw.items() if x["verdicts"].get("store", {}).get("verdict") == "refused"}
    out = {
        "run_id": aws.name, "where": regen["aws"][aws.name]["where"], "instance_type": meta["instance_type"],
        "az": meta["az"], "vtop": meta["vtop_version"], "vtop_commit": meta["vtop_commit"],
        "input_bytes": meta["input"]["bytes"], "batch_max_bytes": meta["batching_max_bytes"],
        "object_lock": meta["object_lock"], "buckets_preflight": meta["preflight"],
        "reimpl_check_ok": meta["reimpl_check"]["ok"],
        "cases": len(cases), "outcomes": outcomes if engine_changes else regen["aws"][aws.name]["outcomes"],
        **({"outcomes_raw": regen["aws"][aws.name]["outcomes"], "engine_changes": engine_changes} if engine_changes else {}),
        "checks": dict(sorted(Counter(k["label"] for x in cases.values() for k in x["checks"]).items())),
        "cases_by_hypothesis": {h: dict(v) for h, v in sorted(by_h.items())},
        "skipped_local_only": skipped,
        "differs_on_aws": sorted(c for c, x in cases.items() if x["outcome"] == "DIFFERS"),
        "agreement_with_local": agree,
        "local_runs": {n: {"where": regen["local"][n]["where"], "outcomes": regen["local"][n]["outcomes"]} for n in loc_cases},
        "local_differs": loc_differs,
        "local_suspend_running_writes": {n: [k["observed"] for k in lc["VER-SUSPEND-RUNNING"]["checks"]
                                             if k["verifier"] == "writes_after_suspension"][0] for n, lc in loc_cases.items()},
        "limit_cases": sorted(c for c, x in cases.items() if "NOT CAUGHT" in x["outcome"]),
        "prevented_cases": sorted(c for c, x in cases.items() if x["outcome"] == "prevented"),
        "forged_content_committed": forged,
        "store_refusals": store_err,
        "locked_version_intact": {c: x["facts"].get("locked_version_intact") for c, x in raw.items()
                                  if "locked_version_intact" in x.get("facts", {})},
        "versioned_original_still_stored": {c: x["facts"].get("original_object_still_stored",
                                                               x["facts"].get("original_manifest_still_stored"))
                                            for c, x in raw.items() if c.startswith("VER-OVR")},
        "rows": rows,
    }
    summary["rq5_tamper"] = out
    note = (f"E-RQ5-TAMPER, Amazon S3 (us-east-1), vtop-engine {C['label'].lstrip('v')}; caught / not caught (stated "
            "limit) per attacker capability; full case table: rq5_tamper_full.tex")
    t = [header(note), r"\begin{tabular}{@{}llll@{}}", r"\toprule",
         r"Attacker & Tamper (cases) & Caught by & Not caught$^a$ \\", r"\midrule"]
    g = [header("E-RQ5-TAMPER grouped (supplement): the paper table with the long labels"),
         r"\begin{tabular}{@{}p{0.14\linewidth}p{0.5\linewidth}p{0.14\linewidth}p{0.14\linewidth}@{}}", r"\toprule",
         r"Attacker & Tamper (cases) & Caught by & Not caught$^a$ \\", r"\midrule"]
    last = None
    for r in rows:
        who = "" if r["attacker"] == last else r["attacker"]
        if who and last is not None:
            t.append(r"\addlinespace[1pt]")
            g.append(r"\addlinespace[1pt]")
        last = r["attacker"]
        cells = f"{', '.join(r['caught_by']) or '--'} & {', '.join(r['not_caught_by']) or '--'} \\\\"
        t.append(f"{who} & {r['tamper']} ({len(r['cases'])}) & " + cells)
        g.append(f"{who} & {r['tamper_long']} ({len(r['cases'])}) & " + cells)
    t += [r"\bottomrule", r"\end{tabular}"]
    g += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_tamper_grouped.tex").write_text("\n".join(g) + "\n")
    (OUT_T / "rq5_tamper.tex").write_text("\n".join(t) + "\n")
    # Full case table (supplement): every case with its verifier verdicts and the agreement with the local runs.
    f = [header("E-RQ5-TAMPER full case table (Amazon S3); L = stated limit (NOT CAUGHT by design)"),
         r"\begin{tabular}{@{}p{0.2\linewidth}p{0.47\linewidth}p{0.27\linewidth}@{}}", r"\toprule",
         r"Case & Verifier verdicts & Outcome \\", r"\midrule"]
    esc = lambda s: str(s).replace("_", r"\_").replace("&", r"\&").replace("+", "+")  # noqa: E731
    for c, x in cases.items():
        if x["outcome"] == "skipped":
            v = "local only"
        else:
            v = "; ".join(f"{esc(k['verifier'])}={esc(str(k['observed']).lower() if isinstance(k['observed'], bool) else k['observed'])}"
                          + (" (L)" if k["label"] == "NOT CAUGHT" else "") for k in x["checks"])
        f.append(f"\\texttt{{{c}}} & {v} & {esc(x['outcome']).replace('NOT CAUGHT (limit)', 'not caught (L)')} \\\\")
    f += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_tamper_full.tex").write_text("\n".join(f) + "\n")


def rq5_scale():
    """E-RQ5-SCALE (H3.7): N = 1, 2, 4, 8 independent engines on separate t4g.large hosts writing to one bucket. The
    campaign analyzer is re-run on a copy of the session and must reproduce summary.json, steps.csv and engines.csv."""
    d = EXP / C["cloud_scale"]
    with tempfile.TemporaryDirectory() as tmp:
        cp = Path(tmp) / d.name
        shutil.copytree(d, cp, ignore=shutil.ignore_patterns("summary.json", "steps.csv", "engines.csv", "ANALYSIS.md"))
        _rerun(EXP / "rq5-cloud/scale/analyze.py", [str(cp)])
        for fn in ("summary.json", "steps.csv", "engines.csv"):
            assert (cp / fn).read_bytes() == (d / fn).read_bytes(), f"E-RQ5-SCALE: {fn} does not reproduce"
    s = json.loads((d / "summary.json").read_text())
    vt = [json.loads(v) for v in s["vtop"]]
    assert len(vt) == 1 and vt[0]["commit"].startswith(summary["vtop_commit"]), "E-RQ5-SCALE: different VTOP build"
    with open(d / "engines.csv") as fh:
        eng = list(csv.DictReader(fh))
    scale_runs = [e for e in eng if e["kind"] == "scale"]
    per_n = {}
    for n, e in sorted(s["per_n"].items(), key=lambda kv: int(kv[0])):
        er = [x for x in scale_runs if x["n"] == n]
        per_n[n] = {k: e[k] for k in ("steps", "engine_runs", "aggregate_mib_s", "per_engine_mib_s", "window_mib_s_median",
                                       "s3_attempts_per_s_median", "s3_peak_attempts_per_s_max", "s3_retries",
                                       "s3_throttle_verdicts", "engine_throttle_events", "ena_allowance_exceeded_sum",
                                       "batches_over_max_bytes", "host_cpu_busy_pct_median", "per_engine_cores_median",
                                       "window_overlap_fraction_min", "start_skew_s_max", "put_latency_ms_p50_median",
                                       "head_latency_ms_p50_median")}
        per_n[n].update({"efficiency": e.get("efficiency"), "per_engine_change_vs_n1_pct": e.get("per_engine_change_vs_n1_pct"),
                         "engine_runs_passing": sum(x["pass"] == "True" for x in er),
                         "unsafe": sum(int(x["unsafe"]) for x in er)})
    eff = [per_n[n]["efficiency"] for n in per_n if n != "1"]
    chg = [per_n[n]["per_engine_change_vs_n1_pct"] for n in per_n if n != "1"]
    ctl = s["control_size_only"]
    out = {"session": s["session"], "hosts": s["hosts"], "instance_types": s["instance_types"],
           "availability_zones": s["availability_zones"], "vtop": vt[0]["version"],
           "steps_planned": s["steps_planned"], "steps_executed": s["steps_executed"],
           "engine_runs": s["engine_runs"], "engine_runs_passing": s["engine_runs_passing"], "unsafe": len(s["unsafe"]),
           "unsafe_commits_engines_csv": sum(int(x["unsafe"]) for x in eng),
           "bootstrap": "analyze_minio_cost ci/pct_ci and scale/analyze.py ratio_ci, B = 10,000, fixed seeds",
           "per_n": per_n,
           "control_size_only": {"n": ctl["n"], "steps": ctl["steps"], "per_engine_mib_s": ctl["per_engine_mib_s"],
                                 "sha256_vs_size_only_aggregate_pct": ctl["sha256_vs_size_only_aggregate_pct"],
                                 "sha256_vs_size_only_per_engine_pct": ctl["sha256_vs_size_only_per_engine_pct"]},
           "claims": {"efficiency_ci_low_min": min(x[1] for x in eff), "efficiency_ci_high_max": max(x[2] for x in eff),
                      "efficiency_point_range": [min(x[0] for x in eff), max(x[0] for x in eff)],
                      "per_engine_change_point_range": [min(x[0] for x in chg), max(x[0] for x in chg)],
                      "aggregate_n8_mib_s": per_n["8"]["aggregate_mib_s"], "efficiency_n8": per_n["8"]["efficiency"],
                      "s3_retries_total": sum(per_n[n]["s3_retries"] for n in per_n),
                      "s3_throttle_verdicts_total": sum(per_n[n]["s3_throttle_verdicts"] for n in per_n),
                      "engine_throttle_events_total": sum(per_n[n]["engine_throttle_events"] for n in per_n),
                      "peak_s3_attempts_per_s_n8": per_n["8"]["s3_peak_attempts_per_s_max"]}}
    summary["rq5_scale"] = out
    f = lambda v, p=1: f"{v[0]:.{p}f} [{v[1]:.{p}f}, {v[2]:.{p}f}]"  # noqa: E731
    t = [header("E-RQ5-SCALE, N independent engines (one ledger each) on t4g.large hosts, one S3 bucket (us-east-1); "
                "medians [95% CI], 10 steps per N"),
         r"\begin{tabular}{@{}rrrrrrr@{}}", r"\toprule",
         r"$N$ & Aggregate MiB/s & Efficiency$^a$ & Per engine$^b$ & Req/s$^c$ & R/T$^d$ & Unsafe \\",
         r"\midrule"]
    for n, e in per_n.items():
        effc = "--" if e["efficiency"] is None else f(e["efficiency"], 2)
        chg_ = "--" if e["per_engine_change_vs_n1_pct"] is None else f(e["per_engine_change_vs_n1_pct"], 1)
        t.append(f"{n} & {f(e['aggregate_mib_s'])} & {effc} & {chg_} & {e['s3_attempts_per_s_median']:.0f} "
                 f"({e['s3_peak_attempts_per_s_max']}) & {e['s3_retries']}/{e['s3_throttle_verdicts'] + e['engine_throttle_events']} & "
                 f"{e['unsafe']}/{e['engine_runs']} \\\\")
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_scale.tex").write_text("\n".join(t) + "\n")
    # Per-step table (supplement): every measured step in execution order (the warm-up step is excluded, as in the
    # campaign analyzer); the size-only control steps are marked.
    order = {x["id"]: i for i, x in enumerate(json.loads((d / "schedule.json").read_text())["steps"])}
    p = [header("E-RQ5-SCALE per step (supplement), execution order; warm-up excluded"),
         r"\begin{tabular}{@{}lrlrrrrrr@{}}", r"\toprule",
         r"Step & $N$ & Mode & Aggr.\ MiB/s & Window MiB/s & Min.\ engine & Req/s (peak) & Retr. & Thr. \\",
         r"\midrule"]
    for sid, x in sorted(s["steps"].items(), key=lambda kv: order[kv[0]]):
        p.append(f"{sid} & {x['n']} & {x['mode']} & {x['agg_mib_s']:.1f} & {x['window_mib_s']:.1f} & "
                 f"{x['per_engine_mib_s_min']:.1f} & {x['s3_attempts_per_s']:.0f} ({x['s3_peak_attempts_per_s']}) & "
                 f"{x['s3_retries']} & {x['s3_throttle_verdicts']} \\\\")
    p += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_scale_steps.tex").write_text("\n".join(p) + "\n")


# E-RQ5-PERF configs in table order; labels for the paper tables (\sysname{} works with and without xspace).
PERF_ORDER = ["vtop-size-only", "vtop-sha256-store", "vtop-blake3-readback", "vtop-sha256-gzip6", "confluent", "vector",
              "fluentbit"]
PERF_LABEL = {"vtop-size-only": r"\sysname{} size-only", "vtop-sha256-store": r"\sysname{} store SHA-256",
              "vtop-blake3-readback": r"\sysname{} BLAKE3 read-back", "vtop-sha256-gzip6": r"\sysname{} SHA-256, gzip-6",
              "confluent": "Confluent S3 Sink", "vector": "Vector", "fluentbit": "Fluent Bit"}
PERF_SINKS = ["confluent", "vector", "fluentbit"]
REF_PERF = "vtop-sha256-store"  # the campaign analyzer's reference (VTOP's strong mode on S3)
LEG_SHORT = {"us-east-1": "us-east-1", "ap-southeast-2": "Sydney"}  # sink region (the bucket is in us-east-1)


def rq5_perf():
    """E-RQ5-PERF (H4.2): VTOP vs Confluent S3 Sink, Vector and Fluent Bit on EC2 + S3, us-east-1 and Sydney,
    plus the storage-fault phase on real S3 (us-east-1). The campaign analyzer is re-run on both legs (each leg alone,
    as its per-leg summary was made, and both together, as the combined summary) and must reproduce all three."""
    dirs = [EXP / rel for rel in C["cloud_perf"]]
    comb = EXP / C["cloud_perf_combined"]
    committed = json.loads((comb / "summary.json").read_text())
    script, root, prof = EXP / "rq5-cloud/perf/analyze.py", R.parent, committed["request_profile"]
    with tempfile.TemporaryDirectory() as tmp:
        _rerun(script, [str(d) for d in dirs] + ["--out", tmp, "--request-profile", prof], cwd=root)
        regen = json.loads((Path(tmp) / "summary.json").read_text())
    assert regen == committed, "E-RQ5-PERF: combined summary.json does not reproduce from the two legs"
    for d, reps in zip(dirs, C["cloud_perf_reps"]):
        with tempfile.TemporaryDirectory() as tmp:
            _rerun(script, [str(d), "--out", tmp, "--request-profile", prof, "--expect-reps", reps], cwd=root)
            assert json.loads((Path(tmp) / "summary.json").read_text()) == json.loads(
                (d / "analysis/summary.json").read_text()), f"E-RQ5-PERF {d.parent.name}: leg summary.json does not reproduce"
    legs, rows = {}, {}
    for d, L in zip(dirs, regen["legs"]):
        info = json.loads((d / "run.json").read_text())
        assert info["leg"] == L["leg"] and info["vtop"]["commit"].startswith(summary["vtop_commit"]), \
            f"E-RQ5-PERF {d.parent.name}: different leg or VTOP build"
        rows[L["leg"]] = jl(d / "runs.jsonl")
        legs[L["leg"]] = (L, info, json.loads((d.parent / "sink.json").read_text()))
    keep = lambda e, ks: {k: e.get(k) for k in ks}  # noqa: E731
    out = {"experiment": regen["experiment"], "reference": regen["reference"],
           "bootstrap": "analyze_minio_cost ci/pct_ci, B = 10,000, fixed seeds (perf/analyze.py)",
           "request_costs": "estimate: per-object request profile measured against MinIO through a counting proxy "
                            "(dry run) x the cloud object counts", "legs": {}}
    for leg, (L, info, sink) in legs.items():
        rs = rows[leg]
        fault_rows = [r for r in rs if r["type"] in ("corrupt", "lost") and not r.get("warmup")]
        e = {"session": L["session"], "sink_region": sink["region"], "sink_az": sink["az"],
             "sink_cpu_count": info["agent"]["cpu_count"], "vtop": info["vtop"]["version"],
             "images": info["images"], "s3sink_zip_sha256": info["s3sink_zip_sha256"],
             "object_bytes_target": info["object_bytes_target"], "records_per_object": info["records_per_object"],
             "age_s": info["age_s"], "confluent_tasks": info["confluent_tasks"], "measurement": info["measurement"],
             "runs": L["runs"], "measured": L["measured"], "passing": L["passing"], "failed": L["failed"],
             # offline re-judgements applied by perf/analyze.py (rejudge-*.json; perf/LESSONS.md 2026-11-22)
             **({"rejudged": L["rejudged"]} if L.get("rejudged") else {}),
             "warmup_runs": sum(bool(r.get("warmup")) for r in rs),
             "rtt_sink_ms_median": {"start": L["rtt"]["rtt_start_sink"]["median_ms"],
                                    "end": L["rtt"]["rtt_end_sink"]["median_ms"]},
             "tput_topic": keep(L["tput_topic"], ("topic", "partitions", "records", "payload_bytes", "mismatches")),
             "tput": {c: keep(x, ("runs", "done_mib_s", "committed_mib_s", "archived_mib_s", "done_vs_ref_pct",
                                  "done_vs_size_only_pct", "cpu_s_per_gib", "cpu_per_gib_vs_ref_pct", "cpu_cores_median",
                                  "peak_anon_mib_median", "data_objects_per_gib", "raw_mib_per_object_median",
                                  "compression_ratio_median", "request_usd_per_gib_est", "sink_ec2_usd_per_gib",
                                  "transfer_usd_per_gib", "ena_allowance_exceeded_sink"))
                      for c, x in L["tput"].items()},
             "live": {c: keep(x, ("runs", "end_to_end_s_p50", "end_to_end_s_p90", "end_to_end_s_p99", "end_to_end_s_p100",
                                  "object_visible_s_p50", "commit_s_p50", "missing_records"))
                      for c, x in L["live"].items()},
             "kill": {c: keep(x, ("runs", "ready_s", "first_commit_after_s", "first_object_after_s", "resteady_s",
                                  "resteady_not_reached", "duplicate_records", "complete"))
                      for c, x in L["kill"].items()},
             "faults": {}, "nonfault_unsafe": sum(x["unsafe"] for k, x in L["correctness"].items()
                                                  if k.split("/")[0] in ("tput", "live", "kill")),
             "nonfault_lost_committed_records": sum(x["lost_committed_records"] for k, x in L["correctness"].items()
                                                    if k.split("/")[0] in ("tput", "live", "kill")),
             "nonfault_incomplete": sum(x["runs"] - x["complete"] for k, x in L["correctness"].items()
                                        if k.split("/")[0] in ("tput", "live", "kill"))}
        if info.get("fault_topic"):
            e["fault_topic"] = keep(info["fault_topic"], ("topic", "partitions", "records", "payload_bytes", "mismatches"))
        for k, x in L["faults"].items():
            f = keep(x, ("trials", "classes", "unsafe_trials", "unsafe_trials_rq4_comparable", "rq4_comparable_trials",
                         "lost_committed_records", "complete", "nth", "vtop_vs_verification", "vtop_post_scrub_rc",
                         "vtop_post_audit_coverage_rc"))
            fk, cfg = k.split("/")
            if cfg.startswith("vtop-"):  # how far before VTOP's logged verification the damage landed (two host clocks)
                pre = [r for r in fault_rows if r["type"] == fk and r["config"] == cfg
                       and r["fault"]["damage_vs_vtop_verification"] == "before_verification"]
                f["damage_before_verification_ms"] = [round((r["fault"]["vtop_verified_at"] - r["fault"]["t_damaged"]) * 1000, 1)
                                                      for r in pre]
            e["faults"][k] = f
        out["legs"][leg] = e
    out["distance_effect"] = regen["distance_effect"]
    # Claims the text states, each derived here from the reproduced summary.
    use, syd = out["legs"]["us-east-1"], out["legs"]["ap-southeast-2"]
    done = lambda e, c: e["tput"][c]["done_mib_s"][0]  # noqa: E731
    ratio = lambda e, a, b: round(done(e, a) / done(e, b), 2)  # noqa: E731
    vf = {k: x for k, x in use["faults"].items() if k.endswith("/vtop-blake3-readback")}
    sf = {k: x for k, x in use["faults"].items() if k.split("/")[1] in PERF_SINKS}
    out["claims"] = {
        "rtt_ms": {leg: e["rtt_sink_ms_median"]["start"] for leg, e in out["legs"].items()},
        "confluent_over_vtop_store": {leg: ratio(e, "confluent", REF_PERF) for leg, e in out["legs"].items()},
        "confluent_over_vtop_size_only": {leg: ratio(e, "confluent", "vtop-size-only") for leg, e in out["legs"].items()},
        "vector_over_vtop_store": {leg: ratio(e, "vector", REF_PERF) for leg, e in out["legs"].items()},
        "size_only_vs_store_pct": {leg: e["tput"]["vtop-size-only"]["done_vs_ref_pct"] for leg, e in out["legs"].items()},
        "faster_than_vtop_store_resolved": {leg: [c for c in PERF_SINKS if e["tput"][c]["done_vs_ref_pct"][1] > 0]
                                            for leg, e in out["legs"].items()},
        "slower_than_vtop_store_resolved": {leg: [c for c in PERF_SINKS if e["tput"][c]["done_vs_ref_pct"][2] < 0]
                                            for leg, e in out["legs"].items()},
        # lowest CPU per GiB: every sink's CPU/GiB above VTOP store SHA-256 with the CI on the change above 0
        "vtop_store_lowest_cpu_per_gib_resolved": {leg: all(e["tput"][c]["cpu_per_gib_vs_ref_pct"][1] > 0 for c in PERF_SINKS)
                                                   for leg, e in out["legs"].items()},
        "cpu_s_per_gib": {leg: {c: e["tput"][c]["cpu_s_per_gib"][0] for c in e["tput"]} for leg, e in out["legs"].items()},
        "peak_anon_mib": {leg: {c: e["tput"][c]["peak_anon_mib_median"] for c in e["tput"]} for leg, e in out["legs"].items()},
        "vtop_store_cores": {leg: e["tput"][REF_PERF]["cpu_cores_median"] for leg, e in out["legs"].items()},
        "live_p50_s": {leg: {c: e["live"][c]["end_to_end_s_p50"][0] for c in e["live"]} for leg, e in out["legs"].items()},
        "kill_first_commit_s": {leg: {c: e["kill"][c]["first_commit_after_s"][0] for c in e["kill"]}
                                for leg, e in out["legs"].items()},
        "kill_duplicate_records": {leg: {c: e["kill"][c]["duplicate_records"] for c in e["kill"]}
                                   for leg, e in out["legs"].items()},
        "kill_complete_all": all(x["complete"] == x["runs"] for e in out["legs"].values() for x in e["kill"].values()),
        "nonfault_unsafe": sum(e["nonfault_unsafe"] for e in out["legs"].values()),
        "nonfault_lost_committed_records": sum(e["nonfault_lost_committed_records"] for e in out["legs"].values()),
        "fault_sinks_trials": sum(x["trials"] for x in sf.values()),
        "fault_sinks_unsafe": sum(x["unsafe_trials"] for x in sf.values()),
        "fault_sinks_rq4_comparable": sum(x["rq4_comparable_trials"] for x in sf.values()),
        "fault_sinks_unsafe_rq4_comparable": sum(x["unsafe_trials_rq4_comparable"] for x in sf.values()),
        "fault_sinks_lost_committed_per_trial_range": [min(v for x in sf.values() for v in x["lost_committed_records"]),
                                                       max(v for x in sf.values() for v in x["lost_committed_records"])],
        "fault_vtop_trials": sum(x["trials"] for x in vf.values()),
        "fault_vtop_rq4_comparable": sum(x["rq4_comparable_trials"] for x in vf.values()),
        "fault_vtop_unsafe_final": sum(x["unsafe_trials"] for x in vf.values()),
        "fault_vtop_lost_committed_per_trial_range": [min(v for x in vf.values() for v in x["lost_committed_records"]),
                                                      max(v for x in vf.values() for v in x["lost_committed_records"])],
        "fault_vtop_vs_verification": dict(sum((Counter(x["vtop_vs_verification"]) for x in vf.values()), Counter())),
        "fault_vtop_damage_before_verification_ms": [v for x in vf.values() for v in x["damage_before_verification_ms"]],
        "fault_vtop_scrub_flagged": sum(rc != 0 for x in vf.values() for rc in x["vtop_post_scrub_rc"]),
        "fault_vtop_audit_coverage_flagged": {k: sum(rc != 0 for rc in x["vtop_post_audit_coverage_rc"]) for k, x in vf.items()},
    }
    summary["rq5_perf"] = out
    _perf_tables(out)


def _perf_tables(out):
    use, syd = out["legs"]["us-east-1"], out["legs"]["ap-southeast-2"]
    n0 = lambda v: f"{v:,.0f}".replace(",", "{,}")  # noqa: E731
    f1 = lambda v: "--" if v is None else f"{v:.1f}"  # noqa: E731
    f2 = lambda v: "--" if v is None else f"{v:.2f}"  # noqa: E731
    ci1 = lambda v: "--" if v is None else f"{v[0]:.1f} [{v[1]:.1f}, {v[2]:.1f}]"  # noqa: E731
    ci2 = lambda v: "--" if v is None else f"{v[0]:.2f} [{v[1]:.2f}, {v[2]:.2f}]"  # noqa: E731
    med_of = lambda e, part, c, k: e[part][c][k][0] if c in e[part] else None  # noqa: E731
    # Compact paper table: done throughput in both legs; CPU, p50 latency and SIGKILL recovery in us-east-1.
    t = [header("E-RQ5-PERF compact: medians; done MiB/s in both legs, CPU s/GiB, live p50 and SIGKILL recovery in "
                "us-east-1 (full tables with CIs: rq5_perf_tput/_live/_kill/_faults.tex)"),
         r"\begin{tabular}{@{}lrrrrrr@{}}", r"\toprule",
         r"& \multicolumn{2}{c}{Done MiB/s$^a$} & CPU & p50 & \multicolumn{2}{c}{After a kill$^b$} \\"
         r" \cmidrule(lr){2-3}\cmidrule(l){6-7}",
         r"System & In-reg. & Sydney & s/GiB & s & Commit s & Dup. \\", r"\midrule"]
    for c in PERF_ORDER:
        k = use["kill"].get(c)
        dup = "--" if k is None else n0(max(k["duplicate_records"]))
        t.append(f"{PERF_LABEL[c]} & {f1(med_of(use, 'tput', c, 'done_mib_s'))} & {f1(med_of(syd, 'tput', c, 'done_mib_s'))} & "
                 f"{f1(med_of(use, 'tput', c, 'cpu_s_per_gib'))} & {f2(med_of(use, 'live', c, 'end_to_end_s_p50'))} & "
                 f"{f1(med_of(use, 'kill', c, 'first_commit_after_s'))} & {dup} \\\\")
        if c == "vtop-sha256-gzip6":
            t.append(r"\addlinespace[1pt]")
    t += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_perf.tex").write_text("\n".join(t) + "\n")
    # Supplement: throughput, CPU, memory and cost per leg.
    p = [header("E-RQ5-PERF throughput, CPU, memory and cost (supplement); medians [95% CI]; request cost is an estimate"),
         r"\begin{tabular}{@{}llrrrrrrr@{}}", r"\toprule",
         r"Leg & System & Done MiB/s & vs ref.\ \% & CPU s/GiB & Cores & Peak MiB & Req.$^a$ & EC2$^b$ \\",
         r"\midrule"]
    for leg, e in out["legs"].items():
        p.append(r"\addlinespace[1pt]" if leg != "us-east-1" else "%")
        for i, c in enumerate([c for c in PERF_ORDER if c in e["tput"]]):
            x = e["tput"][c]
            who = LEG_SHORT[leg] if i == 0 else ""
            p.append(f"{who} & {PERF_LABEL[c]} & {ci2(x['done_mib_s'])} & {ci1(x.get('done_vs_ref_pct'))} & "
                     f"{ci1(x['cpu_s_per_gib'])} & {f2(x['cpu_cores_median'])} & {x['peak_anon_mib_median']:.0f} & "
                     f"{x['request_usd_per_gib_est'] * 1e6:.0f} & {x['sink_ec2_usd_per_gib'] * 1e6:.0f} \\\\")
    p += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_perf_tput.tex").write_text("\n".join([q for q in p if q != "%"]) + "\n")
    # Supplement: latency of the live runs.
    lt = [header("E-RQ5-PERF latency (supplement): produce -> archived intact and committed, live runs at 30,000 "
                 "records/s; medians of the per-run percentiles [95% CI]"),
          r"\begin{tabular}{@{}llrrrrr@{}}", r"\toprule",
          r"Leg & System & p50 s & p90 s & p99 s & Object p50 s & Commit p50 s \\", r"\midrule"]
    for leg, e in out["legs"].items():
        if leg != "us-east-1":
            lt.append(r"\addlinespace[1pt]")
        for i, c in enumerate([c for c in PERF_ORDER if c in e["live"]]):
            x = e["live"][c]
            lt.append(f"{LEG_SHORT[leg] if i == 0 else ''} & {PERF_LABEL[c]} & {ci2(x['end_to_end_s_p50'])} & "
                      f"{ci2(x['end_to_end_s_p90'])} & {ci2(x['end_to_end_s_p99'])} & {f2(x['object_visible_s_p50'][0])} & "
                      f"{f2(x['commit_s_p50'][0])} \\\\")
    lt += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_perf_live.tex").write_text("\n".join(lt) + "\n")
    # Supplement: recovery after one SIGKILL.
    kt = [header("E-RQ5-PERF recovery after one SIGKILL (supplement); seconds from the kill, medians [95% CI]; "
                 "duplicate records per run"),
          r"\begin{tabular}{@{}llrrrrl@{}}", r"\toprule",
          r"Leg & System & Ready & First commit & First object & Re-steady & Duplicate records \\", r"\midrule"]
    for leg, e in out["legs"].items():
        if leg != "us-east-1":
            kt.append(r"\addlinespace[1pt]")
        for i, c in enumerate([c for c in PERF_ORDER if c in e["kill"]]):
            x = e["kill"][c]
            kt.append(f"{LEG_SHORT[leg] if i == 0 else ''} & {PERF_LABEL[c]} & {f2(x['ready_s'][0])} & {ci2(x['first_commit_after_s'])} & "
                      f"{f2(x['first_object_after_s'][0])} & {f1(x['resteady_s'][0])} & "
                      f"{', '.join(n0(v) for v in x['duplicate_records'])} \\\\")
    kt += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_perf_kill.tex").write_text("\n".join(kt) + "\n")
    # Supplement: storage faults on real S3 (us-east-1).
    CL = {"damage_before_commit": "B", "commit_before_object_visible": "V", "damage_after_commit": "A",
          "ambiguous_within_one_poll": "P"}
    ft = [header("E-RQ5-PERF storage faults on Amazon S3 (supplement; us-east-1): native overwrite or delete of the "
                 "N-th data object once listed; RQ4's oracle"),
          r"\begin{tabular}{@{}llrlrrll@{}}", r"\toprule",
          r"Fault & System & Trials & Classes$^a$ & Unsafe & RQ4-comp.$^b$ & Lost committed per trial & \sysname{} check$^c$ \\",
          r"\midrule"]
    last = None
    for k, x in use["faults"].items():
        fk, cfg = k.split("/")
        if last is not None and fk != last:
            ft.append(r"\addlinespace[1pt]")
        cls = ", ".join(f"{CL[a]} {n}" for a, n in sorted(x["classes"].items(), key=lambda kv: list(CL).index(kv[0])))
        lost = x["lost_committed_records"]
        vt = "--"
        if x.get("vtop_vs_verification"):
            v = x["vtop_vs_verification"]
            vt = (f"after {v.get('after_verification', 0)}, before {v.get('before_verification', 0)}; "
                  f"scrub {sum(rc != 0 for rc in x['vtop_post_scrub_rc'])}/{x['trials']}")
        name = ("Corrupted" if fk == "corrupt" else "Deleted") if fk != last else ""
        span = n0(lost[0]) if min(lost) == max(lost) else f"{n0(min(lost))}--{n0(max(lost))}"
        ft.append(f"{name} & {PERF_LABEL[cfg]} & {x['trials']} & {cls} & {x['unsafe_trials']} & "
                  f"{x['unsafe_trials_rq4_comparable']}/{x['rq4_comparable_trials']} & {span} & {vt} \\\\")
        last = fk
    ft += [r"\bottomrule", r"\end{tabular}"]
    (OUT_T / "rq5_perf_faults.tex").write_text("\n".join(ft) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", choices=sorted(SETS), default="v070", help="result set (build) to analyze")
    ap.add_argument("--out", default=None, help="output directory (default: the set's own)")
    ap.add_argument("--stamp", default=None, help="commit to record as generated_from_commit (default: git HEAD)")
    a = ap.parse_args()
    configure(a.set, a.out, a.stamp)
    if C["extras"]:
        rq1_model_v010()
    else:
        rq1_model()
    rq1_crash()
    rq2()
    rq2_norecheck()
    rq3()
    rq3_minio()
    rq3_latency()
    rq4()
    if C["extras"]:
        batch_sizes()
        rq5_cost()
        rq5_wan()
        rq5_tamper()
        rq5_scale()
        rq5_perf()
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=float))
    print("wrote", OUT / "summary.json", sorted(p.name for p in OUT_T.glob("*.tex")),
          sorted(p.name for p in OUT_F.glob("*.pdf")))


if __name__ == "__main__":
    main()
