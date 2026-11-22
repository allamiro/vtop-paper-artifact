#!/usr/bin/env python3
"""Recompute every v0.10.4 number in research/analysis/v0104/summary.json from the run files, independently of
analyze.py (no import of it), and check the regenerated tables. The v0.7.0 counterpart is
verify_manuscript_data.py; this script does not read the manuscript, which still describes v0.7.0.

  research/.venv/bin/python research/analysis/scripts/verify_v0104_data.py [--skip-repro]

Sources: experiments/*/results/20261122-v0104-* (trials.jsonl, runs.jsonl, batches.csv; raw/ engine logs for batch
sizes when present), the v0.7.0-era RQ4 baseline trials, rq5-cloud cost and WAN runs.jsonl, the E-RQ5-TAMPER
cases.jsonl (AWS run and three local runs), the E-RQ5-SCALE hosts' host.json/engines.jsonl with schedule.json and steps.csv,
the E-RQ5-PERF runs.jsonl with the observer series (series/*.json.gz) and latency samples of both legs,
state-machine/results/20261120-v010-sweep*.json. Bootstraps are re-implemented here with the algorithms and seeds
the pipeline documents (data-seeded percentile bootstrap for local results; fixed-seed ci/pct_ci for the cloud).
The last check re-runs `analyze.py --set v070` into a scratch directory and compares the submitted summary.json
byte for byte (skip with --skip-repro).
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import statistics
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
RES = ROOT / "research"
EXP = RES / "experiments"
S = json.loads((RES / "analysis/v0104/summary.json").read_text())
TAB = RES / "analysis/v0104/tables"
BUILD = "be5b419"
checks: list[str] = []


FAILED: list[str] = []
COLLECT = __import__("os").environ.get("VERIFY_COLLECT") == "1"  # list every failing check instead of stopping


def require(cond, label):
    if not cond:
        if COLLECT:
            FAILED.append(label)
            return
        raise AssertionError(label)
    checks.append(label)


def jl(p: Path):
    return [json.loads(x) for x in p.read_text().splitlines() if x.strip()]


def close(a, b, tol=1e-9):
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(close(x, y, tol) for x, y in zip(a, b))
    if a is None or b is None:
        return a is b
    if isinstance(a, float) and np.isnan(a):
        return isinstance(b, float) and np.isnan(b)
    return abs(float(a) - float(b)) <= tol


# ---- bootstraps (re-implemented) -------------------------------------------------------------------------------
def bmed(x, n=10000):
    """Percentile bootstrap of the median, RNG seeded from the sample bytes (analysis convention since the C7 audit)."""
    x = np.asarray(x, dtype=float)
    if len(x) == 0:
        return [np.nan] * 3
    if len(x) == 1:
        return [x[0]] * 3
    seed = int.from_bytes(hashlib.sha256(np.ascontiguousarray(x).tobytes()).digest()[:8], "little")
    meds = np.median(x[np.random.default_rng([20261110, seed]).integers(0, len(x), size=(n, len(x)))], axis=1)
    return [float(np.median(x)), float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5))]


def bdrop(base, x, n=10000):
    base, x = np.asarray(base, dtype=float), np.asarray(x, dtype=float)
    seed = int.from_bytes(hashlib.sha256(np.ascontiguousarray(np.concatenate([base, x])).tobytes()).digest()[:8], "little")
    rng = np.random.default_rng([20261111, seed])
    mb = np.median(base[rng.integers(0, len(base), size=(n, len(base)))], axis=1)
    mx = np.median(x[rng.integers(0, len(x), size=(n, len(x)))], axis=1)
    d = (mb - mx) / mb * 100
    pt = (np.median(base) - np.median(x)) / np.median(base) * 100
    return [round(float(pt), 1), round(float(np.percentile(d, 2.5)), 1), round(float(np.percentile(d, 97.5)), 1)]


def fci(values, seed, other=None, B=10_000):
    """Fixed-seed percentile bootstrap of a median (or of a difference of medians), rounded to 4 places."""
    x = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
    if other is None:
        samples, center = xs, float(np.median(x))
    else:
        y = np.asarray(other, dtype=float)
        ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
        samples, center = xs - ys, float(np.median(x) - np.median(y))
    return [round(center, 4), round(float(np.quantile(samples, 0.025)), 4), round(float(np.quantile(samples, 0.975)), 4)]


def fpct(values, baseline, seed, B=10_000):
    x, y = np.asarray(values, dtype=float), np.asarray(baseline, dtype=float)
    rng = np.random.default_rng(seed)
    xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
    ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
    samples = 100 * (xs - ys) / ys
    center = 100 * (float(np.median(x)) - float(np.median(y))) / float(np.median(y))
    return [round(center, 4), round(float(np.quantile(samples, 0.025)), 4), round(float(np.quantile(samples, 0.975)), 4)]


# ============================================================================== build
D = {k: EXP / v for k, v in {
    "crash": "rq1-source-safety/results/20261122-v0104-crash", "kafka": "rq1-source-safety/results/20261122-v0104-kafka",
    "tamper": "rq2-corruption-recovery/results/20261122-v0104-tamper",
    "norecheck": "rq2-corruption-recovery/results/20261122-v0104-norecheck",
    "cost": "rq3-cost-ablation/results/20261122-v0104-cost", "cost_run": "rq3-cost-ablation/results/20261122-v0104-cost-run",
    "rq4": "rq4-baselines/results/20261122-v0104-rq4", "rq4_v070": "rq4-baselines/results/20261111-v070-rq4",
    "vector": "rq4-baselines/results/20261111-rq4-vector", "fluentbit": "rq4-baselines/results/20261111-rq4-fluentbit"}.items()}
COST = [EXP / "rq5-cloud/cost/results/20261122T021852Z-i-0d197e09de6af68de",
        EXP / "rq5-cloud/cost/results/20261122T041424Z-i-03c2592cd984f1276"]
WAN = [EXP / "rq5-cloud/wan/results" / n for n in (
    "20261122T021853Z-eu-west-1-i-036821b8632514571", "20261122T021917Z-ap-southeast-2-i-0819b6cd4872adbb3",
    "20261122T021935Z-sa-east-1-i-0093971b23ec6758c", "20261122T021956Z-ap-northeast-1-i-047cf9c9a10e500d0")]

for k in ("crash", "kafka", "tamper", "norecheck", "cost", "cost_run", "rq4"):
    require(json.loads((D[k] / "run.json").read_text())["vtop_commit"] == BUILD, f"build: {k} ran vtop-engine {BUILD}")
require(len({json.loads(p.read_text())["vtop_commit"] for p in EXP.glob("*/results/20261122-v0104-*/run.json")}) == 1,
        "build: every 20261122-v0104-* run.json names one engine commit")
for d in COST + WAN:
    require(json.loads((d / "run.json").read_text())["vtop"]["commit"].startswith(BUILD), f"build: cloud {d.name} ran {BUILD}")
require(S["vtop_commit"] == BUILD and S["result_set"] == "v0104", "summary: result set v0104, engine be5b419")
require(json.loads((D["rq4_v070"] / "run.json").read_text())["vtop_commit"] == "6c8f386",
        "RQ4 baselines come from the v0.7.0-era run (sinks not re-run); only its Confluent rows are used")

# ============================================================================== RQ1 crash + Kafka
crash = jl(D["crash"] / "trials.jsonl")
kafka = jl(D["kafka"] / "trials.jsonl")
for r in kafka:
    r["lost_bytes"] = r["lost_records"]
by = defaultdict(list)
for r in crash + kafka:
    by[r["scenario"]].append(r)
SC = S["rq1_crash"]
require(sorted(SC) == sorted(by), "RQ1: same six scenarios as v0.7.0 (file-mix, spool-mix, power-loss, hdfs, 2x Kafka)")
for s, rs in by.items():
    c = SC[s]
    dup = [r["duplicate_objects"] for r in rs]
    amp = [r["replay_amplification"] * 100 for r in rs]
    firsts = [e["first_commit_s"] for r in rs for e in r["events"] if e.get("first_commit_s") is not None and e["life"] > 0]
    ok = (c["trials"] == len(rs) and c["kills"] == sum(r["kills"] for r in rs)
          and c["unsafe_commits"] == sum(r["unsafe_commits_at_crash"] + r["unsafe_commits_final"] for r in rs)
          and c["complete_trials"] == sum(r["complete"] for r in rs)
          and c["unarchived_at_end_uncommitted"] == sum(r["lost_bytes"] for r in rs)
          and c["bad_objects"] == sum(r["bad_objects_final"] for r in rs)
          and close(c["dup_objects_median_ci"], bmed(dup)) and c["dup_objects_max"] == max(dup)
          and close(c["replay_amp_pct_median_ci"], bmed(amp)) and close(c["first_commit_after_restart_s_median_ci"], bmed(firsts))
          and c["manifest_start_understated"] == sum(r["manifest_start_understated"] for r in rs)
          and c["manifests"] == sum(r["manifests"] for r in rs)
          and c["outages"] == sum(1 for r in rs for e in r["events"] if e["outage"])
          and c["wal_deleted"] == sum(1 for r in rs for e in r["events"] if e.get("wal_deleted")))
    require(ok, f"RQ1 {s}: trials, kills, unsafe, complete, duplicates [CI], amplification, manifests recomputed")
tot = lambda k: sum(v[k] for v in SC.values())  # noqa: E731
require(tot("trials") == 49 and tot("kills") == 323 and tot("unsafe_commits") == 0 and tot("complete_trials") == 49,
        "RQ1 totals: 49 trials, 323 kills, 0 unsafe, 49/49 complete")
require(sum(SC[s]["kills"] for s in ("file-mix", "spool-mix", "power-loss", "hdfs")) == 278
        and sum(SC[s]["trials"] for s in ("file-mix", "spool-mix", "power-loss", "hdfs")) == 35,
        "RQ1 file/spool/power-loss/HDFS: 35 trials, 278 kills")
require(SC["kafka-ssh-p1"]["trials"] + SC["kafka-hdfs-p4"]["trials"] == 14
        and SC["kafka-ssh-p1"]["kills"] + SC["kafka-hdfs-p4"]["kills"] == 45
        and SC["kafka-ssh-p1"]["complete_trials"] + SC["kafka-hdfs-p4"]["complete_trials"] == 14,
        "RQ1 Kafka: 14 trials, 45 kills, 14/14 complete (no stall; a trial stops killing once drained)")
require(tot("outages") == 47, "RQ1: 47 storage or broker outages")
require(tot("unarchived_at_end_uncommitted") == 0 and tot("bad_objects") == 0, "RQ1: nothing unarchived, no bad object")
require(all(max(r["duplicate_objects"] for r in rs) == 0 for rs in by.values())
        and sum(r.get("duplicate_bytes", 0) for r in crash) == 0, "RQ1: 0 duplicate objects in every trial (incl. power loss)")
require(tot("manifest_start_understated") == 0 and tot("manifests") == 5825,
        "RQ1: 0 of 5,825 manifests understate their start (#622 fixed)")
cuts = [e["power_cut"] for r in by["power-loss"] for e in r["events"] if e.get("power_cut")]
pl = SC["power-loss"]
require(len(cuts) == 80 == pl["power_cuts"] and tot("wal_deleted") == 0,
        "RQ1 power loss: 80 emulations, all WAL truncations (no whole-WAL deletion)")
require(sum(1 for c in cuts if c["dropped_commits"] > 0) == pl["power_cuts_dropping_commits"] == 47
        and sum(c["dropped_commits"] for c in cuts) == pl["power_cut_dropped_commits_total"] == 408
        and sum(1 for c in cuts if c["wal_bytes"] == 0) == pl["power_cuts_empty_wal"] == 1,
        "RQ1 power loss: 47 of 80 cuts dropped ledger commits (408 in all); 1 found an empty WAL")
require(close(pl["dup_objects_median_ci"], [0, 0, 0]) and close(pl["replay_amp_pct_median_ci"], [0, 0, 0]),
        "RQ1 power loss: median 0 duplicate objects [0, 0], 0% replayed bytes")
kd = [r["final_drain_s"] for r in kafka]
require(round(statistics.median(kd), 1) == 0.0 and sum(d < 0.01 for d in kd) == 8 and all(r["kills"] < 4 for r in kafka if r["scenario"] == "kafka-ssh-p1"),
        "RQ1 Kafka: median final drain 0.0 s (every SSH trial drained within 2-3 lives; HDFS 6.4-13.5 s)")

# ============================================================================== RQ2
tam = jl(D["tamper"] / "trials.jsonl")
cells = defaultdict(lambda: [0, 0, 0])
for r in tam:
    c = cells[(r["mutation"], r["variant"])]
    c[0] += r["outcome"] == "detected"; c[1] += 1; c[2] += r["archive_complete"]
ok = all(S["rq2"]["cells"][m][v] == {"detected": d, "trials": n, "archive_complete_after": a}
         for (m, v), (d, n, a) in cells.items())
require(ok and len(tam) == S["rq2"]["trials"] == 108, "RQ2: 108 trials; every mutation x variant cell recomputed")
strong = sum(cells[(f"M{i}", v)][0] for i in range(1, 9) for v in ("strong", "strong+mac"))
require(strong == 64 and sum(cells[(f"M{i}", v)][1] for i in range(1, 9) for v in ("strong", "strong+mac")) == 64,
        "RQ2: strong and strong+MAC refused 64/64 mutations")
require(cells[("M1", "size-only")][:2] == [0, 4], "RQ2: size-only accepted the same-size byte flip in 4/4 trials")
require(all(cells[(f"M{i}", "size-only")][0] == 4 for i in range(2, 9)), "RQ2: size-only caught M2-M8 (28/28)")
require(all(cells[("M0", v)][0] == 0 for v in ("strong", "strong+mac", "size-only")), "RQ2: controls never flagged")
uns = [r for r in tam if r["unsafe_commit"]]
require(len(uns) == 4 == S["rq2"]["unsafe_commits"] and all(r["mutation"] == "M1" and r["variant"] == "size-only" for r in uns),
        "RQ2: 4 unsafe commits, all size-only M1")
require(all(r["archive_complete"] for r in tam if r["variant"] != "size-only" or r["mutation"] != "M1"),
        "RQ2: every archive complete after recovery except the size-only bit flips")
nr = jl(D["norecheck"] / "trials.jsonl")
mut = [r for r in nr if r["mutation"] != "M0"]
N = S["rq2_norecheck"]
require(len(mut) == 32 == N["mutations"] and sum(r["outcome"] == "accepted" for r in mut) == 32 == N["mutations_accepted"],
        "RQ2 ablation: 32/32 mutations accepted with the restart re-check disabled")
ub = {m: sum(bool(r["unsafe_commit"]) for r in mut if r["mutation"] == m) for m in sorted({r["mutation"] for r in mut})}
require(sum(ub.values()) == 24 == N["unsafe_commits"] and ub == N["unsafe_by_mutation"] and ub["M5"] == ub["M6"] == 0,
        "RQ2 ablation: 24 unsafe commits (M1-M4, M7, M8; none for M5, M6)")
require(json.loads((D["norecheck"] / "run.json").read_text())["ablation_engine_sha256"] == N["patched_source_sha256"],
        "RQ2 ablation: patched engine hash recorded")

# ============================================================================== RQ3 local
runs = jl(D["cost"] / "runs.jsonl")
rm = [r for r in jl(D["cost_run"] / "runs.jsonl") if r.get("drained")]
require(len(rm) == 190 and S["rq3_throughput_source"] == "vtopctl run (continuous)", "RQ3: 190 drained run-mode runs")
tput, cpu, rss = defaultdict(list), defaultdict(list), defaultdict(list)
for r in rm:
    k = (r["dataset"], r["variant"])
    tput[k].append(r["mib_per_s"]); cpu[k].append(r["cpu_s"] / (r["bytes"] / 2**30)); rss[k].append(r["max_rss_mib"])
Q = S["rq3"]
ok = all(close(Q["throughput_mib_s"]["|".join(k)], bmed(v)) and close(Q["cpu_s_per_gib"]["|".join(k)], bmed(cpu[k]))
         and close(Q["max_rss_mib"]["|".join(k)], bmed(rss[k])) for k, v in tput.items())
require(ok and len(Q["throughput_mib_s"]) == len(tput), "RQ3: every throughput, CPU s/GiB and RSS median [CI] recomputed")
V = ["size-only", "sha256", "blake3", "sha256+mac", "sha256-nocomp"]
DS = ["ssh", "bgl", "hdfs"]
require(all(Q["throughput_drop_vs_size_only_pct"][d][v] == round((bmed(tput[(d, "size-only")])[0] - bmed(tput[(d, v)])[0])
                                                                 / bmed(tput[(d, "size-only")])[0] * 100, 1)
            for d in DS for v in V), "RQ3: drop vs size-only recomputed for every cell")
require(all(close(Q["throughput_drop_vs_size_only_ci"][d][v], bdrop(tput[(d, "size-only")], tput[(d, v)]))
            for d in DS for v in V if v != "size-only"), "RQ3: CI on every drop (difference of medians) recomputed")
require(all(close(Q["mac_drop_vs_sha256_ci"][d], bdrop(tput[(d, "sha256")], tput[(d, "sha256+mac")])) for d in DS),
        "RQ3: MAC vs SHA-256 CI recomputed")
ci = Q["throughput_drop_vs_size_only_ci"]
comp = ["sha256", "blake3", "sha256+mac"]
require(all(ci[d][v][1] > 0 for d in DS for v in comp),
        "RQ3: compressed drop resolved on every dataset and variant (v0.10.2: all but SSH sha256+mac)")
drops = [ci[d][v][0] for d in DS for v in comp]
require(min(drops) == 6.9 and max(drops) == 11.1, "RQ3: compressed drops 6.9-11.1% (v0.10.2: 4.3-6.6%; localfs fsync since #827)")
require([ci[d]["sha256-nocomp"][0] for d in DS] == [23.5, 18.2, 19.6] and all(ci[d]["sha256-nocomp"][1] > 0 for d in DS),
        "RQ3: uncompressed drops 23.5 (SSH), 18.2 (BGL), 19.6% (HDFS), all resolved")
require(all(c[1] <= 0 <= c[2] for c in Q["mac_drop_vs_sha256_ci"].values()), "RQ3: MAC vs SHA-256 unresolved on every dataset")
stages = ["compress_ms", "checksum_ms", "object_upload_ms", "manifest_upload_ms", "verify_ms", "commit_ms", "state_write_ms"]
agg, ratio, bsz = defaultdict(lambda: defaultdict(list)), defaultdict(list), defaultdict(list)
with open(D["cost"] / "batches.csv") as fh:
    for row in csv.DictReader(fh):
        bsz[row["batch_mib"]].append(int(row["uncompressed_bytes"]))
        if row["batch_mib"] != "16" or "batch" in row["variant"]:
            continue
        for st in stages:
            agg[(row["dataset"], row["variant"])][st].append(float(row[st] or 0))
        if row["variant"] != "sha256-nocomp":
            ratio[row["dataset"]].append(int(row["uncompressed_bytes"]) / int(row["compressed_bytes"]))
require(Q["compression_ratio_median"] == {d: round(float(np.median(ratio[d])), 1) for d in DS}
        and min(Q["compression_ratio_median"].values()) == 10.3 and max(Q["compression_ratio_median"].values()) == 14.3,
        "RQ3: zstd ratio per 16 MiB batch 10.3-14.3x recomputed")
require(all(close(Q["stage_mean_ms_per_batch"][f"{d}|{v}"][st], float(np.mean(agg[(d, v)][st])))
            for d in DS for v in V for st in stages), "RQ3: per-stage mean ms per 16 MiB batch recomputed")
cv = [np.mean(agg[(d, v)]["checksum_ms"]) + np.mean(agg[(d, v)]["verify_ms"]) for d in DS for v in comp]
cm = [np.mean(agg[(d, v)]["compress_ms"]) for d in DS for v in V[:4]]
nc = [np.mean(agg[(d, "sha256-nocomp")]["checksum_ms"]) + np.mean(agg[(d, "sha256-nocomp")]["verify_ms"]) for d in DS]
require((round(min(cv), 1), round(max(cv), 1), round(min(cm), 1), round(max(cm), 1), round(min(nc), 1), round(max(nc), 1))
        == (0.0, 0.7, 13.0, 18.8, 11.0, 12.0),
        "RQ3 stages: checksum+verify 0.0-0.7 ms compressed, 11.0-12.0 ms uncompressed; compression 13.0-18.8 ms")
ou = [np.mean(agg[(d, v)]["object_upload_ms"]) for d in DS for v in V]
mu = [np.mean(agg[(d, v)]["manifest_upload_ms"]) for d in DS for v in V]
require([round(min(ou), 1), round(max(ou), 1), round(min(mu), 1), round(max(mu), 1)] == [10.2, 20.3, 7.2, 10.7],
        f"RQ3 stages: localfs object upload {min(ou):.1f}-{max(ou):.1f} ms, manifest upload {min(mu):.1f}-{max(mu):.1f} ms "
        "per batch (fsync of file and directory since #827; 0.0 ms on v0.10.2)")
Brun = [r for r in rm if "batch" in r["variant"]]
sizes = sorted({r["batch_mib"] for r in Brun})
for d in ("bgl", "hdfs"):
    require(all(close(Q["batch_sweep"]["datasets"][d][str(s)], bmed([r["mib_per_s"] for r in Brun
                                                                     if r["dataset"] == d and r["batch_mib"] == s]))
                for s in sizes), f"RQ3 batch sweep {d.upper()}: median throughput [CI] per batch size recomputed")
bw = Q["batch_sweep"]["datasets"]["bgl"]
require(round(bw["1"][0]) == 37 and round(bw["64"][0]) == 311, "RQ3 batch sweep: BGL 37 -> 311 MiB/s from 1 to 64 MiB")
Bp = [r for r in runs if "batch" in r["variant"]]
mo = {str(s): bmed([r["manifest_bytes"] / r["compressed_bytes"] * 100 for r in Bp if r["batch_mib"] == s]) for s in sizes}
require(all(close(Q["batch_sweep"]["manifest_overhead_pct"][k], v) for k, v in mo.items()),
        "RQ3: manifest bytes per stored bytes per batch size recomputed")

# ============================================================================== X1 batch sizes
BS = S["batch_sizes"]
for k, e in BS["run_mode_local"].items():
    d, mib = k.split("|")
    rs = [r for r in jl(D["cost_run"] / "runs.jsonl") if r["dataset"] == d and r["batch_mib"] == int(mib)]
    n = {r["states"]["source_committed"] for r in rs}
    means = {round(r["bytes"] / r["states"]["source_committed"] / 2**20, 2) for r in rs}
    need = {-(-r["bytes"] // (int(mib) * 2**20)) for r in rs}
    require(sorted(n) == e["committed_batches"] and sorted(means) == e["mean_mib"] and n == need
            and max(means) <= int(mib), f"X1 run mode {k}: mean {max(means)} MiB <= setting; batch count = ceil(bytes/setting)")
require(all(max(v) <= int(k) * 2**20 for k, v in bsz.items())
        and all(BS["process_once_local"][k]["over_setting"] == 0 and BS["process_once_local"][k]["batches"] == len(v)
                for k, v in bsz.items()), "X1 process-once: no batch above its setting (1/4/16/64 MiB)")


def scan_logs(result_dir: Path, prefix=None):
    out = {}
    for tdir in sorted(p for p in (result_dir / "raw").iterdir() if p.is_dir()):
        if prefix and not tdir.name.startswith(prefix):
            continue
        mb = int(re.search(r"max_bytes:\s*(\d+)", (tdir / "config.yaml").read_text()).group(1))
        g = out.setdefault(re.sub(r"-t\d+(-\d+)?$", "", tdir.name), {"mb": mb, "b": {}})
        for lf in tdir.rglob("*.jsonl"):
            for line in lf.read_text(errors="replace").splitlines():
                if '"batch_metrics"' in line:
                    f = json.loads(line)["fields"]
                    g["b"][f["batch_id"]] = f["uncompressed_bytes"]
    return out


for key, d, pre in (("crash", D["crash"], None), ("kafka_rq1", D["kafka"], None), ("kafka_rq4_vtop", D["rq4"], "vtop-")):
    if not (d / "raw").is_dir():
        checks.append(f"X1 {key}: raw/ not present on this machine, not re-scanned")
        continue
    sc = scan_logs(d, pre)
    ok = all(BS[key][s]["batches"] == len(g["b"]) and BS[key][s]["max_bytes"] == g["mb"]
             and close(BS[key][s]["max_mib"], round(max(g["b"].values()) / 2**20, 3))
             and BS[key][s]["batches_over_setting"] == sum(v > g["mb"] for v in g["b"].values()) for s, g in sc.items())
    require(ok and sorted(sc) == sorted(BS[key]), f"X1 {key}: per-batch sizes re-scanned from engine logs")
require(all(e["batches_over_setting"] == 0 for e in BS["crash"].values()), "X1 files/spools/HDFS: no batch above max_bytes")
kmax = max(e["max_over_setting"] for k in ("kafka_rq1", "kafka_rq4_vtop") for e in BS[k].values())
require(kmax <= 1.01, f"X1 Kafka: largest batch {kmax:.3f}x max_bytes (v0.7.0: up to 2x)")
cl = [r["ledger"] for d in COST for r in jl(d / "runs.jsonl")]
require(max(x["batch_uncompressed_max"] for x in cl) <= 2**24 and sum(x["batches_over_max_bytes"] for x in cl) == 0
        and BS["cloud_cost"]["max_batch_uncompressed_bytes"] == max(x["batch_uncompressed_max"] for x in cl),
        "X1 cloud cost: every batch <= 16 MiB (ledger)")
wr = [r for d in WAN for r in jl(d / "runs.jsonl") if r.get("ledger") and r["ledger"].get("batch_uncompressed_max")]
over = [r for r in wr if r["ledger"]["batches_over_max_bytes"]]
require(len(over) == 4 == BS["cloud_wan"]["runs_with_batch_over_max_bytes"]
        and all(r["condition"] == "policed" and r["ledger"]["batches_over_max_bytes"] == 1
                and r["ledger"]["batch_uncompressed_max"] == 16777265 for r in over)
        and all(r["ledger"]["batch_uncompressed_max"] <= 2**24 for r in wr if r not in over),
        "X1 cloud WAN: one batch of 16,777,265 B (49 B over 16 MiB) in each policed run (uncompressed SSH slice); "
        "every other batch <= 16 MiB")

# ============================================================================== RQ4
t4 = [r for r in jl(D["rq4"] / "trials.jsonl")]
require(all(r["system"] == "vtop" for r in t4) and len(t4) == 25, "RQ4: 25 VTOP trials on v0.10.4")
allr = (t4 + [r for r in jl(D["rq4_v070"] / "trials.jsonl") if r["system"] == "confluent"]
        + jl(D["vector"] / "trials.jsonl") + jl(D["fluentbit"] / "trials.jsonl"))
C4 = S["rq4"]["cells"]
for f in ("corrupt", "lost", "kill"):
    for sysn in ("vtop", "confluent", "vector", "fluentbit"):
        every = [r for r in allr if r["fault"] == f and r["system"] == sysn]
        rs = [r for r in every if (any("fault" in e for e in r["proxy_events"]) if f != "kill" else r["kills"] > 0)]
        c = C4[f"{f}|{sysn}"]
        ok = (c["trials"] == len(rs) and c["trials_run"] == len(every)
              and c["unsafe_trials"] == sum(r["unsafe_final"] > 0 for r in rs)
              and c["lost_committed_records_total"] == sum(r["lost_committed_records"] for r in rs)
              and c["complete"] == sum(r["complete"] for r in rs) and c["kills"] == sum(r["kills"] for r in rs)
              and c["duplicate_objects_median"] == float(np.median([r["duplicate_objects"] for r in rs])))
        require(ok, f"RQ4 {f}|{sysn}: trials, unsafe, lost records, complete, duplicates recomputed")
require(all(C4[f"{f}|vtop"]["unsafe_trials"] == 0 and C4[f"{f}|vtop"]["lost_committed_records_total"] == 0
            for f in ("corrupt", "lost", "kill")), "RQ4: VTOP 0 unsafe, 0 lost in 25 trials")
require(sum(C4[f"{f}|vtop"]["complete"] for f in ("corrupt", "lost", "kill")) == 25 and S["rq4"]["vtop_incomplete"] == 0
        and S["rq4"]["vtop_stalls_before_fault"] == 0, "RQ4: VTOP 25/25 complete, no stall (v0.7.0: 4 of 27 stalled)")
require(max(r["elapsed_s"] for r in t4) == S["rq4"]["vtop_elapsed_s_max"] and round(S["rq4"]["vtop_elapsed_s_max"]) == 40,
        "RQ4: longest VTOP trial 40 s")
require(C4["kill|vtop"]["duplicate_objects_median"] == 0 and C4["kill|vtop"]["kills"] == 15,
        "RQ4: VTOP 0 duplicate objects (median) over 15 kills (v0.7.0: 2)")
for f in ("corrupt", "lost"):
    for sysn in ("confluent", "vector", "fluentbit"):
        require(C4[f"{f}|{sysn}"]["unsafe_trials"] == C4[f"{f}|{sysn}"]["trials"] == 10,
                f"RQ4 {sysn} {f}: committed past the damaged object in 10/10 (v0.7.0-era trials)")
require(S["rq4"]["minio_image_vtop"] != S["rq4"]["minio_image_baselines"],
        "RQ4: MinIO image digest differs between the VTOP v0.10.4 side and the baselines (same release, rebuilt)")

# ============================================================================== E-RQ5-COST
pre_cells = {}
for d in COST:
    info = json.loads((d / "run.json").read_text())
    rows = jl(d / "runs.jsonl")
    it = info["instance_type"]
    R5 = S["rq5_cost"][it]
    require(len(rows) == R5["runs"] and all(r["pass"] for r in rows) and R5["passing_runs"] == len(rows),
            f"E-RQ5-COST {it}: {len(rows)}/{len(rows)} runs pass (drained, oracle byte-exact, evidence agrees)")
    require(sum(r["oracle"]["unsafe_commits"] for r in rows) == 0 and all(r["oracle"]["complete"] for r in rows)
            and sum(r["oracle"]["multipart_objects"] or 0 for r in rows) == 0,
            f"E-RQ5-COST {it}: 0 unsafe, every archive complete, every object single-PUT")
    k = 0
    for ds in [x for x in ("ssh", "bgl", "hdfs") if x in info["matrix"]["datasets"]]:
        base = [r["mib_per_s"] for r in rows if r["dataset"] == ds and r["mode"] == "size-only"]
        for m in ("size-only", "sha256-store", "blake3-readback"):
            rs = [r for r in rows if r["dataset"] == ds and r["mode"] == m]
            thr = [r["mib_per_s"] for r in rs]
            c = R5["cells"][ds][m]
            ok = close(c["throughput_mib_s"], fci(thr, 20261121 + k), 1e-9)
            ok &= close(c["cpu_s_per_gib"], fci([r["cpu_s"] / (r["bytes"] / 2**30) for r in rs], 20261321 + k), 1e-9)
            if m != "size-only":
                ok &= close(c["diff_vs_size_only_mib_s"], fci(thr, 20261421 + k, other=base), 1e-9)
                ok &= close(c["change_vs_size_only_pct"], fpct(thr, base, 20261521 + k), 1e-9)
            ok &= close(c["allowance_exceeded_median"],
                        float(np.median([sum(v for kk, v in r["sys"].items() if kk.endswith("allowance_exceeded"))
                                         for r in rs])))
            require(ok and len(rs) == 10, f"E-RQ5-COST {it} {ds}/{m}: 10 runs; median MiB/s, change vs size-only [CI] recomputed")
            k += 1
med = S["rq5_cost"]["t4g.medium"]["cells"]
lar = S["rq5_cost"]["t4g.large"]["cells"]["hdfs"]
rb = [med[d]["blake3-readback"]["change_vs_size_only_pct"] for d in ("ssh", "bgl", "hdfs")]
require(all(c[2] < 0 for c in rb) and [round(c[0], 1) for c in rb] == [-16.3, -19.3, -17.8],
        "E-RQ5-COST t4g.medium: BLAKE3 read-back -16.3 / -19.3 / -17.8% (SSH/BGL/HDFS), all resolved")
st = [med[d]["sha256-store"]["change_vs_size_only_pct"] for d in ("ssh", "bgl", "hdfs")]
require(st[0][1] <= 0 <= st[0][2] and st[1][2] < 0 and st[2][2] < 0 and [round(c[0], 1) for c in st] == [-3.5, -3.2, -3.3],
        "E-RQ5-COST t4g.medium: store SHA-256 -3.5 / -3.2 / -3.3%, unresolved on SSH, resolved on BGL and HDFS")
require(round(lar["sha256-store"]["change_vs_size_only_pct"][0], 2) == -3.88 and lar["sha256-store"]["change_vs_size_only_pct"][2] < 0
        and round(lar["blake3-readback"]["change_vs_size_only_pct"][0], 2) == -18.95,
        "E-RQ5-COST HDFS re-check (t4g.large): store SHA-256 -3.88%, read-back -18.95%, both resolved")
hm = [med["hdfs"][m]["allowance_exceeded_median"] for m in ("size-only", "sha256-store", "blake3-readback")]
hl = [lar[m]["allowance_exceeded_median"] for m in ("size-only", "sha256-store", "blake3-readback")]
require(hm == [46.5, 11.0, 78.0] and hl == [40.0, 28.0, 112.5],
        "E-RQ5-COST: ENA allowance events (median) HDFS 46.5/11/78 on t4g.medium, 40/28/112.5 on t4g.large")
require(round(med["hdfs"]["size-only"]["per_batch_total_ms_median"]) == 232
        and round(lar["size-only"]["per_batch_total_ms_median"]) == 239 and round(lar["sha256-store"]["per_batch_total_ms_median"]) == 255,
        "E-RQ5-COST: per-batch total_ms HDFS 232 (medium, size-only); 239 -> 255 ms on t4g.large (store SHA-256)")

# ============================================================================== E-RQ5-WAN
W = S["rq5_wan"]
wrows, regions = [], []
for d in WAN:
    info = json.loads((d / "run.json").read_text())
    regions.append(info["region"])
    wrows += [{**r, "_region": info["region"]} for r in jl(d / "runs.jsonl")]
require(len(wrows) == 272 == W["runs"] and all(r["pass"] for r in wrows) and W["passing_runs"] == 272,
        "E-RQ5-WAN: 272/272 runs pass")
require(sum((r.get("oracle") or {}).get("unsafe_commits") or 0 for r in wrows) == 0 == W["unsafe_commits"]
        and all(r["checks"]["no_unsafe_commit"] for r in wrows), "E-RQ5-WAN: 0 unsafe commits")
require(all(sum(r["_region"] == g for r in wrows) == 68 for g in regions), "E-RQ5-WAN: 68 runs per region")
base = defaultdict(list)  # us-east-1 E-RQ5-COST SSH runs, per mode (the distance effect)
for x in jl(COST[0] / "runs.jsonl"):
    if x["dataset"] == "ssh" and x["pass"]:
        base[x["mode"]].append(x["mib_per_s_end_to_end"])
by_mode = defaultdict(list)
for r in wrows:
    by_mode[(r["_region"], r["condition"], r["mode"])].append(r)
k = 0
for g in regions:
    rg = W["regions"][g]
    per_run = [r["rtt_before"]["median_ms"] for r in wrows if r["_region"] == g]
    require(close(rg["rtt_ms_per_run_median"], round(statistics.median(per_run), 3)), f"E-RQ5-WAN {g}: per-run RTT median recomputed")
    b0 = [r["mib_per_s_end_to_end"] for r in by_mode[(g, "native", "size-only")]]
    for m in ("size-only", "sha256-store", "blake3-readback"):
        e2e = [r["mib_per_s_end_to_end"] for r in by_mode[(g, "native", m)]]
        c = rg["native"][m]
        ok = close(c["mib_s"], fci(e2e, 20261122 + k)) and close(c["vs_us_east_1_pct"], fpct(e2e, base[m], 20261322 + k))
        if m != "size-only":
            ok &= close(c["vs_size_only_pct"], fpct(e2e, b0, 20261222 + k))
        require(ok and len(e2e) == 10, f"E-RQ5-WAN {g} native {m}: median MiB/s and changes [CI] recomputed")
        k += 1
    p = [r for r in by_mode[(g, "partition", "sha256-store")]][0]
    require(p["partition"]["commits_during"] == 0 and close(rg["partition"]["first_commit_after_restore_s"],
                                                             p["partition"]["first_commit_after_restore_s"])
            and p["oracle"]["complete"] and p["oracle"]["unsafe_commits"] == 0,
            f"E-RQ5-WAN {g}: 60 s partition, 0 commits during the cut, complete, 0 unsafe")
    for m in ("sha256-store", "blake3-readback"):
        q = by_mode[(g, "sigkill", m)][0]
        require(q["oracle"]["duplicate_bytes"] == 0 and q["oracle"]["unreferenced_data_objects"] == 0
                and q["oracle"]["complete"] and close(rg["sigkill"][m]["restart_drain_s"], q["kill"]["restart_drain_s"]),
                f"E-RQ5-WAN {g} SIGKILL {m}: 0 duplicate bytes, 0 unreferenced objects, complete")
    pol = by_mode[(g, "policed", "sha256-raw-mp")][0]
    fix = by_mode[(g, "policed-fixed", "sha256-raw-mp")][0]
    require(pol["committed_bytes_final"] == pol["bytes"] and fix["committed_bytes_final"] == 0,
            f"E-RQ5-WAN {g}: policed 1 Mbit/s uplink completes with the shipped deadline; fixed 120 s commits nothing")
WC = W["claims"]
require([round(x) for x in WC["rtt_ms_range"]] == [66, 196], "E-RQ5-WAN: RTT 66-196 ms")
require([round(x, 1) for x in WC["store_change_pct_range"]] == [-10.0, -2.9] and WC["store_resolved_regions"]
        == ["sa-east-1", "ap-northeast-1", "ap-southeast-2"], "E-RQ5-WAN: store SHA-256 -2.9 to -10.0%, unresolved only in eu-west-1")
rbr = {g: W["regions"][g]["native"]["blake3-readback"]["vs_size_only_pct"][0] for g in W["regions_by_rtt"]}
require([round(x, 1) for x in WC["readback_change_pct_range"]] == [-36.4, -23.0] and WC["readback_resolved_all"]
        and not WC["readback_slowdown_increases_with_rtt"] and WC["readback_slower_than_store_all"]
        and all(rbr[g] < rbr["eu-west-1"] for g in ("sa-east-1", "ap-northeast-1", "ap-southeast-2"))
        and min(rbr, key=rbr.get) == "ap-southeast-2",
        "E-RQ5-WAN: read-back -23.0 to -36.4%, resolved everywhere, larger on the three longer links than at 66 ms and "
        "largest at 196 ms, but not monotonic in RTT (sa-east-1 -33.9 > ap-northeast-1 -31.8); always above store SHA-256")
xn = [statistics.median(r["drain_s"] for r in by_mode[(g, "loss5", "sha256-store")])
      / statistics.median(r["drain_s"] for r in by_mode[(g, "native", "sha256-store")]) for g in regions]
require(WC["loss_failed_batches"] == 1 and W["regions"]["ap-northeast-1"]["loss"]["loss5"]["failed_batches"] == 1
        and [round(min(xn), 1), round(max(xn), 1)] == [7.9, 12.6]
        and close(WC["loss5_x_native_range"], [round(min(xn), 2), round(max(xn), 2)], 0.006),
        "E-RQ5-WAN: 5% loss slows the drain 7.9-12.6x (drain medians); one failed batch (ap-northeast-1), replayed, run passed")
require(WC["policed_seal_to_commit_over_300s_regions"] == ["ap-southeast-2"] and len(WC["policed_completed_regions"]) == 4
        and WC["policed_fixed_committed_bytes"] == 0, "E-RQ5-WAN: Sydney is the only valid #663 check (> 300 s seal-to-commit)")
require(WC["sigkill_duplicate_bytes"] == 0 and WC["partition_commits_during_cut"] == 0, "E-RQ5-WAN: faults safe, no duplicates")

# ============================================================================== model (v010)
sw = json.loads((RES / "state-machine/results/20261120-v010-sweep.json").read_text())
prot = [r for r in sw if r["params"]["variant"] == "v010" and not r["params"].get("refusals")]
require(len(prot) == 14 and all(r["violation"] is None for r in prot) and all(r["stalled"] == 0 for r in prot)
        and all(r["damaged_commit_states"] == 0 for r in prot), "model v010: I2 holds in 14 configurations, no stall, D2 = 0")
require(max(r["states"] for r in prot) == 2356152 == S["rq1_model_claims"]["largest_safe_states"]
        and max(r["states"] for r in prot if r["params"]["per_cycle"] == 1) == 646783,
        "model v010: largest safe configuration 2,356,152 states (B = 1: 646,783)")
require(sorted({r["params"]["per_cycle"] for r in prot}) == [1, 2, 3], "model v010: safe at B = 1, 2, 3")
nh = [r for r in sw if r["params"]["variant"] == "v010-nohold"]
require(sorted(r["states"] for r in nh if r["violation"]) == [73, 393]
        and all(r["params"]["per_cycle"] == 2 for r in nh if r["violation"])
        and all(r["violation"] is None for r in nh if r["params"]["per_cycle"] == 1),
        "model ablation: no hold-back violates I2 at B = 2 (73 states), not at B = 1")
rf = sorted((r["params"]["per_cycle"], r["states"]) for r in sw if r["params"].get("refusals"))
require(rf == [(1, 53), (2, 198)] and all(r["violation"]["property"] == "I2" for r in sw if r["params"].get("refusals")),
        "model: one refused hand-back violates I2 (53 states at B = 1, 198 at B = 2)")
nm = json.loads((RES / "state-machine/results/20261120-v010-sweep-nonmonotone.json").read_text())
nmp = [r for r in nm if r["params"]["variant"] == "v010" and not r["params"].get("refusals")]
require(all(r["violation"] is None for r in nmp) and max(r["states"] for r in nmp) == 2557887
        and max(r["stalled"] for r in nmp) == 19819 and max(r["damaged_commit_states"] for r in nmp) == 2705,
        "model robustness (non-monotone point): safe to 2,557,887 states; stalls up to 19,819, D2 up to 2,705")
rn = json.loads((RES / "state-machine/results/20261120-v010-sweep-resume-newest.json").read_text())
rnp = [r for r in rn if r["params"]["variant"] == "v010" and not r["params"].get("refusals")]
require(all(r["violation"] is None and r["stalled"] == 0 and r["damaged_commit_states"] == 0 for r in rnp)
        and max(r["states"] for r in rnp) == 2263247, "model robustness (Kafka newest-row resume): safe, no stall, D2 = 0, to 2,263,247")
require(max(r["max_duplicates"] for r in prot if r["params"]["mutations"] == 0) == 0
        and max(r["max_duplicates"] for r in prot) == 4, "model: no duplicates without storage mutation; at most 4 with one")
ref = [r for r in json.loads((EXP / "rq1-source-safety/results/20261110-model-sweep/sweep.json").read_text())
       if r["params"]["variant"] == "v060"]
require(min(r["states"] for r in ref if r["violation"]) == 32, "model reference: guard-only (v0.6.0 logic) counterexample, 32 states")
MD = (RES / "state-machine/results/20261120-v010-sweep.md").read_text()
require("2,356,152 / holds" in MD and "646,783 / holds" in MD and "73 / **I2 violated**" in MD
        and "53 / **I2 violated**" in MD and "198 / **I2 violated**" in MD, "model record (20261120-v010-sweep.md) has the same counts")
require(S["rq1_model_robustness"]["nonmonotone"]["protocol_violations"] == 0
        and S["rq1_model_robustness"]["resume_newest"]["protocol_violations"] == 0
        and S["rq1_model_grouped"]["v010-nohold|B2"]["smallest_violation"] == 73, "summary model block matches the sweeps")
ab = json.loads((RES / "state-machine/results/20261121-v010-rule-ablation.json").read_text())
abh = [r for r in ab if r["params"]["variant"] == "v010-nohandback"]
abo = [r for r in ab if r["params"]["variant"] == "v010-noorder"]
require(len(abh) == 11 and sum(r["violation"] is not None for r in abh) == 7
        and min(r["states"] for r in abh if r["violation"]) == 41
        and all(r["violation"]["property"] == "I2" for r in abh if r["violation"])
        and {r["params"]["per_cycle"] for r in abh if r["violation"]} == {1, 2},
        "model rule ablation: no hand-back violates I2 in 7 of 11 configurations (B = 1 and 2), smallest 41 states")
require(len(abo) == 11 and all(r["violation"] is None for r in abo) and max(r["states"] for r in abo) == 657353,
        "model rule ablation: no read-order commit violates nothing up to 657,353 states (B = 1 and 2)")
ABL = (RES / "state-machine/results/20261121-v010-rule-ablation.md").read_text()
require("41 / **I2 violated**" in ABL and "657,353 / holds" in ABL, "model record (20261121-v010-rule-ablation.md) has the same counts")
SA = S["rq1_model_ablation"]["rows"]
require(SA["v010-nohandback"]["smallest_violation"] == 41 and SA["v010-nohandback"]["violated"] == 7
        and SA["v010-noorder"]["violated"] == 0 and SA["v010-noorder"]["states_max"] == 657353,
        "summary rq1_model_ablation matches the rule-ablation run")

# ============================================================================== E-RQ5-TAMPER (H2.5-H2.7)
TD = EXP / "rq5-cloud/tamper/results/20261122T021852Z-i-0e178e36bc660a51f"
TL = [EXP / "rq5-cloud/tamper/results" / n for n in
      ("20261122-local-minio-v0104", "20261122-local-minio-v0104-rep2", "20261122-local-al2023-linux-v0104")]
TS = S["rq5_tamper"]


def cases_of(d: Path) -> dict:
    out = {}
    for x in jl(d / "cases.jsonl"):
        out[x["id"]] = x  # the campaign's rule: a re-run of a case id replaces the earlier line
    return out


tm = json.loads((TD / "run.json").read_text())
require(tm["vtop_commit"].startswith(BUILD) and tm["vtopctl_version"] == "vtopctl 0.10.4" and tm["endpoint"] == "aws"
        and tm["instance_type"] == "t4g.small" and tm["az"] == "us-east-1a",
        "E-RQ5-TAMPER: Amazon S3 (us-east-1a), t4g.small, vtop-engine 0.10.4 at be5b419")
require(tm["reimpl_check"]["ok"] and all(tm["reimpl_check"]["manifests"]) and all(tm["reimpl_check"]["receipts"])
        and all(tm["reimpl_check"]["audit_links"]) and TS["reimpl_check_ok"],
        "E-RQ5-TAMPER: tamper tool's manifest/receipt hash+MAC and audit links match the engine's own")
pf = tm["preflight"]
require(pf["plain"]["versioning"] is None and "ObjectLockConfigurationNotFound" in str(pf["plain"]["object_lock"])
        and pf["lock"]["versioning"] == "Enabled" and pf["lock"]["object_lock"]["ObjectLockEnabled"] == "Enabled"
        and tm["object_lock"].startswith("governance"),
        "E-RQ5-TAMPER: plain bucket unversioned without Object Lock; lock bucket versioned with Object Lock (governance)")
tc = cases_of(TD)
V = lambda c, k: tc[c]["verdicts"][k]["verdict"]  # noqa: E731
require(len(tc) == 55 == TS["cases"], "E-RQ5-TAMPER: 55 cases")
hyp = defaultdict(int)
for x in tc.values():
    hyp[x["hypothesis"]] += 1
require(dict(hyp) == {"H2.5": 21, "H2.6": 21, "H2.7": 13}
        and {h: sum(v.values()) for h, v in TS["cases_by_hypothesis"].items()} == dict(hyp),
        "E-RQ5-TAMPER: 21 / 21 / 13 cases for H2.5 / H2.6 / H2.7")
skipped = sorted(c for c, x in tc.items() if x.get("skipped"))
require(skipped == ["VER-SUSPEND-NEW", "VER-SUSPEND-RUNNING"] == TS["skipped_local_only"],
        "E-RQ5-TAMPER: only the two suspended-versioning cases are local only")
# v0.10.3+ #882: the recorded expectation (v0.10.2: verify-manifest passes a swapped manifest, a stated limit) no longer
# holds; analyze.py --set v0104 counts these four checks as caught (summary rq5_tamper.engine_changes).
CHG = {("MAC-SWAP-OFF", "vm_noledger"), ("MAC-SWAP-OFF", "vm_ledger"), ("MAC-SWAP-ON", "vm_noledger"), ("MAC-SWAP-ON", "vm_ledger")}
miss = {(c, k["verifier"]) for c, x in tc.items() for k in x["checks"] if not k["match"]}
require(miss == CHG and all(k["observed"] == "failed" for c, x in tc.items() for k in x["checks"] if (c, k["verifier"]) in CHG)
        and {(e["case"], e["verifier"]) for e in TS["engine_changes"]} == CHG
        and not any(x.get("error") for x in tc.values()) and TS["differs_on_aws"] == [],
        "E-RQ5-TAMPER: every observed verdict equals its expected one except the four #882 verify-manifest checks "
        "(observed failed: the swap is caught); no case error")
lim = sorted(c for c, x in tc.items() if any(k["limit"] and k["match"] for k in x["checks"]))
require(lim == TS["limit_cases"] and len(lim) == 15
        and sum(n for o, n in TS["outcomes"].items() if "NOT CAUGHT" in o) == 15,
        "E-RQ5-TAMPER: 15 cases measure a stated limit (NOT CAUGHT by design; v0.10.2: 17, the two swaps are now caught)")
prev = sorted(c for c, x in tc.items() if x["checks"] and all(k["verifier"] in ("store", "sql", "scrub", "audit_verify",
              "scrub.retention") for k in x["checks"]) and any(k["verifier"] in ("store", "sql") and k["observed"] == "refused"
              for k in x["checks"]) and not any(k["verifier"] == "scrub" and k["observed"] != "ok" for k in x["checks"]))
require(prev == TS["prevented_cases"] and len(prev) == 6, "E-RQ5-TAMPER: 6 tampers refused by S3 or the ledger database")
# H2.5 signed manifests
for c in ("MAC-FORGE-ON-B", "MAC-FORGE-ON-BL", "MAC-STRIP-ON-BL", "MAC-FOREIGN-BL"):
    require(V(c, "scrub") == "unauthenticated" and V(c, "vm_noledger") == "failed" and V(c, "recovery") == "replayed",
            f"E-RQ5-TAMPER {c}: MAC on -> scrub unauthenticated, stand-alone verify-manifest failed, recovery replayed")
require(V("MAC-FORGE-OFF-B", "scrub") == "unauthenticated" and V("MAC-FORGE-OFF-B", "vm_ledger") == "failed"
        and V("MAC-FORGE-OFF-B", "recovery") == "replayed" and V("MAC-FORGE-OFF-B", "vm_noledger") == "passed",
        "E-RQ5-TAMPER: MAC off, bucket only -> caught via the ledger digest; stand-alone verify-manifest passes (limit)")
for c in ("MAC-FORGE-OFF-BL", "MAC-FORGE-OFF-BL-RCPT", "MAC-LEAKED-BL"):
    require(V(c, "scrub") == "ok" and V(c, "recovery") == "committed" and V(c, "vm_noledger") == "passed"
            and tc[c]["verdicts"]["recovery"]["forged_content_committed"] is True,
            f"E-RQ5-TAMPER {c}: NOT CAUGHT, recovery committed the forged content (limit)")
require(sorted(TS["forged_content_committed"]) == ["MAC-FORGE-OFF-BL", "MAC-FORGE-OFF-BL-RCPT", "MAC-LEAKED-BL"],
        "E-RQ5-TAMPER: forged content committed only with MAC off + rewritten ledger, or with the leaked key")
require(tc["MAC-FORGE-OFF-BL-RCPT"]["verdicts"]["scrub"]["receipt"] == "mismatched"
        and tc["MAC-FORGE-OFF-BL"]["verdicts"]["scrub"]["receipt"] == "ok",
        "E-RQ5-TAMPER: MAC off + ledger: only an untouched receipt reveals the forgery")
mr = tc["MAC-RETIRED-BL"]["verdicts"]
require(mr["scrub_rotated"]["verdict"] == "ok" and mr["scrub_retired"]["verdict"] == "unauthenticated"
        and mr["vm_noledger_retired"]["verdict"] == "failed" and mr["recovery_retired"]["verdict"] == "replayed",
        "E-RQ5-TAMPER: leaked key -> not caught after rotation, caught once the key is retired")
for c in ("MAC-SWAP-OFF", "MAC-SWAP-ON"):
    require(V(c, "scrub") == "unauthenticated" and V(c, "recovery") == "replayed" and V(c, "vm_noledger") == "failed"
            and V(c, "vm_ledger") == "failed",
            f"E-RQ5-TAMPER {c}: swap caught by scrub, recovery and verify-manifest (#882; v0.10.2 passed verify-manifest)")
require(tc["RCPT-BY-OFF"]["verdicts"]["scrub"]["receipt"] == "ok" and tc["RCPT-METHOD-OFF"]["verdicts"]["scrub"]["receipt"] == "ok"
        and tc["RCPT-CLASS-OFF"]["verdicts"]["scrub"]["receipt"] == "mismatched"
        and all(tc[c]["verdicts"]["scrub"]["receipt"] in ("invalid", "mismatched") for c in ("RCPT-EDIT-ON", "RCPT-EDIT-NOHASH-ON", "RCPT-SWAP-ON")),
        "E-RQ5-TAMPER: receipts: MAC on edits/swap caught; MAC off verified_by/method edits not caught, class edit caught")
require(V("OBJ-FLIP-ON", "check_storage") == "passed" and V("OBJ-FLIP-ON", "scrub") == "mismatched"
        and V("OBJ-TRUNC-ON", "check_storage") == "failed" and V("OBJ-DEL-ON", "check_storage") == "failed"
        and all(V(c, "recovery") == "replayed" for c in ("OBJ-FLIP-ON", "OBJ-TRUNC-ON", "OBJ-DEL-ON")),
        "E-RQ5-TAMPER: object damage replayed; HEAD-only storage audit misses the same-size flip (limit)")
# H2.6 versioning and Object Lock
require(all(V(c, "scrub") == "mismatched" for c in ("VER-OVR-MAN", "VER-OVR-OBJ", "VER-OVR-BOTH-OFF", "VER-ROLLBACK"))
        and V("VER-DELMARK", "scrub") == "missing" and V("VER-DELPIN", "scrub") == "missing"
        and all(TS["versioned_original_still_stored"].values()) and len(TS["versioned_original_still_stored"]) == 3,
        "E-RQ5-TAMPER: versioned bucket -> scrub reports overwrite, rollback, delete marker; originals kept")
require(V("VER-OVR-MAN", "recovery") == "committed" and V("VER-ROLLBACK", "recovery") == "committed"
        and V("VER-DELMARK", "recovery") == "committed" and V("VER-OVR-OBJ", "recovery") == "replayed"
        and V("VER-DELPIN", "recovery") == "replayed" and V("NOVER-ROLLBACK", "recovery") == "replayed"
        and V("NOVER-ROLLBACK", "scrub") == "unauthenticated",
        "E-RQ5-TAMPER: recovery reads the pinned manifest version (not the rollback); pinned version deleted -> replayed")
require(V("VER-ROLLBACK", "vm_noledger") == "passed" and V("VER-ROLLBACK", "vm_ledger") == "passed",
        "E-RQ5-TAMPER: a rollback manifest passes stand-alone verify-manifest (limit)")
for c in ("LOCK-DELVER-OBJ", "LOCK-DELVER-MAN", "LOCK-SHORTEN"):
    require(V(c, "store") == "refused" and "AccessDenied" in tc[c]["verdicts"]["store"]["calls"][0]["error"]
            and tc[c]["facts"]["locked_version_intact"], f"E-RQ5-TAMPER {c}: S3 refused (AccessDenied); locked version intact")
require(V("VER-SUSPEND-LOCKED", "store") == "refused" and tc["VER-SUSPEND-LOCKED"]["facts"]["versioning_after"] == "Enabled",
        "E-RQ5-TAMPER: S3 refused to suspend versioning on the Object Lock bucket")
require(all(V(c, "store") == "accepted" and tc[c]["facts"]["locked_version_intact"] for c in ("LOCK-OVR-OBJ", "LOCK-OVR-MAN", "LOCK-DELMARK-OBJ"))
        and V("LOCK-OVR-OBJ", "scrub") == "mismatched" and V("LOCK-OVR-MAN", "scrub") == "mismatched"
        and V("LOCK-DELMARK-OBJ", "scrub") == "missing",
        "E-RQ5-TAMPER: Object Lock accepts an overwrite or plain delete (new version / marker); scrub reports it; locked bytes kept")
require(V("LOCK-BYPASS-CLEAR", "store") == "accepted" and tc["LOCK-BYPASS-CLEAR"]["verdicts"]["scrub"]["retention"] == "missing",
        "E-RQ5-TAMPER: a governance-bypass retention clear is accepted by S3 and reported by the retention check")
require(V("VER-REFUSE-UNVERSIONED", "engine_start") == "refused" and V("LOCK-REFUSE-PLAIN", "engine_start") == "refused",
        "E-RQ5-TAMPER: the engine refuses to start on a bucket without the versioning or Object Lock it is configured to need")
# H2.7 audit trail and coverage
require(all(V(c, "sql") == "refused" for c in ("AUD-UPDATE-TRIGGER", "AUD-DELETE-TRIGGER"))
        and all(V(c, "audit_verify") == "failed" for c in ("AUD-EDIT", "AUD-DELETE-MID", "AUD-SWAP", "AUD-RECOMPUTE-NOKEY",
                                                            "AUD-REWRITE-ALL-NOKEY")),
        "E-RQ5-TAMPER: audit UPDATE/DELETE refused; edit, mid delete, swap, keyless rewrite break the chain")
require(V("AUD-TRUNCATE", "audit_verify") == "passed" and V("AUD-TRUNCATE", "anchor") == "differs"
        and V("AUD-RECOMPUTE-UNKEYED", "audit_verify") == "passed" and V("AUD-RECOMPUTE-LEAKED", "audit_verify") == "passed"
        and V("COV-DELETE-MID", "coverage") == "hole" and V("COV-DELETE-END", "coverage") == "no_hole",
        "E-RQ5-TAMPER: not caught: truncated tail (only a kept anchor), unkeyed or leaked-key recompute, last ledger row")
# Agreement with the three local runs (lab runs on MinIO)
for d in TL:
    lc = cases_of(d)
    require(json.loads((d / "run.json").read_text())["vtop_commit"].startswith(BUILD), f"E-RQ5-TAMPER local {d.name}: same build")
    comp = [c for c in tc if not tc[c].get("skipped") and not lc[c].get("skipped")]
    same = [c for c in comp if [k["observed"] for k in tc[c]["checks"]] == [k["observed"] for k in lc[c]["checks"]]]
    require(len(comp) == len(same) == 53 and TS["agreement_with_local"][d.name] == {"comparable": 53, "agree": 53, "differ": []},
            f"E-RQ5-TAMPER: AWS agrees with local {d.name} in 53/53 comparable cases")
    require([k["observed"] for k in lc["VER-SUSPEND-NEW"]["checks"]] == ["refused"],
            f"E-RQ5-TAMPER local {d.name}: a new engine refuses a bucket whose versioning was suspended")
    w = [k for k in lc["VER-SUSPEND-RUNNING"]["checks"] if k["verifier"] == "writes_after_suspension"][0]
    r_ = [k for k in lc["VER-SUSPEND-RUNNING"]["checks"] if k["verifier"] == "running_engine"][0]
    require(w["observed"] == 1 and w["expected"] == 0 and r_["observed"] == "committed_nothing",
            f"E-RQ5-TAMPER local {d.name}: suspended versioning under a running engine -> nothing committed, 1 write "
            "(the batch in flight; #883; v0.10.2: 10)")
# Paper table rows, recomputed from the cases with this script's own verifier codes
CODE = {"engine_start": "E", "store": "S3", "sql": "D", "scrub": "S", "scrub.receipt": "C", "scrub_require.receipt": "C",
        "scrub.retention": "T", "scrub_rotated": "S", "scrub_retired": "S$^r$", "vm_noledger": "V",
        "vm_noledger_retired": "V$^r$", "vm_ledger": "V$_\\ell$", "recovery": "R", "recovery_retired": "R$^r$",
        "check_storage": "Z", "audit_verify": "A", "anchor": "H", "coverage": "K"}
CAUGHT_V = {"scrub": lambda v: v not in ("ok", "limited", "locked", None, True), "vm": lambda v: v == "failed",
            "recovery": lambda v: v == "replayed", "check_storage": lambda v: v == "failed",
            "engine_start": lambda v: v == "refused", "audit_verify": lambda v: v == "failed",
            "anchor": lambda v: v == "differs", "coverage": lambda v: v == "hole", "store": lambda v: v == "refused",
            "sql": lambda v: v == "refused"}
ok_rows = True
covered = []
for row in TS["rows"]:
    got_c, got_n = set(), set()
    for c in row["cases"]:
        covered.append(c)
        for k in tc[c]["checks"]:
            code = CODE.get(k["verifier"])
            if code is None:
                continue
            base = k["verifier"].split(".")[0]
            fn = next(f for p, f in CAUGHT_V.items() if base.startswith(p))
            if k["limit"] and k["match"]:
                got_n.add(code)
            elif fn(k["observed"]):
                got_c.add(code)
    ok_rows &= got_c == set(row["caught_by"]) and got_n == set(row["not_caught_by"])
require(ok_rows and len(TS["rows"]) == 19, "E-RQ5-TAMPER: every paper-table row's caught / not-caught verifiers recomputed")
require(len(covered) == len(set(covered)) == 48 and not set(covered) & set(skipped),
        "E-RQ5-TAMPER: paper table covers 48 cases once (55 minus 2 start-up refusals, 3 controls, 2 local-only)")

# ============================================================================== E-RQ5-SCALE (H3.7)
SD = EXP / "rq5-cloud/scale/results/20261122T032729Z-scale"
SS = S["rq5_scale"]
sched = json.loads((SD / "schedule.json").read_text())
hosts = sorted(p for p in SD.iterdir() if p.is_dir() and p.name.startswith("h"))
hj = [json.loads((h / "host.json").read_text()) for h in hosts]
require(len(hj) == 8 and all(h["vtop"]["commit"].startswith(BUILD) and h["vtop"]["version"] == "0.10.4" for h in hj)
        and {h["instance_type"] for h in hj} == {"t4g.large"} and {h["availability_zone"] for h in hj} == {"us-east-1a"}
        and len({h["instance_id"] for h in hj}) == 8, "E-RQ5-SCALE: 8 separate t4g.large hosts in us-east-1a, vtop-engine 0.10.4")
erecs = [json.loads(x) for h in hosts for x in (h / "engines.jsonl").read_text().splitlines() if x.strip()]
require(len(erecs) == 198 == SS["engine_runs"] == SS["engine_runs_passing"] and all(r["pass"] for r in erecs),
        "E-RQ5-SCALE: 198/198 engine-runs pass the oracle and evidence checks")
require(sum(r["oracle"]["unsafe_commits"] for r in erecs) == 0 == SS["unsafe"] and all(r["oracle"]["complete"] for r in erecs)
        and sum(r["oracle"]["bad_objects"] for r in erecs) == 0, "E-RQ5-SCALE: 0 unsafe commits, every archive complete")
require(len({r["prefix"] for r in erecs}) == 198 and all(r["oracle"]["multipart_objects"] == 0 for r in erecs),
        "E-RQ5-SCALE: each engine-run wrote its own prefix, single-PUT objects")
done = {r["step"] for r in erecs}
require(len(sched["steps"]) == 46 and all(s["id"] in done for s in sched["steps"]) and SS["steps_executed"] == 46,
        "E-RQ5-SCALE: 46/46 scheduled steps executed")
by_step = defaultdict(list)
for r in erecs:
    by_step[r["step"]].append(r)
with open(SD / "steps.csv") as fh:
    scsv = {r["step"]: r for r in csv.DictReader(fh)}
require(all(close(float(scsv[s]["agg_mib_s"]), round(sum(r["mib_per_s"] for r in by_step[s]), 3), 1e-6) for s in scsv),
        "E-RQ5-SCALE: every step's aggregate = sum of its engines' steady MiB/s")
scale_ids = defaultdict(list)
for s in sched["steps"]:
    if s["kind"] == "scale":
        scale_ids[s["n"]].append(s["id"])
agg = {n: [sum(r["mib_per_s"] for r in by_step[i]) for i in ids] for n, ids in scale_ids.items()}
agg = {n: [round(x, 3) for x in v] for n, v in agg.items()}
eng = {n: [r["mib_per_s"] for i in ids for r in by_step[i]] for n, ids in scale_ids.items()}
require(sorted(scale_ids) == [1, 2, 4, 8] and all(len(v) == 10 for v in scale_ids.values()), "E-RQ5-SCALE: 10 steps per N = 1, 2, 4, 8")


def ratio(num, den, sc, seed, B=10_000):
    x, y = np.asarray(num, float), np.asarray(den, float)
    rng = np.random.default_rng(seed)
    xs = np.median(x[rng.integers(0, len(x), (B, len(x)))], axis=1)
    ys = np.median(y[rng.integers(0, len(y), (B, len(y)))], axis=1)
    s = xs / (sc * ys)
    return [round(float(np.median(x)) / (sc * float(np.median(y))), 4), round(float(np.quantile(s, 0.025)), 4),
            round(float(np.quantile(s, 0.975)), 4)]


for k, n in enumerate([1, 2, 4, 8]):
    e = SS["per_n"][str(n)]
    require(close(e["aggregate_mib_s"], fci(agg[n], 20261124 + k)) and close(e["per_engine_mib_s"], fci(eng[n], 20261224 + k)),
            f"E-RQ5-SCALE N = {n}: aggregate and per-engine median MiB/s [CI] recomputed")
    if n != 1:
        require(close(e["efficiency"], ratio(agg[n], agg[1], n, 20261324 + k))
                and close(e["per_engine_change_vs_n1_pct"], fpct(eng[n], eng[1], 20261424 + k)),
                f"E-RQ5-SCALE N = {n}: efficiency and per-engine change vs N = 1 [CI] recomputed")
    recs = [r for i in scale_ids[n] for r in by_step[i]]
    peak = max(sum(v for r in by_step[i] for t, v in r["requests"]["attempts_per_s"].items() if int(t) == sec)
               for i in scale_ids[n] for sec in {int(t) for r in by_step[i] for t in r["requests"]["attempts_per_s"]})
    require(sum(r["requests"]["retries_total"] for r in recs) == 0 == e["s3_retries"]
            and sum(r["requests"]["throttle_verdicts_total"] for r in recs) == 0 == e["s3_throttle_verdicts"]
            and sum(r["engine_log"]["events"].get("upload_throttled", 0) for r in recs) == 0 == e["engine_throttle_events"]
            and peak == e["s3_peak_attempts_per_s_max"] and e["unsafe"] == 0 and e["engine_runs"] == len(recs) == 10 * n,
            f"E-RQ5-SCALE N = {n}: 0 S3 retries, 0 throttling, peak {peak} attempts/s, 0 unsafe in {len(recs)} engine-runs")
require([round(SS["per_n"][n]["aggregate_mib_s"][0], 2) for n in ("1", "2", "4", "8")] == [45.28, 91.92, 185.5, 369.29]
        and round(SS["claims"]["efficiency_ci_low_min"], 2) == 0.98 and SS["claims"]["efficiency_point_range"][0] >= 0.99
        and [round(x, 3) for x in SS["claims"]["efficiency_point_range"]] == [1.015, 1.024]
        and all(abs(x) <= 2.4 for x in SS["claims"]["per_engine_change_point_range"]),
        "E-RQ5-SCALE: 45.3 / 91.9 / 185.5 / 369.3 MiB/s; efficiency 1.015-1.024, CI lower bounds >= 0.98; per engine within 2.4%")
# Supplement facts: concurrency of the engines, host load and network shaping per N (host steps.jsonl)
hsteps = defaultdict(list)
for h in hosts:
    for x in (h / "steps.jsonl").read_text().splitlines():
        if x.strip():
            y = json.loads(x)
            if not y.get("idle"):
                hsteps[y["step"]].append(y)
ENA_K = ("bw_in_allowance_exceeded", "bw_out_allowance_exceeded", "pps_allowance_exceeded",
         "conntrack_allowance_exceeded", "linklocal_allowance_exceeded")
for n in (1, 2, 4, 8):
    e = SS["per_n"][str(n)]
    ena = sum((y.get("sys") or {}).get(k, 0) for i in scale_ids[n] for y in hsteps[i] for k in ENA_K)
    ov = min(float(scsv[i]["window_overlap_fraction"]) for i in scale_ids[n])
    sk = max(float(scsv[i]["start_skew_s"]) for i in scale_ids[n])
    require(ena == e["ena_allowance_exceeded_sum"] and close(ov, e["window_overlap_fraction_min"], 1e-6)
            and close(sk, e["start_skew_s_max"], 1e-6) and ov >= 0.95 and sk <= 0.032
            and 25 <= e["host_cpu_busy_pct_median"] <= 26 and 0.44 <= e["per_engine_cores_median"] <= 0.46,
            f"E-RQ5-SCALE N = {n}: {ena} network-allowance events, overlap >= {ov}, start skew <= {sk} s, host CPU ~25%")
ctl_ids = [s["id"] for s in sched["steps"] if s["kind"] == "control"]
ctl_e = [r["mib_per_s"] for i in ctl_ids for r in by_step[i]]
ctl_a = [round(sum(r["mib_per_s"] for r in by_step[i]), 3) for i in ctl_ids]
CS = SS["control_size_only"]
require(len(ctl_ids) == 5 and all(r["mode"] == "size-only" for i in ctl_ids for r in by_step[i])
        and close(CS["per_engine_mib_s"], fci(ctl_e, 20261524))
        and close(CS["sha256_vs_size_only_per_engine_pct"], fpct(eng[8], ctl_e, 20261624))
        and close(CS["sha256_vs_size_only_aggregate_pct"], fpct(agg[8], ctl_a, 20261724))
        and round(CS["sha256_vs_size_only_aggregate_pct"][0], 2) == -4.71,
        "E-RQ5-SCALE: store SHA-256 vs size-only at N = 8: -4.71% aggregate [CI] recomputed")

# ============================================================================== E-RQ5-PERF (H4.2)
# Every per-run rate and CPU figure is recomputed from the observer's series (series/<run>.json.gz: committed and
# archived byte curves on the broker clock, the sink's CPU samples, the kill event), not read from runs.jsonl; the
# bootstraps then use the campaign analyzer's seed sequence (perf/analyze.py leg_summary: k0 = 100000 x leg index).
import gzip  # noqa: E402

PD = [EXP / "rq5-cloud/perf/results/20261122T041404Z-use1-broker-i-009228b687a1a12c0/results",
      EXP / "rq5-cloud/perf/results/20261122T032729Z-apse2-broker-i-0867590bfc4cd5b2a/results"]
PF = S["rq5_perf"]
PORDER = ["vtop-size-only", "vtop-sha256-store", "vtop-blake3-readback", "vtop-sha256-gzip6", "confluent", "vector",
          "fluentbit"]
PREF, PSINKS = "vtop-sha256-store", ["confluent", "vector", "fluentbit"]


def p_cross(ser, total, frac):
    return next(((t, v) for t, v in ser if v >= frac * total), None)


def p_rate(ser, total):
    a, b = p_cross(ser, total, 0.1), p_cross(ser, total, 0.9)
    return None if not a or not b or b[0] <= a[0] else (a[0], b[0], (b[1] - a[1]) / (b[0] - a[0]))


def p_cpu_at(samples, t):
    prev = None
    for s_ in samples:
        if s_["t"] >= t:
            return s_["cpu_s"] if prev is None else prev["cpu_s"] + (t - prev["t"]) / max(1e-9, s_["t"] - prev["t"]) * (s_["cpu_s"] - prev["cpu_s"])
        prev = s_
    return prev["cpu_s"] if prev else None


def p_series(d, r, poll):
    """Recompute committed/archived/done MiB/s, CPU s/GiB and, for a kill run, the recovery times of one run."""
    ser = json.load(gzip.open(d / "series" / f"{r['run']}.json.gz"))
    total = r["throughput"]["total_payload_bytes"]
    cs = [(t, v) for t, v in ser["committed_bytes"]]
    cs = [(t, v) for (t, _), v in zip(cs, np.maximum.accumulate([v for _, v in cs]).tolist())] if cs else []
    as_ = [(t, v) for t, v in ser["archived_bytes"]]
    ds, ci_, ai_, cv, av = [], 0, 0, 0, 0
    for t in sorted({t for t, _ in cs} | {t for t, _ in as_}):  # done = min(committed, archived) as a step series
        while ci_ < len(cs) and cs[ci_][0] <= t:
            cv = cs[ci_][1]
            ci_ += 1
        while ai_ < len(as_) and as_[ai_][0] <= t:
            av = as_[ai_][1]
            ai_ += 1
        ds.append((t, min(cv, av)))
    out = {}
    for nm, s_ in (("committed", cs), ("archived", as_), ("done", ds)):
        x = p_rate(s_, total)
        if x:
            out[nm] = (round(x[2] / 2**20, 3), round(x[0], 3), round(x[1], 3))  # window as stored (ms resolution)
    if r["type"] == "tput" and "done" in out:
        mib, lo, hi = out["done"]
        c_lo, c_hi = p_cpu_at(ser["sink_samples"], lo), p_cpu_at(ser["sink_samples"], hi)
        out["cpu_s_per_gib"] = round((c_hi - c_lo) / (mib * (hi - lo) / 1024), 3)
        out["cores"] = round((c_hi - c_lo) / (hi - lo), 3)
    k_ = ser.get("kill")
    if k_:
        tk, tr = k_["t_kill"], k_["t_restart"]
        pre = [(t, v) for t, v in cs if t <= tk]
        t_base = tr + 3 * poll
        base_v = max([v for t, v in cs if t <= t_base] or [pre[-1][1] if pre else 0])
        fc = next((t for t, v in cs if t > t_base and v > base_v), None)
        out["ready_s"] = round(k_["t_ready"] - tk, 3)
        out["first_commit_after_s"] = None if fc is None else round(fc - tk, 3)
    return out


def p_rejudged(d, rows):
    """Apply offline re-judgements (rejudge-*.json beside the leg's results) and check them: the file's on-VM oracle
    must equal runs.jsonl's, and, when the run's kept S3 objects are present locally (.tmp/perf-diag-<n>/, not in
    git), rejudge.py is run again from them and must give the same re-judged oracle."""
    out = []
    for f in sorted(d.parent.glob("rejudge-*.json")):
        rj = json.loads(f.read_text())
        r = next(x for x in rows if x["run"] == rj["run"])
        require({k: v for k, v in r["oracle"].items() if k != "partitions"} == rj["on_vm_oracle"],
                f"E-RQ5-PERF {rj['run']}: rejudge file's on-VM oracle equals runs.jsonl")
        o = rj["rejudged_oracle"]
        objs = ROOT / ".tmp" / f"perf-diag-{rj['run'].split('-')[0]}" / rj["run"]
        lines = objs.parent / "hdfs.log"
        if objs.is_dir() and lines.exists():
            with tempfile.TemporaryDirectory() as tmp:
                subprocess.run([sys.executable, str(EXP / "rq5-cloud/perf/rejudge.py"), "--run-dir", str(d.parent),
                                "--run", rj["run"], "--objects", str(objs), "--lines-file", str(lines),
                                "--out", f"{tmp}/r.json"], check=True, capture_output=True)
                again = json.loads(Path(f"{tmp}/r.json").read_text())["rejudged_oracle"]
            require(again == o, f"E-RQ5-PERF {rj['run']}: re-judgement reproduced from the {len(list(objs.iterdir()))} kept objects")
        require(o["complete"] and not o["unsafe_final"] and not o["lost_committed_records"] and not o["missing_records"],
                f"E-RQ5-PERF {rj['run']}: re-judged complete, 0 unsafe, 0 lost, 0 missing "
                f"(on the VM: {rj['on_vm_oracle']['missing_records']} missing, a locate() tie-break)")
        r["oracle"] = {k: v for k, v in o.items() if k != "ambiguous_placements"}
        r["pass"] = True
        out.append(r["run"])
    return out


pruns = {}
for i, d in enumerate(PD):
    info = json.loads((d / "run.json").read_text())
    leg, rows = info["leg"], jl(d / "runs.jsonl")
    rejudged = p_rejudged(d, rows)
    require([x["run"] for x in PF["legs"][leg].get("rejudged", [])] == rejudged,
            f"E-RQ5-PERF {leg}: re-judged runs in the summary = {rejudged or 'none'}")
    E = PF["legs"][leg]
    meas = [r for r in rows if not r.get("warmup")]
    require(info["vtop"]["commit"].startswith(BUILD) and info["vtop"]["version"] == "0.10.4"
            and len(rows) == E["runs"] and len(meas) == E["measured"] == E["passing"] and all(r["pass"] for r in meas)
            and E["warmup_runs"] == len(rows) - len(meas) == 4,
            f"E-RQ5-PERF {leg}: vtop-engine 0.10.4; {len(meas)}/{len(meas)} measured runs pass (4 warm-up runs excluded)")
    require(close(E["rtt_sink_ms_median"]["start"], info["rtt_start_sink"]["median_ms"])
            and close(E["rtt_sink_ms_median"]["end"], info["rtt_end_sink"]["median_ms"]),
            f"E-RQ5-PERF {leg}: sink-to-S3 RTT {info['rtt_start_sink']['median_ms']} ms (median of 20 handshakes)")
    nonfault = [r for r in meas if r["type"] in ("tput", "live", "kill")]
    require(all(not r["oracle"]["unsafe_final"] and not r["oracle"]["lost_committed_records"] and r["oracle"]["complete"]
                for r in nonfault) and E["nonfault_unsafe"] == 0 == E["nonfault_lost_committed_records"] == E["nonfault_incomplete"],
            f"E-RQ5-PERF {leg}: {len(nonfault)} no-fault runs (tput, live, kill): oracle complete, 0 unsafe, 0 lost committed")
    poll = info["measurement"]["commit_poll_s"]
    cells = defaultdict(list)
    for r in meas:
        cells[(r["type"], r["config"])].append(r)
    recomputed = 0
    for r in meas:
        if r["type"] not in ("tput", "kill"):
            continue
        x = p_series(d, r, poll)
        ok = all(close(x[nm][0], r["throughput"][nm]["mib_s"], 5e-4) for nm in ("committed", "archived", "done"))
        if r["type"] == "tput":
            ok &= close(x["cpu_s_per_gib"], r["throughput"]["cpu_s_per_gib"], 5e-4) and close(x["cores"], r["throughput"]["cpu_cores_window"], 5e-4)
        else:
            ok &= close(x["ready_s"], r["recovery"]["ready_s"], 5e-4) and close(x["first_commit_after_s"], r["recovery"]["first_commit_after_s"], 5e-4)
        assert ok, f"E-RQ5-PERF {r['run']}: series recomputation differs"
        pruns[(leg, r["run"])] = x
        recomputed += 1
    require(recomputed == sum(len(v) for (t, _), v in cells.items() if t in ("tput", "kill")),
            f"E-RQ5-PERF {leg}: committed/archived/done MiB/s, CPU s/GiB and kill recovery of {recomputed} runs recomputed "
            "from the observer series")
    k = 100000 * (i + 1)
    vals = lambda t, c, f: [f(pruns[(leg, r["run"])]) if (leg, r["run"]) in pruns else f(r) for r in cells[(t, c)]]  # noqa: E731
    for c in [c for c in PORDER if ("tput", c) in cells]:
        e = E["tput"][c]
        ok = close(e["done_mib_s"], fci(vals("tput", c, lambda x: x["done"][0]), 500 + k))
        ok &= close(e["committed_mib_s"], fci(vals("tput", c, lambda x: x["committed"][0]), 1000 + k))
        ok &= close(e["archived_mib_s"], fci(vals("tput", c, lambda x: x["archived"][0]), 2000 + k))
        ok &= close(e["cpu_s_per_gib"], fci(vals("tput", c, lambda x: x["cpu_s_per_gib"]), 3000 + k))
        ok &= close(e["cpu_cores_median"], round(float(np.median(vals("tput", c, lambda x: x["cores"]))), 4))
        ok &= close(e["peak_anon_mib_median"], round(float(np.median([r["sink"]["summary"]["peak_anon_bytes"] / 2**20
                                                                       for r in cells[("tput", c)]])), 4))
        require(ok and e["runs"] == len(cells[("tput", c)]),
                f"E-RQ5-PERF {leg} tput/{c}: {e['runs']} runs; done, committed, archived MiB/s, CPU s/GiB [CI], cores, peak memory")
        k += 1
    ref = vals("tput", PREF, lambda x: x["done"][0])
    so = vals("tput", "vtop-size-only", lambda x: x["done"][0])
    ref_cpu = vals("tput", PREF, lambda x: x["cpu_s_per_gib"])
    for c in [c for c in PORDER if ("tput", c) in cells]:
        e, mine = E["tput"][c], vals("tput", c, lambda x: x["done"][0])
        ok = True
        if c != PREF:
            ok &= close(e["done_vs_ref_pct"], fpct(mine, ref, 4000 + k))
            k += 1
            ok &= close(e["cpu_per_gib_vs_ref_pct"], fpct(vals("tput", c, lambda x: x["cpu_s_per_gib"]), ref_cpu, 4500 + k))
            k += 1
        if c.startswith("vtop-") and c != "vtop-size-only":
            ok &= close(e["done_vs_size_only_pct"], fpct(mine, so, 5000 + k))
            k += 1
        require(ok, f"E-RQ5-PERF {leg} tput/{c}: change vs store SHA-256 (done, CPU/GiB) and vs size-only [CI]")
    for c in [c for c in PORDER if ("live", c) in cells]:
        e, rs = E["live"][c], cells[("live", c)]
        ok = True
        for comp in ("end_to_end_s", "object_visible_s", "commit_s"):
            for q in ("p50", "p90", "p99", "p100"):
                want = fci([r["latency"][comp][q] for r in rs], 6000 + k)
                if f"{comp}_{q}" in e:
                    ok &= close(e[f"{comp}_{q}"], want)
                k += 1
        # the stored per-run percentiles agree with the stored every-100th-record sample (all records)
        for r in rs:
            smp = json.load(gzip.open(d / "latency" / f"{r['run']}.json.gz"))["every_100th_end_to_end_s"]
            ok &= abs(float(np.median(smp)) - r["latency"]["end_to_end_all_records_s"]["p50"]) <= 0.05
        require(ok and e["missing_records"] == 0 == sum(r["latency"]["end_to_end_s"]["missing"] for r in rs),
                f"E-RQ5-PERF {leg} live/{c}: p50/p90/p99/max latency [CI] recomputed; per-run p50 matches its record sample; 0 missing")
    for c in [c for c in PORDER if ("kill", c) in cells]:
        e, rs = E["kill"][c], cells[("kill", c)]
        ok = True
        for m in ("ready_s", "first_commit_after_s", "first_object_after_s", "resteady_s", "pre_kill_mib_s"):
            xs = [pruns[(leg, r["run"])][m] if m in ("ready_s", "first_commit_after_s") else r["recovery"][m] for r in rs]
            xs = [v for v in xs if v is not None]
            if m in e:
                ok &= close(e[m], fci(xs, 7000 + k))
            k += 1
        ok &= e["duplicate_records"] == [r["oracle"]["duplicate_records"] for r in rs]
        ok &= e["complete"] == len(rs) == sum(bool(r["oracle"]["complete"]) for r in rs)
        require(ok, f"E-RQ5-PERF {leg} kill/{c}: ready, first commit [CI] recomputed; duplicates "
                    f"{[r['oracle']['duplicate_records'] for r in rs]}; complete")
    if leg == "us-east-1":
        band = poll + 0.1
        for fk in ("corrupt", "lost"):
            for c in [c for c in PORDER if (fk, c) in cells]:
                rs, e = cells[(fk, c)], E["faults"][f"{fk}/{c}"]
                cls = []
                for r in rs:  # the analyzer's rule, re-applied to the stored times
                    f = r["fault"]
                    td, tv, te = f["t_damaged"], f["t_visible"], f["t_commit_enter"]
                    cls.append("commit_before_object_visible" if te is not None and tv is not None and te < tv else
                               "damage_before_commit" if te is None or td < te - band else
                               "damage_after_commit" if td >= te else "ambiguous_within_one_poll")
                ok = cls == [r["fault"]["damage_vs_commit"] for r in rs] and e["classes"] == dict(Counter(cls))
                comp = [r for r, k_ in zip(rs, cls) if k_ in ("damage_before_commit", "commit_before_object_visible")]
                ok &= e["unsafe_trials"] == sum(bool(r["oracle"]["unsafe_final"]) for r in rs)
                ok &= e["rq4_comparable_trials"] == len(comp) and e["unsafe_trials_rq4_comparable"] == sum(bool(r["oracle"]["unsafe_final"]) for r in comp)
                ok &= e["lost_committed_records"] == [r["oracle"]["lost_committed_records"] for r in rs] and e["complete"] == 0
                if c.startswith("vtop-"):
                    vv = ["before_verification" if r["fault"]["t_damaged"] < r["fault"]["vtop_verified_at"] else "after_verification" for r in rs]
                    ok &= e["vtop_vs_verification"] == dict(Counter(vv))
                    ok &= e["damage_before_verification_ms"] == [round((r["fault"]["vtop_verified_at"] - r["fault"]["t_damaged"]) * 1000, 1)
                                                                 for r, v in zip(rs, vv) if v == "before_verification"]
                    ok &= e["vtop_post_scrub_rc"] == [r["fault"]["vtop_post_scrub_rc"] for r in rs]
                require(ok and len(rs) == 5, f"E-RQ5-PERF faults {fk}/{c}: 5 trials, classes {dict(Counter(cls))} re-derived "
                        f"from the measured times; unsafe {e['unsafe_trials']}, RQ4-comparable {len(comp)}")
    if i == 0:
        pcells0 = cells
    else:
        pcells1 = cells
# distance effect: far leg (Sydney) vs us-east-1, analyzer seeds 9000/9500 + index
for j, c in enumerate([c for c in PORDER if ("tput", c) in pcells1 and ("tput", c) in pcells0]):
    far = [pruns[("ap-southeast-2", r["run"])]["done"][0] for r in pcells1[("tput", c)]]
    near = [pruns[("us-east-1", r["run"])]["done"][0] for r in pcells0[("tput", c)]]
    lx = [r["latency"]["end_to_end_s"]["p50"] for r in pcells1[("live", c)]]
    ly = [r["latency"]["end_to_end_s"]["p50"] for r in pcells0[("live", c)]]
    de = PF["distance_effect"][c]
    require(close(de["done_far_vs_near_pct"], fpct(far, near, 9000 + j)) and close(de["live_p50_far_minus_near_s"], fci(lx, 9500 + j, other=ly)),
            f"E-RQ5-PERF distance effect {c}: Sydney vs us-east-1 done MiB/s and live p50 [CI]")
PC, PU, PS = PF["claims"], PF["legs"]["us-east-1"], PF["legs"]["ap-southeast-2"]
nf = []
for d in PD:  # the re-judged oracle replaces the on-VM one (p_rejudged checked it above)
    rjd = {x["run"]: x["rejudged_oracle"] for x in (json.loads(f.read_text()) for f in d.parent.glob("rejudge-*.json"))}
    for r in jl(d / "runs.jsonl"):
        if r["run"] in rjd:
            r["oracle"], r["pass"] = rjd[r["run"]], True
        if not r.get("warmup") and r["type"] in ("tput", "live", "kill"):
            nf.append(r)
require(len(nf) == 103 and all(r["pass"] and r["oracle"]["complete"] and not r["oracle"]["unsafe_final"] for r in nf)
        and PU["measured"] + PS["measured"] == 143,
        "E-RQ5-PERF: 103 runs without storage faults (65 + 38) pass the oracle (one Vector run after offline re-judgement), 0 unsafe; 143 measured runs")
dn = lambda e, c: e["tput"][c]["done_mib_s"][0]  # noqa: E731
require([round(dn(PU, c), 2) for c in PORDER] == [24.56, 23.09, 22.75, 16.94, 37.82, 9.48, 4.83]
        and [round(dn(PS, c), 2) for c in PORDER if c in PS["tput"]] == [8.7, 7.09, 5.27, 25.9, 9.35, 4.84]
        and round(PC["rtt_ms"]["ap-southeast-2"]) == 196 and PC["rtt_ms"]["us-east-1"] < 1,
        "E-RQ5-PERF: done MiB/s us-east-1 24.56/23.09/22.75/16.94 (VTOP), 37.82/9.48/4.83 (sinks); Sydney (196 ms) 8.70/7.09/5.27, 25.90/9.35/4.84")
require(round(dn(PU, "confluent") / dn(PU, PREF), 2) == 1.64 == PC["confluent_over_vtop_store"]["us-east-1"]
        and round(dn(PS, "confluent") / dn(PS, PREF), 2) == 3.65 == PC["confluent_over_vtop_store"]["ap-southeast-2"]
        and round(dn(PS, "vector") / dn(PS, PREF), 2) == 1.32 and PC["faster_than_vtop_store_resolved"]
        == {"us-east-1": ["confluent"], "ap-southeast-2": ["confluent", "vector"]}
        and PC["slower_than_vtop_store_resolved"] == {"us-east-1": ["vector", "fluentbit"], "ap-southeast-2": ["fluentbit"]},
        "E-RQ5-PERF: Confluent 1.64x (in-region) and 3.65x (Sydney) VTOP store SHA-256; Vector 1.32x in Sydney; all resolved")
so_ = [PU["tput"]["vtop-size-only"]["done_vs_ref_pct"], PS["tput"]["vtop-size-only"]["done_vs_ref_pct"]]
require(round(so_[0][0], 1) == 6.4 and so_[0][1] < 0 < so_[0][2] and round(so_[1][0], 1) == 22.7 and so_[1][1] > 0
        and [round(dn(e, "confluent") / dn(e, "vtop-size-only"), 2) for e in (PU, PS)] == [1.54, 2.98],
        "E-RQ5-PERF: size-only +6.4% (unresolved) / +22.7% (resolved) vs store SHA-256; Confluent 1.54x / 2.98x size-only")
cpu = {leg: {c: e["tput"][c]["cpu_s_per_gib"][0] for c in e["tput"]} for leg, e in PF["legs"].items()}
require(all(min(v, key=v.get).startswith("vtop-") and all(v[PREF] < v[c] for c in PSINKS)
            and all(PF["legs"][leg]["tput"][c]["cpu_per_gib_vs_ref_pct"][1] > 0 for c in PSINKS)
            for leg, v in cpu.items()) and [round(cpu[l][PREF], 1) for l in cpu] == [24.0, 24.0]
        and [round(cpu["us-east-1"][c], 1) for c in PSINKS] == [53.1, 203.8, 26.3],
        "E-RQ5-PERF: VTOP lowest CPU per GiB in both legs (store SHA-256 24.0 / 24.0 s, size-only 23.9 from Sydney; "
        "Confluent 53.1, Vector 203.8, Fluent Bit 26.3), each sink resolved above VTOP store SHA-256")
mem = {leg: {c: e["tput"][c]["peak_anon_mib_median"] for c in e["tput"]} for leg, e in PF["legs"].items()}
require(all(min(v, key=v.get) == "fluentbit" and v["confluent"] > 1000 and v["vector"] > 550
            and all(206 <= v[c] <= 236 for c in v if c.startswith("vtop-")) for v in mem.values()),
        "E-RQ5-PERF: peak memory VTOP 207-235 MiB; Fluent Bit smallest (132/144), Vector 560-573, Confluent 1,013-1,130 MiB")
p50 = PC["live_p50_s"]
require([round(p50["us-east-1"][c], 2) for c in (PREF, "confluent", "vector", "fluentbit")] == [2.7, 2.68, 5.16, 3.18]
        and [round(p50["ap-southeast-2"][c], 2) for c in (PREF, "confluent", "vector", "fluentbit")] == [4.7, 4.18, 6.13, 4.52],
        "E-RQ5-PERF: live p50 VTOP 2.70 / 4.70 s; Confluent lower (2.68 / 4.18), Fluent Bit 3.18 / 4.52, Vector 5.16 / 6.13")
fc = PC["kill_first_commit_s"]
require(all(min(v, key=v.get) == PREF for v in fc.values()) and [round(fc[l][PREF], 1) for l in fc] == [1.7, 5.6]
        and all(v == [0] * len(v) for leg in PC["kill_duplicate_records"] for c, v in PC["kill_duplicate_records"][leg].items() if c != "vector")
        and max(max(PC["kill_duplicate_records"][l]["vector"]) for l in fc) == 240000 and PC["kill_complete_all"],
        "E-RQ5-PERF: after a SIGKILL VTOP commits first (1.7 / 5.6 s) with 0 duplicates; Vector up to 240,000 duplicate records")
require(PC["fault_sinks_trials"] == PC["fault_sinks_unsafe"] == 30 and PC["fault_sinks_rq4_comparable"] == PC["fault_sinks_unsafe_rq4_comparable"] == 26
        and PC["fault_sinks_lost_committed_per_trial_range"] == [40000, 42991],
        "E-RQ5-PERF faults: the three sinks committed past the damaged object in 30/30 trials (26/26 RQ4-comparable), losing 40,000-42,991 committed records each")
require(PC["fault_vtop_trials"] == 10 and PC["fault_vtop_rq4_comparable"] == 0
        and PC["fault_vtop_vs_verification"] == {"after_verification": 8, "before_verification": 2}
        and PC["fault_vtop_damage_before_verification_ms"] == [6.3, 18.1] and PC["fault_vtop_scrub_flagged"] == 10
        and PC["fault_vtop_unsafe_final"] == 10,
        "E-RQ5-PERF faults: VTOP damage after its verification in 8/10, 6.3 and 18.1 ms before its logged verification in 2/10; scrub flagged 10/10")

# ============================================================================== tables and figures
T = {p.name: p.read_text() for p in TAB.glob("*.tex")}
require(all(t.startswith("% generated by research/analysis/scripts/analyze.py") and "(--set v0104)" in t.splitlines()[0]
            for t in T.values()) and len(T) == 20, "tables: 20 v0.10.4 tables, each stamped --set v0104")
require("\\sysname{} store SHA-256 & 23.1 & 7.1 & 24.0 & 2.70 & 1.7 & 0 \\\\" in T["rq5_perf.tex"]
        and "Confluent S3 Sink & 37.8 & 25.9 & 53.1 & 2.68 & 50.1 & 0 \\\\" in T["rq5_perf.tex"]
        and "Vector & 9.5 & 9.4 & 203.8 & 5.16 & 10.7 & 240{,}000 \\\\" in T["rq5_perf.tex"]
        and T["rq5_perf_faults.tex"].count("scrub 5/5") == 2 and T["rq5_perf_tput.tex"].count("\\sysname{}") == 7
        and T["rq5_perf_kill.tex"].count("168{,}529, 0, 240{,}000") == 1, "table rq5_perf: VTOP, Confluent, Vector rows; supplement tables")
require("+ ledger & Forgery, MAC off (1) & -- & S, V, V$_\\ell$, R \\\\" in T["rq5_tamper.tex"]
        and "+ key & Forgery, leaked key (2) & S$^r$, V$^r$, R$^r$ & S, V, R \\\\" in T["rq5_tamper.tex"]
        and "Key holder & Rollback to an older valid manifest (versioned and unversioned) (2) & S, R & V, V$_\\ell$ \\\\"
        in T["rq5_tamper_grouped.tex"]
        and T["rq5_tamper_full.tex"].count("\\texttt{") == 55, "table rq5_tamper: limit rows; full table has 55 cases")
require(T["rq5_scale_steps.tex"].count("sha256-store") == 40 and T["rq5_scale_steps.tex"].count("size-only") == 5
        and all(f"{sid} & " in T["rq5_scale_steps.tex"] for sid in scsv),
        "table rq5_scale_steps: 45 measured steps (40 scale, 5 size-only control)")
require("8 & 369.3 [366.4, 372.3] & 1.02 [0.99, 1.04] & 2.3 [-1.2, 4.7] & 129 (154) & 0/0 & 0/80 \\\\" in T["rq5_scale.tex"],
        "table rq5_scale: N = 8 row")
require("power-loss & 10 & 80 & 0 & 10/10 & 0 [0,0] & 0.0\\% \\\\" in T["rq1_crash.tex"]
        and "kafka-hdfs-p4 & 6 & 24 & 0 & 6/6" in T["rq1_crash.tex"], "table rq1_crash: power-loss and Kafka rows")
require("Protocol & 2 & 5 & 353--2,356,152 & 0 & -- \\\\" in T["rq1_model.tex"]
        and "No hold-back & 2 & 2 & 73--393 & 2 & 73 \\\\" in T["rq1_model.tex"], "table rq1_model: v010 rows")
require("No hand-back & 1--2 & 11 & 25--2,902 & 7 & 41 \\\\" in T["rq1_model.tex"]
        and "No read-order & 1--2 & 11 & 25--657,353 & 0 & -- \\\\" in T["rq1_model.tex"],
        "table rq1_model: rule-ablation rows (no hand-back, no read-order) generated from the ablation run")
require("M1 object byte flip & 4/4 & 4/4 & 0/4 \\\\" in T["rq2_tamper.tex"], "table rq2_tamper: same-size flip row")
require("\\sysname & 0/10 & 0/10 & 0/5 & 0 & 25/25 & 0 \\\\" in T["rq4_baselines.tex"], "table rq4: VTOP row")
require("HDFS & t4g.large & 47.0 [46.8, 48.1] & -3.9\\% [-6.4, -1.9] & -18.9\\% [-21.2, -17.7] \\\\" in T["rq5_cost.tex"],
        "table rq5_cost: HDFS re-check row")
require("Sydney & 196 & 6.76 &" in T["rq5_wan.tex"], "table rq5_wan: Sydney row")
F = RES / "analysis/v0104/figures"
require(all((F / n).exists() for n in ("rq1-duplicates.pdf", "rq3-throughput.pdf", "rq3-stages.pdf", "rq3-batchsize.pdf",
                                       "rq5-cost.pdf", "rq5-wan.pdf")), "figures: six v0.10.4 PDFs")

# ============================================================================== v0.7.0 reproduction
if "--skip-repro" not in sys.argv:
    old = (RES / "analysis/summary.json").read_bytes()
    stamp = json.loads(old)["generated_from_commit"]
    with tempfile.TemporaryDirectory() as tmp:
        r = subprocess.run([sys.executable, str(RES / "analysis/scripts/analyze.py"), "--set", "v070", "--out", tmp,
                            "--stamp", stamp], capture_output=True, text=True)
        require(r.returncode == 0 and (Path(tmp) / "summary.json").read_bytes() == old,
                "reproduction: analyze.py --set v070 rebuilds the submitted summary.json byte for byte")

if FAILED:
    print(f"{len(FAILED)} FAILED checks:\n- " + "\n- ".join(FAILED))
    sys.exit(1)
print(f"Verified {len(checks)} v0.10.4 data checks.")
