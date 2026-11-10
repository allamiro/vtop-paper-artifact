#!/usr/bin/env python3
"""E-RQ3-A: cost of verification-bound commits on real Loghub data.

Drives ``vtopctl process-once --json`` until each dataset is drained, so
every batch reports its per-stage timings (compress, checksum, object
upload, manifest upload, verify, commit, state write). Each invocation is
reaped with ``os.wait4`` to get its CPU time and peak RSS.

Matrix:
  part A  datasets {linux, ssh, bgl, hdfs} x variants x reps, 16 MiB batches
  part B  bgl x sha256 x batch target {1, 4, 16, 64} MiB x reps

Backend: LocalFS (verification streams the stored file through the digest,
i.e. read-back verification). Output: results/<run-id>/{runs.jsonl,batches.csv}.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from vtoplab import DATASET_FILES, VTOP_COMMIT, VTOPCTL, Run, Variant, host_info, oracle  # noqa: E402

VARIANTS = {
    "sha256": Variant("sha256"),
    "blake3": Variant("blake3", checksum="blake3"),
    "size-only": Variant("size-only", checksum="none", strong=False),
    "sha256+mac": Variant("sha256+mac", mac=True),
    "sha256-nocomp": Variant("sha256-nocomp", compression="none"),
}
STAGES = ["compress_ms", "checksum_ms", "object_upload_ms", "manifest_upload_ms", "verify_ms", "commit_ms", "state_write_ms", "total_ms"]


def drive(run: Run, max_calls: int = 100000):
    """Call process-once until drained. Returns (batches, wall_s, cpu_s, max_rss_bytes, calls)."""
    batches, cpu, rss, calls = [], 0.0, 0, 0
    t0 = time.perf_counter()
    while not run.drained() and calls < max_calls:
        calls += 1
        out = run.raw / "logs" / "po.json"
        with open(out, "wb") as fo, open(run.raw / "logs" / "po.err", "ab") as fe:
            p = subprocess.Popen([str(VTOPCTL), "process-once", "--source", "file", "--config", str(run.config), "--json"],
                                 stdout=fo, stderr=fe, env=run.env())
            _, status, ru = os.wait4(p.pid, 0)
        cpu += ru.ru_utime + ru.ru_stime
        rss = max(rss, ru.ru_maxrss)  # bytes on macOS
        if status != 0:
            raise RuntimeError(f"process-once exit {status}; see {run.raw/'logs'/'po.err'}")
        for b in json.loads(out.read_bytes() or b"[]"):
            m = b.get("metrics") or {}
            m["final_state"] = b.get("final_state")
            batches.append(m)
    return batches, time.perf_counter() - t0, cpu, rss, calls


def drive_run(run: Run, timeout: float = 1800):
    """Continuous engine (vtopctl run): wall time from start until the ledger shows the
    dataset fully committed; CPU and peak RSS of the engine process via wait4."""
    t0 = time.perf_counter()
    p = run.start()
    series = []  # (seconds since start, committed bytes) at each change
    last = 0
    while True:
        c = sum(run.committed().values())
        if c != last:
            series.append((time.perf_counter() - t0, c))
            last = c
        if run.drained() or time.perf_counter() - t0 > timeout or p.poll() is not None:
            break
        time.sleep(0.01)
    wall = time.perf_counter() - t0
    p.send_signal(15)
    _, status, ru = os.wait4(p.pid, 0)
    run.proc = None
    return wall, ru.ru_utime + ru.ru_stime, ru.ru_maxrss, series


def one_run_mode(out_dir: Path, ds: str, vname: str, v: Variant, rep: int) -> dict:
    root = out_dir / "raw" / f"run-{ds}-{vname}-r{rep}"
    shutil.rmtree(root, ignore_errors=True)
    run = Run(root, v, {ds: DATASET_FILES[ds]})
    size = run.sources[ds].stat().st_size
    wall, cpu, rss, series = drive_run(run)
    # Steady-state throughput: bytes committed by the last size-sealed batch over
    # the time of that commit. The final partial batch is sealed only by
    # max_batch_age_seconds, a fixed idle wait that is not processing time.
    steady = series[-2] if len(series) >= 2 else series[-1]
    rec = {"mode": "run", "dataset": ds, "variant": vname, "batch_mib": v.max_bytes >> 20, "rep": rep, "bytes": size,
           "wall_s": round(wall, 4), "cpu_s": round(cpu, 4), "max_rss_mib": round(rss / 2**20, 1),
           "mib_per_s_end_to_end": round(size / 2**20 / wall, 2),
           "steady_bytes": steady[1], "steady_s": round(steady[0], 4),
           "mib_per_s": round(steady[1] / 2**20 / steady[0], 2), "commits_observed": len(series),
           "drained": run.drained(), "states": run.ledger_states()}
    shutil.rmtree(root / "raw", ignore_errors=True)
    return rec


def one(out_dir: Path, ds: str, vname: str, v: Variant, rep: int, check: bool, keep: bool) -> dict:
    root = out_dir / "raw" / f"{ds}-{vname}-{v.max_bytes >> 20}MiB-r{rep}"
    shutil.rmtree(root, ignore_errors=True)
    run = Run(root, v, {ds: DATASET_FILES[ds]})
    size = run.sources[ds].stat().st_size
    batches, wall, cpu, rss, calls = drive(run)
    rec = {
        "dataset": ds, "variant": vname, "batch_mib": v.max_bytes >> 20, "rep": rep,
        "bytes": size, "batches": len(batches), "calls": calls,
        "wall_s": round(wall, 4), "cpu_s": round(cpu, 4), "max_rss_mib": round(rss / 2**20, 1),
        "mib_per_s": round(size / 2**20 / wall, 2),
        "compressed_bytes": sum(b.get("compressed_bytes", 0) for b in batches),
        "manifest_bytes": sum(f.stat().st_size for f in run.objects.rglob("*.manifest.json")),
        "states": run.ledger_states(),
    }
    for s in STAGES:
        rec["sum_" + s] = sum(b.get(s, 0) for b in batches)
    if check:
        o = oracle(run)
        rec["oracle"] = {"bad_objects": len(o["bad_objects"]), "unsafe_commits": o["unsafe_commits"],
                         "complete": all(x["complete"] for x in o["sources"].values()),
                         "manifest_start_understated": o["manifest_start_understated"]}
    with open(out_dir / "batches.csv", "a", newline="") as fh:
        w = csv.writer(fh)
        for i, b in enumerate(batches):
            w.writerow([ds, vname, v.max_bytes >> 20, rep, i, b.get("uncompressed_bytes"), b.get("compressed_bytes"), b.get("records"),
                        *[b.get(s) for s in STAGES], b.get("final_state")])
    if not keep:
        shutil.rmtree(root / "raw", ignore_errors=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--datasets", default="linux,ssh,bgl,hdfs")
    ap.add_argument("--part", default="AB")
    ap.add_argument("--seed", type=int, default=20261110)
    ap.add_argument("--mode", choices=["process-once", "run"], default="process-once")
    ap.add_argument("--rep-offset", type=int, default=0)
    ap.add_argument("--batch-sweep", action="store_true", help="run mode: sha256 at 1/4/16/64 MiB instead of the variant matrix")
    a = ap.parse_args()
    if a.mode == "run":
        out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
        meta = {"experiment": "E-RQ3-A (throughput, continuous engine)", "vtop_commit": VTOP_COMMIT, "host": host_info(),
                "backend": "localfs (read-back verification)", "seed": a.seed, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "variants": {k: v.__dict__ for k, v in VARIANTS.items()}}
        if not (out / "run.json").exists():
            (out / "run.json").write_text(json.dumps(meta, indent=1))
        reps = range(a.rep_offset, a.rep_offset + a.reps)
        if a.batch_sweep:
            cells = [(d, f"sha256-batch{m}", Variant("sha256", max_bytes=m * 2**20), r)
                     for d in a.datasets.split(",") for m in (1, 4, 16, 64) for r in reps]
        else:
            cells = [(d, vn, v, r) for d in a.datasets.split(",") for vn, v in VARIANTS.items() for r in reps]
        random.Random(a.seed + a.rep_offset + (7 if a.batch_sweep else 0)).shuffle(cells)
        with open(out / "runs.jsonl", "a") as fh:
            for k, (d, vn, v, r) in enumerate(cells, 1):
                rec = one_run_mode(out, d, vn, v, r)
                fh.write(json.dumps(rec) + "\n"); fh.flush()
                print(f"[{k}/{len(cells)}] {d:5s} {vn:16s} r{r} {rec['mib_per_s']:8.1f} MiB/s cpu={rec['cpu_s']:.1f}s rss={rec['max_rss_mib']}MiB drained={rec['drained']}", flush=True)
        meta = json.loads((out / "run.json").read_text())
        meta.setdefault("appended", []).append({"rep_offset": a.rep_offset, "reps": a.reps, "batch_sweep": a.batch_sweep,
                                                "datasets": a.datasets, "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")})
        (out / "run.json").write_text(json.dumps(meta, indent=1))
        return
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    meta = {"experiment": "E-RQ3-A", "vtop_commit": VTOP_COMMIT, "vtopctl": str(VTOPCTL.relative_to(VTOPCTL.parents[4])), "host": host_info(),
            "backend": "localfs (read-back verification)", "seed": a.seed, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "variants": {k: v.__dict__ for k, v in VARIANTS.items()}}
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    with open(out / "batches.csv", "w", newline="") as fh:
        csv.writer(fh).writerow(["dataset", "variant", "batch_mib", "rep", "i", "uncompressed_bytes", "compressed_bytes", "records", *STAGES, "final_state"])
    cells = []
    if "A" in a.part:
        for ds in a.datasets.split(","):
            for vn, v in VARIANTS.items():
                for rep in range(a.reps):
                    cells.append((ds, vn, v, rep))
    if "B" in a.part:
        for mib in (1, 4, 16, 64):
            for rep in range(a.reps):
                v = Variant(f"sha256", max_bytes=mib * 2**20)
                cells.append(("bgl", f"sha256-batch{mib}", v, rep))
    random.Random(a.seed).shuffle(cells)  # randomized run order
    with open(out / "runs.jsonl", "a") as fh:
        for k, (ds, vn, v, rep) in enumerate(cells, 1):
            rec = one(out, ds, vn, v, rep, check=(rep == 0), keep=False)
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            print(f"[{k}/{len(cells)}] {ds:5s} {vn:18s} rep{rep} {rec['mib_per_s']:8.1f} MiB/s  batches={rec['batches']}"
                  f"  oracle={rec.get('oracle')}", flush=True)
    meta["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (out / "run.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
