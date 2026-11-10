#!/usr/bin/env python3
"""E-RQ1-F: crash and fault injection with an independent oracle.

Each trial archives real Loghub files with ``vtopctl run`` and injects
faults while the engine works:

  * SIGKILL at a random time (K kills per trial), then restart;
  * a storage outage: the object store is made read-only for a random
    window, so uploads fail and batches must be handed back (F6/F7);
  * optional power-loss emulation: after a kill, the SQLite WAL is deleted,
    discarding every ledger transaction not yet checkpointed (F8).

After every kill the oracle (research/experiments/lib/vtoplab.py) checks
that committed progress never exceeds the byte-exact, verified archive
prefix (I2). After the final drain it checks completeness (no lost bytes)
and counts duplicates (I5).

Scenarios:
  file-mix   linux + ssh + bgl as three file sources at once, 4 MiB batches
  spool-mix  linux + ssh as syslog-spool sources, 1 MiB batches
  power-loss file-mix with WAL deletion after each kill
  hdfs       HDFS_v1 alone (1.5 GB), 16 MiB batches
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from vtoplab import DATASET_FILES, VTOP_COMMIT, Run, Variant, host_info, oracle  # noqa: E402

SCENARIOS = {
    "file-mix": dict(files=["linux", "ssh", "bgl"], spool=False, mib=4, wal_loss=False),
    "spool-mix": dict(files=["linux", "ssh"], spool=True, mib=1, wal_loss=False),
    "power-loss": dict(files=["linux", "ssh", "bgl"], spool=False, mib=4, wal_loss=True),
    "hdfs": dict(files=["hdfs"], spool=False, mib=16, wal_loss=False),
}


def set_writable(path: Path, writable: bool):
    mode = "u+w" if writable else "a-w"
    subprocess.run(["chmod", "-R", mode, str(path)], check=False, capture_output=True)


def total_committed(run: Run) -> int:
    return sum(run.committed().values())


def trial(out: Path, scen: str, idx: int, kills: int, rng: random.Random) -> dict:
    cfg = SCENARIOS[scen]
    root = out / "raw" / f"{scen}-t{idx:02d}"
    shutil.rmtree(root, ignore_errors=True)
    run = Run(root, Variant(scen, max_bytes=cfg["mib"] * 2**20, max_age_s=1), {d: DATASET_FILES[d] for d in cfg["files"]},
              spool=cfg["spool"])
    events, snapshots = [], []
    outage_at = rng.randrange(kills)  # which life gets a storage outage
    t_trial = time.time()
    for life in range(kills):
        run.start()
        t_start = time.time()
        c_before = total_committed(run)
        # time to first new commit after (re)start
        first_commit = None
        life_len = rng.uniform(0.2, 1.2)
        outage = None
        if life == outage_at:
            o_start, o_len = rng.uniform(0.1, life_len * 0.7), rng.uniform(0.3, 1.0)
            outage = (round(o_start, 3), round(o_len, 3))
            threading.Timer(o_start, set_writable, (run.objects, False)).start()
            threading.Timer(o_start + o_len, set_writable, (run.objects, True)).start()
        while time.time() - t_start < life_len:
            if first_commit is None and total_committed(run) > c_before:
                first_commit = time.time() - t_start
            if run.drained():
                break
            time.sleep(0.02)
        time.sleep(max(0.0, (outage[0] + outage[1] + 0.05) - (time.time() - t_start)) if outage else 0)
        set_writable(run.objects, True)
        run.kill()  # SIGKILL
        wal = Path(str(run.db) + "-wal")
        wal_lost = False
        if cfg["wal_loss"] and wal.exists():
            wal.unlink()
            Path(str(run.db) + "-shm").unlink(missing_ok=True)
            wal_lost = True
        o = oracle(run)
        snapshots.append({"life": life, "unsafe_commits": o["unsafe_commits"], "bad_objects": len(o["bad_objects"])})
        events.append({"life": life, "ran_s": round(time.time() - t_start, 3), "first_commit_s": first_commit,
                       "outage": outage, "wal_deleted": wal_lost, "committed_bytes": total_committed(run)})
        if run.drained():
            break
    # final life: run to completion
    run.start()
    t_final = time.time()
    c0 = total_committed(run)
    first = None
    while time.time() - t_final < 900 and not run.drained():
        if first is None and total_committed(run) > c0:
            first = time.time() - t_final
        time.sleep(0.05)
    drain_s = time.time() - t_final
    run.stop()
    final = oracle(run)
    src_bytes = sum(p.stat().st_size for p in run.sources.values())
    archived = sum(s["duplicate_bytes"] for s in final["sources"].values())
    rec = {
        "scenario": scen, "trial": idx, "kills": len(events), "events": events,
        "unsafe_commits_at_crash": sum(s["unsafe_commits"] for s in snapshots),
        "unsafe_commits_final": final["unsafe_commits"],
        "bad_objects_final": len(final["bad_objects"]),
        "complete": all(s["complete"] for s in final["sources"].values()),
        "lost_bytes": sum(max(0, s["size"] - s["verified_prefix"]) for s in final["sources"].values()),
        "duplicate_objects": sum(s["duplicate_objects"] for s in final["sources"].values()),
        "duplicate_bytes": archived,
        "source_bytes": src_bytes,
        "replay_amplification": round(archived / src_bytes, 6),
        "manifest_start_understated": final["manifest_start_understated"],
        "manifests": final["manifests"],
        "final_first_commit_s": first, "final_drain_s": round(drain_s, 3),
        "ledger_states": run.ledger_states(),
        "trial_s": round(time.time() - t_trial, 2),
    }
    shutil.rmtree(root / "raw" / "objects", ignore_errors=True)
    shutil.rmtree(root / "raw" / "input", ignore_errors=True)
    shutil.rmtree(root / "raw" / "spool", ignore_errors=True)
    shutil.rmtree(root / "raw" / "work", ignore_errors=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--scenarios", default="file-mix,spool-mix,power-loss,hdfs")
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--kills", type=int, default=8)
    ap.add_argument("--seed", type=int, default=20261110)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(a.seed)
    meta = {"experiment": "E-RQ1-F", "vtop_commit": VTOP_COMMIT, "host": host_info(), "seed": a.seed,
            "scenarios": SCENARIOS, "trials": a.trials, "kills": a.kills, "backend": "localfs",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    with open(out / "trials.jsonl", "a") as fh:
        for scen in a.scenarios.split(","):
            n = a.trials if scen != "hdfs" else max(3, a.trials // 2)
            for i in range(n):
                rec = trial(out, scen, i, a.kills, rng)
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                print(f"{scen:10s} t{i:02d} kills={rec['kills']} unsafe={rec['unsafe_commits_at_crash']}/{rec['unsafe_commits_final']} "
                      f"complete={rec['complete']} lost={rec['lost_bytes']} dupobj={rec['duplicate_objects']} "
                      f"amp={rec['replay_amplification']:.4f} drain={rec['final_drain_s']}s", flush=True)
    meta["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (out / "run.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
