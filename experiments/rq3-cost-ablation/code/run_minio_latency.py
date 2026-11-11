#!/usr/bin/env python3
"""C25 orchestrator: emulated-latency S3 cost chunks with timing hygiene.

Runs exp_minio_cost.py --proxy --latency-ms L in chunks. Before each chunk it waits
for a quiet host (no exp_baseline.py / other exp_minio_cost.py, no foreign Docker
container and no host process above the CPU threshold), records load averages and
top CPU processes in ENVIRONMENT-NOTES.md, and samples foreign container CPU while
the chunk runs. Runs whose [started_at, finished_at] window (±5 s) contains a busy
sample are moved to runs-exposed.jsonl (preserved) and re-run.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve()
DRIVER = HERE.parent / "exp_minio_cost.py"
OURS = ("rq3-minio", "rq3-toxiproxy")
BUSY_CPU = 50.0      # % of one core, per foreign Docker container
HOST_BUSY_CPU = 200.0  # % of one core, per host process (interactive apps spike briefly)
HOST_IGNORE = ("Virtualization.VirtualMachine", "vtopctl", "python", "docker", "com.docker")


def sh(*a):
    return subprocess.run(list(a), capture_output=True, text=True)


def foreign_containers():
    out = sh("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}").stdout
    rows = []
    for line in out.splitlines():
        name, cpu = line.split("\t")
        if name not in OURS:
            rows.append((name, float(cpu.rstrip("%") or 0)))
    return rows


def host_top(n=8):
    lines = sh("ps", "-Ao", "pid,pcpu,comm", "-r").stdout.splitlines()[1:n + 1]
    rows = []
    for line in lines:
        pid, cpu, comm = line.strip().split(None, 2)
        rows.append((int(pid), float(cpu), comm))
    return rows


def other_experiments():
    """Other experiment drivers, excluding the driver this orchestrator started (its child)."""
    import os
    found = []
    for pat in ("exp_baseline.py", "exp_minio_cost.py"):
        for line in sh("pgrep", "-fl", pat).stdout.splitlines():
            parts = line.split()
            if len(parts) > 1 and "python" in parts[1]:
                ppid = sh("ps", "-o", "ppid=", "-p", parts[0]).stdout.strip()
                if ppid != str(os.getpid()):
                    found.append(line)
    return found


def busy_reasons():
    reasons = [f"experiment: {x}" for x in other_experiments()]
    reasons += [f"container {n} {c:.0f}%" for n, c in foreign_containers() if c > BUSY_CPU]
    reasons += [f"host {comm.rsplit('/', 1)[-1]} {c:.0f}%" for _, c, comm in host_top(15)
                if c > HOST_BUSY_CPU and not any(k in comm for k in HOST_IGNORE)]
    return reasons


def now():
    return dt.datetime.now().astimezone()


def stamp(t=None):
    return (t or now()).strftime("%Y-%m-%dT%H:%M:%S%z")


def wait_quiet(log, need=3, poll=20):
    """Return after `need` consecutive quiet checks; log every busy observation."""
    quiet, t0 = 0, time.time()
    while quiet < need:
        r = busy_reasons()
        if r:
            quiet = 0
            log(f"busy {stamp()}: {'; '.join(r)}")
        else:
            quiet += 1
        if quiet < need:
            time.sleep(poll)
    return round(time.time() - t0)


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self.stop = [], threading.Event()

    def run(self):
        while not self.stop.is_set():
            t = now()
            fc = foreign_containers()
            busy = [f"{n} {c:.0f}%" for n, c in fc if c > BUSY_CPU] + [f"exp: {x}" for x in other_experiments()]
            self.samples.append({"t": stamp(t), "busy": busy,
                                 "foreign_cpu_total": round(sum(c for _, c in fc), 1)})
            self.stop.wait(3)


def parse(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--chunks", required=True, help="comma list of LATENCY:REP_OFFSET:REPS[:BANDWIDTH_KBPS]")
    ap.add_argument("--datasets", default="ssh,bgl,hdfs")
    ap.add_argument("--variants", default="size-only,sha256,blake3")
    ap.add_argument("--python", default=sys.executable)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    notes = out / "ENVIRONMENT-NOTES.md"
    if not notes.exists():
        notes.write_text("# Environment notes: emulated-latency S3 cost campaign (C25)\n\n"
                         "Per chunk: quiet gate, load averages, top CPU processes, foreign Docker CPU\n"
                         f"(busy = a foreign container above {BUSY_CPU:.0f}% or a host process above {HOST_BUSY_CPU:.0f}% of one core, or another\n"
                         "exp_baseline.py / exp_minio_cost.py), and the runs exposed to busy samples.\n")
    monitor = out / "raw" / "monitor.jsonl"
    monitor.parent.mkdir(parents=True, exist_ok=True)

    def log(msg):
        print(msg, flush=True)
        with open(notes, "a") as fh:
            fh.write(f"- {msg}\n")

    for chunk in a.chunks.split(","):
        lat, off, reps, *bw = chunk.split(":")
        bw = float(bw[0]) if bw else None
        bw_args = ["--bandwidth-kbps", f"{bw:g}"] if bw is not None else []
        label = f"+{lat} ms RTT" + (f", {bw:g} KB/s each way" if bw is not None else "")
        for attempt in range(1, 11):
            with open(notes, "a") as fh:
                fh.write(f"\n## Chunk {label}, reps {off}..{int(off) + int(reps) - 1} (attempt {attempt})\n\n")
            waited = wait_quiet(log)
            up = sh("uptime").stdout.strip()
            top = host_top(8)
            fc = sorted(foreign_containers(), key=lambda x: -x[1])[:5]
            log(f"quiet gate passed after {waited} s at {stamp()}")
            log(f"uptime: `{up}`")
            log("top CPU: " + "; ".join(f"{comm.rsplit('/', 1)[-1][:60]} {c:.1f}%" for _, c, comm in top))
            log("foreign containers (top 5 CPU): " + "; ".join(f"{n} {c:.1f}%" for n, c in fc))
            sampler = Sampler()
            sampler.start()
            t0 = stamp()
            p = subprocess.run([a.python, str(DRIVER), "--out", str(out), "--datasets", a.datasets,
                                "--variants", a.variants, "--rep-offset", off, "--reps", reps,
                                "--proxy", "--latency-ms", lat, *bw_args])
            sampler.stop.set()
            sampler.join()
            t1 = stamp()
            with open(monitor, "a") as fh:
                for s in sampler.samples:
                    fh.write(json.dumps({"chunk": chunk, "attempt": attempt, **s}) + "\n")
            busy = [s for s in sampler.samples if s["busy"]]
            log(f"chunk ran {t0} .. {t1}; driver exit {p.returncode}; {len(sampler.samples)} monitor samples, "
                f"{len(busy)} busy; post-chunk uptime: `{sh('uptime').stdout.strip()}`")
            if p.returncode != 0:
                log("driver failed; stopping (see console output)")
                sys.exit(p.returncode)
            # exposure: any busy sample within ±5 s of a run of this chunk/latency
            runs_path = out / "runs.jsonl"
            rows = [json.loads(x) for x in runs_path.read_text().splitlines() if x.strip()]
            mine = lambda r: (r.get("latency_ms") == float(lat) and r.get("bandwidth_kbps") == bw
                              and int(off) <= r["rep"] < int(off) + int(reps))
            exposed = []
            for r in rows:
                if not mine(r):
                    continue
                s0, s1 = parse(r["started_at"]).timestamp() - 5, parse(r["finished_at"]).timestamp() + 5
                if any(s0 <= parse(b["t"]).timestamp() <= s1 for b in busy):
                    exposed.append(r)
            if not exposed:
                log("exposed runs: none")
                break
            for b in busy:
                log(f"busy sample {b['t']}: {'; '.join(b['busy'])}")
            keys = {(r["dataset"], r["variant"], r["rep"], r["latency_ms"], r.get("bandwidth_kbps")) for r in exposed}
            log(f"exposed runs moved to runs-exposed.jsonl and re-run: "
                + ", ".join(f"{d}/{v}/r{k}" for d, v, k, _, _ in sorted(keys)))
            with open(out / "runs-exposed.jsonl", "a") as fh:
                for r in exposed:
                    fh.write(json.dumps({**r, "exposure": "busy sample within ±5 s"}) + "\n")
            keep = [r for r in rows
                    if (r["dataset"], r["variant"], r["rep"], r.get("latency_ms"), r.get("bandwidth_kbps")) not in keys]
            runs_path.write_text("".join(json.dumps(r) + "\n" for r in keep))
        else:
            log("chunk still exposed after 10 attempts; stopping")
            sys.exit(3)


if __name__ == "__main__":
    main()
