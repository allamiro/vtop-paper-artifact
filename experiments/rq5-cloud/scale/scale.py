#!/usr/bin/env python3
"""E-RQ5-SCALE driver (proposed H3.7): N independent VTOP engines on N EC2 hosts writing to one S3 bucket.

Subcommands
  plan    (operator, stdlib only) write schedule.json: the steps, their order and which host runs which engine.
  worker  (each VM) run the schedule: for every step, prepare, meet the other hosts at an S3 barrier, start the
          step's engines at a common wall-clock time, record, check every engine with the independent oracle,
          upload, and go to the next step.

One engine = one `vtopctl run` process with its own config, ledger (state store), work dir, input file(s) and S3
prefix scale/data/<session>/<step>/<engine>/. Engines never share a ledger or a source (VTOP is one engine per
state store; multi-writer recovery is out of scope: research/state-machine/v0101-verification.md 8.5).

Reuses, unchanged: cost/cost.py (engine evidence, Linux and ENA counters, the mmap patch of the oracle,
result upload) and smoke/oracle.py (byte-exact oracle). New here: the S3 barrier, concurrent engines, S3 request
accounting from the engine's own AWS SDK debug log (every request attempt, its operation, retries and throttles),
the sustained feeder, and a per-file oracle wrapper for multi-file engines.

Barrier protocol (leaderless). Before step k every host writes coord/<session>/<step>/h<slot>.json holding its
clock reading t (and an abort reason or null). Every host LISTs coord/<session>/ until it sees all hosts'
markers (or any abort/ marker), then starts at T = ceil(max(t) + lead). All hosts read the same markers, so
they compute the same T, the same abort decision and the same deadline decision (T > deadline_epoch).
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import platform
import random
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
MiB = 2**20
POLL_S = 0.02
# Engine log filter: INFO for VTOP, plus the AWS SDK's per-request DEBUG lines ("beginning attempt #n" inside the
# S3.<Operation> span, the retry classifier's verdict), so requests, retries and throttles are counted from the
# engine's own client. Endpoint resolution lines are kept at INFO (noise).
RUST_LOG = ("info,aws_sdk_s3::operation=debug,aws_smithy_runtime::client::orchestrator=debug,"
            "aws_smithy_runtime::client::orchestrator::endpoints=info,aws_smithy_runtime::client::retries=debug")
MODES = ("size-only", "sha256-store", "blake3-readback")


def _cost():
    for p in (HERE, HERE.parent / "cost"):  # VM: staged beside this file; repo: ../cost/cost.py
        if (p / "cost.py").exists():
            if str(p) not in sys.path:
                sys.path.insert(0, str(p))
            break
    import cost  # noqa: E402  (imports smoke/oracle.py and patches its source read with mmap)
    return cost


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ------------------------------------------------------------------ plan (operator side)
def plan(a) -> dict:
    rng = random.Random(a.seed)
    ns = [int(x) for x in a.ns.split(",") if x]
    cells = [{"kind": "scale", "n": n, "mode": "sha256-store", "per_host": 1, "reps": a.reps} for n in ns]
    if a.control_reps:
        cells.append({"kind": "control", "n": a.control_n, "mode": "size-only", "per_host": 1,
                      "reps": a.control_reps})
    if a.dense_reps:
        cells.append({"kind": "dense", "n": 2 * a.hosts, "mode": "sha256-store", "per_host": 2,
                      "reps": a.dense_reps})
    for c in cells:
        assert c["n"] % c["per_host"] == 0 and c["n"] // c["per_host"] <= a.hosts, c
    steps = []
    if a.warmup:
        steps.append({"kind": "warmup", "n": a.hosts, "mode": "sha256-store", "per_host": 1, "rep": 0, "round": -1})
    rounds = max(c["reps"] for c in cells)
    for r in range(rounds):  # round r holds every cell that has a rep r, shuffled: blocks out time drift
        rnd = [{k: v for k, v in c.items() if k != "reps"} | {"rep": r, "round": r} for c in cells if c["reps"] > r]
        rng.shuffle(rnd)
        steps += rnd
    if a.sustain_s:
        steps.append({"kind": "sustain", "n": a.hosts, "mode": "sha256-store", "per_host": 1, "rep": 0,
                      "round": rounds, "sustain_s": a.sustain_s, "ahead": a.sustain_ahead})
    for i, s in enumerate(steps, 1):
        s["id"] = f"s{i:03d}"
        slots = sorted(rng.sample(range(a.hosts), s["n"] // s["per_host"]))  # which hosts: random per step
        s["engines"] = [{"slot": h, "j": j, "engine": f"h{h}e{j}"} for h in slots for j in range(s["per_host"])]
    return {"experiment": "E-RQ5-SCALE: N independent VTOP engines writing to one S3 bucket",
            "session": a.session, "hosts": a.hosts, "seed": a.seed, "created_at": now_iso(),
            "deadline_epoch": a.deadline_epoch, "lead_s": a.lead_s, "boot_timeout_s": a.boot_timeout_s,
            "barrier_timeout_s": a.barrier_timeout_s, "engine_timeout_s": a.engine_timeout_s,
            "max_failures": a.max_failures, "steps": steps}


# ------------------------------------------------------------------ small helpers
class LineIndex:
    """Newline counts per 1 MiB block, so records in [0, n) cost one partial-block read."""

    def __init__(self, path: Path):
        self.path, self.block, self.counts = path, MiB, []
        with open(path, "rb") as fh:
            while b := fh.read(self.block):
                self.counts.append(b.count(b"\n"))
        self.total = sum(self.counts)

    def upto(self, n: int) -> int:
        full, rest = divmod(n, self.block)
        c = sum(self.counts[:full])
        if rest:
            with open(self.path, "rb") as fh:
                fh.seek(full * self.block)
                c += fh.read(rest).count(b"\n")
        return c


def clone(src: Path, dst: Path) -> bool:
    """Copy-on-write clone (XFS reflink on AL2023, APFS clonefile on macOS): a new inode, no data copied."""
    cmd = ["cp", "-c", str(src), str(dst)] if sys.platform == "darwin" else \
        ["cp", "--reflink=always", str(src), str(dst)]
    return subprocess.run(cmd, capture_output=True).returncode == 0


def ledger_progress(db: Path) -> dict[str, int]:
    """Committed end byte per source path (state source_committed)."""
    if not db.exists():
        return {}
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            rows = con.execute(
                "SELECT json_extract(progress_end_json, '$.path'), "
                "MAX(CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER)) "
                "FROM batches WHERE state = 'source_committed' GROUP BY 1").fetchall()
        finally:
            con.close()
    except sqlite3.OperationalError:
        return {}
    return {p: int(e or 0) for p, e in rows}


def rss_kib(pid: int, field: str = "VmRSS") -> int | None:
    """VmRSS (now) or VmHWM (peak) of the running engine, Linux only. Not ru_maxrss: after fork/vfork + exec
    Linux carries the parent's RSS high-water mark into the child's maxrss, so wait4() reports the Python
    driver's peak (seen in the AL2023 container test: 70 -> 139 MiB once the driver grew)."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith(field + ":"):
                return int(line.split()[1])
    except OSError:
        return None
    return None


def _ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def request_stats(log: Path) -> dict:
    """S3 requests made by the engine, from its AWS SDK debug lines (RUST_LOG above).

    requests = operations issued (attempt #1), attempts = every HTTP attempt incl. retries; retry verdicts are
    the SDK retry classifier's own words (a 503 SlowDown is classified as a ThrottlingError). Latency per
    operation = first to last log line inside that operation's span (sdk_invocation_id), i.e. SDK time incl.
    body upload and retries. attempts_per_s maps an epoch second to attempts started in it."""
    ops: dict[str, dict] = {}
    inv: dict[tuple, list] = {}
    verdicts: Counter = Counter()
    samples, per_s = [], Counter()
    lines = 0
    with open(log, errors="replace") as fh:
        for line in fh:
            if '"rpc.method"' not in line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            span = next((s for s in rec.get("spans") or [] if "rpc.method" in s), None)
            if span is None:
                continue
            lines += 1
            m, msg = span["rpc.method"], rec.get("fields", {}).get("message", "")
            t = _ts(rec["timestamp"])
            d = ops.setdefault(m, {"requests": 0, "attempts": 0, "retries": 0, "throttle_verdicts": 0,
                                   "retry_verdicts": 0})
            if msg.startswith("beginning attempt #"):
                d["attempts"] += 1
                per_s[int(t)] += 1
                if msg.strip().endswith("#1"):
                    d["requests"] += 1
                else:
                    d["retries"] += 1
            elif ("classified as" in msg or "retrying after" in msg or "retry quota" in msg
                  or "out of attempts" in msg):
                if "NoActionIndicated" in msg:
                    verdicts["no_action"] += 1
                else:
                    key = "throttling" if "Throttl" in msg else "other"
                    verdicts[key] += 1
                    d["retry_verdicts"] += 1
                    d["throttle_verdicts"] += key == "throttling"
                    if len(samples) < 10:
                        samples.append(f"{m}: {msg[:300]}")
            iid = span.get("sdk_invocation_id")
            if iid is not None:
                e = inv.setdefault((m, iid), [t, t])
                e[0], e[1] = min(e[0], t), max(e[1], t)
    lat: dict[str, list] = {}
    for (m, _), (t0, t1) in inv.items():
        lat.setdefault(m, []).append(round(1000 * (t1 - t0), 3))
    for m, xs in lat.items():
        xs.sort()
        q = lambda p: xs[min(len(xs) - 1, int(p * (len(xs) - 1) + 0.5))]  # noqa: E731
        ops[m]["latency_ms"] = {"n": len(xs), "p50": q(0.5), "p90": q(0.9), "p99": q(0.99), "max": xs[-1]}
    return {"sdk_span_lines": lines, "ops": ops, "verdicts": dict(verdicts), "verdict_samples": samples,
            "attempts_total": sum(d["attempts"] for d in ops.values()),
            "retries_total": sum(d["retries"] for d in ops.values()),
            "throttle_verdicts_total": sum(d["throttle_verdicts"] for d in ops.values()),
            "attempts_per_s": {str(k): v for k, v in sorted(per_s.items())}}


def config_text(cost, engine: str, mode: str, root: Path, src_dir: Path, bucket: str, prefix: str) -> str:
    """cost.config_text with the engine's own name (one engine, one name, one ledger)."""
    txt = cost.config_text(mode, root, src_dir, bucket, prefix)
    txt = txt.replace("# generated by research/experiments/rq5-cloud/cost/cost.py",
                      "# generated by research/experiments/rq5-cloud/scale/scale.py (via cost.config_text)")
    txt = txt.replace("  name: cost\n  tenant: cost\n", f"  name: {engine}\n  tenant: scale\n")
    assert f"name: {engine}" in txt, "cost.config_text changed shape"
    return txt


# ------------------------------------------------------------------ the worker
class Worker:
    def __init__(self, a):
        self.a = a
        self.cost = _cost()
        self.sched = json.loads(Path(a.schedule).read_text())
        self.session, self.hosts, self.slot = self.sched["session"], self.sched["hosts"], a.slot
        assert 0 <= self.slot < self.hosts, (self.slot, self.hosts)
        self.out, self.work, self.inroot = Path(a.out), Path(a.work), Path(a.in_root)
        self.out.mkdir(parents=True, exist_ok=True)
        self.work.mkdir(parents=True, exist_ok=True)
        self.s3 = self.cost.s3_client()
        self.coord = f"{a.stage}/coord/{self.session}"
        self.env = {k: v for k, v in os.environ.items() if k not in self.cost.STRIP_ENV}
        self.env.update({"VTOP_LOG_FORMAT": "json", "RUST_LOG": a.rust_log})
        self.nic, self.disk = self.cost.iface(), self.cost.root_disk()
        self.sent: dict = {}
        self.failures = 0
        self.master = self.inroot / "0" / "hdfs.log"
        self.index: LineIndex | None = None

    # ---- coordination
    def put_marker(self, key: str, body: dict):
        self.s3.put_object(Bucket=self.a.bucket, Key=f"{self.coord}/{key}", Body=json.dumps(body).encode())

    def barrier(self, step_id: str, payload: dict, timeout: float):
        payload = dict(payload, slot=self.slot, t=time.time(), at=now_iso())
        self.put_marker(f"{step_id}/h{self.slot}.json", payload)
        need = {f"{self.coord}/{step_id}/h{h}.json" for h in range(self.hosts)}
        got: dict[str, dict] = {}
        t0, polls = time.time(), 0
        while True:
            polls += 1
            keys = []
            for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.a.bucket,
                                                                          Prefix=f"{self.coord}/"):
                keys += [o["Key"] for o in page.get("Contents", [])]
            aborts = [k for k in keys if "/abort/" in k]
            for k in sorted((set(keys) & need) - set(got)):
                got[k] = json.loads(self.s3.get_object(Bucket=self.a.bucket, Key=k)["Body"].read())
            bad = [m for m in got.values() if m.get("abort")]
            if aborts or bad:
                why = [m["abort"] for m in bad] + aborts
                return None, f"abort seen at barrier {step_id}: {why}", polls, time.time() - t0
            if len(got) == len(need):
                return list(got.values()), None, polls, time.time() - t0
            if time.time() - t0 > timeout:
                return None, f"barrier {step_id} timed out after {timeout:.0f} s; have {sorted(got)}", polls, \
                    time.time() - t0
            time.sleep(self.a.poll_s)

    def abort(self, why: str):
        (self.out / "KEEP_DATA").write_text(why + "\n")
        try:
            self.put_marker(f"abort/h{self.slot}.json", {"slot": self.slot, "why": why, "at": now_iso()})
        except Exception as e:  # noqa: BLE001
            print(f"could not write abort marker: {e}", flush=True)

    # ---- inputs
    def input_dir(self, step: dict, j: int) -> Path:
        if step["kind"] == "sustain":
            return self.inroot / f"w{j}"  # watched dir, filled by feed()
        return self.inroot / str(j)

    def ensure_inputs(self):
        """Engine j>0 on a host needs its own file (own inode, own path): a clone of the master if the file
        system can, else a full copy. The sustained step needs cloning (tens of files)."""
        info = {"master": str(self.master), "master_bytes": self.master.stat().st_size}
        t = self.work / "reflink-test"
        t.unlink(missing_ok=True)
        info["clone_ok"] = clone(self.master, t)
        t.unlink(missing_ok=True)
        js = {e["j"] for s in self.sched["steps"] for e in s["engines"] if e["slot"] == self.slot
              and s["kind"] != "sustain"}
        for j in sorted(js - {0}):
            dst = self.inroot / str(j) / "hdfs.log"
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                if not clone(self.master, dst):
                    shutil.copyfile(self.master, dst)
            info[f"input_{j}"] = {"path": str(dst), "bytes": dst.stat().st_size,
                                  "same_inode_as_master": dst.stat().st_ino == self.master.stat().st_ino}
        self.index = LineIndex(self.master)
        info["master_lines"] = self.index.total
        return info

    # ---- one step
    def prepare(self, step: dict, e: dict) -> dict:
        a, cost = self.a, self.cost
        eng, mode = e["engine"], step["mode"]
        root = self.work / f"{step['id']}{eng}"
        shutil.rmtree(root, ignore_errors=True)
        for d in ("state", "work", "logs"):
            (root / d).mkdir(parents=True)
        os.chmod(root / "state", 0o700)  # v0.10.2 refuses a ledger others can read unless its dir is closed
        src_dir = self.input_dir(step, e["j"])
        src_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"{a.stage}/data/{self.session}/{step['id']}/{eng}/"
        cfg = root / "config.yaml"
        cfg.write_text(config_text(cost, eng, mode, root, src_dir, a.bucket, prefix))
        rec = {"step": step["id"], "kind": step["kind"], "n": step["n"], "per_host": step["per_host"],
               "round": step["round"], "rep": step["rep"], "mode": mode, "engine": eng, "slot": self.slot,
               "j": e["j"], "prefix": prefix, "src_dir": str(src_dir)}
        v = cost.sh(a.vtopctl, "--json", "validate", "--config", cfg, "--print-effective", env=self.env, timeout=120)
        rec["validate_rc"] = v["rc"]
        try:
            eff = json.loads(v["stdout"])["effective"]["default"]
            rec["planned_method"] = eff.get("verification_method", {}).get("value")
            rec["effective_max_bytes"] = eff.get("batching.max_bytes", {}).get("value")
        except (ValueError, KeyError, TypeError):
            rec["validate_output"] = (v["stdout"] or "")[-3000:] + (v["stderr"] or "")[-2000:]
        if step["kind"] != "sustain":
            src = src_dir / "hdfs.log"
            rec["files"] = [str(src)]
            rec["warm_read"] = cost.warm(src, self.nic, self.disk)
        else:
            stage = self.inroot / f"t{e['j']}"  # same file system: rename is atomic
            stage.mkdir(parents=True, exist_ok=True)
            rec["files"] = []
            e["stage"] = stage
        e.update(root=root, cfg=cfg, db=root / "state" / "vtop.db", log=root / "logs" / "engine.jsonl", rec=rec)
        return e

    def feed(self, e: dict, prog: dict, now: float, until: float, ahead: int, size: int):
        """Sustained step: keep `ahead` files waiting; a new file is a clone renamed into the watched dir
        (a log producer's rotation, as the engine sees it)."""
        rec = e["rec"]
        pending = [f for f in rec["files"] if prog.get(f, 0) < size]
        while len(pending) < ahead and now < until:
            i = len(rec["files"])
            name = f"{i:04d}.log"
            tmp, dst = e["stage"] / name, Path(rec["src_dir"]) / name
            if not clone(self.master, tmp):
                raise RuntimeError("clone failed during the sustained step")
            os.rename(tmp, dst)
            rec["files"].append(str(dst))
            rec.setdefault("fed_at", []).append(round(time.time(), 3))
            pending.append(str(dst))

    def run_engines(self, step: dict, mine: list, T: float) -> dict:
        a, cost = self.a, self.cost
        sustain = step["kind"] == "sustain"
        size = self.master.stat().st_size
        until = T + step.get("sustain_s", 0)
        timeout = (step.get("sustain_s", 0) + 900) if sustain else self.sched["engine_timeout_s"]
        while time.time() < T:
            time.sleep(min(0.05, max(0.0, T - time.time())))
        c0 = cost.counters(self.nic, self.disk)
        host_series, last_sample = [], 0.0
        for e in mine:
            r = e["rec"]
            if sustain:
                self.feed(e, {}, time.time(), until, step["ahead"], size)
            e["fh"] = open(e["log"], "wb")
            r["t_spawn"] = time.time()
            r["late_s"] = round(r["t_spawn"] - T, 4)
            e["p"] = subprocess.Popen([str(a.vtopctl), "run", "--config", str(e["cfg"])], stdout=e["fh"],
                                      stderr=subprocess.STDOUT, env=self.env, umask=0o077)
            e.update(series=[], last=0, active=True, rss=[])
        while any(x["active"] for x in mine):
            now = time.time()
            for e in mine:
                if not e["active"]:
                    continue
                r = e["rec"]
                prog = ledger_progress(e["db"])
                c = sum(min(prog.get(f, 0), size) for f in r["files"])
                if c != e["last"]:
                    e["series"].append([round(now, 4), c])
                    e["last"] = c
                if sustain:
                    self.feed(e, prog, now, until, step["ahead"], size)
                done = all(prog.get(f, 0) >= size for f in r["files"]) and (not sustain or now >= until)
                exited = e["p"].poll() is not None
                if done or exited or now - r["t_spawn"] > timeout:
                    r["drained"] = done
                    r["engine_exited_before_drain"] = exited and not done
                    r["t_stop"] = now
                    if not exited:
                        hwm = rss_kib(e["p"].pid, "VmHWM")
                        r["peak_rss_mib"] = round(hwm / 1024, 1) if hwm else None
                        e["p"].send_signal(signal.SIGTERM)
                    e["exited_early"] = exited
                    e["active"] = False
                if sustain and now >= e.get("rss_next", 0):
                    e["rss_next"] = now + 5
                    e["rss"].append([int(now), rss_kib(e["p"].pid)])
            if now - last_sample >= 1.0:
                last_sample = now
                c = cost.counters(self.nic, self.disk)
                host_series.append([round(now, 3), c.get("cpu_jiffies"), c.get("net_tx_bytes"),
                                    c.get("net_rx_bytes"), c.get("disk_read_bytes"),
                                    {k: c[k] for k in cost.ENA if k in c}])
            time.sleep(POLL_S)
        for e in mine:
            r = e["rec"]
            if e["exited_early"]:
                status, ru, killed = e["p"].returncode, None, False
            else:
                status, ru, killed = cost.reap(e["p"].pid, 120)
            e["fh"].close()
            prog = ledger_progress(e["db"])
            r["drained_after_stop"] = all(prog.get(f, 0) >= size for f in r["files"])
            r["exit_status"], r["killed_after_sigterm"] = status, killed
            r["wall_s"] = round(r["t_stop"] - r["t_spawn"], 4)
            r["cpu_s"] = round(ru.ru_utime + ru.ru_stime, 4) if ru else None
            rss = ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024) if ru else None
            r["ru_maxrss_mib"] = round(rss / MiB, 1) if rss else None  # includes the driver's peak (see rss_kib)
            r["bytes"] = size * len(r["files"])
            r["commit_series"] = e["series"]
            steady = e["series"][-2] if len(e["series"]) >= 2 else (e["series"][-1] if e["series"] else [r["t_stop"], 0])
            r["steady_bytes"], r["steady_s"] = steady[1], round(steady[0] - r["t_spawn"], 4)
            r["mib_per_s"] = round(steady[1] / MiB / r["steady_s"], 3) if r["steady_s"] > 0 else None
            r["mib_per_s_end_to_end"] = round(r["bytes"] / MiB / r["wall_s"], 3) if r["wall_s"] else None
            idx = self.index
            whole, part = divmod(steady[1], size)
            recs = whole * idx.total + (idx.upto(part) if part else 0)
            r["steady_records"] = recs
            r["records_per_s"] = round(recs / r["steady_s"], 1) if r["steady_s"] > 0 else None
            r["records_total"] = idx.total * len(r["files"])
            if e["rss"]:
                r["rss_series_kib"] = e["rss"]
        c1 = cost.counters(self.nic, self.disk)
        return {"sys": cost.delta(c0, c1), "host_series": host_series}

    def oracle(self, e: dict, mode: str) -> dict:
        """smoke/oracle.py (unchanged, via cost.py's mmap patch). A multi-file engine is checked file by file:
        each file against a ledger copy holding only that file's rows, then the results are merged."""
        cost, r = self.cost, e["rec"]
        orc = cost.oracle
        if len(r["files"]) == 1:
            return orc.check(str(e["db"]), r["files"][0], self.a.bucket, r["prefix"], mode, s3=self.s3)
        merged = {"pass": True, "complete": True, "unsafe_commits": 0, "bad_objects": 0, "duplicate_bytes": 0,
                  "committed_rows": 0, "ledger_rows": 0, "multipart_objects": 0, "covered_prefix_bytes": 0,
                  "inferred_methods": set(), "receipt_methods": set(), "problems": [], "warnings": [],
                  "per_file": [], "batches": []}
        referenced, listed = set(), None
        for f in r["files"]:
            tmp = e["root"] / f"oracle-{Path(f).stem}.db"
            tmp.unlink(missing_ok=True)
            con = sqlite3.connect(f"file:{tmp}", uri=True)
            con.execute("ATTACH DATABASE ? AS src", (f"file:{e['db']}?mode=ro",))
            con.execute("CREATE TABLE batches AS SELECT * FROM src.batches "
                        "WHERE json_extract(progress_start_json, '$.path') = ?", (f,))
            con.commit()
            con.close()
            o = orc.check(str(tmp), f, self.a.bucket, r["prefix"], mode, s3=self.s3)
            listed = o["listed"]
            for k in ("unsafe_commits", "bad_objects", "duplicate_bytes", "committed_rows", "ledger_rows",
                      "multipart_objects", "covered_prefix_bytes"):
                merged[k] += o.get(k) or 0
            merged["pass"] &= bool(o["pass"])
            merged["complete"] &= bool(o["complete"])
            merged["inferred_methods"] |= set(o["inferred_methods"])
            merged["receipt_methods"] |= set(o["receipt_methods"])
            merged["problems"] += [f"{Path(f).name}: {p}" for p in o["problems"]]
            merged["warnings"] += o["warnings"]
            merged["batches"] += o["batches"]
            referenced |= {b.get("object_key") for b in o["batches"] if b.get("object_key")}
            merged["per_file"].append({"file": f, "pass": o["pass"], "complete": o["complete"],
                                       "committed_rows": o["committed_rows"], "unsafe_commits": o["unsafe_commits"]})
            tmp.unlink(missing_ok=True)
        keys = [k for p in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.a.bucket, Prefix=r["prefix"])
                for k in (o["Key"] for o in p.get("Contents", []))]
        data = {k for k in keys if not k.endswith((".manifest.json", ".receipt.json"))}
        all_ledger = sqlite3.connect(f"file:{e['db']}?mode=ro", uri=True)
        other = all_ledger.execute("SELECT COUNT(*) FROM batches WHERE json_extract(progress_start_json, '$.path') "
                                   "NOT IN (%s)" % ",".join("?" * len(r["files"])), r["files"]).fetchone()[0]
        all_ledger.close()
        if other:
            merged["problems"].append(f"{other} ledger rows name a file that was not fed")
            merged["pass"] = False
        merged["listed"] = listed
        merged["unreferenced_data_objects"] = sorted(data - referenced)
        merged["distinct_object_keys_referenced"] = len(referenced)
        merged["inferred_methods"] = sorted(merged["inferred_methods"] - {None})
        merged["receipt_methods"] = sorted(merged["receipt_methods"] - {None})
        return merged

    def post(self, step: dict, e: dict) -> dict:
        cost, r, mode = self.cost, e["rec"], step["mode"]
        cell = f"{step['id']}-{e['engine']}"
        r["ledger"] = cost.ledger_evidence(e["db"]) if e["db"].exists() else {}
        r["engine_log"] = ls = cost.log_summary(e["log"])
        r["requests"] = request_stats(e["log"])
        for d in ("ledger", "logs", "oracle"):
            (self.out / d).mkdir(exist_ok=True)
        if e["db"].exists():
            dst = sqlite3.connect(self.out / "ledger" / f"{cell}.db")
            src = sqlite3.connect(f"file:{e['db']}?mode=ro", uri=True)
            src.backup(dst)
            src.close()
            dst.close()
        with open(e["log"], "rb") as fi, gzip.open(self.out / "logs" / f"{cell}.jsonl.gz", "wb") as fo:
            shutil.copyfileobj(fi, fo)
        t1 = time.time()
        try:
            o = self.oracle(e, mode)
        except Exception as ex:  # noqa: BLE001
            o = {"pass": False, "problems": [f"oracle raised {type(ex).__name__}: {ex}"]}
        r["oracle_s"] = round(time.time() - t1, 3)
        with gzip.open(self.out / "oracle" / f"{cell}.json.gz", "wt") as fo:
            json.dump(o, fo, default=str)
        r["oracle"] = cost.oracle_summary(o)
        lg, seen = r["ledger"], set(ls["object_checks"])
        r["log_object_checks"] = sorted(seen)
        r["checks"] = {
            "drained": bool(r.get("drained")),
            "oracle_pass": bool(o.get("pass")),
            "no_unsafe": (o.get("unsafe_commits") == 0),
            "log_as_expected": seen == {cost.LOG_EXPECT[mode]},
            "ledger_class_as_expected": lg.get("ledger_classes") == [cost.LEDGER_CLASS[mode]],
            "receipts_agree": o.get("receipt_methods") == o.get("inferred_methods") and bool(o.get("receipt_methods")),
            "single_put": (o.get("multipart_objects") == 0
                           and (lg.get("object_bytes_max") or 0) < cost.MULTIPART_THRESHOLD),
        }
        r["pass"] = all(r["checks"].values())
        r["finished_at"] = now_iso()
        if r["pass"]:
            r["deleted_data_objects"] = cost.delete_prefix(self.s3, self.a.bucket, r["prefix"])
        if step["kind"] == "sustain":
            for f in r["files"]:
                Path(f).unlink(missing_ok=True)
        shutil.rmtree(e["root"], ignore_errors=True)
        return r

    # ---- main loop
    def host_info(self, inputs: dict) -> dict:
        cost, a = self.cost, self.a
        ident = json.loads(cost.imds("dynamic/instance-identity/document") or "{}")
        return {
            "experiment": self.sched["experiment"], "session": self.session, "slot": self.slot,
            "hosts": self.hosts, "started_at": now_iso(),
            "instance_type": ident.get("instanceType"), "ami": ident.get("imageId"), "region": ident.get("region"),
            "availability_zone": ident.get("availabilityZone"), "instance_id": ident.get("instanceId"),
            "launch_index": cost.imds("meta-data/ami-launch-index"),
            "architecture": ident.get("architecture") or platform.machine(), "kernel": platform.release(),
            "os_release": cost._read("/etc/os-release") or platform.platform(), "cpu_count": os.cpu_count(),
            "meminfo": {l.split(":")[0]: l.split(":")[1].strip()
                        for l in (cost._read("/proc/meminfo") or "").splitlines()[:5]},
            "nic": self.nic, "root_disk": self.disk, "ethtool": bool(shutil.which("ethtool")),
            "ldd_version": (cost.sh("ldd", "--version")["stdout"] or "").splitlines()[:1],
            "chrony_tracking_start": (cost.sh("chronyc", "tracking", timeout=10)["stdout"] or "")[-1500:],
            "lab_endpoint": cost.LAB,
            "vtop": {"version": a.vtop_version, "commit": a.vtop_commit, "tarball_sha256": a.tarball_sha256,
                     "version_output": (cost.sh(a.vtopctl, "--version")["stdout"] or "").strip(),
                     "vtopctl_sha256": cost.sha256_file(Path(a.vtopctl)), "sigstore_bundle": a.cosign_status},
            "rust_log": a.rust_log, "aws_sdk_env": {k: os.environ.get(k) for k in cost.SDK_ENV},
            "code_sha256": a.code_sha256,
            "schedule_sha256": hashlib.sha256(Path(a.schedule).read_bytes()).hexdigest(),
            "inputs": inputs | {"master_sha256": cost.sha256_file(self.master)},
            "config": {"batching.max_bytes": cost.MAX_BYTES, "batching.max_concurrent_batches": cost.CONCURRENCY,
                       "batching.max_batch_age_seconds": 2, "compression": "zstd level 3",
                       "upload.multipart_threshold_bytes": cost.MULTIPART_THRESHOLD,
                       "manifest.verification_receipts": True, "poll_s": POLL_S},
        }

    def run(self) -> int:
        a = self.a
        (self.out / "schedule.json").write_text(Path(a.schedule).read_text())
        inputs = self.ensure_inputs()
        info = self.host_info(inputs)
        (self.out / "host.json").write_text(json.dumps(info, indent=1))
        self.cost.sync_results(self.out, self.s3, a.results_uri, self.sent)
        stopped, first = None, True
        efh = open(self.out / "engines.jsonl", "a")
        sfh = open(self.out / "steps.jsonl", "a")
        for step in self.sched["steps"]:
            mine = [dict(e) for e in step["engines"] if e["slot"] == self.slot]
            sustain_ok = inputs["clone_ok"] if step["kind"] == "sustain" else True
            try:
                prepared = [self.prepare(step, e) for e in mine] if sustain_ok else []
                bad = [e["rec"]["engine"] for e in prepared if e["rec"]["validate_rc"] != 0]
            except Exception as ex:  # noqa: BLE001
                prepared, bad = [], [f"prepare raised {type(ex).__name__}: {ex}"]
            payload = {"step": step["id"], "engines": [e["engine"] for e in mine], "sustain_ok": sustain_ok,
                       "abort": f"h{self.slot}: validate failed {bad}" if bad else None}
            timeout = self.sched["boot_timeout_s"] if first else self.sched["barrier_timeout_s"]
            first = False
            markers, err, polls, waited = self.barrier(step["id"], payload, timeout)
            srec = {"step": step["id"], "kind": step["kind"], "n": step["n"], "mode": step["mode"],
                    "slot": self.slot, "engines": [e["engine"] for e in mine], "barrier_polls": polls,
                    "barrier_wait_s": round(waited, 3)}
            if err:
                stopped = err
                if "timed out" in err or bad:
                    self.abort(err)
                srec["stopped"] = err
                sfh.write(json.dumps(srec) + "\n")
                break
            T = math.ceil(max(m["t"] for m in markers) + self.sched["lead_s"])
            srec["T"] = T
            if self.sched.get("deadline_epoch") and T > self.sched["deadline_epoch"]:
                stopped = f"deadline reached before step {step['id']}"
                srec["stopped"] = stopped
                sfh.write(json.dumps(srec) + "\n")
                break
            if step["kind"] == "sustain" and not all(m.get("sustain_ok") for m in markers):
                srec["skipped"] = "a host cannot clone files (no reflink); sustained step skipped on every host"
                sfh.write(json.dumps(srec) + "\n")
                sfh.flush()
                continue
            if prepared:
                srec.update(self.run_engines(step, prepared, T))
                recs = [self.post(step, e) for e in prepared]
                for r in recs:
                    efh.write(json.dumps(r, default=str) + "\n")
                    print(f"[{step['id']} {step['kind']} N={step['n']} {step['mode']}] {r['engine']} pass={r['pass']} "
                          f"steady={r.get('mib_per_s')} MiB/s late={r.get('late_s')} s wall={r.get('wall_s')} "
                          f"req={r['requests']['attempts_total']} retries={r['requests']['retries_total']} "
                          f"oracle_s={r.get('oracle_s')}", flush=True)
                efh.flush()
                self.failures += sum(1 for r in recs if not r["pass"])
                undrained = [r["engine"] for r in recs if not r.get("drained")]
                unsafe = [r["engine"] for r in recs if (r.get("oracle") or {}).get("unsafe_commits")]
                if undrained or unsafe or self.failures >= self.sched["max_failures"]:
                    stopped = (f"h{self.slot} stops after {step['id']}: undrained={undrained} unsafe={unsafe} "
                               f"failures={self.failures}")
                    self.abort(stopped)
            else:
                srec["idle"] = True
            srec["finished_at"] = now_iso()
            sfh.write(json.dumps(srec, default=str) + "\n")
            sfh.flush()
            self.cost.sync_results(self.out, self.s3, a.results_uri, self.sent)
            if stopped:
                break
        efh.close()
        sfh.close()
        info.update(finished_at=now_iso(), stopped=stopped, failed_engine_runs=self.failures,
                    chrony_tracking_end=(self.cost.sh("chronyc", "tracking", timeout=10)["stdout"] or "")[-1500:])
        (self.out / "host.json").write_text(json.dumps(info, indent=1))
        self.cost.sync_results(self.out, self.s3, a.results_uri, self.sent)
        print(json.dumps({"slot": self.slot, "failed": self.failures, "stopped": stopped}), flush=True)
        if self.failures or (stopped and "deadline" not in stopped):
            return 1
        return 3 if stopped else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--session", required=True)
    p.add_argument("--hosts", type=int, default=8)
    p.add_argument("--ns", default="1,2,4,8")
    p.add_argument("--reps", type=int, default=10)
    p.add_argument("--control-n", type=int, default=8)
    p.add_argument("--control-reps", type=int, default=5)
    p.add_argument("--dense-reps", type=int, default=0, help="steps with 2 engines on every host (N = 2 x hosts)")
    p.add_argument("--sustain-s", type=int, default=0, help="sustained step at N = hosts (0 = none)")
    p.add_argument("--sustain-ahead", type=int, default=2)
    p.add_argument("--warmup", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--seed", type=int, default=20261124)
    p.add_argument("--deadline-epoch", type=float, default=None)
    p.add_argument("--lead-s", type=float, default=3.0)
    p.add_argument("--boot-timeout-s", type=float, default=1500)
    p.add_argument("--barrier-timeout-s", type=float, default=900)
    p.add_argument("--engine-timeout-s", type=float, default=600)
    p.add_argument("--max-failures", type=int, default=3)
    p.add_argument("--out", required=True)
    w = sub.add_parser("worker")
    for k in ("schedule", "slot", "vtopctl", "in-root", "bucket", "out", "work", "vtop-version", "vtop-commit",
              "tarball-sha256"):
        w.add_argument(f"--{k}", required=True)
    w.add_argument("--stage", default="scale")
    w.add_argument("--results-uri", default=None)
    w.add_argument("--poll-s", type=float, default=1.0)
    w.add_argument("--rust-log", default=RUST_LOG)
    w.add_argument("--cosign-status", default="not checked")
    w.add_argument("--code-sha256", default="")
    a = ap.parse_args()
    if a.cmd == "plan":
        s = plan(a)
        Path(a.out).write_text(json.dumps(s, indent=1))
        n_eng = sum(len(x["engines"]) for x in s["steps"])
        print(f"{len(s['steps'])} steps, {n_eng} engine-runs -> {a.out}")
        return 0
    a.slot = int(a.slot)
    return Worker(a).run()


if __name__ == "__main__":
    sys.exit(main())
