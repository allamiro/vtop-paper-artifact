#!/usr/bin/env python3
"""E-RQ5-COST driver (H3.5): the RQ3 verification-cost matrix on EC2 + Amazon S3 (runs on the VM).

Matrix: datasets (SSH, BGL, HDFS) x modes (size-only, sha256-store, blake3-readback) x reps, run once each
in an order shuffled with a recorded seed. Per run: a fresh ledger and work dir, a fresh S3 prefix
cost/data/<run_id>/<dataset>-<mode>-r<rep>/ (v0.10.x reuses stably named objects, so a shared prefix would
measure reuse), `vtopctl run` until the ledger shows the input committed to its last byte, SIGTERM, then the
independent oracle of the smoke stage (smoke/oracle.py, imported unchanged) and the evidence checks.

Design follows research/experiments/rq3-cost-ablation/code/exp_minio_cost.py (variants, 16 MiB max_bytes,
8 concurrent batches, 2 s age, steady throughput = bytes at the second-to-last commit over its time) and
smoke/smoke.py (config for real S3, receipts on, log/receipt/ledger/HEAD agreement). Differences:
  * every run is checked by the oracle (RQ3 checked rep 0 only);
  * the input file of a dataset is shared by all its runs (one path, read-only, page cache kept warm and
    measured); each run has its own ledger and prefix, so nothing carries over;
  * batch sizes are measured from the ledger (never reported from the setting);
  * system counters per run (Linux): CPU steal, disk bytes read, NIC bytes and ENA allowance counters.
Outputs (--out): run.json, runs.jsonl (one line per run), oracle/<cell>.json.gz, logs/<cell>.jsonl.gz,
ledger/<cell>.db. With --results-uri the files are uploaded after every run.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import mmap
import os
import platform
import random
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
for _p in (HERE, HERE.parent / "smoke"):  # VM: staged beside this file; repo: ../smoke/oracle.py
    if (_p / "oracle.py").exists():
        sys.path.insert(0, str(_p))
        break
import oracle  # noqa: E402  (smoke stage oracle, unchanged)

MiB = 2**20
MAX_BYTES = 16 * MiB              # batching.max_bytes, as exp_cost.py Variant and smoke.py
MULTIPART_THRESHOLD = 64 * MiB    # above any object: a run-mode batch is <= 2 x max_bytes uncompressed even
                                  # without #824 (LESSONS.md), and objects are compressed; checked per run
PART_SIZE = 8 * MiB
CONCURRENCY = 8
POLL_S = 0.02
# name -> (checksum.algorithm, compression.type, upload.require_strong_verification); exp_cost.py VARIANTS
MODES = {
    "size-only": ("none", "zstd", False),
    "sha256-store": ("sha256", "zstd", True),
    "blake3-readback": ("blake3", "zstd", True),
}
# verification_passed object_check messages (smoke.py LOG_EXPECT; s3_native.rs:1373, base.rs:391 at v0.10.1).
# v0.10.2 is expected to keep them (no verification change, v0101-verification.md 13.6); a changed text
# makes log_as_expected false and is visible in the record (log_object_checks).
LOG_EXPECT = {
    "size-only": "object present and size matches (checksums disabled)",
    "sha256-store": "S3 service-computed SHA-256 verified",
    "blake3-readback": "stored content BLAKE3 verified",
}
LEDGER_CLASS = {"size-only": "size_only", "sha256-store": "strong", "blake3-readback": "strong"}
SDK_ENV = ["AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION", "AWS_ENDPOINT_URL",
           "AWS_ENDPOINT_URL_S3", "VTOP_S3_ENDPOINT_URL", "VTOP_S3_FORCE_PATH_STYLE", "VTOP_S3_VERIFY_TLS",
           "VTOP_S3_TRANSPORT", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"]
STRIP_ENV = ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "VTOP_S3_ENDPOINT_URL", "VTOP_S3_FORCE_PATH_STYLE",
             "VTOP_S3_VERIFY_TLS", "VTOP_S3_TRANSPORT"]
LAB = os.environ.get("COST_LAB_ENDPOINT")  # local MinIO dry run only; never set on EC2
ENA = ("bw_in_allowance_exceeded", "bw_out_allowance_exceeded", "pps_allowance_exceeded",
       "conntrack_allowance_exceeded", "linklocal_allowance_exceeded")


# ------------------------------------------------------------------ oracle: source by mmap
class _Mapped:
    """smoke/oracle.py check() reads the whole source with open(path, "rb").read() (1.58 GB for HDFS).
    Here read() returns a read-only mmap of the same file instead: it supports len() and slicing to bytes,
    which is all check() does with it, and it shares the page cache instead of copying the file."""

    def __init__(self, path):
        self.fh = open(path, "rb")
        self.m = mmap.mmap(self.fh.fileno(), 0, access=mmap.ACCESS_READ)

    def read(self):
        return self.m

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.fh.close()  # the mapping stays valid until check() drops it


def _oracle_open(path, mode="r", *args, **kw):
    return _Mapped(path) if mode == "rb" else open(path, mode, *args, **kw)


oracle.open = _oracle_open  # module global shadows the builtin inside oracle.check only


def s3_client():
    import boto3
    from botocore.config import Config
    cfg = {"retries": {"max_attempts": 5, "mode": "standard"}}
    if LAB:
        cfg["s3"] = {"addressing_style": "path"}
    return boto3.client("s3", region_name="us-east-1", endpoint_url=LAB or None, config=Config(**cfg))


# ------------------------------------------------------------------ config and engine
def config_text(mode: str, root: Path, src_dir: Path, bucket: str, prefix: str) -> str:
    algo, codec, strong = MODES[mode]
    lab = f"  endpoint_url: {LAB}\n  force_path_style: true\n  verify_tls: false\n" if LAB else ""
    return f"""# generated by research/experiments/rq5-cloud/cost/cost.py ({mode})
engine:
  name: cost
  tenant: cost
  state_store: "sqlite://{root / 'state' / 'vtop.db'}"
  work_dir: {root / 'work'}
batching:
  max_records: 1000000
  max_bytes: {MAX_BYTES}
  max_batch_age_seconds: 2
  idle_poll_interval_ms: 200
  max_concurrent_batches: {CONCURRENCY}
compression:
  type: {codec}
  level: 3
checksum:
  algorithm: {algo}
manifest:
  verification_receipts: true
sources:
  kafka: {{enabled: false, bootstrap_servers: ["localhost:9092"], consumer_group: lab}}
  file: {{enabled: true, paths: ["{src_dir}/*.log"]}}
  syslog_spool: {{enabled: false, paths: ["{src_dir}/*.log"]}}
upload:
  backend: s3_native
  region: us-east-1
  bucket: {bucket}
  prefix: "{prefix}"
  create_bucket: false
  require_strong_verification: {str(strong).lower()}
  multipart_threshold_bytes: {MULTIPART_THRESHOLD}
  multipart_part_size_bytes: {PART_SIZE}
""" + lab


def sh(*args, env=None, timeout=900) -> dict:
    try:
        p = subprocess.run([str(a) for a in args], capture_output=True, text=True, env=env, timeout=timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return {"rc": None, "stdout": "", "stderr": repr(e)}
    return {"rc": p.returncode, "stdout": p.stdout, "stderr": p.stderr[-4000:]}


def committed_end(db: Path) -> int:
    if not db.exists():
        return 0
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            v = con.execute("SELECT MAX(CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER)) "
                            "FROM batches WHERE state = 'source_committed'").fetchone()[0]
        finally:
            con.close()
    except sqlite3.OperationalError:
        return 0
    return int(v or 0)


def reap(pid: int, timeout: float):
    """wait4 with a timeout; SIGKILL after it. Returns (status, rusage, killed)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        r = os.wait4(pid, os.WNOHANG)
        if r[0] == pid:
            return r[1], r[2], False
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    _, status, ru = os.wait4(pid, 0)
    return status, ru, True


# ------------------------------------------------------------------ system counters (Linux, best effort)
def _read(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def iface() -> str | None:
    t = _read("/proc/net/route")
    for line in (t or "").splitlines()[1:]:
        f = line.split()
        if len(f) > 1 and f[1] == "00000000":
            return f[0]
    return None


def root_disk() -> str | None:
    t = _read("/proc/mounts")
    for line in (t or "").splitlines():
        dev, mnt = line.split()[:2]
        if mnt == "/" and dev.startswith("/dev/"):
            name = Path(os.path.realpath(dev)).name  # nvme0n1p1 -> count the whole disk nvme0n1
            base = name.rstrip("0123456789")
            return base[:-1] if base.endswith("p") and base[:-1][-1:].isdigit() else base
    return None


def counters(nic, disk) -> dict:
    out = {"t": time.time()}
    st = _read("/proc/stat")
    if st:
        f = [int(x) for x in st.splitlines()[0].split()[1:]]
        out["cpu_jiffies"] = {"user": f[0], "nice": f[1], "system": f[2], "idle": f[3], "iowait": f[4],
                              "irq": f[5], "softirq": f[6], "steal": f[7] if len(f) > 7 else 0}
    ds = _read("/proc/diskstats")
    if ds and disk:
        for line in ds.splitlines():
            f = line.split()
            if f[2] == disk:
                out["disk_read_bytes"], out["disk_write_bytes"] = int(f[5]) * 512, int(f[9]) * 512
    nd = _read("/proc/net/dev")
    if nd and nic:
        for line in nd.splitlines():
            if line.strip().startswith(nic + ":"):
                f = line.split(":", 1)[1].split()
                out["net_rx_bytes"], out["net_tx_bytes"] = int(f[0]), int(f[8])
    if nic and shutil.which("ethtool"):
        e = sh("ethtool", "-S", nic, timeout=10)
        for line in (e["stdout"] or "").splitlines():
            k, _, v = line.strip().partition(":")
            if k in ENA:
                out[k] = int(v)
    return out


def delta(a: dict, b: dict) -> dict:
    d = {}
    for k, v in b.items():
        if k == "cpu_jiffies":
            j = {x: v[x] - a["cpu_jiffies"][x] for x in v}
            tot = sum(j.values()) or 1
            d["cpu_steal_pct"] = round(100 * j["steal"] / tot, 3)
            d["cpu_iowait_pct"] = round(100 * j["iowait"] / tot, 3)
            d["cpu_busy_pct"] = round(100 * (tot - j["idle"] - j["iowait"]) / tot, 3)
        elif isinstance(v, (int, float)) and k in a:
            d[k] = round(v - a[k], 3) if k == "t" else v - a[k]
    return d


def warm(path: Path, nic, disk) -> dict:
    """Read the input once before the run so the engine reads from the page cache; record whether the
    read hit the disk (t4g.medium EBS baseline 43 MB/s would otherwise dominate)."""
    c0, t0 = counters(nic, disk), time.perf_counter()
    with open(path, "rb") as fh:
        while fh.read(8 * MiB):
            pass
    s = time.perf_counter() - t0
    d = delta(c0, counters(nic, disk))
    return {"seconds": round(s, 3), "mib_per_s": round(path.stat().st_size / MiB / s, 1),
            "disk_read_bytes": d.get("disk_read_bytes")}


# ------------------------------------------------------------------ evidence after a run
def ledger_evidence(db: Path) -> dict:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(batches)")}
        want = [c for c in ("state", "progress_start_json", "progress_end_json", "object_size_bytes",
                            "verification_class") if c in cols]
        rows = [dict(zip(want, r)) for r in con.execute(f"SELECT {', '.join(want)} FROM batches")]
    finally:
        con.close()
    states: dict[str, int] = {}
    for r in rows:
        states[r["state"]] = states.get(r["state"], 0) + 1
    com = sorted((int(json.loads(r["progress_start_json"])["start_byte"]),
                  int(json.loads(r["progress_end_json"])["end_byte"]),
                  r.get("object_size_bytes"), r.get("verification_class"))
                 for r in rows if r["state"] == "source_committed")
    unc = [e - s for s, e, _, _ in com]
    comp = [c for _, _, c, _ in com if c is not None]
    return {"states": states, "committed_batches": len(com),
            "batch_uncompressed_bytes": unc, "batch_object_bytes": comp,
            "batch_uncompressed_median": statistics.median(unc) if unc else None,
            "batch_uncompressed_max": max(unc, default=None),
            "batches_over_max_bytes": sum(1 for u in unc if u > MAX_BYTES),
            "object_bytes_max": max(comp, default=None), "object_bytes_total": sum(comp),
            "ledger_classes": sorted({str(c) for *_, c in com})}


def log_summary(log: Path) -> dict:
    out = {"object_checks": {}, "verification_classes": {}, "events": {}, "warn": 0, "error": 0,
           "warn_or_error_samples": [], "shutdown": None, "batch_metrics": 0, "metric_keys": [],
           "stage_ms_sums": {}, "unparsed_lines": 0}
    keys = set()
    for line in log.read_text(errors="replace").splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            out["unparsed_lines"] += 1
            continue
        f, level = rec.get("fields", {}), rec.get("level", "")
        msg = f.get("message", "")
        if msg.startswith("verification_passed"):
            oc, vc = str(f.get("object_check")), str(f.get("verification_class"))
            out["object_checks"][oc] = out["object_checks"].get(oc, 0) + 1
            out["verification_classes"][vc] = out["verification_classes"].get(vc, 0) + 1
        if msg.startswith("shutdown flush"):
            out["shutdown"] = msg
        if "event" in f:
            out["events"][f["event"]] = out["events"].get(f["event"], 0) + 1
        if msg == "batch_metrics":
            # v0.10.1 logs only total_ms and state_write_ms in run mode; #843 (planned for v0.10.2) adds the
            # per-stage timings. Sum every *_ms field that is present; absent fields are not zeros.
            out["batch_metrics"] += 1
            for k, v in f.items():
                keys.add(k)
                if k.endswith("_ms"):
                    try:
                        out["stage_ms_sums"][k] = round(out["stage_ms_sums"].get(k, 0.0) + float(v), 3)
                    except (TypeError, ValueError):
                        pass
        if level in ("WARN", "ERROR"):
            out["warn" if level == "WARN" else "error"] += 1
            if len(out["warn_or_error_samples"]) < 10:
                out["warn_or_error_samples"].append({"level": level, "message": msg[:400]})
    out["metric_keys"] = sorted(keys)
    return out


def oracle_summary(o: dict) -> dict:
    keep = ("pass", "complete", "committed_rows", "ledger_rows", "listed", "distinct_object_keys_referenced",
            "multipart_objects", "bad_objects", "unsafe_commits", "duplicate_bytes", "covered_prefix_bytes",
            "inferred_methods", "receipt_methods", "warnings")
    s = {k: o.get(k) for k in keep}
    s["unreferenced_data_objects"] = len(o.get("unreferenced_data_objects") or [])
    s["problems"] = (o.get("problems") or [])[:10]
    s["store_sha256_matches"] = sorted({str(b.get("store_sha256_matches_bytes")) for b in o.get("batches", [])})
    s["head_checksum_types"] = sorted({str(b.get("head", {}).get("checksum_type")) for b in o.get("batches", [])})
    return s


# ------------------------------------------------------------------ one run
def run_cell(ds: str, mode: str, rep: int, a, env, s3, nic, disk) -> dict:
    cell = f"{ds}-{mode}-r{rep}"
    root = Path(a.work) / cell
    shutil.rmtree(root, ignore_errors=True)
    for d in ("state", "work", "logs"):
        (root / d).mkdir(parents=True)
    os.chmod(root / "state", 0o700)  # v0.10.2 (#775) refuses a ledger others can read unless its dir is closed
    src_dir = Path(a.input_dir) / ds
    src = src_dir / f"{ds}.log"
    size = src.stat().st_size
    prefix = f"{a.stage}/data/{a.run_id}/{cell}/"
    cfg = root / "config.yaml"
    cfg.write_text(config_text(mode, root, src_dir, a.bucket, prefix))
    rec = {"cell": cell, "dataset": ds, "mode": mode, "rep": rep, "prefix": prefix, "bytes": size,
           "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    v = sh(a.vtopctl, "--json", "validate", "--config", cfg, "--print-effective", env=env, timeout=120)
    rec["validate_rc"] = v["rc"]
    try:
        eff = json.loads(v["stdout"])["effective"]["default"]
        rec["planned_method"] = eff.get("verification_method", {}).get("value")
        rec["effective_max_bytes"] = eff.get("batching.max_bytes", {}).get("value")
    except (ValueError, KeyError, TypeError):
        rec["validate_output"] = (v["stdout"] or "")[-3000:] + (v["stderr"] or "")[-2000:]
    rec["warm_read"] = warm(src, nic, disk)
    db, log = root / "state" / "vtop.db", root / "logs" / "engine.jsonl"
    c0 = counters(nic, disk)
    t0 = time.perf_counter()
    with open(log, "wb") as fh:
        p = subprocess.Popen([str(a.vtopctl), "run", "--config", str(cfg)], stdout=fh,
                             stderr=subprocess.STDOUT, env=env, umask=0o077)
        series, last = [], 0
        while True:
            c = committed_end(db)
            if c != last:
                series.append([round(time.perf_counter() - t0, 4), c])
                last = c
            if c >= size or p.poll() is not None or time.perf_counter() - t0 > a.timeout:
                break
            time.sleep(POLL_S)
        wall = time.perf_counter() - t0
        exited_early = p.poll() is not None
        if not exited_early:
            p.send_signal(signal.SIGTERM)
        status, ru, killed = reap(p.pid, 120) if not exited_early else (p.returncode, None, False)
    rec["sys"] = delta(c0, counters(nic, disk))
    # Drained = the ledger showed the last byte committed while the engine ran, before SIGTERM. The
    # shutdown flush can commit the rest after a timeout (seen in the dry run with --timeout 0.3); that
    # is recorded as drained_after_stop and the run fails, since its wall time is not a drain time.
    drained = last >= size
    drained_after_stop = committed_end(db) >= size
    steady = series[-2] if len(series) >= 2 else (series[-1] if series else [wall, 0])
    rss = None
    if ru is not None:
        rss = ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024)  # macOS bytes, Linux KiB
    rec.update({
        "drained": drained, "drained_after_stop": drained_after_stop,
        "engine_exited_before_drain": exited_early and not drained,
        "killed_after_sigterm": killed, "exit_status": status,
        "wall_s": round(wall, 4), "cpu_s": round(ru.ru_utime + ru.ru_stime, 4) if ru else None,
        "max_rss_mib": round(rss / MiB, 1) if rss else None,
        "mib_per_s_end_to_end": round(size / MiB / wall, 3),
        "steady_bytes": steady[1], "steady_s": steady[0],
        "mib_per_s": round(steady[1] / MiB / steady[0], 3) if steady[0] else None,
        "commits_observed": len(series), "commit_series": series,
    })
    rec["ledger"] = ledger_evidence(db) if db.exists() else {}
    rec["engine_log"] = ls = log_summary(log)
    (Path(a.out) / "ledger").mkdir(exist_ok=True)
    if db.exists():
        dst = sqlite3.connect(Path(a.out) / "ledger" / f"{cell}.db")
        srccon = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        srccon.backup(dst)
        srccon.close()
        dst.close()
    (Path(a.out) / "logs").mkdir(exist_ok=True)
    with open(log, "rb") as fi, gzip.open(Path(a.out) / "logs" / f"{cell}.jsonl.gz", "wb") as fo:
        shutil.copyfileobj(fi, fo)
    t1 = time.perf_counter()
    o = oracle.check(str(db), str(src), a.bucket, prefix, mode, s3=s3)
    rec["oracle_s"] = round(time.perf_counter() - t1, 3)
    (Path(a.out) / "oracle").mkdir(exist_ok=True)
    with gzip.open(Path(a.out) / "oracle" / f"{cell}.json.gz", "wt") as fo:
        json.dump(o, fo, default=str)
    rec["oracle"] = oracle_summary(o)
    if rep == 0:  # VTOP's own read-only re-checks, once per dataset x mode (not part of the pass rule)
        rec["vtop_scrub_rc"] = sh(a.vtopctl, "--json", "scrub", "--config", cfg, env=env)["rc"]
        rec["vtop_audit_coverage_rc"] = sh(a.vtopctl, "--json", "audit-coverage", "--config", cfg,
                                           "--check-storage", env=env)["rc"]
    seen = set(ls["object_checks"])
    lg = rec["ledger"]
    checks = {
        "drained": drained,
        "oracle_pass": bool(o.get("pass")),
        "log_as_expected": seen == {LOG_EXPECT[mode]},
        "ledger_class_as_expected": lg.get("ledger_classes") == [LEDGER_CLASS[mode]],
        "receipts_agree": o.get("receipt_methods") == o.get("inferred_methods") and bool(o.get("receipt_methods")),
        "single_put": (o.get("multipart_objects") == 0 and (lg.get("object_bytes_max") or 0) < MULTIPART_THRESHOLD),
    }
    rec["log_object_checks"] = sorted(seen)
    rec["checks"] = checks
    rec["pass"] = all(checks.values())
    rec["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    shutil.rmtree(root, ignore_errors=True)  # ledger copy, log and oracle report are kept in --out
    return rec


# ------------------------------------------------------------------ run metadata
def imds(path: str) -> str | None:
    try:
        tok = urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"}), timeout=2).read().decode()
        return urllib.request.urlopen(urllib.request.Request(
            f"http://169.254.169.254/latest/{path}", headers={"X-aws-ec2-metadata-token": tok}),
            timeout=2).read().decode()
    except Exception:  # noqa: BLE001
        return None


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        while b := fh.read(8 * MiB):
            h.update(b)
    return h.hexdigest()


def run_info(a, cells, nic, disk) -> dict:
    import boto3
    import botocore
    ident = json.loads(imds("dynamic/instance-identity/document") or "{}")
    mem = {l.split(":")[0]: l.split(":")[1].strip() for l in (_read("/proc/meminfo") or "").splitlines()[:5]}
    du = shutil.disk_usage(a.work)
    return {
        "experiment": "E-RQ5-COST (H3.5): verification cost on EC2 + Amazon S3", "stage": a.stage,
        "run_id": a.run_id, "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "instance_type": ident.get("instanceType"), "ami": ident.get("imageId"), "region": ident.get("region"),
        "availability_zone": ident.get("availabilityZone"), "instance_id": ident.get("instanceId"),
        "architecture": ident.get("architecture") or platform.machine(),
        "os_release": _read("/etc/os-release") or platform.platform(), "kernel": platform.release(),
        "cpu_count": os.cpu_count(), "meminfo": mem, "work_disk_free_bytes": du.free,
        "nic": nic, "root_disk": disk, "ethtool": bool(shutil.which("ethtool")),
        "ldd_version": (sh("ldd", "--version")["stdout"] or "").splitlines()[:1],
        "lab_endpoint": LAB,
        "vtop": {"version": a.vtop_version, "commit": a.vtop_commit, "tarball_sha256": a.tarball_sha256,
                 "version_output": (sh(a.vtopctl, "--version")["stdout"] or "").strip(),
                 "vtopctl_sha256": sha256_file(Path(a.vtopctl)), "sigstore_bundle": a.cosign_status},
        "aws_sdk_env": {k: os.environ.get(k) for k in SDK_ENV},
        "oracle_python": {"python": platform.python_version(), "boto3": boto3.__version__,
                          "botocore": botocore.__version__, "oracle_file": oracle.__file__,
                          "oracle_sha256": sha256_file(Path(oracle.__file__))},
        "code_sha256": a.code_sha256, "bucket": a.bucket,
        "inputs": {ds: {"path": str(Path(a.input_dir) / ds / f"{ds}.log"),
                        "bytes": (Path(a.input_dir) / ds / f"{ds}.log").stat().st_size,
                        "sha256": sha256_file(Path(a.input_dir) / ds / f"{ds}.log")} for ds in a.datasets},
        "matrix": {"datasets": a.datasets, "modes": a.modes, "reps": a.reps, "seed": a.seed,
                   "order": [f"{d}-{m}-r{r}" for d, m, r in cells]},
        "config": {"batching.max_bytes": MAX_BYTES, "batching.max_records": 1000000,
                   "batching.max_batch_age_seconds": 2, "batching.max_concurrent_batches": CONCURRENCY,
                   "compression": "zstd level 3", "upload.multipart_threshold_bytes": MULTIPART_THRESHOLD,
                   "upload.multipart_part_size_bytes": PART_SIZE, "manifest.verification_receipts": True,
                   "modes": {m: dict(zip(("checksum.algorithm", "compression.type",
                                          "upload.require_strong_verification"), v)) for m, v in MODES.items()}},
        "measurement": {"poll_s": POLL_S, "engine_timeout_s": a.timeout,
                        "throughput": "steady = committed bytes at the second-to-last commit / its time "
                                      "(exp_cost.py one_run_mode); end-to-end also recorded",
                        "page_cache": "input read once before each run (warm_read)"},
    }


def sync_results(out: Path, s3, uri: str | None, sent: dict):
    """Upload new or changed result files (incremental, so a failsafe stop keeps finished runs)."""
    if not uri:
        return
    bucket, key = uri[5:].split("/", 1)
    for f in sorted(out.rglob("*")):
        if not f.is_file():
            continue
        st = f.stat()
        if sent.get(str(f)) == (st.st_size, st.st_mtime):
            continue
        try:
            s3.upload_file(str(f), bucket, f"{key.rstrip('/')}/{f.relative_to(out)}")
            sent[str(f)] = (st.st_size, st.st_mtime)
        except Exception as e:  # noqa: BLE001  never stop the matrix for a progress upload
            print(f"upload of {f} failed: {e}", flush=True)


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
            n += len(objs)
    return n


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for k in ("run-id", "vtopctl", "input-dir", "bucket", "out", "work", "vtop-version", "vtop-commit",
              "tarball-sha256"):
        ap.add_argument(f"--{k}", required=True)
    ap.add_argument("--stage", default="cost")
    ap.add_argument("--datasets", default="ssh,bgl,hdfs")
    ap.add_argument("--modes", default=",".join(MODES))
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--timeout", type=float, default=600, help="per-run engine limit (s)")
    ap.add_argument("--deadline-epoch", type=float, default=None, help="start no run after this time")
    ap.add_argument("--max-failures", type=int, default=3, help="stop the matrix after this many failed runs")
    ap.add_argument("--results-uri", default=None, help="s3://bucket/prefix/ for incremental uploads")
    ap.add_argument("--delete-data", action="store_true", help="delete <stage>/data/<run_id>/ at the end "
                    "unless a run failed (on the VM the EXIT trap does this with the aws CLI)")
    ap.add_argument("--cosign-status", default="not checked")
    ap.add_argument("--code-sha256", default="")
    a = ap.parse_args()
    a.datasets, a.modes = a.datasets.split(","), a.modes.split(",")
    assert set(a.modes) <= set(MODES), a.modes
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    Path(a.work).mkdir(parents=True, exist_ok=True)
    env = {k: v for k, v in os.environ.items() if k not in STRIP_ENV}
    env["VTOP_LOG_FORMAT"] = "json"
    nic, disk = iface(), root_disk()
    s3 = s3_client()
    cells = [(d, m, r) for d in a.datasets for m in a.modes for r in range(a.reps)]
    random.Random(a.seed).shuffle(cells)
    info = run_info(a, cells, nic, disk)
    (out / "run.json").write_text(json.dumps(info, indent=1))
    sent: dict = {}
    sync_results(out, s3, a.results_uri, sent)
    failures, stopped, done = 0, None, 0
    with open(out / "runs.jsonl", "a") as fh:
        for k, (ds, mode, rep) in enumerate(cells, 1):
            if a.deadline_epoch and time.time() > a.deadline_epoch:
                stopped = f"deadline reached before run {k}/{len(cells)}"
                break
            try:
                rec = run_cell(ds, mode, rep, a, env, s3, nic, disk)
            except Exception as e:  # noqa: BLE001  record and continue
                rec = {"cell": f"{ds}-{mode}-r{rep}", "dataset": ds, "mode": mode, "rep": rep, "pass": False,
                       "error": f"{type(e).__name__}: {e}"}
            rec["order"] = k
            done += 1
            fh.write(json.dumps(rec, default=str) + "\n")
            fh.flush()
            failures += not rec["pass"]
            print(f"[{k}/{len(cells)}] {rec['cell']:28s} pass={rec['pass']} "
                  f"steady={rec.get('mib_per_s')} MiB/s wall={rec.get('wall_s')}s "
                  f"batches={rec.get('ledger', {}).get('committed_batches')} "
                  f"median_batch={rec.get('ledger', {}).get('batch_uncompressed_median')} "
                  f"oracle_s={rec.get('oracle_s')} {rec.get('error', '')}", flush=True)
            sync_results(out, s3, a.results_uri, sent)
            if failures >= a.max_failures or not rec.get("drained", False):
                stopped = f"stopped after run {k}: {failures} failed run(s); last drained={rec.get('drained')}"
                break
    info["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    info["runs_done"], info["runs_planned"], info["failed_runs"], info["stopped"] = done, len(cells), failures, stopped
    if failures:  # the VM's EXIT trap keeps <stage>/data/<run_id>/ for diagnosis when this marker exists
        (out / "KEEP_DATA").write_text(f"{failures} failed run(s); data kept under {a.stage}/data/{a.run_id}/\n")
    elif a.delete_data:
        info["deleted_data_objects"] = delete_prefix(s3, a.bucket, f"{a.stage}/data/{a.run_id}/")
    (out / "run.json").write_text(json.dumps(info, indent=1))
    sync_results(out, s3, a.results_uri, sent)
    print(json.dumps({"runs": info["runs_done"], "failed": failures, "stopped": stopped}), flush=True)
    raise SystemExit(1 if failures else (3 if stopped else 0))  # 3: matrix cut short by the deadline


if __name__ == "__main__":
    main()
