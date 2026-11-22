#!/usr/bin/env python3
"""E-RQ5-WAN driver: VTOP over a real long-distance link (engine VM in a far region, bucket in us-east-1), plus
low bandwidth, loss, jitter, a partition, SIGKILL and a policed uplink applied on the VM (runs as root, Linux).

Built on the E-RQ5-COST driver: cost.py is imported (counters, ledger and log evidence, oracle wrapper, result
sync, prefix deletion) and smoke/oracle.py checks every run unchanged. What this file adds:
  * conditions (COND) applied around the engine run only: kernel shaping with `tc` (tbf egress, ifb+tbf ingress),
    netem loss/jitter on egress, an egress policer, a partition (egress drop for --partition-s), SIGKILL and
    restart, and VTOP's own upload.max_egress_bytes_per_second; the oracle and result uploads run unshaped;
  * a capability preflight on the real NIC: a condition whose tc primitive is missing is skipped with the reason;
  * real RTT to the S3 endpoint (TCP connect and HeadBucket) at start, per run and at the end; per-condition
    link calibration (timed PUT/GET through the shaping);
  * per run: wall, input MiB/s, NIC bytes, CPU, retries/failures from the engine log (with RUST_LOG adding the
    SDK's retry decisions), ledger seal-to-commit times, partition recovery and restart times; incomplete
    multipart uploads under the run's prefix are aborted after every run.
Outputs (--out) as cost.py: run.json, runs.jsonl, oracle/, logs/, ledger/; uploaded after every run.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import gzip
import json
import os
import random
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
for _p in (HERE, HERE.parent / "cost"):  # VM: staged beside this file; repo: ../cost/cost.py
    if (_p / "cost.py").exists():
        sys.path.insert(0, str(_p))
        break
import cost  # noqa: E402  (imports smoke/oracle.py and reads the source by mmap)
from cost import oracle  # noqa: E402

KiB, MiB = 2**10, 2**20
LAB = os.environ.get("WAN_LAB_ENDPOINT")  # local MinIO tests only; never set on EC2
cost.LAB = LAB                            # cost.s3_client() then talks to the lab endpoint
MAX_BYTES = 16 * MiB                      # decision D6
SINGLE_PUT_THRESHOLD = 64 * MiB           # as cost/: store SHA-256 must stay single-PUT
CONCURRENCY = 8
POLL_S = 0.02
SAMPLE_S = 0.5
LINK_LOCAL = "169.254.0.0/16"             # IMDS (credentials), Route 53 resolver, NTP: never shaped or cut
# mode -> (checksum.algorithm, compression.type, require_strong_verification, multipart threshold or None=default)
MODES = {
    "size-only": ("none", "zstd", False, SINGLE_PUT_THRESHOLD),
    "sha256-store": ("sha256", "zstd", True, SINGLE_PUT_THRESHOLD),
    "blake3-readback": ("blake3", "zstd", True, SINGLE_PUT_THRESHOLD),
    # policed regression (#663 workload shape): uncompressed, engine-default 8 MiB multipart, 4 parts in flight
    "sha256-raw-mp": ("sha256", "none", True, None),
}
ORACLE_MODE = {"sha256-raw-mp": "sha256-multipart"}
LEDGER_CLASS = {"size-only": "size_only", "sha256-store": "strong", "blake3-readback": "strong",
                "sha256-raw-mp": "strong"}
# Conditions. rate/police in Mbit/s (10^6 bit/s); cap in bytes/s (VTOP's own limiter); netem on egress only.
COND = {
    "native": {},
    "tc10": {"rate": 10}, "tc2": {"rate": 2},
    "cap10": {"cap": 1_250_000}, "cap2": {"cap": 250_000},
    "loss1": {"netem": "loss 1%"}, "loss5": {"netem": "loss 5%"},
    "jitter": {"netem": "delay 20ms 10ms distribution normal"},
    "partition": {"partition_s": 60, "trigger_tx": 256 * KiB},
    "sigkill": {"kill": True, "trigger_tx": 256 * KiB},
    # 1 Mbit/s: at 2 Mbit/s and 100 ms the lab drained 32 MiB in 242 s (< 300 s, so the old deadline would
    # have passed too); the check only bites when a single request needs more than 300 s
    "policed": {"police": 1, "burst": 5000, "dataset": "ssh32", "timeout": 2700},
    # control: the pre-#663 fixed deadline (body allowance ~0 s), shortened to 120 s so a window of 4 deadlines
    # fits; expected to commit nothing in the window and nothing unsafe
    "policed-fixed": {"police": 1, "burst": 5000, "dataset": "ssh32", "request_timeout": 120,
                      "request_min_bps": 1_000_000_000, "timeout": 480, "expect": "no_commit"},
}
DEFAULT_PLAN = ("native:size-only,sha256-store,blake3-readback:10;"
                "tc10,tc2,cap10,cap2:sha256-store,blake3-readback:3;"
                "loss1,loss5,jitter:sha256-store:3;"
                "partition:sha256-store:1;sigkill:sha256-store,blake3-readback:1;"
                "policed,policed-fixed:sha256-raw-mp:1")
RUST_LOG = "info,aws_smithy_runtime::client::retries=debug"
KEY_MESSAGES = ("batch failed", "batch did not reach VERIFIED", "recovered: REPLAY_REQUIRED",
                "commit failed after verify", "source commit still failing", "process cycle error",
                "shutdown flush incomplete", "shutdown flush: batch(es) did not verify", "recovery_scan")


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parse_plan(text: str) -> list[tuple[str, str, int]]:
    cells = []
    for block in filter(None, (b.strip() for b in text.split(";"))):
        conds, modes, reps = block.split(":")
        for c in conds.split(","):
            assert c in COND, f"unknown condition {c}"
            for m in modes.split(","):
                assert m in MODES, f"unknown mode {m}"
                cells += [(c, m, r) for r in range(int(reps))]
    return cells


# ------------------------------------------------------------------ network: counters, RTT, tc
def nic_bytes(dev):
    try:
        for line in Path("/proc/net/dev").read_text().splitlines():
            if line.strip().startswith(dev + ":"):
                f = line.split(":", 1)[1].split()
                return int(f[0]), int(f[8])
    except (OSError, TypeError):
        pass
    return None


def endpoints(bucket: str) -> list[tuple[str, int]]:
    if LAB:
        u = urllib.parse.urlparse(LAB)
        return [(u.hostname, u.port or 80)]
    # the engine (aws-sdk-rust, region us-east-1) uses the regional name; boto3 uses the global one
    return [(f"{bucket}.s3.us-east-1.amazonaws.com", 443), (f"{bucket}.s3.amazonaws.com", 443)]


def tcp_rtt(host: str, port: int, n: int) -> dict:
    """TCP handshake times (one RTT each) on fresh connections; resolved addresses recorded."""
    out = {"host": host, "port": port, "n": n}
    try:
        addrs = sorted({a[4][0] for a in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)})
    except OSError as e:
        return {**out, "error": repr(e)}
    out["addresses"] = addrs
    xs = []
    for i in range(n):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        t0 = time.perf_counter()
        try:
            s.connect((addrs[i % len(addrs)], port))
            xs.append((time.perf_counter() - t0) * 1000)
        except OSError:
            pass
        finally:
            s.close()
    if xs:
        xs.sort()
        out.update({"ok": len(xs), "min_ms": round(xs[0], 2), "median_ms": round(xs[len(xs) // 2], 2),
                    "p90_ms": round(xs[min(len(xs) - 1, int(0.9 * len(xs)))], 2), "max_ms": round(xs[-1], 2)})
    return out


def rtt_probe(bucket: str, n: int = 20) -> dict:
    res = {"at": utc(), "tcp_connect": [tcp_rtt(h, p, n) for h, p in endpoints(bucket)]}
    try:  # HTTPS request round trips on one kept-alive connection (one RTT plus service time each)
        s3 = cost.s3_client()
        for _ in range(3):
            s3.head_bucket(Bucket=bucket)
        xs = []
        for _ in range(n):
            t0 = time.perf_counter()
            s3.head_bucket(Bucket=bucket)
            xs.append((time.perf_counter() - t0) * 1000)
        xs.sort()
        res["head_bucket"] = {"n": n, "min_ms": round(xs[0], 2), "median_ms": round(xs[n // 2], 2),
                              "max_ms": round(xs[-1], 2)}
    except Exception as e:  # noqa: BLE001
        res["head_bucket"] = {"error": f"{type(e).__name__}: {e}"}
    return res


class Net:
    """tc on the default-route NIC. Every command and its result is recorded; clear() always restores."""

    def __init__(self, dev, exempt, drop_cidrs):
        self.dev, self.exempt, self.drop_cidrs = dev, exempt, drop_cidrs
        self.log: list[dict] = []
        self.offload_saved: dict[str, str] = {}
        self.caps: dict[str, bool] = {}

    def run(self, *args) -> bool:
        r = cost.sh(*args, timeout=30)
        self.log.append({"cmd": " ".join(str(a) for a in args), "rc": r["rc"], "err": (r["stderr"] or "")[-300:]})
        return r["rc"] == 0

    def tc(self, *args) -> bool:
        return self.run("tc", *args)

    def take_log(self) -> list[dict]:
        out, self.log = self.log, []
        return out

    def clear(self):
        d = self.dev
        for args in (("tc", "qdisc", "del", "dev", d, "root"), ("tc", "qdisc", "del", "dev", d, "clsact"),
                     ("ip", "link", "del", "ifb0")):
            cost.sh(*args, timeout=30)  # absent is fine; not logged as a failure
        self.offloads_restore()

    # TSO/GSO/GRO hand tc 64 KB super-packets: a 5,000-byte policer bucket would drop all of them and netem
    # would lose whole bursts, so offloads are off while any kernel condition is applied (restored after).
    def offloads_off(self):
        if not shutil.which("ethtool"):
            return
        r = cost.sh("ethtool", "-k", self.dev, timeout=10)
        names = {"tcp-segmentation-offload": "tso", "generic-segmentation-offload": "gso",
                 "generic-receive-offload": "gro"}
        for line in (r["stdout"] or "").splitlines():
            k, _, v = line.strip().partition(":")
            if k in names and "[fixed]" not in v:
                self.offload_saved[names[k]] = v.strip().split()[0]
        for short in self.offload_saved:
            self.run("ethtool", "-K", self.dev, short, "off")

    def offloads_restore(self):
        for short, val in self.offload_saved.items():
            cost.sh("ethtool", "-K", self.dev, short, val, timeout=10)
        self.offload_saved = {}

    def preflight(self) -> dict:
        """Try each primitive once on the real NIC; conditions needing a missing one are skipped."""
        d, c = self.dev, {}
        self.clear()
        c["tc_binary"] = bool(shutil.which("tc"))
        c["clsact"] = self.tc("qdisc", "add", "dev", d, "clsact")
        c["u32_drop"] = c["clsact"] and self.tc("filter", "add", "dev", d, "egress", "prio", "7", "protocol", "ip",
                                                "u32", "match", "ip", "dst", "192.0.2.0/24", "action", "drop")
        c["police"] = c["clsact"] and self.tc("filter", "add", "dev", d, "egress", "prio", "8", "protocol", "ip",
                                              "u32", "match", "ip", "dst", "198.51.100.0/24", "action", "police",
                                              "rate", "2mbit", "burst", "5000", "conform-exceed", "drop")
        c["ifb"] = self.run("ip", "link", "add", "ifb0", "type", "ifb")
        c["mirred"] = c["ifb"] and c["clsact"] and self.tc(
            "filter", "add", "dev", d, "ingress", "prio", "9", "protocol", "ip", "u32", "match", "ip", "src",
            "203.0.113.0/24", "action", "mirred", "egress", "redirect", "dev", "ifb0")
        c["tbf"] = self.tc("qdisc", "add", "dev", d, "root", "tbf", "rate", "100mbit", "burst", "15140",
                           "latency", "100ms")
        self.tc("qdisc", "del", "dev", d, "root")
        c["netem"] = self.tc("qdisc", "add", "dev", d, "root", "netem", "loss", "0%", "limit", "10000")
        c["ethtool"] = bool(shutil.which("ethtool"))
        self.clear()
        c["ipv6_default_route"] = bool((cost.sh("ip", "-6", "route", "show", "default")["stdout"] or "").strip())
        self.caps = c
        return {"caps": c, "commands": self.take_log()}

    def missing(self, cond: dict) -> list[str]:
        c, need = self.caps, []
        if "rate" in cond:
            need += ["tbf", "clsact"] + (["mirred"] if c.get("ifb") else ["police"])
        if "netem" in cond:
            need.append("netem")
        if "police" in cond:
            need += ["clsact", "police"]
        if "partition_s" in cond:
            need += ["clsact", "u32_drop"]
        return [n for n in need if not c.get(n)]

    def shape(self, cond: dict) -> dict:
        """Apply rate/netem/police of a condition (not the partition). Returns the method record."""
        d, rec = self.dev, {}
        kernel = any(k in cond for k in ("rate", "netem", "police", "partition_s"))
        if not kernel:
            return {"kernel": False}
        self.offloads_off()
        ok = True
        if "rate" in cond or "police" in cond:
            ok &= self.tc("qdisc", "add", "dev", d, "clsact")
            ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "1", "protocol", "ip", "u32", "match",
                          "ip", "dst", LINK_LOCAL, "action", "ok")
            ok &= self.tc("filter", "add", "dev", d, "ingress", "prio", "1", "protocol", "ip", "u32", "match",
                          "ip", "src", LINK_LOCAL, "action", "ok")
        if "rate" in cond:
            r = f"{cond['rate']}mbit"
            ok &= self.tc("qdisc", "add", "dev", d, "root", "tbf", "rate", r, "burst", "15140", "latency", "100ms")
            if self.caps.get("ifb"):  # ingress: redirect to ifb0 and shape there like egress
                ok &= self.run("ip", "link", "add", "ifb0", "type", "ifb") and self.run("ip", "link", "set",
                                                                                        "ifb0", "up")
                ok &= self.tc("qdisc", "add", "dev", "ifb0", "root", "tbf", "rate", r, "burst", "15140",
                              "latency", "100ms")
                ok &= self.tc("filter", "add", "dev", d, "ingress", "prio", "9", "protocol", "ip", "u32", "match",
                              "u32", "0", "0", "action", "mirred", "egress", "redirect", "dev", "ifb0")
                rec["ingress"] = "ifb0 tbf"
            else:  # fallback, recorded: police ingress with a 100 ms bucket (a policer, not a queue)
                burst = str(max(15140, int(cond["rate"] * 1e6 / 8 * 0.1)))
                ok &= self.tc("filter", "add", "dev", d, "ingress", "prio", "9", "protocol", "ip", "u32", "match",
                              "u32", "0", "0", "action", "police", "rate", r, "burst", burst, "conform-exceed",
                              "drop")
                rec["ingress"] = f"police {r} burst {burst} (no ifb on this kernel)"
            rec["egress"] = f"tbf rate {r} burst 15140 latency 100ms"
        if "netem" in cond:
            ok &= self.tc("qdisc", "add", "dev", d, "root", "netem", *cond["netem"].split(), "limit", "10000")
            rec["egress"] = f"netem {cond['netem']} (egress only)"
        if "police" in cond:
            r = f"{cond['police']}mbit"
            ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "9", "protocol", "ip", "u32", "match", "u32",
                          "0", "0", "action", "police", "rate", r, "burst", str(cond["burst"]), "conform-exceed",
                          "drop")
            rec["egress"] = f"police {r} burst {cond['burst']} B, drop (no queue)"
        rec.update({"kernel": True, "ok": bool(ok), "offloads_off": dict(self.offload_saved)})
        return rec

    def partition(self, on: bool) -> bool:
        d = self.dev
        if not on:
            return self.tc("qdisc", "del", "dev", d, "clsact")
        ok = self.tc("qdisc", "add", "dev", d, "clsact")
        ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "1", "protocol", "ip", "u32", "match", "ip",
                      "dst", LINK_LOCAL, "action", "ok")
        if self.drop_cidrs:  # explicit targets (lab: the MinIO address)
            for cidr in self.drop_cidrs:
                ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "3", "protocol", "ip", "u32", "match",
                              "ip", "dst", cidr, "action", "drop")
        else:  # EC2: everything but link-local and the VPC (DNS resolver) is cut, so S3 is unreachable
            for cidr in self.exempt:
                ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "2", "protocol", "ip", "u32", "match",
                              "ip", "dst", cidr, "action", "ok")
            ok &= self.tc("filter", "add", "dev", d, "egress", "prio", "3", "protocol", "ip", "u32", "match",
                          "u32", "0", "0", "action", "drop")
        return bool(ok)

    def stats(self) -> str:
        parts = []
        for args in (("-s", "qdisc", "show", "dev", self.dev), ("-s", "qdisc", "show", "dev", "ifb0"),
                     ("-s", "filter", "show", "dev", self.dev, "egress"),
                     ("-s", "filter", "show", "dev", self.dev, "ingress")):
            r = cost.sh("tc", *args, timeout=10)
            if r["rc"] == 0 and (r["stdout"] or "").strip():
                parts.append(f"$ tc {' '.join(args)}\n{r['stdout'][-3000:]}")
        return "\n".join(parts)


# ------------------------------------------------------------------ engine config and evidence
def config_text(mode, cond, root: Path, src_dir: Path, bucket: str, prefix: str) -> str:
    algo, codec, strong, threshold = MODES[mode]
    up = [f"  require_strong_verification: {str(strong).lower()}"]
    if threshold:
        up += [f"  multipart_threshold_bytes: {threshold}", f"  multipart_part_size_bytes: {8 * MiB}"]
    if "cap" in cond:
        up.append(f"  max_egress_bytes_per_second: {cond['cap']}")
    if "request_timeout" in cond:
        up.append(f"  request_timeout_seconds: {cond['request_timeout']}")
    if "request_min_bps" in cond:
        up.append(f"  request_min_bytes_per_second: {cond['request_min_bps']}")
    if LAB:
        up += [f"  endpoint_url: {LAB}", "  force_path_style: true", "  verify_tls: false"]
    return f"""# generated by research/experiments/rq5-cloud/wan/wan.py ({mode})
engine:
  name: wan
  tenant: wan
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
""" + "\n".join(up) + "\n"


def _ts(v):
    if not v:
        return None
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def ledger_times(db: Path) -> dict:
    """Per-batch seal-to-commit seconds, row states, failure messages, rows sharing an object."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(batches)")}
        want = [c for c in ("state", "created_at", "updated_at", "sealed_at", "error_message", "object_uri")
                if c in cols]
        rows = [dict(zip(want, r)) for r in con.execute(f"SELECT {', '.join(want)} FROM batches")]
    finally:
        con.close()
    s2c, errors = [], collections.Counter()
    uris = collections.Counter()
    for r in rows:
        if r["state"] == "source_committed":
            a, b = _ts(r.get("sealed_at") or r.get("created_at")), _ts(r.get("updated_at"))
            if a and b:
                s2c.append(round(b - a, 3))
            if r.get("object_uri"):
                uris[r["object_uri"]] += 1
        elif r.get("error_message"):
            errors[str(r["error_message"])[:200]] += 1
    return {"rows": len(rows), "seal_to_commit_s": sorted(s2c), "seal_to_commit_max_s": max(s2c, default=None),
            "non_committed_errors": errors.most_common(8),
            "committed_rows_sharing_an_object": sum(n - 1 for n in uris.values() if n > 1)}


def log_extras(paths: list[Path]) -> dict:
    """Failure, retry and throttle evidence. SDK retry decisions appear only with RUST_LOG (debug)."""
    key, events, warn = collections.Counter(), collections.Counter(), collections.Counter()
    retry_lines, retry_msgs, attempts = 0, collections.Counter(), collections.Counter()
    for p in paths:
        if not p.exists():
            continue
        for line in p.read_text(errors="replace").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            f = rec.get("fields", {})
            msg = str(f.get("message", ""))
            for k in KEY_MESSAGES:
                if msg.startswith(k):
                    key[k] += 1
            if "event" in f:
                events[str(f["event"])] += 1
            if rec.get("level") in ("WARN", "ERROR"):
                warn[f"{rec.get('level')}: {msg[:90]}"] += 1
            if "retries" in str(rec.get("target", "")):
                # aws-smithy-runtime logs "attempt #N classified as X, not retrying|..." once per attempt
                retry_lines += 1
                retry_msgs[msg[:90]] += 1
                if msg.startswith("attempt #"):
                    n = msg[len("attempt #"):].split(" ", 1)[0]
                    attempts["first" if n == "1" else "retry"] += 1
                    if not msg.rstrip().endswith("not retrying"):
                        attempts["retry_decided"] += 1
    return {"key_messages": dict(key), "events": dict(events), "warn_error": warn.most_common(12),
            "sdk_retry_lines": retry_lines, "sdk_retry_messages": retry_msgs.most_common(8),
            "sdk_attempts_first": attempts["first"], "sdk_retry_attempts": attempts["retry"],
            "sdk_retry_decisions": attempts["retry_decided"],
            "upload_throttled": events.get("upload_throttled", 0)}


def abort_multipart(s3, bucket: str, prefix: str) -> int:
    n = 0
    try:
        for page in s3.get_paginator("list_multipart_uploads").paginate(Bucket=bucket, Prefix=prefix):
            for u in page.get("Uploads", []):
                s3.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
                n += 1
    except Exception as e:  # noqa: BLE001
        print(f"multipart abort under {prefix} failed: {e}", flush=True)
        return -1
    return n


def first_after(series, t, idx, min_inc):
    """Seconds after t of the first sample whose field idx exceeds its value at t by more than min_inc."""
    base = None
    for row in series:
        if row[0] <= t:
            base = row[idx]
            continue
        if base is not None and row[idx] is not None and row[idx] - base > min_inc:
            return round(row[0] - t, 3)
    return None


# ------------------------------------------------------------------ one run
def run_cell(k, cond_name, mode, rep, a, env, s3, net: Net | None, dev) -> dict:
    cond = COND[cond_name]
    ds = cond.get("dataset", "ssh")
    cell = f"{cond_name}-{mode}-r{rep}"
    root = Path(a.work) / f"c{k}"           # short: object names embed the source path
    shutil.rmtree(root, ignore_errors=True)
    for d in ("state", "work", "logs"):
        (root / d).mkdir(parents=True)
    os.chmod(root / "state", 0o700)
    src_dir = Path(a.input_dir) / ds
    src = src_dir / f"{ds}.log"
    size = src.stat().st_size
    prefix = f"{a.stage}/data/{a.run_id}/{cell}/"
    cfg = root / "config.yaml"
    cfg.write_text(config_text(mode, cond, root, src_dir, a.bucket, prefix))
    expect = cond.get("expect", "complete")
    rec = {"cell": cell, "condition": cond_name, "mode": mode, "rep": rep, "dataset": ds, "bytes": size,
           "prefix": prefix, "expect": expect, "started_at": utc(), "condition_spec": cond}
    v = cost.sh(a.vtopctl, "--json", "validate", "--config", cfg, "--print-effective", env=env, timeout=120)
    rec["validate_rc"] = v["rc"]
    try:
        eff = json.loads(v["stdout"])["effective"]["default"]
        rec["planned_method"] = eff.get("verification_method", {}).get("value")
        rec["effective"] = {kk: eff.get(kk, {}).get("value") for kk in (
            "batching.max_bytes", "resolved_upload_width", "upload.require_strong_verification")}
    except (ValueError, KeyError, TypeError, AttributeError):
        rec["validate_output"] = (v["stdout"] or "")[-3000:] + (v["stderr"] or "")[-2000:]
    rec["rtt_before"] = tcp_rtt(*endpoints(a.bucket)[0], n=5)
    rec["warm_read"] = cost.warm(src, dev, None)
    db, logs = root / "state" / "vtop.db", [root / "logs" / "engine.jsonl"]
    lab_cap = {"cap": a.lab_egress_cap} if a.lab_egress_cap and "cap" not in cond else {}
    if lab_cap:  # lab only: stretch runs so triggers land mid-upload on a loopback-fast link
        cfg.write_text(config_text(mode, {**cond, **lab_cap}, root, src_dir, a.bucket, prefix))
        rec["lab_egress_cap"] = a.lab_egress_cap
    shaping = net.shape(cond) if net else {"kernel": False}
    rec["shaping"] = shaping
    c0 = cost.counters(dev, None)
    n0 = nic_bytes(dev)
    t0 = time.perf_counter()
    timeout = cond.get("timeout", a.timeout)
    series, last, ev = [], 0, {}
    ru_total = {"cpu_s": 0.0, "rss": 0}
    procs = 0

    def start(log):
        nonlocal procs
        procs += 1
        fh = open(log, "ab")
        return subprocess.Popen([str(a.vtopctl), "run", "--config", str(cfg)], stdout=fh, stderr=subprocess.STDOUT,
                                env=env, umask=0o077), fh

    def account(ru):
        if ru is not None:
            ru_total["cpu_s"] += ru.ru_utime + ru.ru_stime
            ru_total["rss"] = max(ru_total["rss"], ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024))

    p, fh = start(logs[0])
    next_sample, drained_at = 0.0, None
    trigger_tx = cond.get("trigger_tx")
    try:
        while True:
            now = time.perf_counter() - t0
            c = cost.committed_end(db)
            if c != last:
                ev.setdefault("commits", []).append([round(now, 4), c])
                last = c
            nb = nic_bytes(dev)
            if now >= next_sample:
                series.append([round(now, 2), c, (nb[1] - n0[1]) if nb and n0 else None,
                               (nb[0] - n0[0]) if nb and n0 else None])
                next_sample = now + SAMPLE_S
            if c >= size and drained_at is None:
                drained_at = now
            # trigger for partition/kill: uploads under way (NIC tx), or a fallback time without NIC counters
            fire = trigger_tx and "trigger_at" not in ev and (
                (nb and n0 and nb[1] - n0[1] >= trigger_tx) or (not (nb and n0) and now >= a.trigger_fallback_s))
            if fire:
                ev["trigger_at"], ev["committed_at_trigger"] = round(now, 3), c
                ev["tx_at_trigger"] = (nb[1] - n0[1]) if nb and n0 else None
                if "partition_s" in cond:
                    ev["partition_on_ok"] = net.partition(True)
                    ev["partition_until"] = now + cond["partition_s"]
                elif cond.get("kill"):
                    p.send_signal(signal.SIGKILL)
                    _, st, ru = os.wait4(p.pid, 0)
                    fh.close()
                    account(ru)
                    ev["killed_exit_status"] = st
                    logs.append(root / "logs" / "engine-restart.jsonl")
                    ev["restart_at"] = round(time.perf_counter() - t0, 3)
                    p, fh = start(logs[1])
            if "partition_until" in ev and "partition_off_at" not in ev and now >= ev["partition_until"]:
                ev["partition_off_ok"] = net.partition(False)
                ev["partition_off_at"] = round(now, 3)
                ev["committed_at_restore"] = c
            if drained_at is not None and "partition_until" in ev and "partition_off_at" not in ev:
                pass  # drained during the partition: keep the cut until its end, as planned
            elif drained_at is not None:
                break
            if p.poll() is not None or now > timeout:
                break
            time.sleep(POLL_S)
        wall = time.perf_counter() - t0
        exited_early = p.poll() is not None
        if not exited_early:
            p.send_signal(signal.SIGTERM)
            status, ru, killed = cost.reap(p.pid, 120)
        else:
            status, ru, killed = p.returncode, None, False
        fh.close()
        account(ru)
    finally:
        if net:
            rec["tc_stats"] = net.stats()[-6000:]
            net.clear()
            rec["tc_commands"] = net.take_log()
    rec["sys"] = cost.delta(c0, cost.counters(dev, None))
    final = cost.committed_end(db)
    drained = drained_at is not None
    commits = ev.get("commits", [])
    steady = commits[-2] if len(commits) >= 2 else (commits[-1] if commits else [wall, 0])
    span = drained_at if drained else wall
    # wire rates from the sampled NIC bytes at drain (or stop) time; rec["sys"] also counts the shutdown/reap
    at = [s for s in series if s[0] <= span and s[2] is not None]
    tx, rx = (at[-1][2], at[-1][3]) if at else (None, None)
    rec.update({
        "drained": drained, "drain_s": round(drained_at, 4) if drained else None,
        "committed_bytes_final": final, "engine_exited_before_drain": exited_early and not drained,
        "killed_after_sigterm": killed, "exit_status": status, "engine_starts": procs,
        "wall_s": round(wall, 4), "cpu_s": round(ru_total["cpu_s"], 4),
        "max_rss_mib": round(ru_total["rss"] / MiB, 1) if ru_total["rss"] else None,
        "mib_per_s_end_to_end": round(size / MiB / span, 3) if drained else None,
        "mib_per_s": round(steady[1] / MiB / steady[0], 3) if steady[0] else None,
        "wire_tx_mbit_s": round(tx * 8 / 1e6 / span, 3) if tx is not None else None,
        "wire_rx_mbit_s": round(rx * 8 / 1e6 / span, 3) if rx is not None else None,
        "events": ev, "series": series,
    })
    if "partition_off_at" in ev:  # recovery after the cut ends
        off = ev["partition_off_at"]
        rec["partition"] = {
            "mid_run": ev.get("committed_at_trigger", size) < size and ev["committed_at_restore"] < size,
            "commits_during": sum(1 for t, _ in commits if ev["trigger_at"] < t <= off),
            "first_commit_after_restore_s": next((round(t - off, 3) for t, _ in commits if t > off), None),
            "tx_resumed_after_restore_s": first_after(series, off, 2, 16 * KiB),
            "drain_after_restore_s": round(drained_at - off, 3) if drained else None,
        }
    if "police" in cond:  # #663: did a request window outlast the pre-#663 fixed 300 s deadline?
        lt = ledger_times(db) if db.exists() else {}
        rec["old_deadline_exceeded"] = (lt.get("seal_to_commit_max_s") or 0) > 300
    if "restart_at" in ev:
        rec["kill"] = {"mid_run": ev["committed_at_trigger"] < size,
                       "restart_drain_s": round(drained_at - ev["restart_at"], 3) if drained else None}
    rec["ledger"] = cost.ledger_evidence(db) if db.exists() else {}
    rec["ledger_times"] = ledger_times(db) if db.exists() else {}
    joined = root / "logs" / "all.jsonl"
    with open(joined, "wb") as fo:
        for lp in logs:
            if lp.exists():
                fo.write(lp.read_bytes())
    rec["engine_log"] = ls = cost.log_summary(joined)
    rec["engine_log_wan"] = log_extras(logs)
    out = Path(a.out)
    for d in ("ledger", "logs", "oracle"):
        (out / d).mkdir(exist_ok=True)
    if db.exists():
        dst = sqlite3.connect(out / "ledger" / f"{cell}.db")
        srccon = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        srccon.backup(dst)
        srccon.close()
        dst.close()
    with open(joined, "rb") as fi, gzip.open(out / "logs" / f"{cell}.jsonl.gz", "wb") as fo:
        shutil.copyfileobj(fi, fo)
    rec["multipart_uploads_aborted"] = abort_multipart(s3, a.bucket, prefix)
    t1 = time.perf_counter()
    omode = ORACLE_MODE.get(mode, mode)
    o = oracle.check(str(db), str(src), a.bucket, prefix, omode, s3=s3) if db.exists() else {}
    rec["oracle_s"] = round(time.perf_counter() - t1, 3)
    with gzip.open(out / "oracle" / f"{cell}.json.gz", "wt") as fo:
        json.dump(o, fo, default=str)
    rec["oracle"] = cost.oracle_summary(o)
    lg = rec["ledger"]
    safe = (o.get("unsafe_commits") == 0 and o.get("bad_objects") == 0) if o else final == 0
    checks = {"no_unsafe_commit": safe}
    if net and shaping.get("kernel"):
        checks["shaping_applied"] = bool(shaping.get("ok"))
    if "cap" in cond:  # validate --print-effective does not list the cap: accepted config + measured bound
        ob = lg.get("object_bytes_total") or 0
        rec["upload_body_rate_bps"] = round(ob / span, 1) if span else None
        checks["cap_accepted"] = rec["validate_rc"] == 0
        checks["cap_respected"] = bool(span) and ob / span <= cond["cap"] * 1.05
    if expect == "no_commit":
        checks["no_commit_as_expected"] = final < size
    else:
        seen = set(ls["object_checks"])
        checks.update({
            "drained": drained, "oracle_pass": bool(o.get("pass")),
            "ledger_class_as_expected": lg.get("ledger_classes") == [LEDGER_CLASS[mode]],
            "receipts_agree": o.get("receipt_methods") == o.get("inferred_methods") and bool(o.get("receipt_methods")),
        })
        if mode in cost.LOG_EXPECT:
            checks["log_as_expected"] = seen == {cost.LOG_EXPECT[mode]}
        if MODES[mode][3]:
            checks["single_put"] = o.get("multipart_objects") == 0
        else:
            checks["multipart_seen"] = (o.get("multipart_objects") or 0) > 0
        if "partition_s" in cond:
            checks["partition_mid_run"] = bool(rec.get("partition", {}).get("mid_run"))
            checks["partition_toggled"] = bool(ev.get("partition_on_ok") and ev.get("partition_off_ok"))
        if cond.get("kill"):
            checks["kill_mid_run"] = bool(rec.get("kill", {}).get("mid_run"))
    rec["log_object_checks"] = sorted(ls["object_checks"])
    rec["checks"] = checks
    rec["pass"] = all(checks.values())
    rec["finished_at"] = utc()
    shutil.rmtree(root, ignore_errors=True)
    return rec


# ------------------------------------------------------------------ calibration and metadata
def calibrate(name, a, s3, net: Net | None) -> dict:
    """Timed PUT and GET of one random object through the condition's kernel shaping (fresh client)."""
    cond = COND[name]
    n = 1 * MiB if ("police" in cond or "netem" in cond) else 2 * MiB
    key = f"{a.stage}/data/{a.run_id}/probe/{name}"
    body = os.urandom(n)
    rec = {"bytes": n}
    shaping = net.shape(cond) if net else {"kernel": False}
    rec["shaping"] = shaping
    try:
        c = cost.s3_client()
        c.head_bucket(Bucket=a.bucket)  # connection and credentials outside the timing
        t0 = time.perf_counter()
        c.put_object(Bucket=a.bucket, Key=key, Body=body)
        t1 = time.perf_counter()
        got = c.get_object(Bucket=a.bucket, Key=key)["Body"].read()
        t2 = time.perf_counter()
        rec.update({"put_s": round(t1 - t0, 3), "get_s": round(t2 - t1, 3), "get_ok": got == body,
                    "put_mbit_s": round(n * 8 / 1e6 / (t1 - t0), 3), "get_mbit_s": round(n * 8 / 1e6 / (t2 - t1), 3)})
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"
    finally:
        if net:
            rec["tc_stats"] = net.stats()[-3000:]
            net.clear()
            rec["tc_commands"] = net.take_log()
    return rec


def run_info(a, cells, dev, preflight) -> dict:
    import boto3
    import botocore
    ident = json.loads(cost.imds("dynamic/instance-identity/document") or "{}")
    return {
        "experiment": "E-RQ5-WAN: VTOP over a real long-distance link to Amazon S3 us-east-1", "stage": a.stage,
        "run_id": a.run_id, "started_at": utc(), "instance_type": ident.get("instanceType"),
        "ami": ident.get("imageId"), "region": ident.get("region"), "availability_zone": ident.get("availabilityZone"),
        "instance_id": ident.get("instanceId"), "kernel": os.uname().release,
        "os_release": cost._read("/etc/os-release"), "cpu_count": os.cpu_count(), "nic": dev,
        "ldd_version": (cost.sh("ldd", "--version")["stdout"] or "").splitlines()[:1], "lab_endpoint": LAB,
        "lab_egress_cap": a.lab_egress_cap, "bucket": a.bucket, "bucket_region": "us-east-1",
        "vtop": {"version": a.vtop_version, "commit": a.vtop_commit, "tarball_sha256": a.tarball_sha256,
                 "version_output": (cost.sh(a.vtopctl, "--version")["stdout"] or "").strip(),
                 "vtopctl_sha256": cost.sha256_file(Path(a.vtopctl)), "sigstore_bundle": a.cosign_status},
        "aws_sdk_env": {k: os.environ.get(k) for k in cost.SDK_ENV}, "rust_log": a.rust_log,
        "oracle_python": {"boto3": boto3.__version__, "botocore": botocore.__version__,
                          "oracle_sha256": cost.sha256_file(Path(oracle.__file__)),
                          "cost_py_sha256": cost.sha256_file(Path(cost.__file__))},
        "code_sha256": a.code_sha256,
        "inputs": {ds: {"path": str(Path(a.input_dir) / ds / f"{ds}.log"),
                        "bytes": (Path(a.input_dir) / ds / f"{ds}.log").stat().st_size,
                        "sha256": cost.sha256_file(Path(a.input_dir) / ds / f"{ds}.log")}
                   for ds in sorted({COND[c].get("dataset", "ssh") for c, _, _ in cells})},
        "plan": a.plan, "seed": a.seed, "order": [f"{c}-{m}-r{r}" for c, m, r in cells],
        "conditions": COND, "modes": {m: dict(zip(("checksum.algorithm", "compression.type",
                                                   "upload.require_strong_verification",
                                                   "upload.multipart_threshold_bytes"), v)) for m, v in MODES.items()},
        "config": {"batching.max_bytes": MAX_BYTES, "batching.max_concurrent_batches": CONCURRENCY,
                   "batching.max_batch_age_seconds": 2, "manifest.verification_receipts": True},
        "network": {"preflight": preflight, "exempt_from_partition": a.exempt, "partition_drop_cidrs": a.partition_cidrs,
                    "never_shaped": LINK_LOCAL},
        "measurement": {"poll_s": POLL_S, "sample_s": SAMPLE_S, "engine_timeout_s": a.timeout,
                        "throughput": "input MiB / time until the ledger shows the last byte committed "
                                      "(end-to-end); steady as cost.py also recorded; wire = NIC bytes / same time"},
    }


def make_slice(src: Path, dst: Path, nbytes: int):
    """First nbytes of src, cut after the last newline (VTOP never commits a partial trailing line)."""
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as fh:
        data = fh.read(nbytes)
    dst.write_bytes(data[: data.rfind(b"\n") + 1])


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for k in ("run-id", "vtopctl", "input-dir", "bucket", "out", "work", "vtop-version", "vtop-commit",
              "tarball-sha256"):
        ap.add_argument(f"--{k}", required=True)
    ap.add_argument("--stage", default="wan")
    ap.add_argument("--plan", default=DEFAULT_PLAN, help="cond[,cond]:mode[,mode]:reps;...")
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--timeout", type=float, default=900, help="per-run engine limit (s) unless the condition sets one")
    ap.add_argument("--deadline-epoch", type=float, default=None, help="start no run after this time")
    ap.add_argument("--hard-stop-epoch", type=float, default=None,
                    help="the VM's failsafe: start no run whose timeout (+5 min) would reach it")
    ap.add_argument("--max-failures", type=int, default=8)
    ap.add_argument("--results-uri", default=None)
    ap.add_argument("--delete-data", action="store_true")
    ap.add_argument("--cosign-status", default="not checked")
    ap.add_argument("--code-sha256", default="")
    ap.add_argument("--rust-log", default=RUST_LOG)
    ap.add_argument("--exempt", default="", help="CIDRs kept reachable during the partition (the VPC: DNS)")
    ap.add_argument("--partition-cidrs", default="", help="lab: drop only these CIDRs (else all but exempt)")
    ap.add_argument("--no-tc", action="store_true", help="skip kernel conditions (macOS dry run)")
    ap.add_argument("--no-calibrate", action="store_true")
    ap.add_argument("--trigger-fallback-s", type=float, default=3.0)
    ap.add_argument("--lab-egress-cap", type=int, default=0, help="lab only: VTOP egress cap on every run")
    a = ap.parse_args()
    a.exempt = [x for x in a.exempt.split(",") if x]
    a.partition_cidrs = [x for x in a.partition_cidrs.split(",") if x]
    assert not (a.lab_egress_cap and not LAB), "--lab-egress-cap is for lab runs only"
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    Path(a.work).mkdir(parents=True, exist_ok=True)
    make_slice(Path(a.input_dir) / "ssh" / "ssh.log", Path(a.input_dir) / "ssh32" / "ssh32.log", 32 * MiB)
    env = {k: v for k, v in os.environ.items() if k not in cost.STRIP_ENV}
    env["VTOP_LOG_FORMAT"] = "json"
    if a.rust_log:
        env["RUST_LOG"] = a.rust_log
    dev = cost.iface()
    s3 = cost.s3_client()
    net = None
    preflight = {"skipped": "--no-tc or not Linux"}
    if not a.no_tc and sys.platform.startswith("linux") and dev and os.geteuid() == 0:
        net = Net(dev, a.exempt, a.partition_cidrs)
        preflight = net.preflight()
    cells = parse_plan(a.plan)
    random.Random(a.seed).shuffle(cells)
    info = run_info(a, cells, dev, preflight)
    info["rtt_start"] = rtt_probe(a.bucket)
    print(json.dumps({"rtt_start": info["rtt_start"], "preflight": preflight.get("caps")}), flush=True)
    sent: dict = {}
    (out / "run.json").write_text(json.dumps(info, indent=1, default=str))
    cost.sync_results(out, s3, a.results_uri, sent)
    if not a.no_calibrate:
        info["calibration"] = {}
        shaped = {c for c, _, _ in cells if any(x in COND[c] for x in ("rate", "netem", "police"))}
        for name in ["native"] + sorted(shaped - {"policed-fixed"}):  # same link as "policed"
            if net and net.missing(COND[name]):
                continue
            if not net and name != "native":
                continue
            info["calibration"][name] = calibrate(name, a, s3, net)
            print(f"calibration {name}: {json.dumps({k: v for k, v in info['calibration'][name].items() if k.endswith(('_s', 'mbit_s', 'error'))})}", flush=True)
        (out / "run.json").write_text(json.dumps(info, indent=1, default=str))
    failures, unsafe, stopped, done, skipped = 0, 0, None, 0, []
    with open(out / "runs.jsonl", "a") as fh:
        for k, (cname, mode, rep) in enumerate(cells, 1):
            if a.deadline_epoch and time.time() > a.deadline_epoch:
                stopped = f"deadline reached before run {k}/{len(cells)}"
                break
            if a.hard_stop_epoch and time.time() + COND[cname].get("timeout", a.timeout) + 300 > a.hard_stop_epoch:
                skipped.append({"cell": f"{cname}-{mode}-r{rep}", "missing": ["would not finish before the failsafe"]})
                continue
            needs_net = any(x in COND[cname] for x in ("rate", "netem", "police", "partition_s"))
            miss = (["tc unavailable"] if not net else net.missing(COND[cname])) if needs_net else []
            if miss:
                skipped.append({"cell": f"{cname}-{mode}-r{rep}", "missing": miss})
                continue
            try:
                rec = run_cell(k, cname, mode, rep, a, env, s3, net, dev)
            except Exception as e:  # noqa: BLE001
                if net:
                    net.clear()
                rec = {"cell": f"{cname}-{mode}-r{rep}", "condition": cname, "mode": mode, "rep": rep,
                       "pass": False, "error": f"{type(e).__name__}: {e}"}
            rec["order"] = k
            done += 1
            fh.write(json.dumps(rec, default=str) + "\n")
            fh.flush()
            failures += not rec["pass"]
            unsafe += int(not rec.get("checks", {}).get("no_unsafe_commit", True))
            print(f"[{k}/{len(cells)}] {rec['cell']:32s} pass={rec['pass']} drain={rec.get('drain_s')}s "
                  f"e2e={rec.get('mib_per_s_end_to_end')} MiB/s tx={rec.get('wire_tx_mbit_s')} Mbit/s "
                  f"rtt={rec.get('rtt_before', {}).get('median_ms')} ms "
                  f"sdk_retries={rec.get('engine_log_wan', {}).get('sdk_retry_attempts')} "
                  f"failed={[c for c, ok in rec.get('checks', {}).items() if not ok]} {rec.get('error', '')}",
                  flush=True)
            cost.sync_results(out, s3, a.results_uri, sent)
            if unsafe:
                stopped = f"unsafe commit in run {k}: stopped, data kept"
                break
            if failures >= a.max_failures:
                stopped = f"stopped after run {k}: {failures} failed run(s)"
                break
    info["rtt_end"] = rtt_probe(a.bucket)
    info["finished_at"] = utc()
    info.update({"runs_done": done, "runs_planned": len(cells), "failed_runs": failures, "unsafe_runs": unsafe,
                 "stopped": stopped, "skipped": skipped})
    info["multipart_uploads_aborted_at_end"] = abort_multipart(s3, a.bucket, f"{a.stage}/data/{a.run_id}/")
    if failures:
        (out / "KEEP_DATA").write_text(f"{failures} failed run(s); data kept under {a.stage}/data/{a.run_id}/\n")
    elif a.delete_data:
        info["deleted_data_objects"] = cost.delete_prefix(s3, a.bucket, f"{a.stage}/data/{a.run_id}/")
    (out / "run.json").write_text(json.dumps(info, indent=1, default=str))
    cost.sync_results(out, s3, a.results_uri, sent)
    print(json.dumps({"runs": done, "failed": failures, "unsafe": unsafe, "skipped": len(skipped),
                      "stopped": stopped}), flush=True)
    raise SystemExit(1 if failures or unsafe else (3 if stopped or skipped else 0))


if __name__ == "__main__":
    main()
