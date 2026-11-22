#!/usr/bin/env python3
"""E-RQ5-PERF (H4.2): VTOP v0.10.2 vs Confluent S3 Sink 12.1.15, Vector 0.61.0 and Fluent Bit 5.2.0 on EC2 + S3.

Two hosts of the same type in one subnet: a Kafka broker host and a sink host. One system at a time runs on the
sink host and archives a Kafka topic to the us-east-1 bucket; the broker host orchestrates, observes and judges.

Subcommands
  agent        (sink host)   HTTP agent: starts/kills/stops the system under test, samples its CPU and memory
                             (cgroup v2), records NIC/disk/ENA/nft counters. Stdlib only at import time.
  orchestrate  (broker host) loads the topics, runs the shuffled schedule, observes source commits (Kafka admin
                             API) and S3 objects (LIST + GET), runs the independent oracle (RQ4's locate()/judge(),
                             imported unchanged from rq4-baselines/code/exp_baseline.py), computes throughput,
                             latency and recovery, cleans each run's S3 prefix, uploads results incrementally.
  produce      (broker host) rate-limited producer for the live (latency) runs; records each record's produce
                             time (subprocess, so the observer threads do not share its GIL).

Run types
  tput   backlog drain of the pre-loaded topic (4 partitions); steady-state committed and archived rates.
  live   fixed-rate production into a fresh 1-partition topic; per-record end-to-end latency
         = max(object first seen in S3, source offset committed) - produce time, all on the broker clock.
  kill   tput with one SIGKILL of the sink at 40% committed, immediate restart; time to resume and to
         re-reach the pre-kill rate; completeness and duplicates.
Every run is judged by the oracle (completeness, unsafe commits, lost committed records, duplicates).

Nothing here modifies a system under test: configs are the RQ4 ones (configs/README.md) with the changes listed
in README.md "Fairness" (16 MiB object target, compression, common 10 s age, tasks.max, cloud endpoints).
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import platform
import random
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
MiB = 2**20

# ------------------------------------------------------------------ pinned systems (RQ4 digests, run.json)
IMAGES = {
    "kafka": "apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed",       # 3.9.1
    "confluent": "confluentinc/cp-kafka-connect@sha256:535b1751f64af95bee4bf15ad2ab6b1ca2b369131711801c93e3ceac836dd2a1",  # 7.9.0
    "vector": "timberio/vector@sha256:f6c23b5a7e16fb2838d33619ef7acf4bad2289a7cf22bc90efc2371f520c4ea2",   # 0.61.0-debian
    # RQ4's digest; the tag fluent/fluent-bit:5.2.0 now resolves to a different index (e2718c47, checked 2026-11-21)
    "fluentbit": "fluent/fluent-bit@sha256:a48190bf67679495a332fd61025421a46e7e4d168380185a8e16da2e9acc3126",
}
IMAGE_TAGS = {"kafka": "apache/kafka:3.9.1", "confluent": "confluentinc/cp-kafka-connect:7.9.0",
              "vector": "timberio/vector:0.61.0-debian", "fluentbit": "fluent/fluent-bit:5.2.0"}
S3SINK = "confluentinc-kafka-connect-s3 12.1.15"
S3SINK_ZIP_SHA256 = "7ba0ea1fbf32251b99a39f47bfcc35a0a49abdeb0a427fbf4866e14df0a32dc7"

# config name -> settings. VTOP rows: (checksum.algorithm, compression.type, level, require_strong_verification)
CONFIGS = {
    "vtop-size-only": {"system": "vtop", "vtop": ("none", "zstd", 3, False)},
    "vtop-sha256-store": {"system": "vtop", "vtop": ("sha256", "zstd", 3, True)},
    "vtop-blake3-readback": {"system": "vtop", "vtop": ("blake3", "zstd", 3, True)},
    "vtop-sha256-gzip6": {"system": "vtop", "vtop": ("sha256", "gzip", 6, True)},   # codec-matched to Confluent
    "confluent": {"system": "confluent", "codec": "gzip (s3.compression.level -1 = zlib default 6)"},
    "vector": {"system": "vector", "codec": "zstd level 3"},
    "fluentbit": {"system": "fluentbit", "codec": "zstd (level 1, fixed in flb_zstd.c)"},
}
# verification_passed object_check messages and ledger classes (cost.py LOG_EXPECT / LEDGER_CLASS)
LOG_EXPECT = {"none": "object present and size matches (checksums disabled)",
              "sha256": "S3 service-computed SHA-256 verified", "blake3": "stored content BLAKE3 verified"}
LEDGER_CLASS = {"none": "size_only", "sha256": "strong", "blake3": "strong"}
OBJECT_BYTES = 16 * MiB          # D6
MULTIPART_THRESHOLD = 64 * MiB   # VTOP: every object single-PUT (cost.py)
PART_SIZE = 8 * MiB
CONCURRENCY = 8


def now() -> float:
    return time.time()


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sh(*args, timeout=120, env=None) -> dict:
    try:
        p = subprocess.run([str(a) for a in args], capture_output=True, text=True, timeout=timeout, env=env)
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return {"rc": None, "stdout": "", "stderr": repr(e)}
    return {"rc": p.returncode, "stdout": p.stdout, "stderr": p.stderr[-4000:]}


# ================================================================== configs (rendered by the orchestrator)
def render(name: str, x: dict) -> dict:
    """x: run, topic, group, bucket, prefix, kafka_native, kafka_container, s3_native, s3_container,
    records_per_object, age_s, tasks. Returns the payload the agent needs to start the system."""
    c = CONFIGS[name]
    sysname = c["system"]
    if sysname == "vtop":
        algo, codec, level, strong = c["vtop"]
        lab = (f"  endpoint_url: {x['s3_native']}\n  force_path_style: true\n  verify_tls: false\n"
               if x["s3_native"] else "")
        text = f"""# generated by rq5-cloud/perf/perf.py ({name}, run {x['run']})
engine:
  name: perf
  tenant: perf
  state_store: "sqlite://@ROOT@/state/vtop.db"
  work_dir: @ROOT@/work
batching:
  max_records: 1000000
  max_bytes: {x['object_bytes']}
  max_batch_age_seconds: {x['age_s']}
  idle_poll_interval_ms: 200
  max_concurrent_batches: {CONCURRENCY}
compression:
  type: {codec}
  level: {level}
checksum:
  algorithm: {algo}
sources:
  kafka:
    enabled: true
    bootstrap_servers: ["{x['kafka_native']}"]
    consumer_group: {x['group']}
    topic_include_regex: "^{x['topic']}$"
    topic_exclude_regex: "^__.*"
    auto_offset_reset: earliest
    enable_auto_commit: false
  file: {{enabled: false, paths: []}}
  syslog_spool: {{enabled: false, paths: []}}
upload:
  backend: s3_native
  region: us-east-1
  bucket: {x['bucket']}
  prefix: "{x['prefix']}"
  create_bucket: false
  require_strong_verification: {str(strong).lower()}
  multipart_threshold_bytes: {MULTIPART_THRESHOLD}
  multipart_part_size_bytes: {PART_SIZE}
""" + lab
        return {"system": "vtop", "files": {"vtop.yaml": text}, "checksum": algo}
    if sysname == "confluent":
        conn = f"perf-{x['run']}"
        cfg = {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector", "tasks.max": str(x["tasks"]),
            "topics": x["topic"], "s3.bucket.name": x["bucket"], "s3.region": "us-east-1",
            "topics.dir": x["prefix"].rstrip("/"),
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
            "format.bytearray.separator": "\n", "s3.part.size": "5242880",
            "s3.compression.type": "gzip",
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            "flush.size": str(x["records_per_object"]), "schema.compatibility": "NONE",
            "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "consumer.override.auto.offset.reset": "earliest", "behavior.on.null.values": "ignore"}
        if x["s3_container"]:
            cfg.update({"store.url": x["s3_container"], "s3.path.style.access.enabled": "true"})
        r = x["run"]
        env = {"CONNECT_BOOTSTRAP_SERVERS": x["kafka_container"], "CONNECT_GROUP_ID": f"perf-w-{r}",
               "CONNECT_CONFIG_STORAGE_TOPIC": f"_perf-{r}-configs",
               "CONNECT_OFFSET_STORAGE_TOPIC": f"_perf-{r}-offsets",
               "CONNECT_STATUS_STORAGE_TOPIC": f"_perf-{r}-status",
               "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR": "1", "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR": "1",
               "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR": "1",
               "CONNECT_KEY_CONVERTER": "org.apache.kafka.connect.converters.ByteArrayConverter",
               "CONNECT_VALUE_CONVERTER": "org.apache.kafka.connect.converters.ByteArrayConverter",
               "CONNECT_REST_ADVERTISED_HOST_NAME": "localhost", "CONNECT_REST_PORT": "8083",
               "CONNECT_PLUGIN_PATH": "/usr/share/java,/usr/share/confluent-hub-components",
               "CONNECT_OFFSET_FLUSH_INTERVAL_MS": "1000"}
        return {"system": "confluent", "connector_name": conn, "connector": cfg, "worker_env": env,
                "group": f"connect-{conn}",
                "internal_topics": [env["CONNECT_CONFIG_STORAGE_TOPIC"], env["CONNECT_OFFSET_STORAGE_TOPIC"],
                                    env["CONNECT_STATUS_STORAGE_TOPIC"]],
                "files": {"connector.json": json.dumps(cfg, indent=1)}}
    if sysname == "vector":
        ep = (f'    endpoint: "{x["s3_container"]}"\n    force_path_style: true\n' if x["s3_container"] else "")
        text = f"""# generated by rq5-cloud/perf/perf.py (vector, run {x['run']}); RQ4 config + README "Fairness"
data_dir: /var/lib/vector
sources:
  kafka_in:
    type: kafka
    bootstrap_servers: "{x['kafka_container']}"
    group_id: "{x['group']}"
    topics: ["{x['topic']}"]
    auto_offset_reset: earliest
    decoding:
      codec: bytes
sinks:
  s3_out:
    type: aws_s3
    inputs: [kafka_in]
    bucket: {x['bucket']}
    region: us-east-1
{ep}    key_prefix: "{x['prefix']}"
    filename_time_format: "%s"
    filename_append_uuid: true
    filename_extension: log
    compression:
      algorithm: zstd
      level: 3
    encoding:
      codec: text
    framing:
      method: newline_delimited
    batch:
      max_events: {x['records_per_object']}
      max_bytes: 1000000000
      timeout_secs: {x['age_s']}
    acknowledgements:
      enabled: true
"""
        return {"system": "vector", "files": {"vector.yaml": text}}
    if sysname == "fluentbit":
        ep = f"    endpoint        {x['s3_container']}\n" if x["s3_container"] else ""
        text = f"""# generated by rq5-cloud/perf/perf.py (fluentbit, run {x['run']}); RQ4 config + README "Fairness"
[SERVICE]
    flush           1
    log_level       info
    storage.path    /tmp/flb-storage/
    storage.sync    full
    storage.checksum on

[INPUT]
    name            kafka
    tag             perf
    brokers         {x['kafka_container']}
    topics          {x['topic']}
    group_id        {x['group']}
    format          none
    storage.type    filesystem
    rdkafka.auto.offset.reset earliest

[OUTPUT]
    name            s3
    match           *
    bucket          {x['bucket']}
    region          us-east-1
{ep}    use_put_object  On
    send_content_md5 On
    compression     zstd
    total_file_size {x['object_bytes']}
    upload_timeout  {x['age_s']}s
    store_dir       /tmp/fluent-bit/s3
    s3_key_format   /{x['prefix']}%Y%m%dT%H%M%S-$UUID.log
    log_key         payload
"""
        return {"system": "fluentbit", "files": {"fluent-bit.conf": text}}
    raise KeyError(name)


# ================================================================== agent (sink host)
def default_iface() -> str | None:
    try:
        for line in Path("/proc/net/route").read_text().splitlines()[1:]:
            f = line.split()
            if len(f) > 1 and f[1] == "00000000":
                return f[0]
    except OSError:
        pass
    return None


def root_disk() -> str | None:   # as cost.py root_disk()
    try:
        for line in Path("/proc/mounts").read_text().splitlines():
            dev, mnt = line.split()[:2]
            if mnt == "/" and dev.startswith("/dev/"):
                name = Path(os.path.realpath(dev)).name
                base = name.rstrip("0123456789")
                return base[:-1] if base.endswith("p") and base[:-1][-1:].isdigit() else base
    except OSError:
        pass
    return None


ENA = ("bw_in_allowance_exceeded", "bw_out_allowance_exceeded", "pps_allowance_exceeded",
       "conntrack_allowance_exceeded", "linklocal_allowance_exceeded")
NFT_TABLE = "rq5perf"
PRIVATE = "{ 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16, 127.0.0.0/8 }"


def host_counters(nic, disk) -> dict:
    """NIC, root-disk, ENA allowance and nft (bytes to/from non-private addresses = S3) counters; Linux only."""
    out = {"t": now()}
    try:
        for line in Path("/proc/net/dev").read_text().splitlines():
            if nic and line.strip().startswith(nic + ":"):
                f = line.split(":", 1)[1].split()
                out["nic_rx_bytes"], out["nic_tx_bytes"] = int(f[0]), int(f[8])
        for line in Path("/proc/diskstats").read_text().splitlines():
            f = line.split()
            if disk and f[2] == disk:
                out["disk_read_bytes"], out["disk_write_bytes"] = int(f[5]) * 512, int(f[9]) * 512
        st = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        out["cpu_jiffies"] = [int(v) for v in st[:8]]
    except OSError:
        pass
    if nic and shutil.which("ethtool"):
        for line in (sh("ethtool", "-S", nic, timeout=10)["stdout"] or "").splitlines():
            k, _, v = line.strip().partition(":")
            if k in ENA:
                out[k] = int(v)
    if shutil.which("nft"):
        r = sh("nft", "-j", "list", "table", "inet", NFT_TABLE, timeout=10)
        try:
            for item in json.loads(r["stdout"])["nftables"]:
                rule = item.get("rule")
                if not rule:
                    continue
                cnt = [e["counter"] for e in rule.get("expr", []) if "counter" in e]
                if cnt:
                    out[f"public_{rule['chain']}_bytes"] = cnt[0]["bytes"]
                    out[f"public_{rule['chain']}_packets"] = cnt[0]["packets"]
        except (ValueError, KeyError, TypeError):
            pass
    return out


def counter_delta(a: dict, b: dict) -> dict:
    d = {}
    for k, v in b.items():
        if k == "cpu_jiffies" and k in a:
            j = [y - x for x, y in zip(a[k], v)]
            tot = sum(j) or 1
            d["cpu_busy_pct"] = round(100 * (tot - j[3] - j[4]) / tot, 2)
            d["cpu_steal_pct"] = round(100 * j[7] / tot, 3) if len(j) > 7 else None
            d["cpu_iowait_pct"] = round(100 * j[4] / tot, 3)
        elif isinstance(v, (int, float)) and k in a:
            d[k] = round(v - a[k], 3)
    return d


def nft_setup() -> str:
    if not shutil.which("nft"):
        return "nft not installed"
    sh("nft", "delete", "table", "inet", NFT_TABLE)
    cmds = [["add", "table", "inet", NFT_TABLE],
            ["add", "chain", "inet", NFT_TABLE, "out", "{ type filter hook output priority 0 ; policy accept ; }"],
            ["add", "chain", "inet", NFT_TABLE, "in", "{ type filter hook input priority 0 ; policy accept ; }"],
            ["add", "rule", "inet", NFT_TABLE, "out", "ip", "daddr", "!=", PRIVATE, "counter"],
            ["add", "rule", "inet", NFT_TABLE, "in", "ip", "saddr", "!=", PRIVATE, "counter"]]
    for c in cmds:
        r = sh("nft", *c)
        if r["rc"] != 0:
            return f"nft failed: {' '.join(c)}: {r['stderr'][-300:]}"
    return "ok"


def parse_ps_time(s: str) -> float:
    s = s.strip()
    days = 0
    if "-" in s:
        d, s = s.split("-", 1)
        days = int(d)
    parts = [float(p) for p in s.split(":")]
    sec = 0.0
    for p in parts:
        sec = sec * 60 + p
    return days * 86400 + sec


class Meter:
    """Samples CPU seconds and memory of the system under test every `interval` s.

    Linux: cgroup v2 of the container (docker-<id>.scope) or of VTOP (a cgroup the agent creates and the
    launcher shell joins before exec). macOS dry run: the same cgroup files read through a helper container
    (--cgroupns=host) for containers, `ps` for the native VTOP. A restart of a container creates a new cgroup
    whose counters start at 0; a drop in usage starts a new generation and the previous total is carried over."""

    def __init__(self, agent, interval=0.5):
        self.ag, self.interval = agent, interval
        self.samples: list[dict] = []
        self.target = None
        self.base = 0.0
        self.last_raw = None
        self.lock = threading.Lock()
        self.stop_ev = threading.Event()
        self.th = threading.Thread(target=self._loop, daemon=True)

    def set_target(self, kind, val):
        with self.lock:
            self.target = (kind, val)

    def start(self):
        self.th.start()

    def stop(self):
        self.stop_ev.set()
        self.th.join(timeout=10)

    def _read(self):
        with self.lock:
            tgt = self.target
        if not tgt:
            return None
        kind, val = tgt
        if kind == "cg":
            vals = self.ag.read_cgroup(val)
            if not vals or "usage_usec" not in vals:
                return None
            return {"cpu_raw": vals["usage_usec"] / 1e6, "anon": vals.get("anon"), "current": vals.get("current"),
                    "peak": vals.get("peak")}
        if kind == "pid":
            r = sh("ps", "-o", "rss=", "-o", "time=", "-p", str(val), timeout=5)
            f = (r["stdout"] or "").split()
            if len(f) < 2:
                return None
            return {"cpu_raw": parse_ps_time(f[1]), "anon": int(f[0]) * 1024, "current": None, "peak": None}
        return None

    def _loop(self):
        nic = self.ag.nic
        while not self.stop_ev.is_set():
            t = now()
            r = self._read()
            if r is not None:
                if self.last_raw is not None and r["cpu_raw"] + 1e-6 < self.last_raw:
                    self.base += self.last_raw   # new generation (restart)
                self.last_raw = r["cpu_raw"]
                s = {"t": round(t, 3), "cpu_s": round(self.base + r["cpu_raw"], 4), "anon": r["anon"],
                     "current": r["current"], "peak": r["peak"]}
                if nic:
                    try:
                        for line in Path("/proc/net/dev").read_text().splitlines():
                            if line.strip().startswith(nic + ":"):
                                f = line.split(":", 1)[1].split()
                                s["rx"], s["tx"] = int(f[0]), int(f[8])
                    except OSError:
                        pass
                self.samples.append(s)
            self.stop_ev.wait(max(0.0, self.interval - (now() - t)))


class SinkRun:
    def __init__(self, agent, payload):
        self.ag, self.p = agent, payload
        self.run, self.system = payload["run"], payload["system"]
        self.root = Path(agent.a.work) / self.run
        self.out = Path(agent.a.out) / "runs" / self.run
        self.proc = None
        self.rusage = []
        self.container = f"rq5perf-{self.system}"
        self.cgroup = None
        self.events: list[dict] = []
        self.meter = Meter(agent)

    # ---- helpers
    def docker_base(self) -> list[str]:
        a = self.ag.a
        args = ["docker", "run", "-d", "--name", self.container, "--network", a.docker_network,
                "-e", "AWS_REGION=us-east-1"]
        for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):   # lab only; on EC2 the role via IMDSv2
            if os.environ.get(k):
                args += ["-e", f"{k}={os.environ[k]}"]
        if a.lab:
            args += ["--add-host", "host.docker.internal:host-gateway"]
        return args

    def container_cgroup(self):
        cid = sh("docker", "inspect", "-f", "{{.Id}}", self.container)["stdout"].strip()
        return self.ag.find_cgroup(cid) if cid else None

    def connect_api(self, method, path, body=None, timeout=30):
        req = urllib.request.Request(f"http://127.0.0.1:{self.ag.a.connect_rest_port}{path}", method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read()
            return json.loads(txt) if txt else None

    def wait_connect(self, timeout):
        t0 = now()
        while now() - t0 < timeout:
            try:
                if self.connect_api("GET", "/connectors", timeout=5) is not None:
                    return True
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        return False

    def wait_task_running(self, timeout):
        t0 = now()
        name = self.p["connector_name"]
        while now() - t0 < timeout:
            try:
                st = self.connect_api("GET", f"/connectors/{name}/status", timeout=5)
                tasks = st.get("tasks") or []
                if tasks and all(t.get("state") == "RUNNING" for t in tasks):
                    return True
                if any(t.get("state") == "FAILED" for t in tasks):
                    self.events.append({"connect_task_failed": [t.get("trace", "")[:2000] for t in tasks]})
                    return False
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        return False

    def spawn_vtop(self):
        a = self.ag.a
        cfg = self.root / "vtop.yaml"
        log = open(self.root / "logs" / "engine.jsonl", "ab")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("VTOP_S3_", "AWS_ENDPOINT"))}
        env.update(VTOP_LOG_FORMAT="json", AWS_REGION="us-east-1")
        if self.cgroup and self.ag.linux:   # join the run's cgroup before exec, so the engine is accounted from t0
            cmd = ["/bin/sh", "-c", f'echo $$ > {self.cgroup}/cgroup.procs 2>/dev/null; exec "$0" "$@"',
                   a.vtopctl, "run", "--config", str(cfg)]
        else:
            cmd = [a.vtopctl, "run", "--config", str(cfg)]
        self.proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, umask=0o077)
        log.close()
        if not self.cgroup:
            self.meter.set_target("pid", self.proc.pid)

    def reap_vtop(self, sig, timeout):
        if self.proc is None:
            return None
        try:
            self.proc.send_signal(sig)
        except ProcessLookupError:
            pass
        t0 = now()
        while now() - t0 < timeout:
            pid, status, ru = os.wait4(self.proc.pid, os.WNOHANG)
            if pid == self.proc.pid:
                self.rusage.append({"cpu_s": ru.ru_utime + ru.ru_stime,
                                    "maxrss_bytes": ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                                    "status": status, "signal": int(sig)})
                self.proc.returncode = status
                return status
            time.sleep(0.05)
        os.kill(self.proc.pid, signal.SIGKILL)
        _, status, ru = os.wait4(self.proc.pid, 0)
        self.rusage.append({"cpu_s": ru.ru_utime + ru.ru_stime,
                            "maxrss_bytes": ru.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                            "status": status, "signal": int(sig), "killed_after_timeout": True})
        return status

    # ---- lifecycle
    def start(self) -> dict:
        a = self.ag.a
        shutil.rmtree(self.root, ignore_errors=True)
        for d in ("state", "work", "logs"):
            (self.root / d).mkdir(parents=True)
        os.chmod(self.root / "state", 0o700)
        self.out.mkdir(parents=True, exist_ok=True)
        for fn, text in self.p.get("files", {}).items():
            (self.root / fn).write_text(text.replace("@ROOT@", str(self.root)))
            (self.out / fn).write_text(text.replace("@ROOT@", str(self.root)))
        self.c0 = host_counters(self.ag.nic, self.ag.disk)
        t_start = now()
        ok, err = True, None
        if self.system == "vtop":
            if self.ag.linux:
                cg = Path("/sys/fs/cgroup") / f"rq5perf-{self.run}"
                try:
                    cg.mkdir(exist_ok=True)
                    self.cgroup = str(cg)
                    self.meter.set_target("cg", self.cgroup)
                except OSError as e:
                    self.events.append({"cgroup_error": repr(e)})
            self.spawn_vtop()
            time.sleep(0.5)
            ok = self.proc.poll() is None
            err = None if ok else f"vtopctl exited rc={self.proc.returncode}"
        else:
            sh("docker", "rm", "-f", self.container)
            if self.system == "confluent":
                args = self.docker_base()
                if a.docker_network != "host":
                    args += ["-p", f"{a.connect_rest_port}:8083"]
                for k, v in self.p["worker_env"].items():
                    args += ["-e", f"{k}={v}"]
                args += ["-v", f"{a.plugins_dir}:/usr/share/confluent-hub-components:ro", IMAGES["confluent"]]
            elif self.system == "vector":
                args = self.docker_base() + ["-v", f"{self.root / 'vector.yaml'}:/etc/vector/vector.yaml:ro",
                                             IMAGES["vector"], "--config", "/etc/vector/vector.yaml"]
            else:
                args = self.docker_base() + ["-v", f"{self.root / 'fluent-bit.conf'}:/fluent-bit/etc/fluent-bit.conf:ro",
                                             IMAGES["fluentbit"]]
            r = sh(*args, timeout=120)
            if r["rc"] != 0:
                return {"ok": False, "error": f"docker run: {r['stderr'][-800:]}", "t_start": t_start}
            cg = self.container_cgroup()
            if cg:
                self.cgroup = cg
                self.meter.set_target("cg", cg)
            if self.system == "confluent":
                if not self.wait_connect(a.connect_timeout):
                    ok, err = False, "Connect REST did not come up"
                else:
                    self.connect_api("PUT", f"/connectors/{self.p['connector_name']}/config", self.p["connector"])
                    if not self.wait_task_running(180):
                        ok, err = False, "connector task not RUNNING"
            else:
                time.sleep(0.5)
                st = sh("docker", "inspect", "-f", "{{.State.Running}}", self.container)["stdout"].strip()
                ok = st == "true"
                err = None if ok else "container not running"
        self.meter.start()
        return {"ok": ok, "error": err, "t_start": t_start, "t_ready": now(), "cgroup": self.cgroup}

    def kill_restart(self) -> dict:
        c = host_counters(self.ag.nic, self.ag.disk)
        if self.system == "vtop":
            t_kill = now()
            self.reap_vtop(signal.SIGKILL, 10)
            t_dead = now()
            self.spawn_vtop()
            t_restart = now()
            t_ready = t_restart
        else:
            t_kill = now()
            sh("docker", "kill", "-s", "KILL", self.container)
            t_dead = now()
            sh("docker", "start", self.container, timeout=120)
            t_restart = now()
            cg = self.container_cgroup()
            if cg:
                self.cgroup = cg
                self.meter.set_target("cg", cg)
            if self.system == "confluent":
                self.wait_connect(self.ag.a.connect_timeout)
                self.wait_task_running(180)
            t_ready = now()
        ev = {"t_kill": t_kill, "t_dead": t_dead, "t_restart": t_restart, "t_ready": t_ready, "counters": c}
        self.events.append({"kill": ev})
        return ev

    def stop(self) -> dict:
        t_stop = now()
        evidence = {}
        if self.system == "vtop":
            self.reap_vtop(signal.SIGTERM, 120)
            self.meter.stop()
            evidence = vtop_evidence(self.root / "state" / "vtop.db", self.root / "logs" / "engine.jsonl",
                                     verify_times=bool(self.p.get("post_checks")))
            if self.p.get("post_checks"):   # fault runs: VTOP's own after-the-fact checks of the archive (cost.py)
                env = {k: v for k, v in os.environ.items() if not k.startswith(("VTOP_S3_", "AWS_ENDPOINT"))}
                env.update(VTOP_LOG_FORMAT="json", AWS_REGION="us-east-1")
                cfgp = self.root / "vtop.yaml"
                for nm, args in (("scrub", ["scrub"]), ("audit_coverage", ["audit-coverage", "--check-storage"])):
                    r = sh(self.ag.a.vtopctl, "--json", args[0], "--config", cfgp, *args[1:], timeout=600, env=env)
                    evidence[f"post_{nm}"] = {"rc": r["rc"], "stdout_tail": (r["stdout"] or "")[-3000:],
                                              "stderr_tail": (r["stderr"] or "")[-1500:]}
            db = self.root / "state" / "vtop.db"
            if db.exists():
                dst = sqlite3.connect(self.out / "ledger.db")
                src = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
                src.backup(dst)
                src.close()
                dst.close()
            with open(self.root / "logs" / "engine.jsonl", "rb") as fi, gzip.open(self.out / "engine.jsonl.gz", "wb") as fo:
                shutil.copyfileobj(fi, fo)
            if self.cgroup:
                try:
                    Path(self.cgroup).rmdir()
                except OSError:
                    pass
        else:
            self.meter.stop()
            st = sh("docker", "inspect", "-f", "{{json .State}}", self.container)["stdout"]
            logs = subprocess.run(["docker", "logs", self.container], capture_output=True, timeout=120)
            with gzip.open(self.out / f"{self.system}.log.gz", "wb") as fo:
                fo.write(logs.stdout + logs.stderr)
            txt = (logs.stdout + logs.stderr).decode(errors="replace")
            evidence = {"container_state": st.strip()[:2000],
                        "log_error_lines": sum(1 for l in txt.splitlines() if re.search(r"\b(ERROR|error\]|FATAL)\b", l)),
                        "log_warn_lines": sum(1 for l in txt.splitlines() if re.search(r"\b(WARN|warn\])\b", l)),
                        "log_tail": txt.splitlines()[-15:]}
            sh("docker", "rm", "-f", self.container, timeout=60)
        c1 = host_counters(self.ag.nic, self.ag.disk)
        s = self.meter.samples
        anon = [x["anon"] for x in s if x.get("anon") is not None]
        peak = [x["peak"] for x in s if x.get("peak") is not None]
        summ = {"cpu_s_sampled": s[-1]["cpu_s"] if s else None,
                "peak_anon_bytes": max(anon) if anon else None,
                "peak_cgroup_memory_bytes": max(peak) if peak else None,
                "samples": len(s), "rusage": self.rusage, "counters_delta": counter_delta(self.c0, c1)}
        if self.rusage:
            summ["cpu_s_rusage"] = round(sum(r["cpu_s"] for r in self.rusage), 4)
            summ["maxrss_bytes_rusage"] = max(r["maxrss_bytes"] for r in self.rusage)
        shutil.rmtree(self.root, ignore_errors=True)
        (self.out / "samples.json").write_text(json.dumps(s))
        return {"t_stop": t_stop, "summary": summ, "evidence": evidence, "events": self.events, "samples": s,
                "counters_start": self.c0, "counters_end": c1}


def vtop_evidence(db: Path, log: Path, verify_times=False) -> dict:
    out = {"ledger_states": {}, "ledger_classes": [], "object_checks": {}, "warn": 0, "error": 0,
           "warn_or_error_samples": [], "shutdown": None, "events": {}}
    vt_batch, batch_obj = {}, {}
    if db.exists():
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(batches)")}
            for st, n in con.execute("SELECT state, COUNT(*) FROM batches GROUP BY state"):
                out["ledger_states"][st] = n
            if verify_times:
                for b, u in con.execute("SELECT batch_id, object_uri FROM batches WHERE object_uri IS NOT NULL"):
                    batch_obj[b] = u.split("/", 3)[3] if u.startswith("s3://") else u
            if "verification_class" in cols:
                out["ledger_classes"] = sorted({str(r[0]) for r in con.execute(
                    "SELECT DISTINCT verification_class FROM batches WHERE state='source_committed'")})
        finally:
            con.close()
    if log.exists():
        for line in log.read_text(errors="replace").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            f, level = rec.get("fields", {}), rec.get("level", "")
            msg = str(f.get("message", ""))
            if msg.startswith("verification_passed"):
                oc = str(f.get("object_check"))
                out["object_checks"][oc] = out["object_checks"].get(oc, 0) + 1
                if verify_times and f.get("batch_id") and rec.get("timestamp"):
                    import datetime as _dt
                    try:
                        vt_batch[f["batch_id"]] = _dt.datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        pass
            if msg.startswith("verification_failed") or "verification_failed" in str(f.get("event", "")):
                out["verification_failed"] = out.get("verification_failed", 0) + 1
            if msg.startswith("shutdown flush"):
                out["shutdown"] = msg
            if "event" in f:
                out["events"][f["event"]] = out["events"].get(f["event"], 0) + 1
            if level in ("WARN", "ERROR"):
                out["warn" if level == "WARN" else "error"] += 1
                if len(out["warn_or_error_samples"]) < 10:
                    out["warn_or_error_samples"].append({"level": level, "message": msg[:400]})
    if verify_times:   # object key -> first verification_passed time (sink clock) of a batch stored in it
        vt = {}
        for b, t in vt_batch.items():
            k = batch_obj.get(b)
            if k:
                vt[k] = min(t, vt.get(k, t))
        out["verify_times"] = vt
    return out


class Agent:
    def __init__(self, a):
        self.a = a
        self.linux = sys.platform.startswith("linux")
        self.nic, self.disk = default_iface(), root_disk()
        self.cur: SinkRun | None = None
        self.lock = threading.Lock()
        self.last_contact = now()
        self.contacted = False
        self.nft = nft_setup() if self.linux else "not linux"

    def read_cgroup(self, path) -> dict | None:
        names = ["cpu.stat", "memory.stat", "memory.peak", "memory.current"]
        if self.linux:
            txt = {}
            for n in names:
                try:
                    txt[n] = Path(path, n).read_text()
                except OSError:
                    txt[n] = ""
        else:   # macOS dry run: Docker Desktop's VM cgroup tree through the helper container
            r = sh("docker", "exec", self.a.cg_helper, "sh", "-c",
                   "; ".join(f"echo '##{n}'; cat {path}/{n} 2>/dev/null" for n in names), timeout=10)
            txt, cur = {}, None
            for line in (r["stdout"] or "").splitlines():
                if line.startswith("##"):
                    cur = line[2:]
                    txt[cur] = ""
                elif cur:
                    txt[cur] += line + "\n"
        out = {}
        for line in txt.get("cpu.stat", "").splitlines():
            k, _, v = line.partition(" ")
            if k == "usage_usec":
                out["usage_usec"] = int(v)
        for line in txt.get("memory.stat", "").splitlines():
            k, _, v = line.partition(" ")
            if k == "anon":
                out["anon"] = int(v)
        for k in ("memory.peak", "memory.current"):
            v = txt.get(k, "").strip()
            if v.isdigit():
                out[k.split(".")[1]] = int(v)
        return out or None

    def find_cgroup(self, cid) -> str | None:
        if self.linux:
            for p in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope", f"/sys/fs/cgroup/docker/{cid}"):
                if Path(p).is_dir():
                    return p
            r = sh("find", "/sys/fs/cgroup", "-maxdepth", "4", "-type", "d", "-name", f"*{cid}*", timeout=10)
        else:
            r = sh("docker", "exec", self.a.cg_helper, "find", "/sys/fs/cgroup", "-maxdepth", "4", "-type", "d",
                   "-name", f"*{cid}*", timeout=10)
        cands = (r["stdout"] or "").split()
        return cands[0] if cands else None

    def info(self) -> dict:
        a = self.a
        vt = {}
        if a.vtopctl and Path(a.vtopctl).exists():
            vt = {"version_output": sh(a.vtopctl, "--version")["stdout"].strip(), "path": a.vtopctl}
            import hashlib
            h = hashlib.sha256()
            with open(a.vtopctl, "rb") as fh:
                while b := fh.read(MiB):
                    h.update(b)
            vt["vtopctl_sha256"] = h.hexdigest()
        imgs = {k: sh("docker", "image", "inspect", v, "--format", "{{.Id}} {{.Architecture}}")["stdout"].strip()
                for k, v in IMAGES.items() if k != "kafka"}
        return {"host": socket.gethostname(), "platform": platform.platform(), "machine": platform.machine(),
                "cpu_count": os.cpu_count(), "linux": self.linux, "nic": self.nic, "disk": self.disk,
                "nft": self.nft, "ethtool": bool(shutil.which("ethtool")), "vtop": vt, "images": imgs,
                "docker_network": a.docker_network, "plugins_dir": a.plugins_dir, "lab": a.lab,
                "meminfo": (Path("/proc/meminfo").read_text().splitlines()[:3] if self.linux else None),
                "docker_version": sh("docker", "version", "--format", "{{.Server.Version}}")["stdout"].strip(),
                "time": now()}

    def rtt(self, n=5) -> dict:
        """TCP handshakes to the bucket's regional S3 name (as wan.py tcp_rtt; the sink host is the far end)."""
        host = self.a.rtt_host
        if not host:
            return {"skipped": "no --rtt-host (lab)"}
        try:
            sys.path.insert(0, str(HERE))
            import wan  # noqa: F401  staged beside perf.py on the VM (imports cost.py and smoke/oracle.py)
            return wan.tcp_rtt(host, 443, n)
        except Exception as e:  # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}"}

    def handle(self, path, payload) -> tuple[int, dict]:
        self.last_contact, self.contacted = now(), True
        with self.lock:
            if path == "/info":
                return 200, self.info()
            if path == "/rtt":
                return 200, self.rtt(int(payload.get("n", 5)))
            if path == "/start":
                if self.cur is not None:
                    return 409, {"error": f"run {self.cur.run} still active"}
                self.cur = SinkRun(self, payload)
                res = self.cur.start()
                res["rtt"] = self.rtt(5)
                return 200, res
            if path == "/kill":
                if self.cur is None:
                    return 409, {"error": "no active run"}
                return 200, self.cur.kill_restart()
            if path == "/stop":
                if self.cur is None:
                    return 409, {"error": "no active run"}
                try:
                    return 200, self.cur.stop()
                finally:
                    self.cur = None
            if path == "/shutdown":
                threading.Thread(target=self.shutdown, args=("requested",), daemon=True).start()
                return 200, {"ok": True}
        return 404, {"error": "unknown path"}

    def shutdown(self, why):
        time.sleep(0.5)
        if self.cur is not None:
            try:
                self.cur.stop()
            except Exception:  # noqa: BLE001
                pass
        (Path(self.a.out) / "agent-exit.json").write_text(json.dumps({"reason": why, "at": utc()}))
        os._exit(0)

    def watchdog(self):
        while True:
            time.sleep(10)
            limit = self.a.idle_min * 60 if self.contacted else self.a.first_contact_min * 60
            if now() - self.last_contact > limit:
                self.shutdown(f"no contact for {limit} s")


def main_agent(a):
    Path(a.out).mkdir(parents=True, exist_ok=True)
    Path(a.work).mkdir(parents=True, exist_ok=True)
    ag = Agent(a)

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _reply(self, code, obj):
            body = json.dumps(obj, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _go(self):
            if self.headers.get("X-Perf-Token") != a.token:
                return self._reply(403, {"error": "bad token"})
            n = int(self.headers.get("Content-Length") or 0)
            try:
                payload = json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                return self._reply(400, {"error": "bad json"})
            try:
                code, obj = ag.handle(self.path, payload)
            except Exception as e:  # noqa: BLE001
                code, obj = 500, {"error": f"{type(e).__name__}: {e}"}
            self._reply(code, obj)

        do_GET = do_POST = _go

    threading.Thread(target=ag.watchdog, daemon=True).start()
    srv = ThreadingHTTPServer((a.listen, a.port), H)
    srv.daemon_threads = True
    print(f"agent listening on {a.listen}:{a.port} (linux={ag.linux}, nic={ag.nic}, nft={ag.nft})", flush=True)
    srv.serve_forever()


# ================================================================== producer (live runs)
def line_offsets(path: str, n: int):
    """Byte offsets of the first n lines (n+1 entries; entry i+1 is one past line i's newline)."""
    import numpy as np
    offs = [np.zeros(1, dtype=np.int64)]
    found, pos = 0, 0
    with open(path, "rb") as fh:
        while found < n:
            buf = fh.read(64 * MiB)
            if not buf:
                break
            nl = np.flatnonzero(np.frombuffer(buf, dtype=np.uint8) == 10).astype(np.int64) + pos + 1
            take = nl[: n - found]
            offs.append(take)
            found += len(take)
            pos += len(buf)
    o = np.concatenate(offs)
    if found < n:
        raise SystemExit(f"{path}: only {found} lines, need {n}")
    return o


def count_lines(path: str) -> int:
    n = 0
    with open(path, "rb") as fh:
        while b := fh.read(64 * MiB):
            n += b.count(b"\n")
    return n


def main_produce(a):
    """Produce lines [0, n) of --lines-file to --topic partition 0 at --rate records/s; write produce times."""
    import mmap
    from array import array
    from confluent_kafka import Producer
    offs = line_offsets(a.lines_file, a.n)
    fh = open(a.lines_file, "rb")
    mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
    errors = []

    def on_err(err, msg):
        if err is not None and len(errors) < 100:
            errors.append(str(err))

    p = Producer({"bootstrap.servers": a.bootstrap, "acks": "all", "enable.idempotence": True,
                  "compression.type": "lz4", "linger.ms": 5, "queue.buffering.max.messages": 1000000,
                  "delivery.report.only.error": True, "on_delivery": on_err})
    ts = array("d", bytes(8 * a.n))
    t0 = now()
    i = 0
    while i < a.n:
        due = min(a.n, int((now() - t0) * a.rate) + 1)
        while i < due:
            try:
                ts[i] = now()
                p.produce(a.topic, mm[offs[i]:offs[i + 1] - 1], partition=0)
                i += 1
            except BufferError:
                p.poll(0.01)
        p.poll(0)
        time.sleep(0.002)
    t_last = now()
    left = p.flush(120)
    with open(a.out, "wb") as fo:
        ts.tofile(fo)
    Path(a.out + ".json").write_text(json.dumps({
        "n": a.n, "rate_target": a.rate, "t_first": ts[0], "t_last": t_last,
        "achieved_rate": round(a.n / max(1e-9, t_last - ts[0]), 1), "undelivered": left, "errors": errors}))


# ================================================================== orchestrator (broker host)
def import_rq4():
    """RQ4's oracle functions, imported unchanged (locate, judge, the content-index cache)."""
    for p in (HERE.parents[1] / "rq4-baselines" / "code", HERE / "rq4" / "code"):
        if (p / "exp_baseline.py").exists():
            sys.path.insert(0, str(p))
            break
    import exp_baseline
    return exp_baseline


class PartView:
    """truth[p] for RQ4's locate()/judge(): len() and integer indexing; lines come from the mmap'd file."""

    def __init__(self, tv, p):
        self.tv, self.p = tv, p
        self.n = (tv.n - p + tv.P - 1) // tv.P

    def __len__(self):
        return self.n

    def __getitem__(self, o):
        if o < 0 or o >= self.n:
            raise IndexError(o)
        i = o * self.tv.P + self.p
        return self.tv.mm[int(self.tv.off[i]):int(self.tv.off[i + 1]) - 1]


class TruthView(dict):
    """Ground truth of a topic whose record i (line i of the file) went to partition i % P at offset i // P.
    A dict {p: PartView} so RQ4's judge() and locate() can use it without holding 11 M byte strings."""

    def __init__(self, mm, off, n, P):
        super().__init__()
        self.mm, self.off, self.n, self.P = mm, off, n, P
        for p in range(P):
            self[p] = PartView(self, p)

    def payload_lengths(self, p):
        import numpy as np
        idx = np.arange(p, self.n, self.P)
        return (self.off[idx + 1] - self.off[idx] - 1).astype(np.int64)


class HashIndex:
    """Stands in for RQ4's content_index() dict (line -> [(partition, offset)]): 64-bit hashes sorted with numpy,
    every candidate confirmed byte for byte, candidates in (partition, offset) order like the dict's lists."""

    def __init__(self, tv: TruthView):
        import numpy as np
        self.tv = tv
        mm, off = tv.mm, tv.off
        h = np.fromiter((hash(mm[int(off[i]):int(off[i + 1]) - 1]) for i in range(tv.n)), dtype=np.int64, count=tv.n)
        self.order = np.argsort(h, kind="stable")
        self.h = h[self.order]
        self.np = np

    def get(self, x, default=None):
        np = self.np
        hx = hash(bytes(x))
        lo = int(np.searchsorted(self.h, hx, "left"))
        hi = int(np.searchsorted(self.h, hx, "right"))
        out = []
        for g in self.order[lo:hi]:
            g = int(g)
            p, o = g % self.tv.P, g // self.tv.P
            if self.tv[p][o] == x:
                out.append((p, o))
        return sorted(out) or default


def decompress(blob: bytes) -> bytes:
    import zstandard
    if blob[:4] == b"\x28\xb5\x2f\xfd":
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(blob), read_across_frames=True) as r:
            return r.read()
    if blob[:2] == b"\x1f\x8b":
        return gzip.decompress(blob)
    return blob


def object_kind(key: str) -> str:
    if key.endswith(".manifest.json"):
        return "manifest"
    if key.endswith(".receipt.json"):
        return "receipt"
    return "data"


class Observer:
    """Polls the consumer group's committed offsets (Kafka admin API) and lists the run's S3 prefix; GETs each
    new data object once (cached for the oracle) and counts its lines, all on the broker host's clock."""

    def __init__(self, o, group, topic, P, prefix, list_s, fault=None):
        self.o, self.group, self.topic, self.P, self.prefix, self.list_s = o, group, topic, P, prefix, list_s
        self.fault_spec = fault          # (kind, nth): damage the nth data object as soon as it is listed
        self.fault: dict | None = None
        self.fault_original: bytes | None = None
        self.data_order: list[str] = []
        self.present: set[str] = set()
        self.commits: list[tuple[float, tuple]] = []
        self.latest = {p: 0 for p in range(P)}
        self.objects: dict[str, dict] = {}
        self.blobs: dict[str, bytes] = {}
        self.archived_lines = 0
        self.last_new = now()
        self.lists = 0
        self.errors: list[str] = []
        self.stop_ev = threading.Event()
        self.ths = [threading.Thread(target=self._commits, daemon=True), threading.Thread(target=self._lister, daemon=True)]

    def start(self):
        for t in self.ths:
            t.start()

    def stop(self):
        self.stop_ev.set()
        for t in self.ths:
            t.join(timeout=30)

    def fetch_commits(self) -> dict | None:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        try:
            fs = self.o.admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(self.group, [TopicPartition(self.topic, p) for p in range(self.P)])],
                request_timeout=10)
            res = list(fs.values())[0].result()
            return {tp.partition: max(0, tp.offset) for tp in res.topic_partitions}
        except Exception as e:  # noqa: BLE001
            if len(self.errors) < 20:
                self.errors.append(f"commits: {type(e).__name__}: {e}")
            return None

    def _commits(self):
        while not self.stop_ev.is_set():
            t = now()
            c = self.fetch_commits()
            if c is not None:
                c = {p: c.get(p, 0) for p in range(self.P)}
                if c != self.latest or not self.commits:
                    self.commits.append((round(t, 4), tuple(c[p] for p in range(self.P))))
                    self.latest = c
            self.stop_ev.wait(max(0.0, self.o.a.commit_poll_s - (now() - t)))

    def list_once(self) -> int:
        new = 0
        t_lo = now()
        pag = self.o.s3.get_paginator("list_objects_v2")
        seen = []
        for page in pag.paginate(Bucket=self.o.a.bucket, Prefix=self.prefix):
            seen += page.get("Contents", [])
        t_hi = now()
        self.lists += 1
        self.present = {ob["Key"] for ob in seen}
        for ob in sorted((ob for ob in seen if ob["Key"] not in self.objects),
                         key=lambda ob: (ob["LastModified"], ob["Key"])):
            k = ob["Key"]
            kind = object_kind(k)
            etag = ob.get("ETag", "").strip('"')
            self.objects[k] = {"first_seen": round(t_hi, 4), "list_started": round(t_lo, 4), "size": ob["Size"],
                               "etag_parts": int(etag.split("-")[1]) if "-" in etag else 0,
                               "last_modified": ob["LastModified"].timestamp(), "kind": kind}
            new += 1
            if kind == "data":
                try:
                    blob = self.o.s3.get_object(Bucket=self.o.a.bucket, Key=k)["Body"].read()
                    self.blobs[k] = blob
                    raw = decompress(blob)
                    n = raw.count(b"\n") + (1 if raw and not raw.endswith(b"\n") else 0)
                    self.objects[k]["lines"] = n
                    self.archived_lines += n
                except Exception as e:  # noqa: BLE001
                    self.objects[k]["get_error"] = f"{type(e).__name__}: {e}"
                self.data_order.append(k)
                if self.fault_spec and self.fault is None and len(self.data_order) == self.fault_spec[1]:
                    self.inject(k)
        if new:
            self.last_new = t_hi
        return new

    def inject(self, k):
        """RQ4's storage faults, native on S3: the nth data object the sink has written (and S3 has acknowledged,
        since it is listed) is overwritten with the same bytes with one byte flipped in the middle (`corrupt`,
        as s3_fault_proxy.py) or deleted (`lost`). Manifests are never faulted (object_kind)."""
        kind, nth = self.fault_spec
        s3, b = self.o.s3, self.o.a.bucket
        t0 = now()
        f = {"kind": kind, "nth_faulted_write": nth, "key": k, "t_visible": self.objects[k]["first_seen"], "t_start": t0}
        try:
            blob = self.blobs.get(k) or s3.get_object(Bucket=b, Key=k)["Body"].read()
            self.fault_original = blob
            if kind == "lost":
                s3.delete_object(Bucket=b, Key=k)
            else:
                body = bytearray(blob)
                i = len(body) // 2
                body[i] ^= 0x01
                s3.put_object(Bucket=b, Key=k, Body=bytes(body))
                f.update(offset=i, size=len(body))
            f["t_damaged"] = now()
        except Exception as e:  # noqa: BLE001  a fault that could not be applied is recorded, never hidden
            f["error"] = f"{type(e).__name__}: {e}"
        self.blobs.pop(k, None)   # the oracle must read the damaged state
        self.fault = f

    def _lister(self):
        while not self.stop_ev.is_set():
            t = now()
            try:
                self.list_once()
            except Exception as e:  # noqa: BLE001
                if len(self.errors) < 20:
                    self.errors.append(f"list: {type(e).__name__}: {e}")
            self.stop_ev.wait(max(0.0, self.list_s - (now() - t)))


class Orchestrator:
    def __init__(self, a):
        import boto3
        import numpy as np
        from botocore.config import Config
        from confluent_kafka.admin import AdminClient
        self.a, self.np = a, np
        self.eb = import_rq4()
        self.admin = AdminClient({"bootstrap.servers": a.bootstrap})
        cfg = {"retries": {"max_attempts": 5, "mode": "standard"}, "max_pool_connections": 20}
        if a.s3_endpoint:
            cfg["s3"] = {"addressing_style": "path"}
        self.s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=a.s3_endpoint or None, config=Config(**cfg))
        self.out = Path(a.out)
        self.out.mkdir(parents=True, exist_ok=True)
        for d in ("series", "objects", "latency"):
            (self.out / d).mkdir(exist_ok=True)
        self.sent: dict = {}
        self.nic, self.disk = default_iface(), root_disk()

    # ---- agent client
    def agent(self, path, payload=None, timeout=900):
        req = urllib.request.Request(self.a.agent + path, method="POST",
                                     data=json.dumps(payload or {}).encode(),
                                     headers={"Content-Type": "application/json", "X-Perf-Token": self.a.token})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return {"ok": False, "error": f"HTTP {e.code}: {e.read()[:2000].decode(errors='replace')}"}

    # ---- topics and truth
    def load_truth(self):
        import mmap
        a = self.a
        self.n_tput = a.partitions * a.objects_per_partition * a.records_per_object
        self.n_live = a.live_objects * a.records_per_object
        n = max(self.n_tput, self.n_live)
        t0 = now()
        off = line_offsets(a.lines_file, n)
        self.fh = open(a.lines_file, "rb")
        self.mm = mmap.mmap(self.fh.fileno(), 0, access=mmap.ACCESS_READ)
        self.truth_tput = TruthView(self.mm, off, self.n_tput, a.partitions)
        self.truth_live = TruthView(self.mm, off, self.n_live, 1)
        views = [("tput", self.truth_tput), ("live", self.truth_live)]
        self.truth_ssh = None
        if a.reps_fault and a.fault_configs:   # RQ4's ssh-p1: the whole SSH log, one partition
            n_ssh = count_lines(a.ssh_lines_file)
            self.fh_ssh = open(a.ssh_lines_file, "rb")
            self.mm_ssh = mmap.mmap(self.fh_ssh.fileno(), 0, access=mmap.ACCESS_READ)
            self.truth_ssh = TruthView(self.mm_ssh, line_offsets(a.ssh_lines_file, n_ssh), n_ssh, 1)
            views.append(("ssh", self.truth_ssh))
        for _, tv in views:
            self.eb._INDEX[id(tv)] = HashIndex(tv)   # RQ4's content_index() returns this cached index
        self.cum = {}
        for name, tv in views:
            self.cum[name] = [self.np.concatenate([[0], self.np.cumsum(tv.payload_lengths(p))]) for p in range(tv.P)]
        return round(now() - t0, 1)

    def create_topic(self, name, P):
        from confluent_kafka.admin import NewTopic
        fs = self.admin.create_topics([NewTopic(name, P, 1)], request_timeout=30)
        for f in fs.values():
            f.result()

    def delete_topics(self, names):
        if not names:
            return
        try:
            for f in self.admin.delete_topics(list(names), request_timeout=30).values():
                try:
                    f.result()
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass

    def end_offsets(self, topic, P) -> dict:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": self.a.bootstrap, "group.id": "perf-watermarks", "enable.auto.commit": False})
        try:
            return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1] for p in range(P)}
        finally:
            c.close()

    def load_topic(self, topic, tv, cumname) -> dict:
        """Topic: line i -> partition i % P with an idempotent lz4 producer (order kept per partition);
        then every record is consumed once and compared byte for byte with the file (ground-truth check)."""
        from confluent_kafka import Consumer, Producer, TopicPartition
        a, P = self.a, tv.P
        t0 = now()
        self.create_topic(topic, P)
        errors = []

        def on_err(err, msg):
            if err is not None and len(errors) < 100:
                errors.append(str(err))

        p = Producer({"bootstrap.servers": a.bootstrap, "acks": "all", "enable.idempotence": True,
                      "compression.type": "lz4", "linger.ms": 20, "batch.size": 1000000,
                      "queue.buffering.max.messages": 2000000, "delivery.report.only.error": True,
                      "on_delivery": on_err})
        mm, off = tv.mm, tv.off
        for i in range(tv.n):
            while True:
                try:
                    p.produce(topic, mm[int(off[i]):int(off[i + 1]) - 1], partition=i % P)
                    break
                except BufferError:
                    p.poll(0.05)
            if i % 50000 == 0:
                p.poll(0)
        left = p.flush(300)
        t_prod = now() - t0
        ends = self.end_offsets(topic, P)
        want = {q: len(tv[q]) for q in range(P)}
        assert not errors and left == 0, f"producer errors {errors[:3]} undelivered {left}"
        assert ends == want, f"end offsets {ends} != {want}"
        mism, seen = 0, 0
        c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "perf-truthcheck", "enable.auto.commit": False,
                      "auto.offset.reset": "earliest", "fetch.max.bytes": 52428800})
        c.assign([TopicPartition(topic, q, 0) for q in range(P)])
        t1 = now()
        while seen < tv.n and now() - t1 < 1800:
            for m in c.consume(10000, timeout=1.0):
                if m.error():
                    continue
                seen += 1
                if m.value() != tv[m.partition()][m.offset()]:
                    mism += 1
        c.close()
        assert seen == tv.n and mism == 0, f"truth check: seen {seen}/{tv.n}, mismatches {mism}"
        return {"topic": topic, "partitions": P, "records": tv.n, "end_offsets": ends, "produce_s": round(t_prod, 1),
                "verify_s": round(now() - t1, 1), "records_verified": seen, "mismatches": mism,
                "payload_bytes": int(sum(self.cum[cumname][q][-1] for q in range(P)))}

    # ---- results upload
    def sync(self):
        if not self.a.results_uri:
            return
        bucket, key = self.a.results_uri[5:].split("/", 1)
        for f in sorted(self.out.rglob("*")):
            if not f.is_file():
                continue
            st = f.stat()
            if self.sent.get(str(f)) == (st.st_size, st.st_mtime):
                continue
            try:
                self.s3.upload_file(str(f), bucket, f"{key.rstrip('/')}/{f.relative_to(self.out)}")
                self.sent[str(f)] = (st.st_size, st.st_mtime)
            except Exception as e:  # noqa: BLE001
                print(f"upload of {f} failed: {e}", flush=True)

    def delete_prefix(self, prefix) -> dict:
        n = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.a.bucket, Prefix=prefix):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                self.s3.delete_objects(Bucket=self.a.bucket, Delete={"Objects": objs, "Quiet": True})
                n += len(objs)
        ab = 0
        for page in self.s3.get_paginator("list_multipart_uploads").paginate(Bucket=self.a.bucket, Prefix=prefix):
            for u in page.get("Uploads", []):
                self.s3.abort_multipart_upload(Bucket=self.a.bucket, Key=u["Key"], UploadId=u["UploadId"])
                ab += 1
        return {"deleted_objects": n, "aborted_multipart_uploads": ab}

    # ---- analysis helpers
    def committed_bytes(self, cum, offs):
        return int(sum(int(cum[p][min(o, len(cum[p]) - 1)]) for p, o in enumerate(offs)))

    def crossing(self, series, total, frac):
        """First (t, value) of a nondecreasing [(t, value)] series at or above frac * total."""
        for t, v in series:
            if v >= frac * total:
                return t, v
        return None

    def rate(self, series, total, lo=0.1, hi=0.9):
        a, b = self.crossing(series, total, lo), self.crossing(series, total, hi)
        if not a or not b or b[0] <= a[0]:
            return None
        return {"t_lo": a[0], "t_hi": b[0], "bytes": b[1] - a[1], "bytes_per_s": (b[1] - a[1]) / (b[0] - a[0])}

    @staticmethod
    def cpu_at(samples, t):
        prev = None
        for s in samples:
            if s["t"] >= t:
                if prev is None:
                    return s["cpu_s"]
                f = (t - prev["t"]) / max(1e-9, s["t"] - prev["t"])
                return prev["cpu_s"] + f * (s["cpu_s"] - prev["cpu_s"])
            prev = s
        return prev["cpu_s"] if prev else None

    # ---- one run
    def run_one(self, order, rtype, cfg, rep) -> dict:
        a, np = self.a, self.np
        run = f"{order:03d}-{rtype}-{cfg}-r{rep}"
        prefix = f"{a.stage}/data/{a.session}/{run}/"
        live = rtype == "live"
        fault = rtype in ("corrupt", "lost")
        topic = f"live-{order:03d}" if live else (a.ssh_topic if fault else a.topic)
        tv = self.truth_live if live else (self.truth_ssh if fault else self.truth_tput)
        cum = self.cum["live" if live else ("ssh" if fault else "tput")]
        P = tv.P
        rec = {"run": run, "order": order, "type": rtype, "config": cfg, "system": CONFIGS[cfg]["system"],
               "rep": rep, "topic": topic, "prefix": prefix, "started_at": utc()}
        x = {"run": run, "topic": topic, "group": f"perf-{run}", "bucket": a.bucket, "prefix": prefix,
             "kafka_native": a.kafka_native, "kafka_container": a.kafka_container, "s3_native": a.s3_native,
             "s3_container": a.s3_container, "records_per_object": a.records_per_object, "age_s": a.age,
             "tasks": a.confluent_tasks, "object_bytes": a.object_bytes}
        if fault:   # RQ4's object size: 40,000 records (Confluent flush.size, Vector max_events), 4 MiB (VTOP)
            x.update(records_per_object=a.fault_records_per_object, object_bytes=a.fault_object_bytes)
        payload = render(cfg, x)
        if fault:
            payload["post_checks"] = True
        payload["render_context"] = x
        payload["run"] = run
        group = payload.get("group", x["group"])
        rec["group"] = group
        (self.out / "configs").mkdir(exist_ok=True)
        (self.out / "configs" / f"{run}.json").write_text(json.dumps(payload, indent=1))
        if live:
            self.create_topic(topic, 1)
        target = {p: len(tv[p]) for p in range(P)}
        if rec["system"] == "confluent":   # no time rotation (RQ4): the tail below flush.size is never written
            F = x["records_per_object"]
            target = {p: n - n % F for p, n in target.items()}
        target_records = sum(target.values())
        total_bytes = self.committed_bytes(cum, [target[p] for p in range(P)])
        hc0 = host_counters(self.nic, self.disk)
        nth = self.nth.get((rtype, cfg, rep)) if fault else None
        rec["nth_faulted_write"] = nth
        obs = Observer(self, group, topic, P, prefix, a.list_live_s if live else (a.list_fault_s if fault else a.list_s),
                       fault=(rtype, nth) if fault else None)
        obs.start()
        st = self.agent("/start", payload, timeout=a.connect_timeout + 300)
        rec["start"] = {k: st.get(k) for k in ("ok", "error", "t_start", "t_ready", "cgroup", "rtt")}
        kill_ev, prod, end, killed = None, None, None, False
        t_ready = st.get("t_ready") or now()
        if not st.get("ok"):
            end = f"start_failed: {st.get('error')}"
        elif live:
            time.sleep(a.live_settle)
            pout = str(self.out / "latency" / f"{run}.produce.f64")
            prod = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "produce", "--bootstrap", a.bootstrap,
                                     "--topic", topic, "--lines-file", a.lines_file, "--n", str(self.n_live),
                                     "--rate", str(a.live_rate), "--out", pout])
        t_at_target = None
        last_progress, last_c = now(), None
        while end is None:
            c = dict(obs.latest)
            if c != last_c:
                last_c, last_progress = c, now()
            if obs.last_new > last_progress:
                last_progress = obs.last_new
            at = all(c.get(p, 0) >= target[p] for p in range(P))
            if at and t_at_target is None:
                t_at_target = now()
            settled = now() - obs.last_new
            if at and obs.archived_lines >= target_records and (not killed or settled >= a.age + 2):
                end = "archive_holds_target"
            elif at and settled >= a.settle_s:
                end = f"listing_unchanged_{a.settle_s:.0f}s"
            elif rtype == "kill" and not killed and max(sum(c.values()), obs.archived_lines) >= a.kill_at * target_records:
                kill_ev = self.agent("/kill", {}, timeout=a.connect_timeout + 300)
                killed = True
                last_progress = now()
            elif now() - t_ready > a.run_timeout:
                end = "timeout"
            elif now() - last_progress > (a.fault_stall_s if fault else a.stall_s) and (prod is None or prod.poll() is not None):
                end = f"stalled_{(a.fault_stall_s if fault else a.stall_s):.0f}s"
            time.sleep(0.25)
        t_end = now()
        if prod is not None:
            try:
                prod.wait(timeout=300)
            except subprocess.TimeoutExpired:
                prod.kill()
        stop = self.agent("/stop", {}, timeout=300)
        obs.stop()
        try:
            obs.list_once()   # final listing (objects completed after the last poll)
        except Exception as e:  # noqa: BLE001
            obs.errors.append(f"final list: {e}")
        committed = obs.fetch_commits() or dict(obs.latest)
        hc1 = host_counters(self.nic, self.disk)
        rec.update({"end": end, "t_ready": t_ready, "t_end": t_end, "t_at_target": t_at_target,
                    "elapsed_s": round(t_end - (st.get("t_start") or t_ready), 3),
                    "committed_final": committed, "target": target, "kill": kill_ev,
                    "observer": {"lists": obs.lists, "commit_points": len(obs.commits), "errors": obs.errors},
                    "broker_host": counter_delta(hc0, hc1)})
        # ---------------- oracle (RQ4 locate + judge, unchanged)
        t0 = now()
        data = sorted((k for k, v in obs.objects.items() if v["kind"] == "data" and k in obs.present),
                      key=lambda k: obs.objects[k]["first_seen"])
        intervals, per_obj = [], []
        orig_ivs = None
        if obs.fault_original is not None:   # what the damaged object held before the fault
            orig_ivs = self.eb.locate(decompress(obs.fault_original), tv)[0]
        for k in data:
            blob = obs.blobs.get(k)
            if blob is None:
                blob = self.s3.get_object(Bucket=a.bucket, Key=k)["Body"].read()
            try:
                raw = decompress(blob)
                ivs, stt = self.eb.locate(raw, tv)
            except Exception as e:  # noqa: BLE001  e.g. a corrupted zstd/gzip frame: the object is not intact
                raw = b""
                ivs = [(p_, s_, e_, False) for p_, s_, e_, _ in (orig_ivs or [])] if (obs.fault or {}).get("key") == k \
                    else [(0, None, None, False)]
                stt = {"lines": 0, "unmatched_lines": 0, "ambiguous_lines": 0, "runs": 0,
                       "decode_error": f"{type(e).__name__}: {e}"}
            intervals += ivs
            m = obs.objects[k]
            per_obj.append({"key": k[len(prefix):], "bytes": len(blob), "raw_bytes": len(raw), **stt,
                            "intact": all(iv[3] for iv in ivs), "first_seen": m["first_seen"],
                            "last_modified": m["last_modified"], "etag_parts": m["etag_parts"],
                            "ivs": [[int(p), None if s is None else int(s), None if e is None else int(e), bool(ok)]
                                    for p, s, e, ok in ivs]})
        # identical records split across objects (RQ4 reconcile(), added 2026-11-22 after 106-tput-vector-r4)
        intervals, rmoves = self.eb.reconcile(intervals, [pl for q in per_obj for pl in q.get("ambiguous_placements", [])])
        j = self.eb.judge(intervals, tv, committed, target)
        ps = j["partitions"].values()
        o = {"unsafe_final": j["unsafe_partitions"], "complete": all(q["complete"] for q in ps),
             "missing_records": sum(max(0, q["target"] - q["intact_prefix"]) for q in ps),
             "lost_committed_records": sum(q["lost_committed_records"] for q in ps),
             "duplicate_objects": sum(q["duplicate_objects"] for q in ps),
             "duplicate_records": sum(q["duplicate_records"] or 0 for q in ps),
             "bad_intervals": j["bad_objects"], "intervals": j["objects"],
             "s3_data_objects": len(per_obj), "s3_data_objects_bad": sum(not q["intact"] for q in per_obj),
             "unmatched_lines": sum(q["unmatched_lines"] for q in per_obj),
             "ambiguous_lines": sum(q["ambiguous_lines"] for q in per_obj),
             "reconcile_moves": rmoves, "oracle_s": round(now() - t0, 2), "partitions": j["partitions"]}
        rec["oracle"] = o
        if fault:
            f = dict(obs.fault or {})
            f["key"] = (f.get("key") or "")[len(prefix):]
            rng_ = [(int(p_), int(s_), int(e_)) for p_, s_, e_, _ in (orig_ivs or []) if s_ is not None]
            f["records"] = [[p_, s_, e_] for p_, s_, e_ in rng_]

            def first_commit(cond):
                return next((t for t, offs in obs.commits if cond(offs)), None)
            t_enter = first_commit(lambda offs: any(offs[p_] > s_ for p_, s_, _ in rng_)) if rng_ else None
            t_cover = first_commit(lambda offs: all(offs[p_] >= e_ for p_, _, e_ in rng_)) if rng_ else None
            td, tvis = f.get("t_damaged"), f.get("t_visible")
            band = a.commit_poll_s + 0.1   # a commit is observed up to one poll (+ RPC) after it happened
            if td is None:
                cls = "not_injected"
            elif t_enter is not None and tvis is not None and t_enter < tvis:
                cls = "commit_before_object_visible"
            elif t_enter is None or td < t_enter - band:
                cls = "damage_before_commit"
            elif td >= t_enter:
                cls = "damage_after_commit"
            else:
                cls = "ambiguous_within_one_poll"
            f.update(t_commit_enter=t_enter, t_commit_cover=t_cover, damage_vs_commit=cls,
                     visible_to_damage_s=round(td - tvis, 4) if td and tvis else None)
            if rec["system"] == "vtop":
                vt = ((stop.get("evidence") or {}).get("verify_times") or {}).get(prefix + f["key"])
                f["vtop_verified_at"] = vt
                f["damage_vs_vtop_verification"] = (None if vt is None or td is None else
                                                    "before_verification" if td < vt else "after_verification")
                ev = stop.get("evidence") or {}
                f["vtop_post_scrub_rc"] = (ev.get("post_scrub") or {}).get("rc")
                f["vtop_post_audit_coverage_rc"] = (ev.get("post_audit_coverage") or {}).get("rc")
            rec["fault"] = f
        kinds = {}
        for v in obs.objects.values():
            kinds[v["kind"]] = kinds.get(v["kind"], 0) + 1
        rec["s3"] = {"objects_by_kind": kinds,
                     "data_bytes_stored": sum(q["bytes"] for q in per_obj),
                     "data_bytes_raw": sum(q["raw_bytes"] for q in per_obj),
                     "data_multipart_objects": sum(1 for q in per_obj if q["etag_parts"]),
                     "data_multipart_parts": sum(q["etag_parts"] for q in per_obj),
                     "nondata_bytes_stored": sum(v["size"] for v in obs.objects.values() if v["kind"] != "data"),
                     "records_per_object_median": (float(np.median([q["lines"] for q in per_obj])) if per_obj else None),
                     "raw_bytes_per_object_median": (float(np.median([q["raw_bytes"] for q in per_obj])) if per_obj else None)}
        # ---------------- sink metrics and evidence
        samples = stop.get("samples") or []
        rec["sink"] = {"summary": stop.get("summary"), "evidence": stop.get("evidence"), "events": stop.get("events"),
                       "error": stop.get("error")}
        # ---------------- throughput (committed and archived curves on the broker clock)
        cseries = [(t, self.committed_bytes(cum, offs)) for t, offs in obs.commits]
        cseries = [(t, v) for t, v in zip([t for t, _ in cseries], np.maximum.accumulate([v for _, v in cseries]).tolist())] if cseries else []
        covered = [np.zeros(len(tv[p]), dtype=bool) for p in range(P)]
        aseries, acc = [], 0
        lens = [np.diff(cum[p]) for p in range(P)]
        for q in per_obj:
            for p, s, e, ok in q["ivs"]:
                if ok and s is not None:
                    seg = covered[p][s:e]
                    acc += int(lens[p][s:e][~seg].sum())
                    seg[:] = True
            aseries.append((q["first_seen"], acc))
        # "done": records both archived intact and source-committed (the latency's end point), as a step series
        ev_t = sorted({t for t, _ in cseries} | {t for t, _ in aseries})
        dseries, ci_, ai_, cv, av = [], 0, 0, 0, 0
        for t in ev_t:
            while ci_ < len(cseries) and cseries[ci_][0] <= t:
                cv = cseries[ci_][1]
                ci_ += 1
            while ai_ < len(aseries) and aseries[ai_][0] <= t:
                av = aseries[ai_][1]
                ai_ += 1
            dseries.append((t, min(cv, av)))
        thr = {"total_payload_bytes": total_bytes}
        for nm, ser in (("committed", cseries), ("archived", aseries), ("done", dseries)):
            r = self.rate(ser, total_bytes) if rtype != "live" else None
            if r:
                rps = r["bytes_per_s"] / (total_bytes / target_records)
                thr[nm] = {"mib_s": round(r["bytes_per_s"] / MiB, 3), "records_s_approx": round(rps, 1),
                           "window": [round(r["t_lo"], 3), round(r["t_hi"], 3)]}
        wkey = next((k_ for k_ in ("done", "committed", "archived") if k_ in thr), None)
        if rtype == "tput" and wkey:   # CPU and NIC over the steady window of the done rate (else committed/archived)
            thr["cpu_window_basis"] = wkey
            lo, hi = thr[wkey]["window"]
            c_lo, c_hi = self.cpu_at(samples, lo), self.cpu_at(samples, hi)
            if c_lo is not None and c_hi is not None:
                gib = (thr[wkey]["mib_s"] * (hi - lo)) / 1024
                thr["cpu_s_window"] = round(c_hi - c_lo, 3)
                thr["cpu_cores_window"] = round((c_hi - c_lo) / (hi - lo), 3)
                thr["cpu_s_per_gib"] = round((c_hi - c_lo) / gib, 3) if gib else None
            tt = [s for s in samples if lo <= s["t"] <= hi and s.get("rx") is not None]
            if len(tt) >= 2:
                thr["sink_nic_rx_mib_s_window"] = round((tt[-1]["rx"] - tt[0]["rx"]) / MiB / (tt[-1]["t"] - tt[0]["t"]), 3)
                thr["sink_nic_tx_mib_s_window"] = round((tt[-1]["tx"] - tt[0]["tx"]) / MiB / (tt[-1]["t"] - tt[0]["t"]), 3)
        if cseries:
            thr["end_to_end_committed_mib_s"] = round(total_bytes / MiB / max(1e-9, (t_at_target or t_end) - t_ready), 3)
        rec["throughput"] = thr
        # ---------------- recovery after SIGKILL
        if kill_ev and "t_kill" in kill_ev:
            tk = kill_ev["t_kill"]
            pre = [(t, v) for t, v in cseries if t <= tk]
            before = pre[-1][1] if pre else 0
            pre_rate = None
            lo_pt = self.crossing(pre, total_bytes, 0.1) if pre else None
            if lo_pt and pre and tk > lo_pt[0]:
                pre_rate = (before - lo_pt[1]) / (tk - lo_pt[0])
            # a commit made just before the kill can be observed up to one poll (+ RPC) later: the baseline is the
            # largest value observed until 3 polls after the restart; the first commit beyond it is the resumption
            t_base = kill_ev["t_restart"] + 3 * a.commit_poll_s
            base_v = max([v for t, v in cseries if t <= t_base] or [before])
            first_commit = next((t for t, v in cseries if t > t_base and v > base_v), None)
            first_obj = next((q["first_seen"] for q in per_obj if q["first_seen"] > kill_ev["t_restart"]), None)
            gaps = np.diff([t for t, _ in pre]) if len(pre) > 2 else np.array([])
            W = max(a.recovery_window_s, 3 * float(np.median(gaps))) if len(gaps) else a.recovery_window_s
            resteady = None
            if pre_rate:
                post = [(t, v) for t, v in cseries if t >= kill_ev["t_restart"]]
                for t, v in post:
                    if t - W < kill_ev["t_restart"]:
                        continue
                    base = max((vv for tt_, vv in cseries if tt_ <= t - W), default=0)
                    if (v - base) / W >= 0.9 * pre_rate:
                        resteady = t
                        break
            rec["recovery"] = {"pre_kill_mib_s": round(pre_rate / MiB, 3) if pre_rate else None,
                               "committed_at_kill_fraction": round(before / total_bytes, 4),
                               "dead_s": round(kill_ev["t_dead"] - tk, 3),
                               "restart_s": round(kill_ev["t_restart"] - tk, 3),
                               "ready_s": round(kill_ev["t_ready"] - tk, 3),
                               "first_commit_after_s": round(first_commit - tk, 3) if first_commit else None,
                               "first_commit_rule": "first observed commit beyond the largest value seen until 3 polls "
                                                    "after the restart (upper bound by <= 3 polls)",
                               "first_object_after_s": round(first_obj - tk, 3) if first_obj else None,
                               "resteady_s": round(resteady - tk, 3) if resteady else None,
                               "resteady_window_s": round(W, 2), "resteady_rule": "first t after restart with "
                               "committed rate over (t-W, t] >= 0.9 x pre-kill rate (10% -> kill)"}
        # ---------------- latency (live)
        if live:
            info = {}
            pout = self.out / "latency" / f"{run}.produce.f64"
            if pout.exists():
                info = json.loads(Path(str(pout) + ".json").read_text())
                tp = np.fromfile(pout, dtype=np.float64)
                n = len(tp)
                t_obj = np.full(n, np.inf)
                for q in per_obj:
                    for p, s, e, ok in q["ivs"]:
                        if ok and s is not None:
                            seg = t_obj[s:e]
                            np.minimum(seg, q["first_seen"], out=seg)
                ts_c = np.array([t for t, _ in obs.commits]) if obs.commits else np.array([np.inf])
                off_c = np.maximum.accumulate(np.array([offs[0] for _, offs in obs.commits])) if obs.commits else np.array([0])
                idx = np.searchsorted(off_c, np.arange(n), side="right")
                t_com = np.where(idx < len(ts_c), ts_c[np.minimum(idx, len(ts_c) - 1)], np.inf)
                done = np.maximum(t_obj, t_com)
                qs = [1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9, 100]

                def pct(v):
                    f = v[np.isfinite(v)]
                    return {"n": int(len(f)), "missing": int(len(v) - len(f)),
                            **({f"p{q:g}": round(float(np.percentile(f, q)), 4) for q in qs} if len(f) else {})}
                lat, lo_, lc_ = done - tp, t_obj - tp, t_com - tp
                # steady window: records produced >= live_warm s after the first and >= live_tail s before the last,
                # so neither the ramp nor the final age-flushed partial object enters the steady figures
                win = (tp >= tp[0] + a.live_warm) & (tp <= tp[-1] - a.live_tail)
                rec["latency"] = {"producer": info, "end_to_end_s": pct(lat[win]), "object_visible_s": pct(lo_[win]),
                                  "commit_s": pct(lc_[win]), "end_to_end_all_records_s": pct(lat),
                                  "steady_window_s": [a.live_warm, a.live_tail], "steady_records": int(win.sum()),
                                  "commit_before_object_records": int(np.sum(t_com < t_obj)),
                                  "resolution": {"list_poll_s": a.list_live_s, "commit_poll_s": a.commit_poll_s}}
                f = lat[np.isfinite(lat)]
                hist = np.histogram(f, bins=np.arange(0, 600.05, 0.1))[0] if len(f) else []
                with gzip.open(self.out / "latency" / f"{run}.json.gz", "wt") as fo:
                    json.dump({"hist_0p1s_to_600s": [int(v) for v in hist], "every_100th_end_to_end_s":
                               [round(float(v), 4) for v in lat[::100]]}, fo)
                pout.unlink()
        # ---------------- pass rule and evidence
        checks = {"started": bool(st.get("ok")), "drained": end == "archive_holds_target",
                  "complete": o["complete"], "no_unsafe_commit": o["unsafe_final"] == 0,
                  "no_lost_committed": o["lost_committed_records"] == 0}
        if fault:   # outcomes (unsafe, lost) are results here, not failures; RQ4 reported them the same way
            checks = {"started": bool(st.get("ok")), "fault_injected": bool((obs.fault or {}).get("t_damaged")),
                      "ended": not str(end).startswith(("start_failed", "timeout"))}
        if rtype == "kill":
            checks["killed_before_drain"] = bool(kill_ev and "t_kill" in kill_ev and rec.get("recovery", {}).get("committed_at_kill_fraction", 1) < 1)
        if rec["system"] == "vtop" and not fault:
            algo = CONFIGS[cfg]["vtop"][0]
            ev = stop.get("evidence") or {}
            checks["vtop_log_method"] = set(ev.get("object_checks", {})) == {LOG_EXPECT[algo]}
            checks["vtop_ledger_class"] = ev.get("ledger_classes") == [LEDGER_CLASS[algo]]
        rec["checks"] = checks
        rec["pass"] = all(checks.values())
        with gzip.open(self.out / "series" / f"{run}.json.gz", "wt") as fo:
            json.dump({"commits": obs.commits, "committed_bytes": cseries, "archived_bytes": aseries,
                       "sink_samples": samples, "kill": kill_ev}, fo)
        with gzip.open(self.out / "objects" / f"{run}.json.gz", "wt") as fo:
            json.dump({"objects": obs.objects, "data": per_obj}, fo)
        # ---------------- clean up the run's data and topics
        cleanup = {}
        if rec["pass"] or not a.keep_failed_data:
            cleanup = self.delete_prefix(prefix)
        else:
            (self.out / "KEEP_DATA").write_text(f"failed run {run}: data kept under {prefix}\n")
        drop = list(payload.get("internal_topics", []))
        if live:
            drop.append(topic)
        self.delete_topics(drop)
        rec["cleanup"] = cleanup
        rec["finished_at"] = utc()
        return rec

    def schedule(self) -> list[tuple[str, str, object]]:
        a = self.a
        rng = random.Random(a.seed)
        mats = {"tput": a.tput_configs.split(","), "live": a.live_configs.split(","), "kill": a.kill_configs.split(","),
                "corrupt": a.fault_configs.split(","), "lost": a.fault_configs.split(",")}
        reps = {"tput": a.reps_tput, "live": a.reps_live, "kill": a.reps_kill, "corrupt": a.reps_fault,
                "lost": a.reps_fault}
        for v in mats.values():
            assert set(v) <= set(CONFIGS) | {""}, v
        cells = []
        warm = [("tput", c, "w") for c in a.warmup_configs.split(",") if c]
        rng.shuffle(warm)
        cells += warm
        for r in range(max(reps.values())):
            rnd = [(t, c, r) for t, cs in mats.items() for c in cs if c and r < reps[t]]
            rng.shuffle(rnd)
            cells += rnd
        # RQ4's choice of the write to damage: N uniform in 2..8 (exp_baseline.trial), from its own seeded stream
        frng = random.Random(a.fault_seed)
        self.nth = {c: frng.randint(2, 8) for c in cells if c[0] in ("corrupt", "lost")}
        return cells


def main_orchestrate(a):
    o = Orchestrator(a)
    info = {"experiment": "E-RQ5-PERF (H4.2): VTOP vs Confluent S3 Sink, Vector, Fluent Bit on EC2 + S3",
            "session": a.session, "leg": a.leg, "started_at": utc(), "argv": sys.argv, "seed": a.seed,
            "images": IMAGES, "image_tags": IMAGE_TAGS, "s3sink": S3SINK, "s3sink_zip_sha256": S3SINK_ZIP_SHA256,
            "configs": CONFIGS, "object_bytes_target": a.object_bytes, "records_per_object": a.records_per_object,
            "age_s": a.age, "confluent_tasks": a.confluent_tasks, "python": platform.python_version(),
            "broker_platform": platform.platform(), "lab_s3_endpoint": a.s3_endpoint or None,
            "measurement": {
                "commit_poll_s": a.commit_poll_s, "list_poll_s": a.list_s, "list_poll_live_s": a.list_live_s,
                "throughput": "committed (and archived) payload bytes between the first observation at >= 10% and "
                              "at >= 90% of the topic, divided by the time between them (broker clock)",
                "latency": "per record: max(first LIST that shows an intact object holding it, first committed-offset "
                           "poll beyond it) - produce() call time; all on the broker clock",
                "recovery": "kill at >= kill_at of the records committed; times from SIGKILL"},
            "vtop": {"version": a.vtop_version, "commit": a.vtop_commit, "tarball_sha256": a.tarball_sha256}}
    try:
        import confluent_kafka
        info["confluent_kafka"] = [confluent_kafka.version(), confluent_kafka.libversion()]
    except Exception:  # noqa: BLE001
        pass
    info["agent"] = o.agent("/info", {}, timeout=120)
    if a.rtt:
        try:
            sys.path.insert(0, str(HERE.parent / "wan"))
            sys.path.insert(0, str(HERE))
            import wan
            info["rtt_start_broker"] = wan.rtt_probe(a.bucket, 20)
        except Exception as e:  # noqa: BLE001
            info["rtt_start_broker"] = {"error": f"{type(e).__name__}: {e}"}
        info["rtt_start_sink"] = o.agent("/rtt", {"n": 20}, timeout=120)
    info["truth_load_s"] = o.load_truth()
    info["tput_topic"] = o.load_topic(a.topic, o.truth_tput, "tput")
    if o.truth_ssh is not None:
        info["fault_topic"] = o.load_topic(a.ssh_topic, o.truth_ssh, "ssh")
    cells = o.schedule()
    info["order"] = [f"{t}-{c}-r{r}" for t, c, r in cells]
    (o.out / "run.json").write_text(json.dumps(info, indent=1, default=str))
    o.sync()
    failures, stopped, done = 0, None, 0
    with open(o.out / "runs.jsonl", "a") as fh:
        for k, (t, c, r) in enumerate(cells, 1):
            if a.deadline_epoch and now() > a.deadline_epoch:
                stopped = f"deadline reached before run {k}/{len(cells)}"
                break
            try:
                rec = o.run_one(k, t, c, r)
            except Exception as e:  # noqa: BLE001
                import traceback
                rec = {"run": f"{k:03d}-{t}-{c}-r{r}", "type": t, "config": c, "rep": r, "pass": False,
                       "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-3000:]}
                try:
                    o.agent("/stop", {}, timeout=300)
                except Exception:  # noqa: BLE001
                    pass
            rec["warmup"] = r == "w"
            done += 1
            fh.write(json.dumps(rec, default=str) + "\n")
            fh.flush()
            failures += not rec["pass"]
            th = rec.get("throughput", {})
            print(f"[{k}/{len(cells)}] {rec['run']:40s} pass={rec['pass']} end={rec.get('end')} "
                  f"committed={th.get('committed', {}).get('mib_s')} done={th.get('done', {}).get('mib_s')} MiB/s "
                  f"lat_p50={rec.get('latency', {}).get('end_to_end_s', {}).get('p50')} "
                  f"rec={rec.get('recovery', {}).get('first_commit_after_s')} "
                  f"dup={rec.get('oracle', {}).get('duplicate_records')} {rec.get('error', '')}", flush=True)
            o.sync()
            if failures >= a.max_failures:
                stopped = f"stopped after run {k}: {failures} failed runs"
                break
    if a.rtt:
        try:
            import wan
            info["rtt_end_broker"] = wan.rtt_probe(a.bucket, 20)
        except Exception as e:  # noqa: BLE001
            info["rtt_end_broker"] = {"error": f"{type(e).__name__}: {e}"}
        info["rtt_end_sink"] = o.agent("/rtt", {"n": 20}, timeout=120)
    info.update(finished_at=utc(), runs_done=done, runs_planned=len(cells), failed_runs=failures, stopped=stopped)
    if a.shutdown_agent:
        o.agent("/shutdown", {}, timeout=30)
    o.delete_topics([a.topic, a.ssh_topic])
    (o.out / "run.json").write_text(json.dumps(info, indent=1, default=str))
    o.sync()
    print(json.dumps({"runs": done, "failed": failures, "stopped": stopped}), flush=True)
    raise SystemExit(1 if failures else (3 if stopped else 0))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("agent")
    g.add_argument("--listen", default="0.0.0.0")
    g.add_argument("--port", type=int, default=18080)
    g.add_argument("--token", required=True)
    g.add_argument("--out", required=True)
    g.add_argument("--work", required=True)
    g.add_argument("--vtopctl", required=True)
    g.add_argument("--plugins-dir", required=True)
    g.add_argument("--docker-network", default="host")
    g.add_argument("--connect-rest-port", type=int, default=8083)
    g.add_argument("--connect-timeout", type=float, default=300)
    g.add_argument("--cg-helper", default="", help="macOS dry run: container with the VM's cgroup tree")
    g.add_argument("--lab", action="store_true")
    g.add_argument("--rtt-host", default="", help="S3 name for TCP-handshake RTT probes (cloud)")
    g.add_argument("--idle-min", type=float, default=30)
    g.add_argument("--first-contact-min", type=float, default=90)
    p = sub.add_parser("produce")
    for k in ("bootstrap", "topic", "lines-file", "out"):
        p.add_argument(f"--{k}", required=True)
    p.add_argument("--n", type=int, required=True)
    p.add_argument("--rate", type=float, required=True)
    o = sub.add_parser("orchestrate")
    for k in ("bootstrap", "kafka-native", "kafka-container", "agent", "token", "bucket", "session", "lines-file", "out"):
        o.add_argument(f"--{k}", required=True)
    o.add_argument("--leg", default="lab")
    o.add_argument("--stage", default="perf")
    o.add_argument("--topic", default="hdfs-p4")
    o.add_argument("--s3-endpoint", default="", help="lab: observer/oracle endpoint (MinIO)")
    o.add_argument("--s3-native", default="", help="lab: endpoint for the native VTOP")
    o.add_argument("--s3-container", default="", help="lab: endpoint for the containers")
    o.add_argument("--partitions", type=int, default=4)
    o.add_argument("--records-per-object", type=int, default=120000)
    o.add_argument("--object-bytes", type=int, default=OBJECT_BYTES, help="VTOP max_bytes, Fluent Bit total_file_size")
    o.add_argument("--objects-per-partition", type=int, default=23)
    o.add_argument("--live-objects", type=int, default=15)
    o.add_argument("--live-rate", type=float, default=30000)
    o.add_argument("--live-settle", type=float, default=10)
    o.add_argument("--live-warm", type=float, default=5, help="latency steady window: skip the first s of production")
    o.add_argument("--live-tail", type=float, default=15, help="latency steady window: skip the last s (> age)")
    o.add_argument("--age", type=int, default=10)
    o.add_argument("--confluent-tasks", type=int, default=4)
    o.add_argument("--tput-configs", default=",".join(CONFIGS))
    o.add_argument("--live-configs", default="vtop-size-only,vtop-sha256-store,vtop-blake3-readback,confluent,vector,fluentbit")
    o.add_argument("--kill-configs", default="vtop-sha256-store,confluent,vector,fluentbit")
    o.add_argument("--warmup-configs", default="vtop-sha256-store,confluent,vector,fluentbit")
    o.add_argument("--reps-tput", type=int, default=5)
    o.add_argument("--reps-live", type=int, default=3)
    o.add_argument("--reps-kill", type=int, default=3)
    o.add_argument("--fault-configs", default="vtop-blake3-readback,confluent,vector,fluentbit")
    o.add_argument("--reps-fault", type=int, default=0, help="trials per system and fault (corrupt, lost)")
    o.add_argument("--fault-seed", type=int, default=20261111, help="RQ4's seed for nth_faulted_write")
    o.add_argument("--ssh-lines-file", default="")
    o.add_argument("--ssh-topic", default="ssh-p1")
    o.add_argument("--fault-records-per-object", type=int, default=40000)
    o.add_argument("--fault-object-bytes", type=int, default=4 * MiB)
    o.add_argument("--list-fault-s", type=float, default=0.1)
    o.add_argument("--fault-stall-s", type=float, default=120)
    o.add_argument("--seed", type=int, default=20261123)
    o.add_argument("--kill-at", type=float, default=0.4)
    o.add_argument("--commit-poll-s", type=float, default=0.25)
    o.add_argument("--list-s", type=float, default=1.0)
    o.add_argument("--list-live-s", type=float, default=0.5)
    o.add_argument("--settle-s", type=float, default=45)
    o.add_argument("--stall-s", type=float, default=240)
    o.add_argument("--run-timeout", type=float, default=1200)
    o.add_argument("--connect-timeout", type=float, default=300)
    o.add_argument("--recovery-window-s", type=float, default=10)
    o.add_argument("--max-failures", type=int, default=8)
    o.add_argument("--deadline-epoch", type=float, default=None)
    o.add_argument("--results-uri", default=None)
    o.add_argument("--keep-failed-data", action="store_true")
    o.add_argument("--rtt", action="store_true", help="RTT probes as wan.py (cloud)")
    o.add_argument("--shutdown-agent", action="store_true")
    o.add_argument("--vtop-version", default="")
    o.add_argument("--vtop-commit", default="")
    o.add_argument("--tarball-sha256", default="")
    a = ap.parse_args()
    {"agent": main_agent, "produce": main_produce, "orchestrate": main_orchestrate}[a.cmd](a)


if __name__ == "__main__":
    main()
