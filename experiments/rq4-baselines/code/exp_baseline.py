#!/usr/bin/env python3
"""E-RQ4-B: VTOP v0.7.0 versus the Confluent S3 Sink connector under the same faults.

Both systems read the same Kafka topic and write through the same S3 fault proxy
(``s3_fault_proxy.py``) to the same MinIO server. Faults:

* ``kill``:    SIGKILL of the sink process (``vtopctl run``, or the Kafka Connect worker container)
               K times at random points, then a drain;
* ``corrupt``: the store acknowledges the N-th data-object write, then flips one byte of it;
* ``lost``:    the store acknowledges the N-th data-object write, then deletes it.

One independent oracle judges both systems. It lists the objects in MinIO (directly, not through
the proxy), decodes each one, compares its records byte-exactly with the topic's ground truth,
computes the contiguous intact prefix of every partition, and compares it with the consumer
group's committed offset on the broker. ``unsafe`` = committed offset beyond the intact prefix,
i.e. the source was told data is archived when it is not (lost or corrupted).

Pinned components (recorded in run.json): apache/kafka:3.9.1; MinIO RELEASE.2025-09-07T16-13-09Z
built from source (vtop-engine docker/minio/Dockerfile, commit 07c3a429); Confluent Platform
cp-kafka-connect 7.9.0 with kafka-connect-s3 12.1.15 (Confluent Hub zip, SHA-1 checked); VTOP
v0.7.0 (6c8f386), s3_native backend, BLAKE3, strong verification.

C23 adds two more response-based sinks under the same topics, proxy and oracle (one system per
campaign, ``--systems vector`` or ``--systems fluentbit``): Vector 0.61.0 (``kafka`` source,
``aws_s3`` sink, end-to-end acknowledgements on) and Fluent Bit 5.2.0 (``kafka`` input with
filesystem storage, ``s3`` output with PutObject). Their objects carry no offsets, so the oracle
locates every line of an object in the topic by content (``locate``); configs/README.md has the
commit semantics of each sink and its sources.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3
import zstandard
from botocore.config import Config

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from vtoplab import DATASET_FILES, REPO, VTOP_COMMIT, Run, Variant, host_info  # noqa: E402

NET = "rq4net"
KAFKA, MINIO, CONNECT = "rq4-kafka", "rq4-minio", "rq4-connect"
VECTOR, FLUENTBIT = "rq4-vector", "rq4-fluentbit"
VECTOR_IMAGE = "timberio/vector:0.61.0-debian"
FLUENTBIT_IMAGE = "fluent/fluent-bit:5.2.0"
KAFKA_IMAGE = "apache/kafka:3.9.1"
MINIO_IMAGE = "vtop-minio:RELEASE.2025-09-07T16-13-09Z"
CONNECT_IMAGE = "confluentinc/cp-kafka-connect:7.9.0"
S3SINK = "kafka-connect-s3 12.1.15"
PLUGINS = REPO / ".tmp/rq4/plugins"
KEY, SECRET = "rq4lab", "rq4lab-secret"
MINIO_PORT, PROXY_PORT = 9000, 9100
KBIN = "/opt/kafka/bin"
TOPICS = {"ssh-p1": ("ssh", None, 1), "hdfs-p4": ("hdfs", 2_000_000, 4)}
PY = sys.executable


def sh(*args, check=False, **kw):
    r = subprocess.run(list(args), capture_output=True, text=True, **kw)
    if check and r.returncode:
        raise RuntimeError(f"{args}: {r.stderr[-800:]}")
    return r


def kexec(*args, stdin=None):
    cmd = ["docker", "exec"] + (["-i"] if stdin else []) + [KAFKA, *args]
    return subprocess.run(cmd, stdin=stdin, capture_output=True, text=True)


def s3():
    return boto3.client("s3", endpoint_url=f"http://127.0.0.1:{MINIO_PORT}", region_name="us-east-1",
                        aws_access_key_id=KEY, aws_secret_access_key=SECRET,
                        config=Config(s3={"addressing_style": "path"}))


def connect_api(method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:8083{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        txt = r.read()
        return json.loads(txt) if txt else None


def wait_for(fn, timeout, what):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if fn():
                return time.time() - t0
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"timeout waiting for {what}")


# ------------------------------------------------------------------ infrastructure

def infra_up(systems=("vtop", "confluent")):
    for c in (CONNECT, VECTOR, FLUENTBIT, KAFKA, MINIO):
        sh("docker", "rm", "-f", c)
    sh("docker", "network", "create", NET)
    sh("docker", "run", "-d", "--name", KAFKA, "--network", NET, "-p", "9092:9092",
       "-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=broker,controller",
       "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093",
       "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,INTERNAL://{KAFKA}:29092",
       "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
       "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093", "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
       "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL", "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
       "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1", "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
       "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0", KAFKA_IMAGE, check=True)
    sh("docker", "run", "-d", "--name", MINIO, "--network", NET, "-p", f"{MINIO_PORT}:9000",
       "-e", f"MINIO_ROOT_USER={KEY}", "-e", f"MINIO_ROOT_PASSWORD={SECRET}",
       MINIO_IMAGE, "server", "/data", check=True)
    wait_for(lambda: kexec(f"{KBIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list").returncode == 0,
             120, "kafka")
    wait_for(lambda: s3().list_buckets() is not None, 60, "minio")
    for b in ("archive", "confluent", "vector", "fluentbit"):
        s3().create_bucket(Bucket=b)
    if "confluent" in systems:
        connect_start(first=True)


def infra_restart(truth: dict):
    """Start the stopped Kafka and MinIO containers again (after a pause) and check the topics survived."""
    sh("docker", "start", MINIO, KAFKA, check=True)
    wait_for(lambda: kexec(f"{KBIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list").returncode == 0,
             120, "kafka")
    wait_for(lambda: s3().list_buckets() is not None, 60, "minio")
    for topic, parts in truth.items():
        r = kexec(f"{KBIN}/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", topic)
        ends = {int(x.split(":")[1]): int(x.split(":")[2]) for x in r.stdout.split()}
        assert ends == {p: len(v) for p, v in parts.items()}, f"{topic}: end offsets {ends} after restart"


def connect_start(first=False):
    if first:
        sh("docker", "run", "-d", "--name", CONNECT, "--network", NET, "-p", "8083:8083",
           "--add-host", "host.docker.internal:host-gateway",
           "-v", f"{PLUGINS}:/usr/share/confluent-hub-components:ro",
           "-e", f"CONNECT_BOOTSTRAP_SERVERS={KAFKA}:29092", "-e", "CONNECT_GROUP_ID=rq4-connect",
           "-e", "CONNECT_CONFIG_STORAGE_TOPIC=_rq4-configs", "-e", "CONNECT_OFFSET_STORAGE_TOPIC=_rq4-offsets",
           "-e", "CONNECT_STATUS_STORAGE_TOPIC=_rq4-status",
           "-e", "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1", "-e", "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1",
           "-e", "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1",
           "-e", "CONNECT_KEY_CONVERTER=org.apache.kafka.connect.converters.ByteArrayConverter",
           "-e", "CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.converters.ByteArrayConverter",
           "-e", f"CONNECT_REST_ADVERTISED_HOST_NAME={CONNECT}",
           "-e", "CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components",
           "-e", "CONNECT_OFFSET_FLUSH_INTERVAL_MS=1000",
           "-e", f"AWS_ACCESS_KEY_ID={KEY}", "-e", f"AWS_SECRET_ACCESS_KEY={SECRET}",
           CONNECT_IMAGE, check=True)
    else:
        sh("docker", "start", CONNECT, check=True)
    return wait_for(lambda: connect_api("GET", "/connectors") is not None, 240, "connect REST")


def prepare_topics(work: Path, names: list[str]) -> dict:
    """Create and fill topics; return ground truth {topic: {partition: [values]}}."""
    truth = {}
    for topic in names:
        ds, limit, parts = TOPICS[topic]
        kexec(f"{KBIN}/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--topic", topic,
              "--partitions", str(parts), "--replication-factor", "1")
        src = work / f"{topic}.txt"
        with open(DATASET_FILES[ds], "rb") as fi, open(src, "wb") as fo:
            for i, line in enumerate(fi):
                if limit and i >= limit:
                    break
                fo.write(line if line.endswith(b"\n") else line + b"\n")
        with open(src, "rb") as fh:
            r = kexec(f"{KBIN}/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic,
                      "--producer-property", "linger.ms=20", "--producer-property", "batch.size=262144", stdin=fh)
        assert r.returncode == 0, r.stderr[-500:]
        n = sum(1 for _ in open(src, "rb"))
        dump = kexec(f"{KBIN}/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic,
                     "--from-beginning", "--timeout-ms", "20000", "--property", "print.partition=true",
                     "--property", "print.offset=true", "--max-messages", str(n))
        t: dict[int, dict[int, bytes]] = {}
        for line in dump.stdout.splitlines():
            p_, o_, v = line.split("\t", 2)
            t.setdefault(int(p_.split(":")[1]), {})[int(o_.split(":")[1])] = v.encode()
        assert sum(len(x) for x in t.values()) == n, f"{topic}: incomplete ground truth"
        for p, d in t.items():
            assert sorted(d) == list(range(len(d))), f"{topic}[{p}] offsets not dense"
        truth[topic] = {p: [d[o] for o in sorted(d)] for p, d in t.items()}
        src.unlink()
    return truth


def group_committed(group: str, topic: str) -> dict[int, int]:
    r = kexec(f"{KBIN}/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--group", group)
    out = {}
    for line in r.stdout.splitlines():
        f = line.split()
        if len(f) >= 4 and f[1] == topic and f[2].isdigit():
            out[int(f[2])] = int(f[3]) if f[3].isdigit() else 0
    return out


class Proxy:
    def __init__(self, mode: str, nth: int, log: Path):
        self.log = log
        self.p = subprocess.Popen([PY, str(HERE / "s3_fault_proxy.py"), "--listen", str(PROXY_PORT),
                                   "--upstream", f"127.0.0.1:{MINIO_PORT}", "--mode", mode, "--nth", str(nth),
                                   "--log", str(log), "--access-key", KEY, "--secret-key", SECRET],
                                  stdout=subprocess.DEVNULL, stderr=open(str(log) + ".err", "w"))
        wait_for(lambda: urllib.request.urlopen(f"http://127.0.0.1:{PROXY_PORT}/minio/health/live", timeout=2), 20, "proxy")

    def events(self) -> list[dict]:
        return [json.loads(x) for x in open(self.log)] if self.log.exists() else []

    def stop(self):
        self.p.terminate()
        self.p.wait(timeout=10)


# ------------------------------------------------------------------ the two systems

class VtopS3Run(Run):
    def __init__(self, root, topic, group, prefix):
        self.topic, self.group, self.prefix = topic, group, prefix
        super().__init__(root, Variant(f"vtop-{topic}", checksum="blake3", max_bytes=4 * 2**20, max_age_s=1), {})

    def _write_config(self):
        v = self.v
        self.config.write_text(f"""# generated by exp_baseline.py
engine:
  name: lab
  tenant: lab
  state_store: "sqlite://{self.db}"
  work_dir: {self.work}
batching:
  max_records: {v.max_records}
  max_bytes: {v.max_bytes}
  max_batch_age_seconds: {v.max_age_s}
  idle_poll_interval_ms: 200
  max_concurrent_batches: {v.concurrency}
compression:
  type: {v.compression}
  level: {v.level}
checksum:
  algorithm: {v.checksum}
sources:
  kafka:
    enabled: true
    bootstrap_servers: ["localhost:9092"]
    consumer_group: {self.group}
    topic_include_regex: "^{self.topic}$"
    topic_exclude_regex: "^__.*"
    auto_offset_reset: earliest
    enable_auto_commit: false
  file: {{enabled: false, paths: []}}
  syslog_spool: {{enabled: false, paths: []}}
upload:
  backend: s3_native
  endpoint_url: http://127.0.0.1:{PROXY_PORT}
  region: us-east-1
  force_path_style: true
  verify_tls: false
  bucket: archive
  prefix: "{self.prefix}/"
  create_bucket: false
  require_strong_verification: true
""")

    def env(self):
        e = super().env()
        e.update(AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, AWS_REGION="us-east-1")
        return e


class Vtop:
    name = "vtop"

    def __init__(self, root: Path, topic: str, tag: str):
        self.topic, self.prefix = topic, tag
        self.group = f"rq4-vtop-{tag}"
        self.run = VtopS3Run(root, topic, self.group, tag)

    def start(self):
        self.run.start()

    def kill(self):
        self.run.kill()

    def stop(self):
        self.run.stop()

    def restart_s(self):
        return 0.0

    def committed(self):
        return group_committed(self.group, self.topic)

    def intervals(self, truth):
        """[(partition, start, end_exclusive, intact)] from VTOP's manifests and objects in MinIO."""
        c, out = s3(), []
        for page in c.get_paginator("list_objects_v2").paginate(Bucket="archive", Prefix=self.prefix + "/"):
            for o in page.get("Contents", []):
                if not o["Key"].endswith(".manifest.json"):
                    continue
                m = json.loads(c.get_object(Bucket="archive", Key=o["Key"])["Body"].read())
                sp = m["source_progress"]
                p, end = int(sp["partition"]), int(sp["end_offset"])
                okey = m["object"]["uri"].split("/", 3)[3]
                try:
                    blob = c.get_object(Bucket="archive", Key=okey)["Body"].read()
                    lines = zstandard.ZstdDecompressor().decompress(blob, max_output_size=1 << 31).split(b"\n")
                except Exception:
                    out.append((p, None, end + 1, False)); continue
                if lines and lines[-1] == b"":
                    lines = lines[:-1]
                s = end - len(lines) + 1  # the manifest under-states multi-read starts (F1)
                out.append((p, s, end + 1, s >= 0 and lines == truth[p][s:end + 1]))
        return out


class Confluent:
    name = "confluent"

    def __init__(self, root: Path, topic: str, tag: str, flush_size: int):
        self.topic, self.tag, self.flush = topic, tag, flush_size
        self.conn = f"rq4-s3-{tag}"
        self.group = f"connect-{self.conn}"
        root.mkdir(parents=True, exist_ok=True)
        self.cfg = {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector", "tasks.max": "1", "topics": topic,
            "s3.bucket.name": "confluent", "store.url": f"http://host.docker.internal:{PROXY_PORT}",
            "s3.path.style.access.enabled": "true", "s3.region": "us-east-1", "topics.dir": tag,
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
            "format.bytearray.separator": "\n", "s3.part.size": "5242880",
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            "flush.size": str(flush_size), "schema.compatibility": "NONE",
            "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "consumer.override.auto.offset.reset": "earliest", "behavior.on.null.values": "ignore"}
        (root / "connector.json").write_text(json.dumps(self.cfg, indent=1))
        self._restart = []

    def start(self):
        if not self._restart:
            connect_api("PUT", f"/connectors/{self.conn}/config", self.cfg)
            self._restart.append(0.0)
        else:
            self._restart.append(round(connect_start(), 2))

    def kill(self):
        sh("docker", "kill", "-s", "KILL", CONNECT)

    def stop(self):
        try:
            connect_api("DELETE", f"/connectors/{self.conn}")
        except Exception:
            pass

    def restart_s(self):
        return self._restart[-1] if self._restart else 0.0

    def committed(self):
        return group_committed(self.group, self.topic)

    def intervals(self, truth):
        c, out = s3(), []
        for page in c.get_paginator("list_objects_v2").paginate(Bucket="confluent", Prefix=self.tag + "/"):
            for o in page.get("Contents", []):
                name = o["Key"].rsplit("/", 1)[-1]          # <topic>+<partition>+<startOffset>.bin
                stem = name.rsplit(".", 1)[0]
                _, p_, s_ = stem.rsplit("+", 2)
                p, s = int(p_), int(s_)
                lines = c.get_object(Bucket="confluent", Key=o["Key"])["Body"].read().split(b"\n")
                if lines and lines[-1] == b"":
                    lines = lines[:-1]
                out.append((p, s, s + len(lines), lines == truth[p][s:s + len(lines)]))
        return out


# ------------------------------------------------------------------ C23: Vector and Fluent Bit

_INDEX: dict[int, dict[bytes, list[tuple[int, int]]]] = {}


def content_index(truth) -> dict[bytes, list[tuple[int, int]]]:
    """line -> [(partition, offset)] for one topic's ground truth (cached per topic)."""
    if id(truth) not in _INDEX:
        idx: dict[bytes, list[tuple[int, int]]] = {}
        for p, values in truth.items():
            for o, v in enumerate(values):
                idx.setdefault(v, []).append((p, o))
        _INDEX[id(truth)] = idx
    return _INDEX[id(truth)]


def locate(blob: bytes, truth) -> tuple[list[tuple[int, int, int, bool]], dict]:
    """Decompose one newline-separated object into runs of consecutive topic records.

    Vector and Fluent Bit write raw record values without their offsets, so each line is located in
    the topic by content: a line extends an open run of its partition when it equals that run's
    next record, otherwise it starts a new run at the offset where the topic holds that line (on a
    duplicate line, the candidate whose next record equals the object's next line wins). Every run
    of an object that has any line not found in the topic is reported not intact, one interval per
    partition, so a damaged object counts once per partition it holds. Returns intervals in the
    oracle's (partition, start, end_exclusive, intact) form and per-object statistics.
    """
    idx = content_index(truth)
    lines = blob.split(b"\n")
    if lines and lines[-1] == b"":
        lines = lines[:-1]
    runs: list[list[int]] = []
    open_: dict[int, int] = {}
    unmatched = ambiguous = 0
    for i, x in enumerate(lines):
        hit = False
        for p, ri in open_.items():
            r = runs[ri]
            if r[2] < len(truth[p]) and truth[p][r[2]] == x:
                r[2] += 1
                hit = True
                break
        if hit:
            continue
        cands = idx.get(x)
        if not cands:
            unmatched += 1
            continue
        p, o = cands[0]
        if len(cands) > 1:
            ambiguous += 1
            nxt = lines[i + 1] if i + 1 < len(lines) else None
            for q, oo in cands:
                if nxt is not None and oo + 1 < len(truth[q]) and truth[q][oo + 1] == nxt:
                    p, o = q, oo
                    break
        runs.append([p, o, o + 1])
        open_[p] = len(runs) - 1
    stats = {"lines": len(lines), "unmatched_lines": unmatched, "ambiguous_lines": ambiguous, "runs": len(runs)}
    if unmatched:
        span: dict[int, list[int]] = {}
        for p, s_, e_ in runs:
            a = span.setdefault(p, [s_, e_])
            a[0], a[1] = min(a[0], s_), max(a[1], e_)
        return [(p, a[0], a[1], False) for p, a in span.items()] or [(0, None, None, False)], stats
    return [(p, s_, e_, True) for p, s_, e_ in runs], stats


class ContainerSink:
    """A sink that runs as one Docker container per trial and writes raw lines to its own bucket."""
    name = image = container = bucket = ""

    def __init__(self, root: Path, topic: str, tag: str):
        self.root, self.topic, self.tag = root.resolve(), topic, tag   # docker -v needs an absolute path
        self.group = f"rq4-{self.name}-{tag}"
        self.root.mkdir(parents=True, exist_ok=True)
        self._restart: list[float] = []
        self.object_stats: list[dict] = []
        self.write_config()

    def write_config(self):
        raise NotImplementedError

    def run_args(self) -> list[str]:
        raise NotImplementedError

    def start(self):
        t0 = time.time()
        if not self._restart:
            sh("docker", "rm", "-f", self.container)
            sh("docker", "run", "-d", "--name", self.container, "--network", NET,
               "--add-host", "host.docker.internal:host-gateway",
               "-e", f"AWS_ACCESS_KEY_ID={KEY}", "-e", f"AWS_SECRET_ACCESS_KEY={SECRET}",
               *self.run_args(), check=True)
            self._restart.append(0.0)
        else:
            sh("docker", "start", self.container, check=True)
            self._restart.append(round(time.time() - t0, 2))

    def kill(self):
        sh("docker", "kill", "-s", "KILL", self.container)

    def stop(self):
        log = sh("docker", "logs", self.container)
        (self.root / f"{self.name}.log").write_text(log.stdout + log.stderr)
        sh("docker", "rm", "-f", self.container)

    def restart_s(self):
        return self._restart[-1] if self._restart else 0.0

    def committed(self):
        return group_committed(self.group, self.topic)

    def listing(self) -> list[dict]:
        c, out = s3(), []
        for page in c.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.tag + "/"):
            out += page.get("Contents", [])
        return out

    def intervals(self, truth):
        c, out, self.object_stats = s3(), [], []
        for o in self.listing():
            blob = c.get_object(Bucket=self.bucket, Key=o["Key"])["Body"].read()
            ivs, st = locate(blob, truth)
            out += ivs
            self.object_stats.append({"key": o["Key"].rsplit("/", 1)[-1], "bytes": len(blob), **st,
                                      "intact": all(iv[3] for iv in ivs)})
        return out


class VectorSink(ContainerSink):
    """Vector: kafka source -> aws_s3 sink, end-to-end acknowledgements on (memory buffer)."""
    name, image, container, bucket = "vector", VECTOR_IMAGE, VECTOR, "vector"

    def write_config(self):
        self.cfg = self.root / "vector.yaml"
        self.cfg.write_text(f"""# generated by exp_baseline.py (C23)
data_dir: /var/lib/vector
sources:
  kafka_in:
    type: kafka
    bootstrap_servers: "{KAFKA}:29092"
    group_id: "{self.group}"
    topics: ["{self.topic}"]
    auto_offset_reset: earliest
    decoding:
      codec: bytes
sinks:
  s3_out:
    type: aws_s3
    inputs: [kafka_in]
    bucket: {self.bucket}
    region: us-east-1
    endpoint: "http://host.docker.internal:{PROXY_PORT}"
    force_path_style: true
    key_prefix: "{self.tag}/"
    filename_time_format: "%s"
    filename_append_uuid: true
    filename_extension: log
    compression: none
    encoding:
      codec: text
    framing:
      method: newline_delimited
    batch:
      max_events: 40000
      max_bytes: 100000000
      timeout_secs: 5
    acknowledgements:
      enabled: true
""")

    def run_args(self):
        return ["-v", f"{self.cfg}:/etc/vector/vector.yaml:ro", self.image, "--config", "/etc/vector/vector.yaml"]


class FluentBitSink(ContainerSink):
    """Fluent Bit: kafka input (filesystem storage) -> s3 output (PutObject, log_key payload)."""
    name, image, container, bucket = "fluentbit", FLUENTBIT_IMAGE, FLUENTBIT, "fluentbit"

    def write_config(self):
        self.cfg = self.root / "fluent-bit.conf"
        self.cfg.write_text(f"""# generated by exp_baseline.py (C23)
[SERVICE]
    flush           1
    log_level       info
    storage.path    /tmp/flb-storage/
    storage.sync    full
    storage.checksum on

[INPUT]
    name            kafka
    tag             rq4
    brokers         {KAFKA}:29092
    topics          {self.topic}
    group_id        {self.group}
    format          none
    storage.type    filesystem
    rdkafka.auto.offset.reset earliest

[OUTPUT]
    name            s3
    match           *
    bucket          {self.bucket}
    region          us-east-1
    endpoint        http://host.docker.internal:{PROXY_PORT}
    use_put_object  On
    send_content_md5 On
    total_file_size 4M
    upload_timeout  10s
    store_dir       /tmp/fluent-bit/s3
    s3_key_format   /{self.tag}/%Y%m%dT%H%M%S-$UUID.log
    log_key         payload
""")

    def run_args(self):
        return ["-v", f"{self.cfg}:/fluent-bit/etc/fluent-bit.conf:ro", self.image]


SINKS = {"vector": VectorSink, "fluentbit": FluentBitSink}


# ------------------------------------------------------------------ oracle

def judge(intervals, truth, committed, target=None) -> dict:
    rep, unsafe = {"partitions": {}}, 0
    bad = [iv for iv in intervals if not iv[3]]
    for p, values in truth.items():
        good = sorted((s, e) for q, s, e, ok in intervals if q == p and ok and s is not None)
        prefix, reach, dup, covered = 0, -1, 0, 0
        for s, e in good:
            if s <= prefix:
                prefix = max(prefix, e)
            if s < reach:
                dup += 1
            reach = max(reach, e)
            covered += e - s
        c = committed.get(p, 0)
        unsafe += c > prefix
        # committed records with no intact copy anywhere in the archive
        covered_below, cur = 0, 0
        for s, e in good:
            s, e = max(s, cur), min(e, c)
            if e > s:
                covered_below += e - s
                cur = e
        lost_committed = min(c, len(values)) - covered_below
        goal = (target or {}).get(p, len(values))
        rep["partitions"][p] = {"records": len(values), "target": goal, "committed": c, "intact_prefix": prefix,
                                "complete": prefix >= goal, "duplicate_objects": dup,
                                "unarchived_below_commit": max(0, min(c, len(values)) - prefix),
                                "lost_committed_records": lost_committed,
                                "duplicate_records": covered - min(prefix, len(values)) if prefix >= len(values) else None}
    rep.update(unsafe_partitions=unsafe, bad_objects=len(bad), objects=len(intervals))
    return rep


# ------------------------------------------------------------------ trials

def trial(out: Path, system: str, topic: str, fault: str, idx: int, truth, rng: random.Random, kills: int,
          drain_timeout: float, settle_s: float = 30.0) -> dict:
    tag = f"{system}-{topic}-{fault}-t{idx:02d}-{int(time.time())}"
    root = out / "raw" / tag
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    total = {p: len(v) for p, v in truth.items()}
    if system == "confluent":   # no time-based rotation: the tail below flush.size is never written
        total = {p: n - n % 40_000 for p, n in total.items()}
    nth = rng.randint(2, 8) if fault in ("corrupt", "lost") else 0
    proxy = Proxy(fault if fault in ("corrupt", "lost") else "none", nth, root / "proxy-events.jsonl")
    if system in SINKS:
        sut = SINKS[system](root, topic, tag)
    else:
        sut = Vtop(root, topic, tag) if system == "vtop" else Confluent(root, topic, tag, flush_size=40_000)

    def drained():
        c = sut.committed()
        return all(c.get(p, 0) >= n for p, n in total.items())

    # C23 sinks: a committed offset at the target does not imply the archive is written (a sink may
    # commit on ingest), so their run ends when the commit reached the target AND the archive holds
    # at least the topic's bytes, or, failing that, when the object listing has not changed for
    # settle_s seconds after the commit reached the target. The first time the commit is seen at the
    # target, the oracle is also run once ("at_target"); it reads the archive after the commit, so it
    # can only under-state a gap between commit and archive.
    truth_bytes = sum(len(v) + 1 for vals in truth.values() for v in vals)
    extra = {"at_target": None, "drain_end": None}

    def finished_new():
        c = sut.committed()
        if not all(c.get(p, 0) >= n for p, n in total.items()):
            return False
        if extra["at_target"] is None:
            o = judge(sut.intervals(truth), truth, c, total)
            ps = o["partitions"].values()
            extra["at_target"] = {"t_s": round(time.time() - t_begin, 2), "committed": c,
                                  "unsafe": o["unsafe_partitions"],
                                  "lost_committed_records": sum(q["lost_committed_records"] for q in ps),
                                  "missing_records": sum(max(0, q["target"] - q["intact_prefix"]) for q in ps),
                                  "s3_objects": len(sut.object_stats)}
        lst = sut.listing()
        extra["listing"] = (len(lst), sum(x["Size"] for x in lst))
        return extra["listing"][1] >= truth_bytes

    finished = finished_new if system in SINKS else drained

    events = []
    t_begin = time.time()
    sut.start()
    if fault == "kill":
        for life in range(kills):
            t0 = time.time()
            life_len = rng.uniform(1.0, 5.0)
            while time.time() - t0 < life_len and not finished():
                time.sleep(0.05)
            if finished():
                break
            sut.kill()
            o = judge(sut.intervals(truth), truth, sut.committed())
            events.append({"life": life, "ran_s": round(time.time() - t0, 3), "unsafe": o["unsafe_partitions"],
                           "committed": sut.committed()})
            sut.start()
            events[-1]["restart_s"] = sut.restart_s()
    t0 = time.time()
    if system in SINKS:
        last, changed = None, time.time()
        while True:
            if finished_new():
                extra["drain_end"] = "archive_holds_topic_bytes"
                break
            if extra["at_target"] is not None:
                if extra.get("listing") != last:
                    last, changed = extra.get("listing"), time.time()
                elif time.time() - changed >= settle_s:
                    extra["drain_end"] = f"listing_unchanged_{settle_s:.0f}s"
                    break
            if time.time() - t0 >= drain_timeout:
                extra["drain_end"] = "timeout"
                break
            time.sleep(0.5)
    else:
        while time.time() - t0 < drain_timeout and not drained():
            time.sleep(0.2)
    elapsed = time.time() - t_begin
    time.sleep(2.0)
    committed = sut.committed()
    o = judge(sut.intervals(truth), truth, committed, total)
    if system == "vtop":
        sut.stop()
        states = sut.run.ledger_states()
    else:
        sut.stop()
        states = None
    proxy.stop()
    parts = o["partitions"].values()
    rec = {"system": system, "topic": topic, "fault": fault, "trial": idx, "nth_faulted_write": nth,
           "proxy_events": [e for e in proxy.events() if "fault" in e or "error" in e],
           "kills": len(events), "events": events,
           "unsafe_at_kill": sum(e["unsafe"] for e in events), "unsafe_final": o["unsafe_partitions"],
           "unarchived_below_commit": sum(p["unarchived_below_commit"] for p in parts),
           "lost_committed_records": sum(p["lost_committed_records"] for p in parts),
           "bad_objects_final": o["bad_objects"], "objects": o["objects"],
           "complete": all(p["complete"] for p in parts),
           "missing_records": sum(max(0, p["target"] - p["intact_prefix"]) for p in parts),
           "records_target": sum(total.values()),
           "duplicate_objects": sum(p["duplicate_objects"] for p in parts),
           "duplicate_records": sum(p["duplicate_records"] or 0 for p in parts),
           "records": sum(total.values()), "elapsed_s": round(elapsed, 2), "drained": drained(),
           "committed_final": committed, "ledger_states": states}
    if system in SINKS:
        st = sut.object_stats
        rec.update(at_target=extra["at_target"], drain_end=extra["drain_end"], settle_s=settle_s,
                   s3_objects=len(st), s3_objects_bad=sum(not x["intact"] for x in st),
                   unmatched_lines=sum(x["unmatched_lines"] for x in st),
                   ambiguous_lines=sum(x["ambiguous_lines"] for x in st), object_stats=st)
    if system == "vtop":
        shutil.rmtree(root / "raw" / "work", ignore_errors=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--systems", default="vtop,confluent")
    ap.add_argument("--faults", default="corrupt,lost,kill")
    ap.add_argument("--topics", default="ssh-p1")
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--kills", type=int, default=3)
    ap.add_argument("--drain-timeout", type=float, default=600)
    ap.add_argument("--seed", type=int, default=20261111)
    ap.add_argument("--pause-while", default="", help="before each trial, wait while `pgrep -f` matches this "
                    "pattern (another timing-sensitive campaign on the host); pauses are logged in run.json")
    ap.add_argument("--quiet-polls", type=int, default=5, help="consecutive absent 60 s polls before resuming")
    ap.add_argument("--restart-infra", action="store_true", help="with --no-infra: docker start the stopped Kafka and "
                    "MinIO containers first and check the topics")
    ap.add_argument("--settle", type=float, default=30.0, help="C23 sinks: seconds of unchanged listing that end a drain")
    ap.add_argument("--no-infra", action="store_true", help="reuse running containers and topics")
    ap.add_argument("--cells", default="", help="topic:fault:trials,... (overrides --topics/--faults/--trials)")
    ap.add_argument("--index-offset", type=int, default=0, help="first trial index (top-up runs append to trials.jsonl)")
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(a.seed)
    work = out / "raw"; work.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    systems = a.systems.split(",")
    if not a.no_infra:
        infra_up(systems)
    import pickle
    cache = work / "truth-cache.pkl"   # raw/ is git-ignored
    if a.no_infra and cache.exists():
        truth = pickle.loads(cache.read_bytes())
        if a.restart_infra:
            infra_restart(truth)
    else:
        names = sorted({c.split(":")[0] for c in a.cells.split(",")}) if a.cells else a.topics.split(",")
        truth = prepare_topics(work, names)
        cache.write_bytes(pickle.dumps(truth))
    def digest(img):
        return sh("docker", "image", "inspect", img, "--format", "{{.Id}}").stdout.strip()
    meta = {"experiment": "E-RQ4-B", "vtop_commit": VTOP_COMMIT, "host": host_info(), "seed": a.seed,
            "images": {i: digest(i) for i in (KAFKA_IMAGE, MINIO_IMAGE)
                       + ((CONNECT_IMAGE,) if "confluent" in systems else ())
                       + tuple(SINKS[x].image for x in systems if x in SINKS)},
            "minio": "RELEASE.2025-09-07T16-13-09Z (commit 07c3a429, built from source)",
            "faults": a.faults, "systems": a.systems,
            "topics": {t: {"dataset": d, "limit": l, "partitions": p} for t, (d, l, p) in TOPICS.items()},
            "prepare_s": round(time.time() - t0, 1), "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    if "confluent" in systems:
        meta["confluent_s3_sink"] = S3SINK
    if any(x in SINKS for x in systems):
        meta.update(settle_s=a.settle, drain_timeout_s=a.drain_timeout, kills_per_trial=a.kills,
                    sink_configs={x: SINKS[x].__doc__ for x in systems if x in SINKS})
    topup = (out / "run.json").exists()
    if topup:   # a top-up keeps the campaign's metadata and records itself
        prev = json.loads((out / "run.json").read_text())
        prev.setdefault("top_ups", []).append({k: meta[k] for k in ("seed", "started_at")} | {"cells": a.cells,
                                                "index_offset": a.index_offset, "systems": a.systems})
        meta = prev
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    cells = [tuple(c.split(":")) for c in a.cells.split(",")] if a.cells else \
        [(t, f, str(a.trials)) for t in a.topics.split(",") for f in a.faults.split(",")]
    if not topup:
        meta["cells"] = cells
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    with open(out / "trials.jsonl", "a") as fh:
        for topic, fault, n in cells:
                for system in a.systems.split(","):
                    for i in range(a.index_offset, a.index_offset + int(n)):
                        if a.pause_while and sh("pgrep", "-f", a.pause_while).returncode == 0:
                            pause = {"before": f"{system} {topic} {fault} t{i:02d}", "paused_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
                            print(f"pause: {a.pause_while} running", flush=True)
                            sh("docker", "stop", KAFKA, MINIO)     # leave the host quiet while paused
                            # the other campaign runs as back-to-back invocations with short gaps, so resume
                            # only after the pattern has been absent at a.quiet_polls consecutive 60 s polls
                            quiet = 0
                            while quiet < a.quiet_polls:
                                time.sleep(60)
                                quiet = 0 if sh("pgrep", "-f", a.pause_while).returncode == 0 else quiet + 1
                            infra_restart(truth)
                            pause["resumed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
                            print(f"resume at {pause['resumed_at']}", flush=True)
                            meta.setdefault("pauses", []).append(pause)
                            (out / "run.json").write_text(json.dumps(meta, indent=1))
                        rec = trial(out, system, topic, fault, i, truth[topic], rng, a.kills, a.drain_timeout,
                                    a.settle)
                        fh.write(json.dumps(rec) + "\n"); fh.flush()
                        print(f"{system:9s} {topic} {fault:7s} t{i:02d} unsafe={rec['unsafe_at_kill']}/{rec['unsafe_final']} "
                              f"lost_committed={rec['lost_committed_records']} bad={rec['bad_objects_final']} "
                              f"complete={rec['complete']} dupobj={rec['duplicate_objects']} {rec['elapsed_s']}s", flush=True)
    if topup:
        meta["top_ups"][-1]["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    else:
        meta["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (out / "run.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
