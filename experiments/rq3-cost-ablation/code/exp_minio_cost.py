#!/usr/bin/env python3
"""E-RQ3-A networked MinIO cost campaign for VTOP v0.7.0.

Runs the continuous-engine RQ3 variants against the pinned RQ4 MinIO image using
VTOP's native S3 backend. No fault proxy and no injected faults. Each run uses an
isolated S3 prefix and ledger. Results are resumable: existing dataset/variant/rep
keys in runs.jsonl are skipped.

This driver measures engine timings, throughput, CPU/RSS, stored object bytes,
manifest bytes, and batch metrics. It does not infer unobserved HTTP request counts.

Optional emulated latency (C25): with --proxy, the engine's S3 traffic (PUT, HEAD,
GET) goes through a pinned Toxiproxy container on the same Docker network as MinIO;
--latency-ms R adds latency toxics of R/2 ms in each direction (jitter 0), so the
added round-trip time is R. The harness's own S3 calls (cleanup, listing, oracle)
stay on the direct endpoint. Without --proxy the behaviour is unchanged.
--bandwidth-kbps K adds Toxiproxy bandwidth toxics of K KB/s in each direction
(emulated low-bandwidth, high-latency link); the cap is verified with timed PUT/GET.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "lib"))
sys.path.insert(0, str(HERE.parent))
from vtoplab import DATASET_FILES, VTOP_COMMIT, Run, Variant, _decompress, host_info  # noqa: E402
from exp_cost import STAGES, VARIANTS, drive_run  # noqa: E402

NAME = "rq3-minio"
NET = "rq3net"
PORT = 19000
IMAGE = "vtop-minio:RELEASE.2025-09-07T16-13-09Z"
KEY, SECRET = "rq3lab", "rq3lab-secret"
ENDPOINT = f"http://127.0.0.1:{PORT}"
ENGINE_ENDPOINT = ENDPOINT  # replaced by the proxy endpoint under --proxy

PROXY_NAME = "rq3-toxiproxy"
PROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0"
PROXY_PORT = 19100      # host port of the proxied S3 listener
PROXY_API_PORT = 18474  # host port of the Toxiproxy API and /metrics
PROXY_API = f"http://127.0.0.1:{PROXY_API_PORT}"
PROXY_ENDPOINT = f"http://127.0.0.1:{PROXY_PORT}"


def sh(*args, check=False):
    return subprocess.run(list(args), capture_output=True, text=True, check=check)


def s3(endpoint=ENDPOINT):
    return boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id=KEY, aws_secret_access_key=SECRET,
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))


def wait_ready(timeout=90):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            s3().list_buckets()
            return
        except Exception as e:
            last = e
            time.sleep(1)
    raise RuntimeError(f"MinIO did not become ready: {last}")


def infra_up():
    sh("docker", "rm", "-f", NAME)
    sh("docker", "network", "create", NET)
    sh("docker", "run", "-d", "--name", NAME, "--network", NET,
       "-p", f"{PORT}:9000", "-e", f"MINIO_ROOT_USER={KEY}",
       "-e", f"MINIO_ROOT_PASSWORD={SECRET}", IMAGE, "server", "/data", check=True)
    wait_ready()
    buckets = {b["Name"] for b in s3().list_buckets().get("Buckets", [])}
    if "archive" not in buckets:
        s3().create_bucket(Bucket="archive")


def infra_down():
    sh("docker", "rm", "-f", NAME)


# ------------------------------------------------------------ Toxiproxy (C25)

def proxy_api(method, path, body=None):
    import urllib.request
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(PROXY_API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        text = resp.read().decode()
    return json.loads(text) if text.strip().startswith(("{", "[")) else text


def proxy_toxics():
    return proxy_api("GET", "/proxies/s3/toxics")


def proxy_bytes():
    """Toxiproxy per-direction byte counters for the S3 proxy (Prometheus /metrics)."""
    import urllib.request
    with urllib.request.urlopen(PROXY_API + "/metrics", timeout=10) as resp:
        text = resp.read().decode()
    out = {}
    for line in text.splitlines():
        if line.startswith("toxiproxy_proxy_sent_bytes_total{") and 'proxy="s3"' in line:
            direction = line.split('direction="', 1)[1].split('"', 1)[0]
            out[direction] = int(float(line.rsplit(" ", 1)[1]))
    return out


def time_head(endpoint, n=50, warmup=5):
    """Median/p05/p95 latency (ms) of n HEAD requests on a kept-alive boto3 client."""
    import statistics
    c = s3(endpoint)
    for _ in range(warmup):
        c.head_object(Bucket="archive", Key="latency-probe")
    xs = []
    for _ in range(n):
        t0 = time.perf_counter()
        c.head_object(Bucket="archive", Key="latency-probe")
        xs.append((time.perf_counter() - t0) * 1000)
    xs.sort()
    return {"n": n, "median_ms": round(statistics.median(xs), 3),
            "p05_ms": round(xs[int(0.05 * n)], 3), "p95_ms": round(xs[int(0.95 * n) - 1], 3)}


def link_utilization(run, objects, variant, steady_s, steady_bytes, bandwidth_kbps):
    """Payload-based link utilization over the steady interval [engine start, steady commit].

    Toxiproxy's byte counters are updated only when a connection closes, so they cannot be
    read at the steady commit. Instead: the steady interval ends at the commit of the
    batch that brought committed progress to steady_bytes, so every batch ending at or before
    steady_bytes was uploaded, and for BLAKE3 read back, inside it. Upload bytes = their stored object +
    manifest bytes (S3 listing sizes); download bytes (BLAKE3) = their object bytes.
    HTTP headers, HEADs and later batches' in-flight bytes are excluded (lower bound).
    """
    sizes = {o["Key"]: o["Size"] for o in objects}
    con = sqlite3.connect(f"file:{run.db}?mode=ro", uri=True, timeout=30)
    rows = con.execute("SELECT progress_end_json, object_uri, manifest_uri FROM batches").fetchall()
    con.close()
    steady_rows = [r for r in rows if int(json.loads(r[0])["end_byte"]) <= steady_bytes]
    key = lambda uri: uri.split("/", 3)[3] if uri else None
    obj = sum(sizes.get(key(r[1]), 0) for r in steady_rows)
    man = sum(sizes.get(key(r[2]), 0) for r in steady_rows)
    cap = bandwidth_kbps * 1000  # Toxiproxy bandwidth rate is KB/s (1000 bytes)
    out = {"basis": "stored payload bytes of batches ending at or before steady_bytes (lower bound)",
           "steady_s": round(steady_s, 4), "cap_bytes_per_s": cap, "steady_batches": len(steady_rows),
           "upload_bytes": obj + man, "upload": round((obj + man) / (cap * steady_s), 4)}
    if variant == "blake3":
        out.update({"download_bytes": obj, "download": round(obj / (cap * steady_s), 4)})
    return out


def time_transfer(nbytes=8 * 2**20):
    """Time one PUT and one GET of a known-size object through the proxy (kept-alive client)."""
    import os
    body = os.urandom(nbytes)
    c = s3(PROXY_ENDPOINT)
    c.head_object(Bucket="archive", Key="latency-probe")  # open the connection first
    t0 = time.perf_counter()
    c.put_object(Bucket="archive", Key="bandwidth-probe", Body=body)
    put_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    got = c.get_object(Bucket="archive", Key="bandwidth-probe")["Body"].read()
    get_s = time.perf_counter() - t0
    assert got == body
    s3().delete_object(Bucket="archive", Key="bandwidth-probe")
    return {"object_bytes": nbytes, "put_s": round(put_s, 3), "get_s": round(get_s, 3),
            "put_kbytes_per_s": round(nbytes / put_s / 1000, 1), "get_kbytes_per_s": round(nbytes / get_s / 1000, 1),
            "put_mbit_per_s": round(nbytes * 8 / put_s / 1e6, 3), "get_mbit_per_s": round(nbytes * 8 / get_s / 1e6, 3)}


def proxy_up(latency_ms, bandwidth_kbps=None):
    """Start Toxiproxy in front of MinIO, add symmetric latency, and measure the added delay."""
    sh("docker", "rm", "-f", PROXY_NAME)
    sh("docker", "run", "-d", "--name", PROXY_NAME, "--network", NET,
       "-p", f"{PROXY_PORT}:{PROXY_PORT}", "-p", f"{PROXY_API_PORT}:8474", PROXY_IMAGE,
       "-host=0.0.0.0", "-port=8474", "-proxy-metrics", "-seed=20261111", check=True)
    t0 = time.time()
    while True:
        try:
            proxy_api("GET", "/version")
            break
        except Exception:
            if time.time() - t0 > 30:
                raise
            time.sleep(0.5)
    proxy_api("POST", "/proxies", {"name": "s3", "listen": f"0.0.0.0:{PROXY_PORT}",
                                   "upstream": f"{NAME}:9000", "enabled": True})
    s3().put_object(Bucket="archive", Key="latency-probe", Body=b"x")
    check = {"direct": time_head(ENDPOINT), "proxy_no_toxic": time_head(PROXY_ENDPOINT)}
    half = latency_ms / 2
    for stream in ("upstream", "downstream"):
        proxy_api("POST", "/proxies/s3/toxics", {
            "name": f"latency_{stream}", "type": "latency", "stream": stream, "toxicity": 1.0,
            "attributes": {"latency": int(half) if half == int(half) else half, "jitter": 0}})
    check["proxy_with_toxics"] = time_head(PROXY_ENDPOINT)
    check["added_vs_proxy_no_toxic_median_ms"] = round(
        check["proxy_with_toxics"]["median_ms"] - check["proxy_no_toxic"]["median_ms"], 3)
    check["added_vs_direct_median_ms"] = round(
        check["proxy_with_toxics"]["median_ms"] - check["direct"]["median_ms"], 3)
    if bandwidth_kbps is not None:
        for stream in ("upstream", "downstream"):
            proxy_api("POST", "/proxies/s3/toxics", {
                "name": f"bandwidth_{stream}", "type": "bandwidth", "stream": stream, "toxicity": 1.0,
                "attributes": {"rate": int(bandwidth_kbps)}})
        check["proxy_with_toxics_and_bandwidth"] = time_head(PROXY_ENDPOINT, n=20)
        check["bandwidth_kbps"] = bandwidth_kbps
        check["transfer"] = time_transfer()
    check["toxics"] = proxy_toxics()
    check["proxy_version"] = proxy_api("GET", "/version")
    check["measured_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    s3().delete_object(Bucket="archive", Key="latency-probe")
    return check


def proxy_down():
    sh("docker", "rm", "-f", PROXY_NAME)


def delete_prefix(prefix):
    c = s3()
    for page in c.get_paginator("list_objects_v2").paginate(Bucket="archive", Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            c.delete_objects(Bucket="archive", Delete={"Objects": objects, "Quiet": True})


def list_prefix(prefix):
    out = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket="archive", Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def s3_oracle(run: MinioRun, prefix: str) -> dict:
    """Independent byte-exact coverage check using ledger ranges and stored S3 bytes."""
    c = s3()
    con = sqlite3.connect(f"file:{run.db}?mode=ro", uri=True, timeout=30)
    rows = con.execute("SELECT batch_id, progress_start_json, progress_end_json, manifest_uri, state FROM batches").fetchall()
    con.close()
    source_paths = {str(p): p for p in run.sources.values()}
    coverage, bad, understated, digest_checks = {}, [], 0, 0
    for batch_id, start_json, end_json, manifest_uri, state in rows:
        start, end = json.loads(start_json), json.loads(end_json)
        path = end.get("path") or end.get("spool_path") or end.get("spool_id")
        s0, e0 = int(start["start_byte"]), int(end["end_byte"])
        valid = False
        try:
            manifest = json.loads(c.get_object(Bucket="archive", Key=manifest_uri.split("/", 3)[3])["Body"].read())
            if int(manifest["source_progress"].get("start_byte", s0)) != s0:
                understated += 1
            obj = manifest["object"]
            data = c.get_object(Bucket="archive", Key=obj["uri"].split("/", 3)[3])["Body"].read()
            algorithm = obj.get("checksum_algorithm")
            if algorithm == "sha256":
                digest_checks += 1
                valid = hashlib.sha256(data).hexdigest() == obj["checksum"]
            elif algorithm == "blake3":
                import blake3
                digest_checks += 1
                valid = blake3.blake3(data).hexdigest() == obj["checksum"]
            else:
                valid = True  # size-only ablation: byte comparison below remains mandatory
            raw = _decompress(data, manifest["compression"])
            with open(source_paths[path], "rb") as fh:
                fh.seek(s0)
                expected = fh.read(e0 - s0)
            valid = valid and raw == expected
        except Exception:
            valid = False
        coverage.setdefault(path, []).append((s0, e0, valid))
        if not valid:
            bad.append(batch_id)
    complete = True
    for path, ranges in coverage.items():
        position, source_size = 0, source_paths[path].stat().st_size
        for s0, e0, valid in sorted(ranges):
            if valid and s0 <= position:
                position = max(position, e0)
        complete = complete and position >= source_size
    return {"checked_batches": len(rows), "bad_objects": len(bad),
            "unsafe_commits": sum(1 for row in rows if row[4] == "source_committed" and row[0] in bad),
            "complete": complete, "manifest_start_understated": understated,
            "digest_checks": digest_checks}


class MinioRun(Run):
    def __init__(self, root: Path, variant: Variant, files: dict[str, Path], prefix: str):
        self.prefix = prefix
        super().__init__(root, variant, files)

    def _write_config(self):
        super()._write_config()
        text = self.config.read_text()
        text = text[:text.index("upload:\n")] + f"""upload:
  backend: s3_native
  endpoint_url: {ENGINE_ENDPOINT}
  region: us-east-1
  force_path_style: true
  verify_tls: false
  bucket: archive
  prefix: "{self.prefix}/"
  create_bucket: false
  require_strong_verification: {str(self.v.strong).lower()}
"""
        self.config.write_text(text)

    def env(self):
        e = super().env()
        e.update(AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, AWS_REGION="us-east-1")
        return e


def one(out: Path, ds: str, vn: str, v: Variant, rep: int, keep: bool,
        latency_ms: float | None = None, bandwidth_kbps: float | None = None) -> dict:
    tag = f"{ds}-{vn}-r{rep}" if latency_ms is None else f"lat{latency_ms:g}-{ds}-{vn}-r{rep}"
    if bandwidth_kbps is not None:
        tag = f"lat{latency_ms:g}-bw{bandwidth_kbps:g}-{ds}-{vn}-r{rep}"
    prefix = f"rq3-cost/{tag}"
    root = out / "raw" / f"run-{tag}"
    shutil.rmtree(root, ignore_errors=True)
    delete_prefix(prefix + "/")
    baseline = sh("pgrep", "-fl", "exp_baseline.py")
    # A concurrent `pgrep -fl exp_baseline.py` (e.g. a monitoring loop) is not an experiment.
    active = [line for line in baseline.stdout.splitlines() if line.split()[1:2] != ["pgrep"]]
    if active:
        raise RuntimeError(f"refusing timed run while exp_baseline.py is active: {baseline.stdout}")
    proxy_rec = {}
    if latency_ms is not None:
        import os
        # Python interpreters running the driver, other than this one (shell wrappers whose
        # command line merely mentions the script are not experiments).
        others = [line for line in sh("pgrep", "-fl", "exp_minio_cost.py").stdout.splitlines()
                  if line.split()[0] != str(os.getpid()) and "python" in line.split()[1]]
        if others:
            raise RuntimeError(f"refusing timed run while another exp_minio_cost.py is active: {others}")
        all_toxics = proxy_toxics()
        toxics = {t["stream"]: t["attributes"] for t in all_toxics if t["type"] == "latency"}
        expected = {"latency": latency_ms / 2, "jitter": 0}
        if latency_ms and any(float(toxics.get(s, {}).get(k, -1)) != expected[k]
                              for s in ("upstream", "downstream") for k in expected):
            raise RuntimeError(f"latency toxics not as configured: {toxics}")
        proxy_rec = {"latency_ms": latency_ms, "toxics_checked": toxics,
                     "quiet_check": "no exp_baseline.py and no other exp_minio_cost.py at run start"}
        if bandwidth_kbps is not None:
            bw = {t["stream"]: t["attributes"] for t in all_toxics if t["type"] == "bandwidth"}
            if any(float(bw.get(s, {}).get("rate", -1)) != bandwidth_kbps for s in ("upstream", "downstream")):
                raise RuntimeError(f"bandwidth toxics not as configured: {bw}")
            proxy_rec.update({"bandwidth_kbps": bandwidth_kbps, "bandwidth_checked": bw})
        bytes_before = proxy_bytes()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    run = MinioRun(root, v, {ds: DATASET_FILES[ds]}, prefix)
    size = run.sources[ds].stat().st_size
    try:
        wall, cpu, rss, series = drive_run(run, timeout=1800)
        if bandwidth_kbps is not None:
            proxy_rec["run_over_15_min"] = wall > 900
        if latency_ms is not None:
            bytes_after = proxy_bytes()
            # upstream = client (engine) -> MinIO; downstream = MinIO -> engine
            proxy_rec["proxy_bytes_to_store"] = bytes_after.get("upstream", 0) - bytes_before.get("upstream", 0)
            proxy_rec["proxy_bytes_from_store"] = bytes_after.get("downstream", 0) - bytes_before.get("downstream", 0)
        drained = run.drained()
        metrics = [m.get("metrics", m) for m in run.batch_metrics()]
        objects = list_prefix(prefix + "/")
        data_objects = [o for o in objects if not o["Key"].endswith(".manifest.json")]
        manifests = [o for o in objects if o["Key"].endswith(".manifest.json")]
        steady = series[-2] if len(series) >= 2 else series[-1]
        rec = {
            "mode": "run-minio", "dataset": ds, "variant": vn, "batch_mib": v.max_bytes >> 20,
            "started_at": started_at, "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "quiet_check": "no exp_baseline.py process at run start",
            "rep": rep, "bytes": size, "wall_s": round(wall, 4), "cpu_s": round(cpu, 4),
            "max_rss_mib": round(rss / 2**20, 1),
            "mib_per_s_end_to_end": round(size / 2**20 / wall, 3),
            "steady_bytes": steady[1], "steady_s": round(steady[0], 4),
            "mib_per_s": round(steady[1] / 2**20 / steady[0], 3),
            "commits_observed": len(series), "drained": drained, "states": run.ledger_states(),
            "batches": len(metrics), "stored_object_bytes": sum(o["Size"] for o in data_objects),
            "manifest_bytes": sum(o["Size"] for o in manifests),
            "data_object_count": len(data_objects), "manifest_count": len(manifests),
        }
        for stage in STAGES:
            rec["sum_" + stage] = round(sum(float(m.get(stage, 0) or 0) for m in metrics), 4)
        if bandwidth_kbps is not None:
            proxy_rec["link_utilization"] = link_utilization(run, objects, vn, steady[0], steady[1], bandwidth_kbps)
        rec.update(proxy_rec)
        if not drained and bandwidth_kbps is not None:
            # low-bandwidth level: record the undrained run (1800 s limit) and continue the campaign
            rec["not_drained_after_s"] = round(wall, 1)
            return rec
        if not drained:
            raise RuntimeError(f"run did not drain: {tag}: {rec['states']}")
        if not manifests or rec["batches"] != len(manifests):
            raise RuntimeError(f"batch/manifest mismatch: {tag}: batches={rec['batches']} manifests={len(manifests)}")
        if rep == 0:
            rec["oracle"] = s3_oracle(run, prefix)
            if rec["oracle"]["bad_objects"] or rec["oracle"]["unsafe_commits"] or not rec["oracle"]["complete"]:
                raise RuntimeError(f"S3 oracle failed: {tag}: {rec['oracle']}")
        return rec
    finally:
        if not keep:
            delete_prefix(prefix + "/")
            shutil.rmtree(root / "raw", ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--rep-offset", type=int, default=0)
    ap.add_argument("--datasets", default="linux")
    ap.add_argument("--variants", default="sha256")
    ap.add_argument("--seed", type=int, default=20261111)
    ap.add_argument("--no-infra", action="store_true")
    ap.add_argument("--keep-infra", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep S3 objects and run raw directories")
    ap.add_argument("--proxy", action="store_true",
                    help="route the engine's S3 traffic through a pinned Toxiproxy container (C25)")
    ap.add_argument("--bandwidth-kbps", type=float, default=None,
                    help="Toxiproxy bandwidth toxic rate (KB/s) in each direction; requires --proxy")
    ap.add_argument("--latency-ms", type=float, default=None,
                    help="added round-trip time in ms (half per direction, jitter 0); requires --proxy")
    a = ap.parse_args()
    if a.latency_ms is not None and not a.proxy:
        ap.error("--latency-ms requires --proxy")
    if a.bandwidth_kbps is not None and not a.proxy:
        ap.error("--bandwidth-kbps requires --proxy")
    if a.proxy and a.no_infra:
        ap.error("--proxy starts its own infrastructure; do not combine with --no-infra")
    latency = (a.latency_ms or 0.0) if a.proxy else None
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    latency_check = None
    if not a.no_infra:
        infra_up()
    try:
        if a.proxy:
            global ENGINE_ENDPOINT
            ENGINE_ENDPOINT = PROXY_ENDPOINT
            latency_check = proxy_up(latency, a.bandwidth_kbps)
        meta = {
            "experiment": "E-RQ3-A (networked MinIO, continuous engine)", "vtop_commit": VTOP_COMMIT,
            "host": host_info(), "backend": "s3_native", "endpoint": ENDPOINT,
            "network_path": "host -> Docker MinIO; no fault proxy; no injected faults",
            "minio": "RELEASE.2025-09-07T16-13-09Z (commit 07c3a429)",
            "minio_image_id": sh("docker", "image", "inspect", IMAGE, "--format", "{{.Id}}").stdout.strip(),
            "seed": a.seed, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "variants": {k: VARIANTS[k].__dict__ for k in a.variants.split(",")},
            "http_request_counts_observed": False,
        }
        if a.proxy:
            meta.update({
                "experiment": "E-RQ3-A (S3 over a local Docker network with emulated latency, continuous engine)",
                "engine_endpoint": PROXY_ENDPOINT,
                "network_path": ("host engine -> Toxiproxy container -> MinIO container on one Docker network; "
                                 "emulated latency (symmetric latency toxics, jitter 0); no injected faults; "
                                 "harness cleanup/listing/oracle use the direct endpoint"),
                "toxiproxy_image": PROXY_IMAGE,
                "toxiproxy_image_id": sh("docker", "image", "inspect", PROXY_IMAGE, "--format", "{{.Id}}").stdout.strip(),
                "bandwidth_limited": False,
            })
        if a.proxy:
            meta["engine_upload_concurrency"] = {
                "batching.max_concurrent_batches": VARIANTS[a.variants.split(",")[0]].concurrency,
                "batching.adaptive_width.enabled": False,
                "upload.transports tuning (max_concurrency)": "not set",
                "resolved_upload_ceiling": VARIANTS[a.variants.split(",")[0]].concurrency,
                "multipart_threshold_bytes": "engine default 8 MiB (not set)",
                "multipart_max_parallelism": "engine default 4 (not set)",
                "source": "generated config (research/experiments/lib/vtoplab.py) and vtop-core/src/config.rs at 6c8f386",
                "same_as_clean_campaign": True,
            }
        if not (out / "run.json").exists():
            (out / "run.json").write_text(json.dumps(meta, indent=1))
        cells = [(d, vn, VARIANTS[vn], r) for d in a.datasets.split(",")
                 for vn in a.variants.split(",") for r in range(a.rep_offset, a.rep_offset + a.reps)]
        random.Random(a.seed + a.rep_offset).shuffle(cells)
        done = set()
        if (out / "runs.jsonl").exists():
            for line in (out / "runs.jsonl").read_text().splitlines():
                r = json.loads(line)
                done.add((r["dataset"], r["variant"], r["rep"], r.get("latency_ms"), r.get("bandwidth_kbps")))
        cells = [c for c in cells if (c[0], c[1], c[3], latency, a.bandwidth_kbps) not in done]
        with open(out / "runs.jsonl", "a") as fh:
            for k, (ds, vn, v, rep) in enumerate(cells, 1):
                rec = one(out, ds, vn, v, rep, a.keep, latency, a.bandwidth_kbps)
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                print(f"[{k}/{len(cells)}] {ds:5s} {vn:16s} r{rep} {rec['mib_per_s']:8.2f} MiB/s "
                      f"verify={rec['sum_verify_ms']:8.1f}ms batches={rec['batches']}", flush=True)
        appended = {"rep_offset": a.rep_offset, "reps": a.reps, "datasets": a.datasets,
                    "variants": a.variants, "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
        if a.proxy:
            appended.update({"latency_ms": latency, "latency_check": latency_check})
            if a.bandwidth_kbps is not None:
                appended["bandwidth_kbps"] = a.bandwidth_kbps
        current = json.loads((out / "run.json").read_text())
        current.setdefault("appended", []).append(appended)
        if a.bandwidth_kbps is not None:
            current["bandwidth_limited"] = "only in appended entries that carry bandwidth_kbps"
        if a.proxy:
            current.setdefault("engine_upload_concurrency", meta["engine_upload_concurrency"])
        (out / "run.json").write_text(json.dumps(current, indent=1))
    finally:
        if not a.no_infra and not a.keep_infra:
            if a.proxy:
                proxy_down()
            infra_down()


if __name__ == "__main__":
    main()
