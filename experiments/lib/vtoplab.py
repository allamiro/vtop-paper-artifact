"""Shared harness for the VTOP experiments.

* ``Run`` lays out one run directory, writes a VTOP config, and drives
  ``vtopctl run`` (start, wait until drained, SIGTERM or SIGKILL).
* ``oracle()`` is the independent correctness oracle. It does **not** use
  VTOP code: it reads every manifest in the object store, re-hashes and
  decompresses the object, and compares the bytes with the source file at
  the manifest's byte range. It then compares archived coverage with the
  committed progress recorded in the ledger.

Only file and syslog-spool sources are handled here (byte-range markers).

Engine build: ``VTOP_BUILD`` selects one of ``BUILDS`` (default v0.7.0, so archived
v0.7.0 runs reproduce unchanged); ``VTOPCTL``/``VTOP_COMMIT`` override the path and
commit. One build per result set (decisions D4/D5). v0.10.x differences handled here:
objects are reused across replays (stable keys, #626), so the oracle counts distinct
objects, not manifests; state files are kept 0600 (#775 refuses looser ones after
v0.10.1); ``power_loss_truncate()`` emulates a power loss faithfully for v0.10.x, which
fsyncs the WAL at VERIFIED and SOURCE_COMMITTED (deleting the whole WAL, the v0.7.0
method, would discard fsynced commits).
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import zstandard

REPO = Path(__file__).resolve().parents[3]
BUILDS = {
    # tag v0.7.0; results dated 20261110-* without -v070 used a8bce8e
    "v0.7.0": (REPO / ".tmp/vtop-engine-v0.7.0/target/release/vtopctl", "6c8f386"),
    # official release archive, SHA-256 checked against the release's SHA256SUMS
    "v0.10.1": (REPO / ".tmp/vtop-engine-v0.10.1-bin/vtop-engine-0.10.1-aarch64-apple-darwin/vtopctl", "44f6619"),
    # decision D5 pin; archive checked against SHA256SUMS and cosign (release.yml@refs/tags/v0.10.2), 2026-11-21
    "v0.10.2": (REPO / ".tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin/vtopctl", "0abf116"),
    # decision D16 re-pin; archive checked against SHA256SUMS (90e807f5…) and cosign (release.yml@refs/tags/v0.10.4), 2026-11-22
    "v0.10.4": (REPO / ".tmp/vtop-engine-v0.10.4-dl/vtop-engine-0.10.4-aarch64-apple-darwin/vtopctl", "be5b419"),
}
VTOP_BUILD = os.environ.get("VTOP_BUILD", "v0.7.0")
if VTOP_BUILD not in BUILDS and not os.environ.get("VTOPCTL"):
    raise SystemExit(f"VTOP_BUILD={VTOP_BUILD!r}: add it to BUILDS or set VTOPCTL and VTOP_COMMIT")
VTOPCTL = Path(os.environ.get("VTOPCTL") or BUILDS[VTOP_BUILD][0])
VTOP_COMMIT = os.environ.get("VTOP_COMMIT") or BUILDS.get(VTOP_BUILD, (None, "unknown"))[1]
DATASETS = REPO / "research/datasets/loghub/raw"
DATASET_FILES = {
    "linux": DATASETS / "Linux/Linux.log",
    "ssh": DATASETS / "SSH/SSH.log",
    "bgl": DATASETS / "BGL/BGL.log",
    "hdfs": DATASETS / "HDFS_v1/HDFS.log",
}


def host_info() -> dict:
    def sysctl(k):
        return subprocess.run(["sysctl", "-n", k], capture_output=True, text=True).stdout.strip()
    return {
        "cpu": sysctl("machdep.cpu.brand_string"),
        "cores": int(sysctl("hw.ncpu") or 0),
        "memory_gb": round(int(sysctl("hw.memsize") or 0) / 2**30),
        "os": "macOS " + subprocess.run(["sw_vers", "-productVersion"], capture_output=True, text=True).stdout.strip(),
        "disk": "Apple SSD AP4096Z (APFS)",
    }


@dataclass
class Variant:
    name: str
    checksum: str = "sha256"          # sha256 | blake3 | none
    compression: str = "zstd"         # zstd | gzip | none
    level: int = 3
    mac: bool = False
    strong: bool = True               # require_strong_verification
    max_bytes: int = 16 * 2**20
    max_records: int = 1_000_000
    max_age_s: int = 2
    concurrency: int = 8


def _predicted_name_len(src: Path, digits: int, engine: str = "lab") -> int:
    import re
    tok = re.sub(r"[^A-Za-z0-9_-]", "_", str(Path(src).resolve()))
    return len(f"vtop-{engine}-{tok}-b-") + 2 * digits + len("-i") + 20 + 1 + 32 + len(".raw.zst") + len(".vtopck")


def _check_object_name_length(src: Path, engine: str = "lab"):
    """v0.10.1 names objects vtop-<engine>-<sanitized full path>-b<start>-<end>-i<inode>-<32 hex>.raw.zst with no
    length bound (batch.rs build_object_name/sanitize_token); over NAME_MAX (255) the localfs upload fails with
    ENAMETOOLONG on every pass and the run never drains (found 2026-11-20). Refuse such inputs up front."""
    n = _predicted_name_len(src, len(str(max(src.stat().st_size, 1))), engine)
    if n > 240:
        raise SystemExit(f"{src}: predicted object file name {n} bytes (> 240, NAME_MAX 255); "
                         "use a shorter results directory (see rq5-cloud/LESSONS.md)")


MAC_KEY = "6b" * 32  # fixed lab key (64 hex chars); not a secret


class Run:
    def __init__(self, root: Path, variant: Variant, files: dict[str, Path], spool: bool = False):
        self.root = Path(root)
        self.v = variant
        self.spool = spool
        self.raw = self.root / "raw"
        self.input = self.raw / ("spool" if spool else "input")
        longest = max((len(n) for n in files), default=0)
        if _predicted_name_len(self.input / ("x" * longest + ".log"), 10) > 240:
            # Object names embed the full input path (v0.10.x, no length bound): keep inputs on a short path.
            # The run directory still records where they are (raw/input-location.txt).
            import hashlib as _h
            self.input = REPO / ".tmp/in" / _h.sha1(str(self.root.resolve()).encode()).hexdigest()[:10]
            self.raw.mkdir(parents=True, exist_ok=True)
            (self.raw / "input-location.txt").write_text(str(self.input) + "\n")
        self.objects = self.raw / "objects"
        self.state = self.raw / "state"
        self.work = self.raw / "work"
        self.logs = self.raw / "logs"
        for d in (self.input, self.objects, self.state, self.work, self.logs):
            d.mkdir(parents=True, exist_ok=True)
        self.sources: dict[str, Path] = {}
        for name, src in files.items():
            dst = self.input / f"{name}.log"
            if not dst.exists():
                subprocess.run(["cp", "-c", str(src), str(dst)], check=True)  # APFS clone
                # VTOP never commits a partial trailing line (by design). Some
                # Loghub files end without a newline; terminate the working
                # copy so a drained run is observable. The dataset is unchanged.
                with open(dst, "rb+") as fh:
                    fh.seek(-1, os.SEEK_END)
                    if fh.read(1) != b"\n":
                        fh.write(b"\n")
            self.sources[name] = dst
            _check_object_name_length(dst)
        self.db = self.state / "vtop.db"
        self.config = self.root / "config.yaml"
        self._write_config()
        self.proc: Optional[subprocess.Popen] = None
        self.n_starts = 0

    def _write_config(self):
        v = self.v
        glob = str(self.input / "*.log")
        file_src = f'{{enabled: {str(not self.spool).lower()}, paths: ["{glob}"]}}'
        spool_src = f'{{enabled: {str(self.spool).lower()}, paths: ["{glob}"]}}'
        mac = "manifest_mac_key_env: VTOP_MANIFEST_MAC_KEY\n" if v.mac else ""
        self.config.write_text(f"""# generated by research/experiments/lib/vtoplab.py ({v.name})
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
{mac}sources:
  kafka: {{enabled: false, bootstrap_servers: ["localhost:9092"], consumer_group: lab}}
  file: {file_src}
  syslog_spool: {spool_src}
upload:
  backend: localfs
  local_path: {self.objects}
  bucket: archive
  require_strong_verification: {str(v.strong).lower()}
""")

    # ------------------------------------------------------------ engine

    def env(self):
        e = dict(os.environ)
        if self.v.mac:
            e["VTOP_MANIFEST_MAC_KEY"] = MAC_KEY
        e["VTOP_LOG_FORMAT"] = "json"
        return e

    def secure_state(self):
        """Owner-only state files: v0.10.2 (#775) refuses a ledger others can read. Harmless before."""
        for f in [self.db, Path(f"{self.db}-wal"), Path(f"{self.db}-shm"), *self.state.glob("*audit*")]:
            if f.exists():
                os.chmod(f, 0o600)

    def start(self):
        self.secure_state()
        self.n_starts += 1
        log = open(self.logs / f"engine-{self.n_starts:03d}.jsonl", "wb")
        self.proc = subprocess.Popen([str(VTOPCTL), "run", "--config", str(self.config)],
                                     stdout=log, stderr=subprocess.STDOUT, env=self.env())
        self.t_start = time.time()
        return self.proc

    def kill(self, sig=signal.SIGKILL):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def stop(self):
        self.kill(signal.SIGTERM)

    def committed(self) -> dict[str, int]:
        """Highest committed end byte per source path, read from the ledger."""
        if not self.db.exists():
            return {}
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=30)
        try:
            rows = con.execute("SELECT progress_end_json FROM batches WHERE state='source_committed'").fetchall()
        except sqlite3.OperationalError:
            return {}
        finally:
            con.close()
        out: dict[str, int] = {}
        for (pj,) in rows:
            p = json.loads(pj)
            path = p.get("path") or p.get("spool_path") or p.get("spool_id")
            out[path] = max(out.get(path, 0), int(p["end_byte"]))
        return out

    def ledger_states(self) -> dict[str, int]:
        con = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=30)
        try:
            return dict(con.execute("SELECT state, COUNT(*) FROM batches GROUP BY state").fetchall())
        finally:
            con.close()

    def drained(self) -> bool:
        c = self.committed()
        return all(c.get(str(p), 0) >= p.stat().st_size for p in self.sources.values())

    def wait_drained(self, timeout=3600, poll=0.5) -> bool:
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.drained():
                return True
            if self.proc and self.proc.poll() is not None:
                return self.drained()
            time.sleep(poll)
        return False

    def batch_metrics(self) -> list[dict]:
        out = []
        for f in sorted(self.logs.glob("engine-*.jsonl")):
            with open(f, "rb") as fh:
                for line in fh:
                    if b'"batch_metrics"' not in line:
                        continue
                    try:
                        out.append(json.loads(line)["fields"])
                    except (ValueError, KeyError):
                        pass
        return out


# ---------------------------------------------------------------- oracle

def _decompress(data: bytes, codec: str) -> bytes:
    if codec == "zstd":
        # streamed: decompress(max_output_size=2 GiB) preallocates the limit (MemoryError on small VMs)
        import io
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data), read_across_frames=True) as r:
            return r.readall()
    if codec == "gzip":
        return gzip.decompress(data)
    return data


def _local_path(objects: Path, uri: str) -> Path:
    assert uri.startswith("s3://")
    return objects / uri[len("s3://"):]


def oracle(run: Run) -> dict:
    """Independent check of archive vs. sources vs. committed progress."""
    by_path: dict[str, list[tuple[int, int]]] = {}
    bad: list[dict] = []
    n_manifests = 0
    understated = 0  # manifests whose start_byte is later than the object's real start
    seen_objects: set[str] = set()  # v0.10.x: replays usually reuse a stably named object (#626)
    reused = 0
    for mf in sorted(run.objects.rglob("*.manifest.json")):
        n_manifests += 1
        try:
            m = json.loads(mf.read_bytes())
        except ValueError:
            bad.append({"manifest": mf.name, "problem": "unparseable manifest"})
            continue
        sp = m["source_progress"]
        path = sp.get("path") or sp.get("spool_path") or sp.get("spool_id")
        start, end = int(sp["start_byte"]), int(sp["end_byte"])
        obj = _local_path(run.objects, m["object"]["uri"])
        if m["object"]["uri"] in seen_objects:  # checked once; a reused object is not a duplicate
            reused += 1
            continue
        seen_objects.add(m["object"]["uri"])
        if not obj.exists():
            bad.append({"manifest": mf.name, "problem": "object missing"})
            continue
        data = obj.read_bytes()
        algo = m["object"].get("checksum_algorithm", "sha256")
        if algo == "sha256" and hashlib.sha256(data).hexdigest() != m["object"]["checksum"]:
            bad.append({"manifest": mf.name, "problem": "object digest mismatch"})
            continue
        try:
            plain = _decompress(data, m["compression"])
        except Exception as e:  # noqa: BLE001
            bad.append({"manifest": mf.name, "problem": f"decompress: {e}"})
            continue
        # The object holds the bytes that END at end_byte. Its real start is
        # end - len(plain); VTOP v0.7.0 manifests record the last read's
        # start, which is later when a batch spans several reads.
        real_start = end - len(plain)
        if real_start < 0:
            bad.append({"manifest": mf.name, "problem": "object longer than source prefix"})
            continue
        with open(path, "rb") as fh:
            fh.seek(real_start)
            expect = fh.read(len(plain))
        if plain != expect:
            bad.append({"manifest": mf.name, "problem": "content differs from source bytes"})
            continue
        if real_start != start:
            understated += 1
        by_path.setdefault(path, []).append((real_start, end))

    committed = run.committed()
    report = {"manifests": n_manifests, "distinct_objects": len(seen_objects), "manifests_reusing_object": reused,
              "bad_objects": bad, "manifest_start_understated": understated, "sources": {}}
    unsafe = 0
    for name, p in run.sources.items():
        size = p.stat().st_size
        iv = sorted(by_path.get(str(p), []))
        # contiguous verified prefix from byte 0
        prefix = 0
        for s, e in iv:
            if s <= prefix:
                prefix = max(prefix, e)
            else:
                break
        covered = sum(e - s for s, e in iv)
        dup_bytes = covered - _union_len(iv)
        c = committed.get(str(p), 0)
        if c > prefix:
            unsafe += 1
        report["sources"][name] = {
            "size": size, "committed": c, "verified_prefix": prefix,
            "objects": len(iv), "duplicate_objects": _overlapping(iv),
            "duplicate_bytes": dup_bytes, "unsafe_commit": c > prefix,
            "complete": prefix >= size,
        }
    report["unsafe_commits"] = unsafe
    return report


def _union_len(iv):
    total, cur_s, cur_e = 0, None, None
    for s, e in iv:
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    return total


def _overlapping(iv):
    """Objects whose range overlaps bytes already covered by earlier objects."""
    n, reach = 0, -1
    for s, e in iv:
        if s < reach:
            n += 1
        reach = max(reach, e)
    return n


# ---------------------------------------------------------------- power loss (v0.10.x)

def _wal_commit_boundaries(wal: bytes) -> list[int]:
    """Byte offsets just after each valid commit frame of the WAL's current generation.

    SQLite WAL format: 32-byte header (magic, version, page size, checkpoint seq, salt-1, salt-2,
    checksum), then frames of 24-byte header + page. A frame belongs to the current generation if its
    salts match the header's and the running checksum matches; a commit frame has a non-zero
    "database size after commit" field. Frames after the first mismatch are stale or torn.
    """
    import struct
    if len(wal) < 32:
        return []
    magic, _ver, psz, _seq, s1, s2, c1, c2 = struct.unpack(">8I", wal[:32])
    if magic not in (0x377F0682, 0x377F0683):
        return []
    big = magic & 1

    def cks(buf: bytes, a: int, b: int) -> tuple[int, int]:
        fmt = (">" if big else "<") + f"{len(buf) // 4}I"
        w = struct.unpack(fmt, buf)
        for i in range(0, len(w), 2):
            a = (a + w[i] + b) & 0xFFFFFFFF
            b = (b + w[i + 1] + a) & 0xFFFFFFFF
        return a, b

    a, b = cks(wal[:24], 0, 0)
    if (a, b) != (c1, c2):
        return []
    out, off, fsz = [], 32, 24 + psz
    while off + fsz <= len(wal):
        hdr = wal[off:off + 24]
        _pg, commit, fs1, fs2, fc1, fc2 = struct.unpack(">6I", hdr)
        if (fs1, fs2) != (s1, s2):
            break
        a, b = cks(hdr[:8], a, b)
        a, b = cks(wal[off + 24:off + fsz], a, b)
        if (a, b) != (fc1, fc2):
            break
        off += fsz
        if commit:
            out.append(off)
    return out


def _states_at(db: Path, wal: bytes, upto: int, tmp: Path) -> dict[str, str]:
    """batch_id -> state as SQLite would read it from db + the first `upto` WAL bytes (on a copy)."""
    for f in tmp.glob("p.db*"):
        f.unlink()
    shutil.copyfile(db, tmp / "p.db")
    (tmp / "p.db-wal").write_bytes(wal[:upto])
    con = sqlite3.connect(tmp / "p.db")
    try:
        return dict(con.execute("SELECT batch_id, state FROM batches").fetchall())
    except sqlite3.OperationalError:
        return {}
    finally:
        con.close()


def power_loss_truncate(run: "Run", worst_case: bool = True) -> dict:
    """Emulate a power loss after SIGKILL for v0.10.x: keep the WAL up to the last commit that wrote a
    VERIFIED or SOURCE_COMMITTED state (written on a synchronous=FULL connection, so fsynced), and drop
    every later frame (synchronous=NORMAL writes may be lost). Same method as the engine's
    crates/vtop-cli/tests/ledger_power_loss.rs, reconstructed from the WAL instead of recorded in-process.
    Frames already checkpointed into the main file are durable (the checkpoint fsyncs) and are kept.

    worst_case=True drops everything after the last fsynced commit (the most a power loss can lose).
    Returns a record of what was cut. Call only while the engine is not running.
    """
    import tempfile
    wal_p = Path(f"{run.db}-wal")
    rec = {"wal_bytes": wal_p.stat().st_size if wal_p.exists() else 0}
    if not wal_p.exists() or rec["wal_bytes"] == 0:
        return {**rec, "kept_bytes": rec["wal_bytes"], "commits": 0, "dropped_commits": 0}
    wal = wal_p.read_bytes()
    bounds = _wal_commit_boundaries(wal)
    rec["commits"] = len(bounds)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        prev = _states_at(run.db, wal, 32 if bounds else 0, tmp)
        last_full = 32  # nothing of this generation fsynced yet: keep only the header
        for end in bounds:
            cur = _states_at(run.db, wal, end, tmp)
            if any(st in ("verified", "source_committed") and prev.get(bid) != st for bid, st in cur.items()):
                last_full = end
            prev = cur
    keep = min(last_full, len(wal)) if worst_case else len(wal)
    dropped = sum(1 for e in bounds if e > keep)
    with open(wal_p, "r+b") as fh:
        fh.truncate(keep)
    Path(f"{run.db}-shm").unlink(missing_ok=True)  # the index is rebuilt from the WAL on open
    return {**rec, "kept_bytes": keep, "dropped_commits": dropped}


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str))
