#!/usr/bin/env python3
"""E-RQ5-TAMPER driver (H2.5 signed manifests, H2.6 Object Lock and versioning, H2.7 audit trail) for vtop-engine
v0.10.2 on S3. Runs on the EC2 VM against Amazon S3, or locally against MinIO (--endpoint, lab only).

Every case is one pair: an attacker changes the archive (bucket, and where the case says so the ledger), then a
VTOP defence is asked for its verdict. Each case states the verdict it expects from each verifier. Where a defence
is off or cannot see the change, the expected verdict is the WRONG one ("limit": the tamper is NOT CAUGHT), so a
limit is measured, not just written down. A case passes when every observed verdict equals its expected one.

The tampering is done here, independently of the engine and of its test suite (vtop-engine PR #859):
  * mutation classes follow research/experiments/rq2-corruption-recovery/code/exp_tamper.py (M1 byte flip, M2
    truncate, M3 delete, M4 another batch's evidence, M6 field edit + recomputed self-hash, M7 consistent forgery of
    object + manifest with the same "Failed password" -> "Accepted passwd" edit), extended to manifest version 0.4
    (canonical JSON hash) and to S3 (versions, retention);
  * the 0.4 self-hash, the keyed BLAKE3 MAC, receipts and the audit hash chain are re-implemented from the published
    format (docs/VTOP_PROTOCOL_DRAFT.md §11, docs/operate/auditing.md) and checked against what the engine wrote
    before any case relies on them (`reimpl_check` in run.json);
  * recovery is driven as in exp_tamper.py: the target batch (highest end byte) is set back to VERIFIED in the ledger
    (fault F4: crash between verification and source commit), then `vtopctl run` restarts and recovery decides;
  * an independent content oracle (no VTOP code) re-reads every committed object and compares it with the source.

Verifiers: `vtopctl scrub` (ledger present; batch, receipt and retention results of the target), `vtopctl
verify-manifest` with an EMPTY ledger (the no-ledger arm of H2.5) and with the case's ledger, `vtopctl audit-coverage
--check-storage`, recovery (restart with a VERIFIED row), `vtopctl audit verify`, the store's own answer to the
tamper request (Object Lock), and engine start-up refusals.

Usage (VM): see user-data.sh. Local dry run: README.md "Local test".
"""
from __future__ import annotations

import argparse
import base64
import copy
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import platform
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
from pathlib import Path

import blake3
import boto3
import zstandard
from botocore.config import Config
from botocore.exceptions import ClientError

# Lab MAC keys (64 hex). Not secrets: they exist only for this experiment and are published with it.
KEYS = {"k1": "6b" * 32, "k2": "6c" * 32, "mallory": "a7" * 32}
KEY_ENV = {"k1": "VTOP_TAMPER_K1", "k2": "VTOP_TAMPER_K2"}
AUDIT_CHAIN_CONTEXT = "vtop-engine 2026 audit_events chain v1"   # manifest.rs:94 at v0.10.2
MAX_BYTES = 1 << 20          # 1 MiB batches: a 3 MiB input gives 3 committed batches, all single-PUT objects
ARCHIVE_TIMEOUT_S = 180
RECOVERY_TIMEOUT_S = 180
START_REFUSAL_WAIT_S = 45


# ------------------------------------------------------------------------------------------------ small helpers
def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def split(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), uri
    b, k = uri[5:].split("/", 1)
    return b, k


def sh(args, env=None, timeout=300) -> dict:
    t0 = time.monotonic()
    try:
        p = subprocess.run([str(a) for a in args], capture_output=True, text=True, env=env, timeout=timeout)
        rc, out, err = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        rc, out, err = None, (e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or ""), "timeout"
    return {"rc": rc, "stdout": out, "stderr": err[-3000:], "s": round(time.monotonic() - t0, 2)}


def parse_json_out(text: str):
    """vtopctl --json prints the report as JSON; log lines (JSON with "level") may precede it on stdout."""
    text = text.strip()
    for start in [i for i, c in enumerate(text) if c == "{" and (i == 0 or text[i - 1] == "\n")]:
        try:
            v = json.loads(text[start:])
        except ValueError:
            continue
        if isinstance(v, dict) and "level" not in v:
            return v
    return None


def zstd_decompress(data: bytes) -> bytes:
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data), read_across_frames=True) as r:
        return r.readall()


def client_error(e: ClientError) -> str:
    return f"{e.response.get('Error', {}).get('Code')}: {e.response.get('Error', {}).get('Message')}"


# --------------------------------------------------------- re-implementation of the published formats (no VTOP code)
def canonical(doc: dict, block: str) -> bytes:
    """0.4 manifest / receipt hash+MAC input (manifest.rs canonical_auth_bytes_of): <block>.sha256 = "" (and
    <block>.mac = "" when present), keys sorted at every depth, compact JSON."""
    d = copy.deepcopy(doc)
    d[block]["sha256"] = ""
    if "mac" in d[block]:
        d[block]["mac"] = ""
    return json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def seal(doc: dict, block: str, mac: str) -> dict:
    """Recompute <block>.sha256 and set the MAC. mac: 'stale' keep the old MAC (an attacker without the key),
    'strip' remove MAC and key id, 'none' no MAC (signing off), or a key id in KEYS (sign with that key)."""
    d = copy.deepcopy(doc)
    if mac == "strip" or mac == "none":
        d[block].pop("mac", None)
        d[block].pop("mac_key_id", None)
    elif mac in KEYS:
        d[block]["mac_key_id"] = mac
        d[block]["mac"] = ""
    c = canonical(d, block)
    d[block]["sha256"] = hashlib.sha256(c).hexdigest()
    if mac in KEYS:
        d[block]["mac"] = blake3.blake3(c, key=bytes.fromhex(KEYS[mac])).hexdigest()
    return d


def doc_bytes(doc: dict) -> bytes:
    return json.dumps(doc, indent=2, ensure_ascii=False).encode()


def audit_link(prev: str | None, record: str, key_id: str | None) -> str:
    data = (prev or "").encode() + b"\n" + record.encode()
    if key_id is None:
        return blake3.blake3(data).hexdigest()
    dk = blake3.blake3(bytes.fromhex(KEYS[key_id]), derive_key_context=AUDIT_CHAIN_CONTEXT).digest()
    return blake3.blake3(data, key=dk).hexdigest()


# --------------------------------------------------------------------------------------------------- profiles
PROFILES = {
    # bucket role, manifest MAC keyring, upload.require_object_versioning, upload.object_lock
    "plain-off": dict(bucket="plain", mac=None, versioning=False, lock=False),
    "plain-mac": dict(bucket="plain", mac="k1", versioning=False, lock=False),
    "ver-off": dict(bucket="lock", mac=None, versioning=True, lock=False),
    "ver-mac": dict(bucket="lock", mac="k1", versioning=True, lock=False),
    "lock-mac": dict(bucket="lock", mac="k1", versioning=True, lock=True),
}
KEYRINGS = {
    "k1": "  mac:\n    active: k1\n    keys:\n      - {id: k1, env: VTOP_TAMPER_K1}\n",
    # k1 rotated out normally: still verifies what it signed
    "rotated": "  mac:\n    active: k2\n    keys:\n      - {id: k1, env: VTOP_TAMPER_K1}\n      - {id: k2, env: VTOP_TAMPER_K2}\n",
    # k1 withdrawn (leaked): tombstoned, no key material
    "retired": "  mac:\n    active: k2\n    keys:\n      - {id: k2, env: VTOP_TAMPER_K2}\n    retired: [k1]\n",
}


class Lab:
    """Everything shared by the cases: binary, buckets, input, S3 client, results."""

    def __init__(self, a):
        self.a = a
        self.vtopctl = Path(a.vtopctl).resolve()
        self.input = Path(a.input).resolve()
        self.src = self.input.read_bytes()
        self.buckets = {"plain": a.plain_bucket, "lock": a.lock_bucket, "susp": a.susp_bucket}
        self.work = Path(a.work).resolve()
        self.out = Path(a.out).resolve()
        cfg = {"retries": {"max_attempts": 5, "mode": "standard"}}
        if a.endpoint:
            cfg["s3"] = {"addressing_style": "path"}
        self.s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=a.endpoint or None, config=Config(**cfg))
        self.env = dict(os.environ)
        self.env.update({KEY_ENV["k1"]: KEYS["k1"], KEY_ENV["k2"]: KEYS["k2"], "VTOP_LOG_FORMAT": "json"})
        for k in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "VTOP_S3_ENDPOINT_URL"):
            self.env.pop(k, None)

    # ------------------------------------------------------------------ config
    def config(self, root: Path, prefix: str, profile: dict, keyring: str | None = "default",
               state: Path | None = None, input_dir: Path | None = None, bucket: str | None = None,
               versioning: bool | None = None, lock: bool | None = None) -> Path:
        mac = profile["mac"] if keyring == "default" else keyring
        versioning = profile["versioning"] if versioning is None else versioning
        lock = profile["lock"] if lock is None else lock
        bucket = bucket or self.buckets[profile["bucket"]]
        state = state or root / "state"
        state.mkdir(parents=True, exist_ok=True)
        os.chmod(state, 0o700)   # #775: a ledger in a closed directory is accepted whatever sqlite3 gives -wal/-shm
        (root / "work").mkdir(parents=True, exist_ok=True)
        indir = input_dir or self.input.parent
        lab = (f"  endpoint_url: {self.a.endpoint}\n  force_path_style: true\n  verify_tls: false\n"
               if self.a.endpoint else "")
        text = f"""# generated by research/experiments/rq5-cloud/tamper/tamper.py
engine:
  name: tamper
  tenant: tamper
  state_store: "sqlite://{state / 'vtop.db'}"
  work_dir: {root / 'work'}
batching:
  max_records: 1000000
  max_bytes: {MAX_BYTES}
  max_batch_age_seconds: 1
  idle_poll_interval_ms: 200
  max_concurrent_batches: 4
compression: {{type: zstd, level: 3}}
checksum: {{algorithm: sha256}}
manifest:
  write_version: "0.4"
  verification_receipts: true
{KEYRINGS[mac] if mac else ''}sources:
  kafka: {{enabled: false, bootstrap_servers: ["localhost:9092"], consumer_group: lab}}
  file: {{enabled: true, paths: ["{indir}/*.log"]}}
  syslog_spool: {{enabled: false, paths: ["{indir}/*.log"]}}
upload:
  backend: s3_native
  region: us-east-1
  bucket: {bucket}
  prefix: "{prefix}"
  create_bucket: false
  require_strong_verification: true
  require_object_versioning: {str(versioning).lower()}
""" + ("  object_lock: {mode: governance, retain_days: 1}\n" if lock else "") + lab
        name = "config.yaml" if keyring == "default" and state == root / "state" else f"config-{mac or 'off'}-{state.parent.name}.yaml"
        p = root / name
        p.write_text(text)
        return p

    # ------------------------------------------------------------------ engine
    def start(self, cfg: Path, log: Path) -> subprocess.Popen:
        fh = open(log, "ab")
        return subprocess.Popen([str(self.vtopctl), "run", "--config", str(cfg)], stdout=fh, stderr=subprocess.STDOUT,
                                env=self.env)

    @staticmethod
    def stop(p: subprocess.Popen):
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(60)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def vtop(self, *args, timeout=300) -> dict:
        r = sh([self.vtopctl, "--json", *args], env=self.env, timeout=timeout)
        r["json"] = parse_json_out(r["stdout"])
        return r


# ------------------------------------------------------------------------------------------------ ledger access
def db_connect(db: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def committed_end(db: Path) -> int:
    if not db.exists():
        return 0
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            v = con.execute("SELECT MAX(CAST(json_extract(progress_end_json,'$.end_byte') AS INTEGER)) FROM batches "
                            "WHERE state='source_committed'").fetchone()[0]
        finally:
            con.close()
    except sqlite3.OperationalError:
        return 0
    return int(v or 0)


def rows(db: Path) -> list[dict]:
    con = db_connect(db)
    try:
        return [dict(r) for r in con.execute("SELECT * FROM batches")]
    finally:
        con.close()


def secure(state: Path):
    for f in state.glob("vtop.db*"):
        os.chmod(f, 0o600)


# --------------------------------------------------------------------------------------------- the case context
class Case:
    def __init__(self, lab: Lab, spec: dict):
        self.lab, self.spec = lab, spec
        self.id = spec["id"]
        self.profile = PROFILES[spec["profile"]]
        self.root = lab.work / self.id
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True)
        self.prefix = f"{lab.a.prefix.rstrip('/')}/{self.id}/"
        self.bucket = lab.buckets[self.profile["bucket"]]
        self.cfg = lab.config(self.root, self.prefix, self.profile)
        self.db = self.root / "state" / "vtop.db"
        self.logs = self.root / "logs"
        self.logs.mkdir()
        self.rec: dict = {"id": self.id, "hypothesis": spec["hyp"], "group": spec["group"], "profile": spec["profile"],
                          "attacker": spec.get("attacker"), "action": spec["action_text"], "verdicts": {},
                          "facts": {}, "started_at": now()}
        self.n_engine = 0

    @property
    def s3(self):
        return self.lab.s3

    # ------------------------------------------------------------------ archive
    def archive(self):
        p = self.lab.start(self.cfg, self.logs / "engine-1-archive.jsonl")
        self.n_engine = 1
        t0 = time.monotonic()
        while time.monotonic() - t0 < ARCHIVE_TIMEOUT_S:
            if committed_end(self.db) >= len(self.lab.src) or p.poll() is not None:
                break
            time.sleep(0.25)
        time.sleep(0.5)
        self.lab.stop(p)
        secure(self.db.parent)
        rs = [r for r in rows(self.db) if r["state"] == "source_committed"]
        rs.sort(key=lambda r: int(json.loads(r["progress_end_json"])["end_byte"]))
        drained = committed_end(self.db) >= len(self.lab.src)
        self.rec["archive"] = {"drained": drained, "seconds": round(time.monotonic() - t0, 2),
                               "committed_batches": len(rs),
                               "pinned_manifest_versions": sum(1 for r in rs if r.get("manifest_version_id")),
                               "states": {s: sum(1 for r in rows(self.db) if r["state"] == s)
                                          for s in {r["state"] for r in rows(self.db)}}}
        if not drained or len(rs) < 2:
            raise RuntimeError(f"archive did not drain ({committed_end(self.db)} of {len(self.lab.src)} bytes)")
        self.target, self.other = rs[-1], rs[0]   # exp_tamper.py: target = highest end byte, other = lowest
        self.rec["target_batch"] = self.target["batch_id"]
        self.rec["target_pinned_version"] = self.target.get("manifest_version_id")

    # ------------------------------------------------------------------ object helpers
    def get(self, uri: str, version: str | None = None) -> bytes:
        b, k = split(uri)
        kw = {"VersionId": version} if version else {}
        return self.s3.get_object(Bucket=b, Key=k, **kw)["Body"].read()

    def put(self, uri: str, body: bytes) -> dict:
        b, k = split(uri)
        r = self.s3.put_object(Bucket=b, Key=k, Body=body)
        return {"put": k.rsplit("/", 1)[-1][-40:], "version_id": r.get("VersionId")}

    def versions(self, uri: str) -> list[dict]:
        b, k = split(uri)
        out = []
        for page in self.s3.get_paginator("list_object_versions").paginate(Bucket=b, Prefix=k):
            out += [{"version_id": v["VersionId"], "latest": v["IsLatest"], "size": v["Size"]}
                    for v in page.get("Versions", []) if v["Key"] == k]
            out += [{"version_id": v["VersionId"], "latest": v["IsLatest"], "delete_marker": True}
                    for v in page.get("DeleteMarkers", []) if v["Key"] == k]
        return out

    def store_call(self, what: str, fn, **kw) -> dict:
        """One tamper request to the store, recorded with the store's answer (accepted / refused + error)."""
        try:
            r = fn(**kw)
            return {"request": what, "result": "accepted", "version_id": r.get("VersionId") if isinstance(r, dict) else None}
        except ClientError as e:
            return {"request": what, "result": "refused", "error": client_error(e)}

    def manifest(self, row=None) -> dict:
        row = row or self.target
        return json.loads(self.get(row["manifest_uri"], row.get("manifest_version_id")))

    def receipt_uri(self, row=None) -> str:
        row = row or self.target
        return row["manifest_uri"][: -len(".manifest.json")] + ".receipt.json"

    def forged_object(self) -> tuple[bytes, bytes]:
        """exp_tamper.py M7: the same edit to the decompressed batch, recompressed (zstd level 3)."""
        orig = self.get(self.target["object_uri"])
        plain = zstd_decompress(orig)
        forged_plain = (plain.replace(b"Failed password", b"Accepted passwd", 1) if b"Failed password" in plain
                        else plain[:-1] + b"X")
        return orig, zstandard.ZstdCompressor(level=3).compress(forged_plain)

    def rewrite_ledger(self, obj_sha: str, obj_size: int, man_sha: str):
        """The bucket + ledger attacker: make the ledger row agree with the forgery (UPDATE is not guarded)."""
        con = db_connect(self.db)
        con.execute("UPDATE batches SET object_sha256=?, object_size_bytes=?, manifest_sha256=? WHERE batch_id=?",
                    (obj_sha, obj_size, man_sha, self.target["batch_id"]))
        con.commit()
        con.close()
        secure(self.db.parent)

    # ------------------------------------------------------------------ verifiers
    def target_finding(self, report: dict) -> dict:
        for f in (report or {}).get("findings", []):
            if f.get("batch_id") == self.target["batch_id"]:
                return f
        return {}

    def scrub(self, name="scrub", cfg: Path | None = None, extra=()) -> dict:
        r = self.lab.vtop("scrub", "--config", cfg or self.cfg, *extra)
        (self.logs / f"{name}.json").write_text(r["stdout"] or r["stderr"])
        rep = r["json"] or {}
        f = self.target_finding(rep)

        def sub(field: str, counts_key: str, good: str):
            """The target's receipt / retention result. A batch is listed in `findings` only when something about it
            is a finding; otherwise read it from the counts: only the target was tampered, so a single non-good
            count is the target's (recorded as inferred)."""
            if f.get(field):
                return f[field]
            counts = rep.get(counts_key) or {}
            if not counts:
                return None
            odd = [k for k, n in counts.items() if n and k != good]
            if len(odd) == 1 and counts[odd[0]] == 1 and not f:
                inferred.append(field)
                return odd[0]
            return good if counts.get(good) else None
        inferred: list[str] = []
        out = {"verdict": f.get("result", "ok") if rep else "error", "rc": r["rc"], "passed": rep.get("passed"),
               "receipt": sub("receipt", "receipts", "ok"),
               "retention": sub("retention", "retention", "locked"),
               "receipt_retention": sub("receipt_retention", "receipt_retention", "locked"),
               "inferred_from_counts": inferred,
               "detail": f.get("detail"), "results": rep.get("results"), "receipts": rep.get("receipts"),
               "retention_counts": rep.get("retention"), "receipt_retention_counts": rep.get("receipt_retention"),
               "manifest_keys": rep.get("manifest_keys"),
               "other_findings": sorted({x.get("result") for x in rep.get("findings", [])
                                         if x.get("batch_id") != self.target["batch_id"]})}
        if not rep:
            out["stderr"] = r["stderr"][-800:]
        self.rec["verdicts"][name] = out
        # Sub-verdicts from the same run, so each expectation compares one value.
        return out

    def verify_manifest(self, name: str, ledger: bool, keyring="default") -> dict:
        if ledger:
            cfg = self.cfg if keyring == "default" else self.lab.config(self.root, self.prefix, self.profile, keyring)
        else:  # a fresh, empty ledger: the auditor who holds only the config (+ key) and the bucket
            nl = self.root / f"noledger-{keyring}"
            cfg = self.lab.config(nl, self.prefix, self.profile, keyring, state=nl / "state")
        r = self.lab.vtop("verify-manifest", "--manifest", self.target["manifest_uri"], "--config", cfg)
        (self.logs / f"{name}.json").write_text(r["stdout"] + "\n--- stderr ---\n" + r["stderr"][-3000:])
        j = r["json"] or {}
        verdict = "passed" if (r["rc"] == 0 and j.get("passed")) else "failed"
        out = {"verdict": verdict, "rc": r["rc"], "authentication": j.get("manifest_authentication"),
               "object_content": j.get("object_content"), "ledger": j.get("ledger"), "batch_id": j.get("batch_id")}
        if verdict == "failed" and not j:
            out["error"] = next((ln for ln in r["stderr"].splitlines()[::-1] if "error" in ln.lower()), "")[-400:]
        self.rec["verdicts"][name] = out
        return out

    def check_storage(self) -> dict:
        r = self.lab.vtop("audit-coverage", "--config", self.cfg, "--check-storage")
        (self.logs / "check_storage.json").write_text(r["stdout"] or r["stderr"])
        j = r["json"] or {}
        storage = j.get("storage") or []
        mine = [s for s in storage if self.target["batch_id"] in json.dumps(s)]
        out = {"verdict": "failed" if (mine or r["rc"] not in (0,)) else "passed", "rc": r["rc"],
               "storage_findings": mine[:4], "n_storage_findings": len(storage)}
        self.rec["verdicts"]["check_storage"] = out
        return out

    def recovery(self, name="recovery", keyring="default") -> dict:
        """F4 by state injection (exp_tamper.py), then restart; the target row's final state is the verdict."""
        con = db_connect(self.db)
        con.execute("UPDATE batches SET state='verified' WHERE batch_id=?", (self.target["batch_id"],))
        con.commit()
        con.close()
        secure(self.db.parent)
        cfg = self.cfg if keyring == "default" else self.lab.config(self.root, self.prefix, self.profile, keyring)
        self.n_engine += 1
        log = self.logs / f"engine-{self.n_engine}-{name}.jsonl"
        p = self.lab.start(cfg, log)
        t0 = time.monotonic()
        state = "verified"
        while time.monotonic() - t0 < RECOVERY_TIMEOUT_S:
            state = next((r["state"] for r in rows(self.db) if r["batch_id"] == self.target["batch_id"]), None)
            if state != "verified" and committed_end(self.db) >= len(self.lab.src):
                break
            if p.poll() is not None:
                break
            time.sleep(0.25)
        time.sleep(0.5)
        self.lab.stop(p)
        secure(self.db.parent)
        row = next(r for r in rows(self.db) if r["batch_id"] == self.target["batch_id"])
        reasons = []
        for line in log.read_text(errors="replace").splitlines():
            if "re-check" in line or "recovery" in line:
                try:
                    msg = json.loads(line)["fields"].get("message", "")
                except (ValueError, KeyError):
                    continue
                if "re-check" in msg or "replaying" in msg:
                    reasons.append(msg[:300])
        verdict = {"source_committed": "committed", "replay_required": "replayed", "failed": "replayed",
                   "superseded": "replayed"}.get(row["state"], row["state"])
        orc = self.oracle()
        out = {"verdict": verdict, "final_state": row["state"], "error": (row.get("error_message") or "")[:300],
               "seconds": round(time.monotonic() - t0, 2), "drained": committed_end(self.db) >= len(self.lab.src),
               "reasons": reasons[:3], "oracle": orc,
               "forged_content_committed": orc["target_range_bad_committed"] > 0}
        self.rec["verdicts"][name] = out
        return out

    def oracle(self) -> dict:
        """Independent: every committed row's object (current version at its key) decompressed vs the source."""
        bad, covered, tgt_bad = [], [], 0
        t0, t1 = (int(json.loads(self.target["progress_start_json"])["start_byte"]),
                  int(json.loads(self.target["progress_end_json"])["end_byte"]))
        for r in rows(self.db):
            if r["state"] != "source_committed":
                continue
            s0 = int(json.loads(r["progress_start_json"])["start_byte"])
            e0 = int(json.loads(r["progress_end_json"])["end_byte"])
            try:
                ok = zstd_decompress(self.get(r["object_uri"])) == self.lab.src[s0:e0]
            except Exception as e:  # noqa: BLE001
                ok = False
                bad.append({"batch_id": r["batch_id"][-12:], "problem": type(e).__name__})
            if not ok:
                bad.append({"batch_id": r["batch_id"][-12:], "range": [s0, e0]})
                if s0 < t1 and e0 > t0:
                    tgt_bad += 1
            else:
                covered.append((s0, e0))
        pos = 0
        for s0, e0 in sorted(covered):
            if s0 <= pos:
                pos = max(pos, e0)
        return {"committed_bad": len(bad), "bad": bad[:4], "complete_and_exact": pos >= len(self.lab.src),
                "target_range_bad_committed": tgt_bad}

    def store_verdict(self, calls: list[dict]):
        res = {c["result"] for c in calls}
        v = "refused" if res == {"refused"} else ("accepted" if res == {"accepted"} else "mixed")
        self.rec["verdicts"]["store"] = {"verdict": v, "calls": calls}

    # ------------------------------------------------------------------ clean-up
    def cleanup(self):
        """Delete everything this case wrote that the store lets us delete. Locked versions stay (retention 1 day;
        the bucket's lifecycle rule removes them afterwards); they are counted."""
        left = 0
        try:
            if self.profile["bucket"] == "plain":
                for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix):
                    for o in page.get("Contents", []):
                        self.s3.delete_object(Bucket=self.bucket, Key=o["Key"])
            else:
                for page in self.s3.get_paginator("list_object_versions").paginate(Bucket=self.bucket, Prefix=self.prefix):
                    for v in page.get("Versions", []) + page.get("DeleteMarkers", []):
                        try:
                            self.s3.delete_object(Bucket=self.bucket, Key=v["Key"], VersionId=v["VersionId"])
                        except ClientError:
                            left += 1
        except ClientError as e:
            self.rec["cleanup_error"] = client_error(e)
        self.rec["locked_versions_left"] = left


# ------------------------------------------------------------------------------------------------- tamper actions
def act_forge(mac: str, ledger: bool, receipt: bool = True):
    """M7 consistent forgery: object replaced by an edited batch, manifest rewritten to describe it (digest, size,
    self-hash recomputed). mac: stale (no key), strip, none (signing off), mallory (attacker's own key, made-up id),
    k1 (the real key, leaked). ledger: the attacker also rewrites the ledger row to agree. receipt: the attacker also
    rewrites the batch's verification receipt to agree (same MAC mode); False leaves it as the engine wrote it."""
    def act(c: Case):
        orig, forged = c.forged_object()
        m = c.manifest()
        m["object"]["checksum"] = hashlib.sha256(forged).hexdigest()
        m["object"]["size_bytes"] = len(forged)
        m2 = seal(m, "manifest", mac)
        calls = [c.put(c.target["object_uri"], forged), c.put(c.target["manifest_uri"], doc_bytes(m2))]
        if receipt:
            r = json.loads(c.get(c.receipt_uri()))
            r["manifest"]["sha256"] = m2["manifest"]["sha256"]
            r["object"]["checksum"], r["object"]["size_bytes"] = m["object"]["checksum"], len(forged)
            if r["object"].get("version_id") and calls[0]["version_id"]:
                r["object"]["version_id"] = calls[0]["version_id"]
            c.put(c.receipt_uri(), doc_bytes(seal(r, "receipt", mac)))
        c.rec["facts"]["receipt_rewritten"] = receipt
        if ledger:
            c.rewrite_ledger(m["object"]["checksum"], len(forged), m2["manifest"]["sha256"])
        c.rec["facts"]["object_put_version_id"] = calls[0]["version_id"]
        c.rec["facts"]["original_object_still_stored"] = original_stored(c, c.target["object_uri"], orig)
    return act


def act_manifest_edit(c: Case):
    """M6 on the manifest only: record_count + 1, self-hash recomputed, MAC (if any) left stale."""
    m = c.manifest()
    m["record_count"] += 1
    orig = c.get(c.target["manifest_uri"], c.target.get("manifest_version_id"))
    c.rec["facts"]["put"] = c.put(c.target["manifest_uri"], doc_bytes(seal(m, "manifest", "stale")))
    c.rec["facts"]["original_manifest_still_stored"] = original_stored(c, c.target["manifest_uri"], orig,
                                                                        c.target.get("manifest_version_id"))


def act_swap(c: Case):
    """M4 for manifests: another batch's genuine manifest copied over this batch's manifest key."""
    c.put(c.target["manifest_uri"], c.get(c.other["manifest_uri"], c.other.get("manifest_version_id")))


def act_object(kind: str):
    def act(c: Case):
        uri = c.target["object_uri"]
        orig = c.get(uri)
        if kind == "flip":       # M1
            b = bytearray(orig); b[len(b) // 2] ^= 0x01
            c.rec["facts"]["put"] = c.put(uri, bytes(b))
        elif kind == "truncate":  # M2
            c.rec["facts"]["put"] = c.put(uri, orig[: len(orig) // 2])
        elif kind == "delete":    # M3 (a delete marker on a versioned bucket)
            b, k = split(uri)
            c.rec["facts"]["delete"] = c.store_call("DeleteObject", c.s3.delete_object, Bucket=b, Key=k)
        if c.profile["versioning"]:
            c.rec["facts"]["original_object_still_stored"] = original_stored(c, uri, orig)
    return act


def original_stored(c: Case, uri: str, orig: bytes, version: str | None = None) -> bool | None:
    """Is a version with the original bytes still stored? None on an unversioned bucket (no versions to list):
    there the PUT replaced the only copy."""
    if not c.profile["versioning"]:
        return False
    for v in c.versions(uri):
        if v.get("delete_marker"):
            continue
        if version and v["version_id"] != version:
            continue
        try:
            if c.get(uri, v["version_id"]) == orig:
                return True
        except ClientError:
            pass
    return False


def act_rollback(c: Case):
    """The current key made to serve a validly signed manifest of the same batch that is not the pinned one (as an
    earlier genuine write would be): created_at one hour earlier and record_count - 1, signed with k1. VTOP writes
    each manifest key once (per-attempt batch ids), so producing one needs the key; the case isolates the pin."""
    m = c.manifest()
    t = dt.datetime.fromisoformat(m["created_at"].replace("Z", "+00:00")) - dt.timedelta(hours=1)
    m["created_at"] = t.isoformat()
    m["record_count"] -= 1
    c.rec["facts"]["put"] = c.put(c.target["manifest_uri"], doc_bytes(seal(m, "manifest", "k1")))


def act_restore(c: Case):
    """Overwrite with an M6 edit, then copy the pinned version back over the key (a rollback to the genuine bytes)."""
    act_manifest_edit(c)
    b, k = split(c.target["manifest_uri"])
    c.s3.copy_object(Bucket=b, Key=k, CopySource={"Bucket": b, "Key": k, "VersionId": c.target["manifest_version_id"]})
    c.rec["facts"]["manifest_versions"] = len(c.versions(c.target["manifest_uri"]))


def act_manifest_delete(pinned: bool):
    def act(c: Case):
        b, k = split(c.target["manifest_uri"])
        kw = {"VersionId": c.target["manifest_version_id"]} if pinned else {}
        c.rec["facts"]["delete"] = c.store_call("DeleteObject" + ("Version" if pinned else ""), c.s3.delete_object,
                                                Bucket=b, Key=k, **kw)
    return act


def act_receipt(kind: str, mac: str = "stale"):
    def act(c: Case):
        ruri = c.receipt_uri()
        r = json.loads(c.get(ruri))
        if kind == "delete":
            b, k = split(ruri)
            c.s3.delete_object(Bucket=b, Key=k)
            return
        if kind == "swap":
            c.put(ruri, c.get(c.receipt_uri(c.other)))
            return
        if kind == "method":
            r["verification"]["method"] = "read_back" if r["verification"]["method"] != "read_back" else "store_checksum"
        elif kind == "class":
            r["verification"]["class"] = "size_only"
        elif kind == "verified_by":
            r["verified_by"] = "auditor-of-record"
        c.rec["facts"]["receipt_edit"] = kind
        c.put(ruri, doc_bytes(seal(r, "receipt", mac) if mac != "nohash" else r))
    return act


# ---- Object Lock
def act_lock(kind: str):
    def act(c: Case):
        ob, ok = split(c.target["object_uri"])
        mb, mk = split(c.target["manifest_uri"])
        ov = c.s3.head_object(Bucket=ob, Key=ok)["VersionId"]
        mv = c.target["manifest_version_id"]
        orig_obj, orig_man = c.get(c.target["object_uri"], ov), c.get(c.target["manifest_uri"], mv)
        calls = []
        if kind == "delete-version-object":
            calls.append(c.store_call("DeleteObjectVersion(object)", c.s3.delete_object, Bucket=ob, Key=ok, VersionId=ov))
        elif kind == "delete-version-manifest":
            calls.append(c.store_call("DeleteObjectVersion(manifest)", c.s3.delete_object, Bucket=mb, Key=mk, VersionId=mv))
        elif kind == "shorten":
            h = c.s3.head_object(Bucket=ob, Key=ok, VersionId=ov)
            earlier = h["ObjectLockRetainUntilDate"] - dt.timedelta(hours=12)
            calls.append(c.store_call("PutObjectRetention(shorter, no bypass)", c.s3.put_object_retention, Bucket=ob,
                                      Key=ok, VersionId=ov, Retention={"Mode": "GOVERNANCE", "RetainUntilDate": earlier}))
        elif kind == "overwrite-object":
            b = bytearray(orig_obj); b[len(b) // 2] ^= 0x01
            calls.append(c.store_call("PutObject(object)", c.s3.put_object, Bucket=ob, Key=ok, Body=bytes(b)))
        elif kind == "overwrite-manifest":
            m = json.loads(orig_man); m["record_count"] += 1
            calls.append(c.store_call("PutObject(manifest)", c.s3.put_object, Bucket=mb, Key=mk,
                                      Body=doc_bytes(seal(m, "manifest", "stale"))))
        elif kind == "delete-marker-object":
            calls.append(c.store_call("DeleteObject(object, no version)", c.s3.delete_object, Bucket=ob, Key=ok))
        elif kind == "bypass-clear":
            for what, b_, k_, v_ in (("object", ob, ok, ov), ("manifest", mb, mk, mv)):
                calls.append(c.store_call(f"PutObjectRetention({what}, cleared, bypass)", c.s3.put_object_retention,
                                          Bucket=b_, Key=k_, VersionId=v_, Retention={}, BypassGovernanceRetention=True))
        elif kind == "suspend-versioning":
            calls.append(c.store_call("PutBucketVersioning(Suspended)", c.s3.put_bucket_versioning, Bucket=ob,
                                      VersioningConfiguration={"Status": "Suspended"}))
            c.rec["facts"]["versioning_after"] = c.s3.get_bucket_versioning(Bucket=ob).get("Status")
            if c.rec["facts"]["versioning_after"] != "Enabled":   # never leave the lock bucket suspended
                c.s3.put_bucket_versioning(Bucket=ob, VersioningConfiguration={"Status": "Enabled"})
        c.store_verdict(calls)
        # Is the locked version still there, with its bytes and its lock?
        intact = {}
        for what, b_, k_, v_, orig in (("object", ob, ok, ov, orig_obj), ("manifest", mb, mk, mv, orig_man)):
            try:
                h = c.s3.head_object(Bucket=b_, Key=k_, VersionId=v_)
                intact[what] = {"bytes_same": c.get(f"s3://{b_}/{k_}", v_) == orig,
                                "lock_mode": h.get("ObjectLockMode"),
                                "retain_until": str(h.get("ObjectLockRetainUntilDate"))}
            except ClientError as e:
                intact[what] = {"error": client_error(e)}
        c.rec["facts"]["locked_versions_after"] = intact
        c.rec["facts"]["locked_version_intact"] = all(v.get("bytes_same") and v.get("lock_mode") == "GOVERNANCE"
                                                      for v in intact.values())
    return act


def act_none(c: Case):
    h = c.s3.head_object(**dict(zip(("Bucket", "Key"), split(c.target["object_uri"]))))
    c.rec["facts"]["object_head_lock"] = {"mode": h.get("ObjectLockMode"),
                                          "retain_until": str(h.get("ObjectLockRetainUntilDate"))}


# ------------------------------------------------------------------------------------------------- the case list
# expect: verifier -> expected verdict.  limit: verifiers whose expected verdict is the WRONG one (NOT CAUGHT).
# verifiers run in this order: tamper, scrub (+ variants), verify-manifest, check-storage, recovery (mutates, last).
L = "limit"
CASES = [
    # ---------------- H2.5: signed manifests, with and without the ledger as anchor (plain = unversioned bucket)
    dict(id="MAC-FORGE-OFF-B", hyp="H2.5", group="mac", profile="plain-off", attacker="bucket",
         action=act_forge("none", False), action_text="M7 consistent forgery (object + manifest, digest and self-hash recomputed), MAC off",
         expect={"scrub": "unauthenticated", "vm_noledger": "passed", "vm_ledger": "failed", "recovery": "replayed"},
         limit=["vm_noledger"]),
    dict(id="MAC-FORGE-OFF-BL", hyp="H2.5", group="mac", profile="plain-off", attacker="bucket+ledger",
         action=act_forge("none", True), action_text="M7 forgery, MAC off, ledger row rewritten to agree",
         expect={"scrub": "ok", "scrub.passed": True, "vm_noledger": "passed", "vm_ledger": "passed",
                 "recovery": "committed"},
         limit=["scrub", "scrub.passed", "vm_noledger", "vm_ledger", "recovery"],
         note="receipt rewritten too (MAC off: its self-hash is recomputable)"),
    dict(id="MAC-FORGE-OFF-BL-RCPT", hyp="H2.5", group="mac", profile="plain-off", attacker="bucket+ledger",
         action=act_forge("none", True, receipt=False),
         action_text="M7 forgery, MAC off, ledger rewritten, verification receipt left as written",
         expect={"scrub": "ok", "scrub.receipt": "mismatched", "scrub.passed": False, "vm_noledger": "passed",
                 "recovery": "committed"},
         limit=["scrub", "vm_noledger", "recovery"],
         note="the receipt still binds the original manifest digest: only the scrub's receipt check reports it"),
    dict(id="MAC-FORGE-ON-B", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket",
         action=act_forge("stale", False), action_text="M7 forgery, MAC on, attacker has no key (MAC left stale)",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "vm_ledger": "failed", "recovery": "replayed"}),
    dict(id="MAC-FORGE-ON-BL", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket+ledger",
         action=act_forge("stale", True), action_text="M7 forgery, MAC on (stale), ledger rewritten",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "vm_ledger": "failed", "recovery": "replayed"}),
    dict(id="MAC-STRIP-ON-BL", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket+ledger",
         action=act_forge("strip", True), action_text="M7 forgery, MAC and key id stripped, ledger rewritten",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "recovery": "replayed"}),
    dict(id="MAC-FOREIGN-BL", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket+ledger+own key",
         action=act_forge("mallory", True), action_text="M7 forgery signed with the attacker's own key under a made-up id, ledger rewritten",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "recovery": "replayed"}),
    dict(id="MAC-SWAP-OFF", hyp="H2.5", group="mac", profile="plain-off", attacker="bucket",
         action=act_swap, action_text="M4: another batch's genuine manifest copied over this batch's manifest, MAC off",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "vm_ledger": "failed", "recovery": "replayed"},
         note="v0.10.3+ (#882, 77579228): verify-manifest fails a manifest read from another key than the one it names "
              "(commands.rs verify_manifest_deep step 1b; observed failed in 20261122T021852Z and the v0104 local runs). "
              "Until v0.10.2 this was a stated limit (passed)"),
    dict(id="MAC-SWAP-ON", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket",
         action=act_swap, action_text="M4 manifest swap, MAC on (the swapped manifest is genuinely signed)",
         expect={"scrub": "unauthenticated", "vm_noledger": "failed", "vm_ledger": "failed", "recovery": "replayed"},
         note="v0.10.3+ (#882): the key binding catches the swap; the MAC alone does not (the swapped manifest is "
              "genuinely signed, and its MAC covers the key it names). Until v0.10.2 a stated limit (passed)"),
    dict(id="MAC-LEAKED-BL", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket+ledger+leaked k1",
         action=act_forge("k1", True), action_text="M7 forgery signed with the real key (leaked, not yet retired), ledger rewritten",
         expect={"scrub": "ok", "scrub.passed": True, "vm_noledger": "passed", "recovery": "committed"},
         limit=["scrub", "scrub.passed", "vm_noledger", "recovery"]),
    dict(id="MAC-RETIRED-BL", hyp="H2.5", group="mac", profile="plain-mac", attacker="bucket+ledger+leaked k1",
         action=act_forge("k1", True), action_text="the same leaked-key forgery, checked after k1 is rotated out (rotated) and retired",
         expect={"scrub_rotated": "ok", "scrub_rotated.passed": True, "scrub_retired": "unauthenticated",
                 "vm_noledger_retired": "failed", "recovery_retired": "replayed"},
         limit=["scrub_rotated", "scrub_rotated.passed"], extra=["retired"],
         note="retiring k1 also fails every honest k1 batch (by design); counted in scrub_retired.other_findings"),
    # ---------------- receipts (H2.5 family): batch results stay ok; the receipt result is the verdict
    dict(id="RCPT-EDIT-ON", hyp="H2.5", group="receipt", profile="plain-mac", attacker="bucket",
         action=act_receipt("method", "stale"), action_text="receipt verification.method edited, self-hash recomputed, MAC stale",
         expect={"scrub.receipt": "invalid", "scrub": "ok"}),
    dict(id="RCPT-EDIT-NOHASH-ON", hyp="H2.5", group="receipt", profile="plain-mac", attacker="bucket",
         action=act_receipt("method", "nohash"), action_text="receipt edited, self-hash not recomputed",
         expect={"scrub.receipt": "invalid", "scrub": "ok"}),
    dict(id="RCPT-SWAP-ON", hyp="H2.5", group="receipt", profile="plain-mac", attacker="bucket",
         action=act_receipt("swap"), action_text="another batch's genuine receipt copied over this batch's receipt",
         expect={"scrub.receipt": "mismatched", "scrub": "ok"}),
    dict(id="RCPT-DEL-ON", hyp="H2.5", group="receipt", profile="plain-mac", attacker="bucket",
         action=act_receipt("delete"), action_text="receipt deleted",
         expect={"scrub.receipt": "missing", "scrub_require.receipt": "missing", "scrub_require.passed": False,
                 "scrub.passed": True},
         extra=["require_receipts"], note="missing is a finding only with --require-receipts"),
    dict(id="RCPT-CLASS-OFF", hyp="H2.5", group="receipt", profile="plain-off", attacker="bucket",
         action=act_receipt("class", "none"), action_text="MAC off: receipt verification.class strong -> size_only, hash recomputed",
         expect={"scrub.receipt": "mismatched"}),
    dict(id="RCPT-BY-OFF", hyp="H2.5", group="receipt", profile="plain-off", attacker="bucket",
         action=act_receipt("verified_by", "none"), action_text="MAC off: receipt verified_by edited, hash recomputed",
         expect={"scrub.receipt": "ok"}, limit=["scrub.receipt"]),
    dict(id="RCPT-METHOD-OFF", hyp="H2.5", group="receipt", profile="plain-off", attacker="bucket",
         action=act_receipt("method", "none"), action_text="MAC off: receipt verification.method edited, hash recomputed",
         expect={"scrub.receipt": "ok"}, limit=["scrub.receipt"]),
    # ---------------- object behind an honest manifest (as RQ2 M1-M3; HEAD-only audit limit)
    dict(id="OBJ-FLIP-ON", hyp="H2.5", group="object", profile="plain-mac", attacker="bucket",
         action=act_object("flip"), action_text="M1 one object byte flipped (same size)",
         expect={"scrub": "mismatched", "check_storage": "passed", "vm_noledger": "failed", "recovery": "replayed"},
         limit=["check_storage"]),
    dict(id="OBJ-TRUNC-ON", hyp="H2.5", group="object", profile="plain-mac", attacker="bucket",
         action=act_object("truncate"), action_text="M2 object truncated to half",
         expect={"scrub": "mismatched", "check_storage": "failed", "recovery": "replayed"}),
    dict(id="OBJ-DEL-ON", hyp="H2.5", group="object", profile="plain-mac", attacker="bucket",
         action=act_object("delete"), action_text="M3 object deleted",
         expect={"scrub": "missing", "check_storage": "failed", "recovery": "replayed"}),
    # ---------------- H2.6: versioning (lock bucket used without the lock profile: versioned, unlocked writes)
    dict(id="VER-OVR-MAN", hyp="H2.6", group="versioning", profile="ver-mac", attacker="bucket",
         action=act_manifest_edit, action_text="versioned: manifest overwritten (M6 edit, MAC stale)",
         expect={"scrub": "mismatched", "vm_noledger": "failed", "recovery": "committed"},
         note="recovery reads the pinned version (intact) and commits: the overwrite is not followed, which is correct"),
    dict(id="VER-OVR-OBJ", hyp="H2.6", group="versioning", profile="ver-mac", attacker="bucket",
         action=act_object("flip"), action_text="versioned: object overwritten (M1 byte flip)",
         expect={"scrub": "mismatched", "recovery": "replayed"}),
    dict(id="VER-OVR-BOTH-OFF", hyp="H2.6", group="versioning", profile="ver-off", attacker="bucket",
         action=act_forge("none", False), action_text="versioned, MAC off: consistent forgery of object + manifest",
         expect={"scrub": "mismatched", "vm_noledger": "passed", "recovery": "replayed"}, limit=["vm_noledger"]),
    dict(id="VER-ROLLBACK", hyp="H2.6", group="versioning", profile="ver-mac", attacker="key holder (or earlier genuine write)",
         action=act_rollback, action_text="versioned: current key serves another validly signed manifest of the batch (rollback)",
         expect={"scrub": "mismatched", "vm_noledger": "passed", "vm_ledger": "passed", "recovery": "committed"},
         limit=["vm_noledger", "vm_ledger"],
         note="scrub FLAGS it (current key != pinned version); recovery does not follow it (reads the pin)"),
    dict(id="NOVER-ROLLBACK", hyp="H2.6", group="versioning", profile="plain-mac", attacker="key holder (or earlier genuine write)",
         action=act_rollback, action_text="unversioned: the same valid rollback manifest written over the key",
         expect={"scrub": "unauthenticated", "vm_noledger": "passed", "recovery": "replayed"}, limit=["vm_noledger"],
         note="no pin: caught by the ledger's recorded manifest digest only"),
    dict(id="VER-RESTORE", hyp="H2.6", group="versioning", profile="ver-mac", attacker="bucket",
         action=act_restore, action_text="versioned: manifest overwritten, then the pinned version copied back (control)",
         expect={"scrub": "ok"}, note="scrub compares bytes with the pinned version, not version ids"),
    dict(id="VER-DELMARK", hyp="H2.6", group="versioning", profile="ver-mac", attacker="bucket",
         action=act_manifest_delete(False), action_text="versioned: manifest DELETE without version (delete marker)",
         expect={"scrub": "missing", "vm_noledger": "failed", "recovery": "committed"},
         note="recovery reads the pinned version, which the marker does not remove"),
    dict(id="VER-DELPIN", hyp="H2.6", group="versioning", profile="ver-mac", attacker="bucket (+DeleteObjectVersion)",
         action=act_manifest_delete(True), action_text="versioned: the pinned manifest version deleted",
         expect={"scrub": "missing", "vm_noledger": "failed", "recovery": "replayed"}, note="fails closed (#135)"),
    # ---------------- H2.6: Object Lock, governance, 1 day
    dict(id="LOCK-CONTROL", hyp="H2.6", group="lock", profile="lock-mac", attacker="none",
         action=act_none, action_text="no tamper: retention as written",
         expect={"scrub": "ok", "scrub.retention": "locked", "scrub.receipt": "ok"}),
    dict(id="LOCK-DELVER-OBJ", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket (+DeleteObjectVersion)",
         action=act_lock("delete-version-object"), action_text="locked: DeleteObjectVersion of the object version",
         expect={"store": "refused", "scrub": "ok"}),
    dict(id="LOCK-DELVER-MAN", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket (+DeleteObjectVersion)",
         action=act_lock("delete-version-manifest"), action_text="locked: DeleteObjectVersion of the pinned manifest version",
         expect={"store": "refused", "scrub": "ok"}),
    dict(id="LOCK-SHORTEN", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket (+PutObjectRetention)",
         action=act_lock("shorten"), action_text="locked: retention shortened by 12 h without bypass",
         expect={"store": "refused", "scrub.retention": "locked"}),
    dict(id="LOCK-OVR-OBJ", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket",
         action=act_lock("overwrite-object"), action_text="locked: object overwritten (PUT, new version)",
         expect={"store": "accepted", "scrub": "mismatched", "recovery": "replayed"}),
    dict(id="LOCK-OVR-MAN", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket",
         action=act_lock("overwrite-manifest"), action_text="locked: manifest overwritten (PUT, new version)",
         expect={"store": "accepted", "scrub": "mismatched"}),
    dict(id="LOCK-DELMARK-OBJ", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket",
         action=act_lock("delete-marker-object"), action_text="locked: object DELETE without version (delete marker)",
         expect={"store": "accepted", "scrub": "missing"}),
    dict(id="LOCK-BYPASS-CLEAR", hyp="H2.6", group="lock", profile="lock-mac", attacker="break-glass (+BypassGovernanceRetention)",
         action=act_lock("bypass-clear"), action_text="locked: retention cleared with the governance bypass",
         expect={"store": "accepted", "scrub.retention": "missing"}, optional_permission="bypass"),
    dict(id="VER-SUSPEND-LOCKED", hyp="H2.6", group="lock", profile="lock-mac", attacker="bucket admin (+PutBucketVersioning)",
         action=act_lock("suspend-versioning"), action_text="versioning suspension requested on the Object Lock bucket",
         expect={"store": "refused"}, optional_permission="suspend"),
]

# Start-up refusals (no archive first). bucket/profile overrides; expect engine_start.
STARTUP = [
    dict(id="VER-REFUSE-UNVERSIONED", hyp="H2.6", group="versioning", profile="plain-mac", versioning=True,
         action_text="require_object_versioning: true on the unversioned bucket", expect={"engine_start": "refused"}),
    dict(id="LOCK-REFUSE-PLAIN", hyp="H2.6", group="lock", profile="plain-mac", versioning=True, lock=True,
         action_text="upload.object_lock on a bucket without Object Lock", expect={"engine_start": "refused"}),
    dict(id="VER-SUSPEND-NEW", hyp="H2.6", group="versioning", profile="ver-mac", bucket="susp", local_only=True,
         action_text="versioning enabled then suspended; a new engine starts against the bucket",
         expect={"engine_start": "refused"}),
]

# Audit trail and coverage (ledger-only; one archive per keying, then a fresh copy of the ledger per case).
AUDIT = [
    dict(id="AUD-CONTROL", keyed=True, kind="none", expect={"audit_verify": "passed"}),
    dict(id="AUD-UPDATE-TRIGGER", keyed=True, kind="update", expect={"sql": "refused", "audit_verify": "passed"}),
    dict(id="AUD-DELETE-TRIGGER", keyed=True, kind="delete", expect={"sql": "refused", "audit_verify": "passed"}),
    dict(id="AUD-EDIT", keyed=True, kind="edit", expect={"audit_verify": "failed"}),
    dict(id="AUD-DELETE-MID", keyed=True, kind="delete-mid", expect={"audit_verify": "failed"}),
    dict(id="AUD-SWAP", keyed=True, kind="swap", expect={"audit_verify": "failed"}),
    dict(id="AUD-TRUNCATE", keyed=True, kind="truncate", expect={"audit_verify": "passed", "anchor": "differs"},
         limit=["audit_verify"]),
    dict(id="AUD-RECOMPUTE-UNKEYED", keyed=False, kind="recompute-unkeyed", expect={"audit_verify": "passed"},
         limit=["audit_verify"]),
    dict(id="AUD-RECOMPUTE-NOKEY", keyed=True, kind="recompute-unkeyed", expect={"audit_verify": "failed"}),
    dict(id="AUD-REWRITE-ALL-NOKEY", keyed=True, kind="rewrite-all-unkeyed", expect={"audit_verify": "failed"}),
    dict(id="AUD-RECOMPUTE-LEAKED", keyed=True, kind="recompute-keyed", expect={"audit_verify": "passed"},
         limit=["audit_verify"]),
    dict(id="COV-DELETE-MID", keyed=True, kind="ledger-delete-mid", expect={"coverage": "hole"}),
    dict(id="COV-DELETE-END", keyed=True, kind="ledger-delete-end", expect={"coverage": "no_hole"}, limit=["coverage"]),
]


# ------------------------------------------------------------------------------------------------ case runners
def lookup(verdicts: dict, key: str):
    if key in verdicts:
        return verdicts[key]["verdict"]
    base, _, sub = key.rpartition(".")
    if base in verdicts and sub in verdicts[base]:
        return verdicts[base][sub]
    return "not run"


def judge(rec: dict, expect: dict, limit: list[str]):
    rows_ = []
    for k, want in expect.items():
        got = lookup(rec["verdicts"], k)
        rows_.append({"verifier": k, "expected": want, "observed": got, "match": got == want,
                      "limit": k in (limit or [])})
    rec["checks"] = rows_
    rec["as_expected"] = all(r["match"] for r in rows_)


def run_case(lab: Lab, spec: dict) -> dict:
    c = Case(lab, spec)
    try:
        c.archive()
        spec["action"](c)
        exp = spec["expect"]
        need = lambda k: any(x == k or x.startswith(k + ".") for x in exp)  # noqa: E731
        if need("scrub") or c.profile["lock"]:
            c.scrub("scrub", extra=("--check-retention",) if c.profile["lock"] else ())
        if "require_receipts" in spec.get("extra", []):
            c.scrub("scrub_require", extra=("--require-receipts",))
        if "retired" in spec.get("extra", []):
            c.scrub("scrub_rotated", cfg=lab.config(c.root, c.prefix, c.profile, "rotated"))
            c.scrub("scrub_retired", cfg=lab.config(c.root, c.prefix, c.profile, "retired"))
            c.verify_manifest("vm_noledger_retired", ledger=False, keyring="retired")
        if need("vm_noledger"):
            c.verify_manifest("vm_noledger", ledger=False)
        if need("vm_ledger"):
            c.verify_manifest("vm_ledger", ledger=True)
        if need("check_storage"):
            c.check_storage()
        if need("recovery_retired"):
            c.recovery("recovery_retired", keyring="retired")
        elif need("recovery"):
            c.recovery("recovery")
    except Exception as e:  # noqa: BLE001
        c.rec["error"] = f"{type(e).__name__}: {e}"
        c.rec["traceback"] = traceback.format_exc()[-2000:]
    judge(c.rec, spec["expect"], spec.get("limit"))
    c.rec["note"] = spec.get("note")
    c.cleanup()
    c.rec["finished_at"] = now()
    save_logs(lab, c)
    return c.rec


def run_startup(lab: Lab, spec: dict) -> dict:
    root = lab.work / spec["id"]
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    prof = PROFILES[spec["profile"]]
    rec = {"id": spec["id"], "hypothesis": spec["hyp"], "group": spec["group"], "profile": spec["profile"],
           "action": spec["action_text"], "verdicts": {}, "facts": {}, "started_at": now()}
    bucket_role = spec.get("bucket", prof["bucket"])
    bucket = lab.buckets.get(bucket_role)
    if spec.get("local_only") and (lab.a.endpoint is None or not bucket):
        rec.update({"skipped": "local only (needs a versioned bucket whose versioning can be suspended)"})
        judge(rec, {}, [])
        return rec
    prefix = f"{lab.a.prefix.rstrip('/')}/{spec['id']}/"
    try:
        if spec["id"] == "VER-SUSPEND-NEW":
            lab.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            lab.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
            rec["facts"]["versioning"] = lab.s3.get_bucket_versioning(Bucket=bucket).get("Status")
        cfg = lab.config(root, prefix, prof, bucket=bucket, versioning=spec.get("versioning"), lock=spec.get("lock"))
        log = root / "engine.jsonl"
        p = lab.start(cfg, log)
        t0 = time.monotonic()
        while time.monotonic() - t0 < START_REFUSAL_WAIT_S and p.poll() is None:
            time.sleep(0.25)
        running = p.poll() is None
        lab.stop(p)
        msgs = []
        for line in log.read_text(errors="replace").splitlines():
            if '"ERROR"' in line or "did not start" in line or "refus" in line:
                try:
                    msgs.append(json.loads(line)["fields"].get("message", "")[:400])
                except (ValueError, KeyError):
                    msgs.append(line[:400])
        written = sum(len(pg.get("Contents", [])) for pg in
                      lab.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix))
        rec["verdicts"]["engine_start"] = {"verdict": "started" if running else "refused", "exit_code": p.returncode,
                                           "messages": msgs[-3:], "objects_written": written,
                                           "committed_end": committed_end(root / "state" / "vtop.db")}
        shutil.copy(log, root / "engine-1.jsonl")
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"
    judge(rec, spec["expect"], [])
    rec["finished_at"] = now()
    d = lab.out / "logs" / spec["id"]
    d.mkdir(parents=True, exist_ok=True)
    for f in root.glob("*.jsonl"):
        (d / (f.name + ".gz")).write_bytes(gzip.compress(f.read_bytes()))
    return rec


def run_suspend_running(lab: Lab) -> dict:
    """Local only: versioning suspended under a running engine (needs a non-locked versioned bucket)."""
    rec = {"id": "VER-SUSPEND-RUNNING", "hypothesis": "H2.6", "group": "versioning", "profile": "ver-mac",
           "action": "versioning suspended while the engine runs; more input appended", "verdicts": {}, "facts": {},
           "started_at": now()}
    bucket = lab.buckets.get("susp")
    if lab.a.endpoint is None or not bucket:
        rec["skipped"] = "local only"
        judge(rec, {}, [])
        return rec
    root = lab.work / rec["id"]
    shutil.rmtree(root, ignore_errors=True)
    (root / "in").mkdir(parents=True)
    half = lab.src[: len(lab.src) // 2]
    half = half[: half.rfind(b"\n") + 1]
    rest = lab.src[len(half):]
    inp = root / "in" / "s.log"
    inp.write_bytes(half)
    prefix = f"{lab.a.prefix.rstrip('/')}/{rec['id']}/"
    lab.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    cfg = lab.config(root, prefix, PROFILES["ver-mac"], bucket=bucket, input_dir=root / "in")
    db = root / "state" / "vtop.db"
    log = root / "engine.jsonl"
    p = lab.start(cfg, log)
    t0 = time.monotonic()
    while time.monotonic() - t0 < ARCHIVE_TIMEOUT_S and committed_end(db) < len(half):
        time.sleep(0.25)
    before = committed_end(db)
    lab.s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    n_before = sum(len(pg.get("Contents", [])) for pg in
                   lab.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix))
    with open(inp, "ab") as fh:
        fh.write(rest)
    time.sleep(25)
    after = committed_end(db)
    alive = p.poll() is None
    lab.stop(p)
    n_after = sum(len(pg.get("Contents", [])) for pg in
                  lab.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix))
    events = {}
    for line in log.read_text(errors="replace").splitlines():
        try:
            f = json.loads(line)["fields"]
        except (ValueError, KeyError):
            continue
        if "event" in f:
            events[f["event"]] = events.get(f["event"], 0) + 1
    fails = sorted({json.loads(l)["fields"].get("error", "")[:160] for l in log.read_text(errors="replace").splitlines()
                    if '"batch failed"' in l})
    rec["verdicts"]["running_engine"] = {
        "verdict": "committed_nothing" if after == before else "committed",
        "committed_before": before, "committed_after": after, "input_bytes": len(lab.src), "engine_alive": alive,
        "objects_before": n_before, "objects_after": n_after, "events": events, "batch_failed_errors": fails}
    rec["verdicts"]["writes_after_suspension"] = {"verdict": n_after - n_before}
    # engine maintainer (hypotheses.md H2.6, PR #859): "a running one commits nothing and writes nothing"
    judge(rec, {"running_engine": "committed_nothing", "writes_after_suspension": 0}, [])
    rec["finished_at"] = now()
    d = lab.out / "logs" / rec["id"]
    d.mkdir(parents=True, exist_ok=True)
    (d / "engine.jsonl.gz").write_bytes(gzip.compress(log.read_bytes()))
    return rec


# ---- audit trail
def audit_base(lab: Lab, keyed: bool) -> dict:
    """One archive, then three `vtopctl replay` (no incomplete batch: each writes an attempt and a success record),
    giving a six-row chain. Snapshot taken with the SQLite backup API (one file, no WAL)."""
    spec = {"id": f"AUD-BASE-{'KEYED' if keyed else 'UNKEYED'}", "hyp": "H2.7", "group": "audit",
            "profile": "plain-mac" if keyed else "plain-off", "action_text": "archive + 3 replays", "expect": {}}
    c = Case(lab, spec)
    c.archive()
    for _ in range(3):
        r = sh([lab.vtopctl, "replay", "--config", c.cfg], env=lab.env)
        if r["rc"] != 0:
            raise RuntimeError(f"replay failed: {r['stderr'][-400:]}")
    snap = c.root / "snapshot.db"
    src = sqlite3.connect(c.db)
    dst = sqlite3.connect(snap)
    src.backup(dst)
    src.close()
    dst.close()
    base = {"case": c, "snapshot": snap}
    base["control"] = audit_verify(lab, c, snap, c.root / "control")
    return base


def audit_copy(c: Case, snap: Path, root: Path) -> tuple[Path, Path]:
    shutil.rmtree(root, ignore_errors=True)
    (root / "state").mkdir(parents=True)
    os.chmod(root / "state", 0o700)
    db = root / "state" / "vtop.db"
    shutil.copy(snap, db)
    os.chmod(db, 0o600)
    cfg = c.lab.config(root, c.prefix, c.profile, state=root / "state")
    return db, cfg


def audit_verify(lab: Lab, c: Case, snap: Path, root: Path, db_cfg=None) -> dict:
    db, cfg = db_cfg or audit_copy(c, snap, root)
    r = lab.vtop("audit", "verify", "--config", cfg)
    j = r["json"] or {}
    return {"verdict": "passed" if (r["rc"] == 0 and j.get("passed")) else "failed", "rc": r["rc"],
            "rows": j.get("rows"), "keyed_rows": j.get("keyed_rows"), "last_seq": j.get("last_seq"),
            "last_hash": j.get("last_hash"), "problems": [p[:200] for p in (j.get("problems") or [])][:4]}


def run_audit_case(lab: Lab, base: dict, spec: dict) -> dict:
    c, snap = base["case"], base["snapshot"]
    root = lab.work / spec["id"]
    rec = {"id": spec["id"], "hypothesis": "H2.7", "group": "audit" if spec["id"].startswith("AUD") else "coverage",
           "profile": c.spec["profile"], "action": spec["kind"], "verdicts": {}, "facts": {}, "started_at": now()}
    try:
        db, cfg = audit_copy(c, snap, root)
        con = sqlite3.connect(db)
        rows_ = con.execute("SELECT seq, audit_id, time, action, outcome, actor, target, audit_version, record, "
                            "prev_hash, hash, chain FROM audit_events ORDER BY seq").fetchall()
        rec["facts"]["rows_before"] = len(rows_)
        kind = spec["kind"]
        drop = "DROP TRIGGER IF EXISTS trg_audit_events_no_update; DROP TRIGGER IF EXISTS trg_audit_events_no_delete;"
        mid = rows_[2][0]   # third row
        if kind in ("update", "delete"):
            try:
                if kind == "update":
                    con.execute("UPDATE audit_events SET outcome='failure' WHERE seq=?", (mid,))
                else:
                    con.execute("DELETE FROM audit_events WHERE seq=?", (mid,))
                con.commit()
                rec["verdicts"]["sql"] = {"verdict": "accepted"}
            except sqlite3.DatabaseError as e:
                rec["verdicts"]["sql"] = {"verdict": "refused", "error": str(e)}
        elif kind == "edit":
            con.executescript(drop)
            recd = json.loads(rows_[2][8]); recd["outcome"] = "failure"
            con.execute("UPDATE audit_events SET record=?, outcome='failure' WHERE seq=?", (json.dumps(recd), mid))
        elif kind == "delete-mid":
            con.executescript(drop)
            con.execute("DELETE FROM audit_events WHERE seq=?", (mid,))
        elif kind == "truncate":
            con.executescript(drop)
            con.execute("DELETE FROM audit_events WHERE seq=?", (rows_[-1][0],))
        elif kind == "swap":
            con.executescript(drop)
            a, b = rows_[2], rows_[3]
            con.execute("DELETE FROM audit_events WHERE seq IN (?,?)", (a[0], b[0]))
            ins = ("INSERT INTO audit_events (seq, audit_id, time, action, outcome, actor, target, audit_version, "
                   "record, prev_hash, hash, chain) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            con.execute(ins, (a[0], *b[1:]))
            con.execute(ins, (b[0], *a[1:]))
        elif kind in ("recompute-unkeyed", "recompute-keyed", "rewrite-all-unkeyed"):
            con.executescript(drop)
            start = 0 if kind == "rewrite-all-unkeyed" else 2
            key = "k1" if kind == "recompute-keyed" else None
            prev = rows_[start - 1][10] if start else None
            for i, r in enumerate(rows_):
                if i < start:
                    continue
                record = r[8]
                if i == 2:
                    recd = json.loads(record); recd["outcome"] = "failure"; record = json.dumps(recd)
                h = audit_link(prev, record, key)
                chain = f"blake3-keyed:{key}" if key else "blake3"
                con.execute("UPDATE audit_events SET record=?, prev_hash=?, hash=?, chain=? WHERE seq=?",
                            (record, prev, h, chain, r[0]))
                prev = h
        elif kind in ("ledger-delete-mid", "ledger-delete-end"):
            bs = con.execute("SELECT batch_id, CAST(json_extract(progress_end_json,'$.end_byte') AS INTEGER) e FROM "
                             "batches WHERE state='source_committed' ORDER BY e").fetchall()
            victim = bs[len(bs) // 2][0] if kind == "ledger-delete-mid" else bs[-1][0]
            con.execute("DELETE FROM batches WHERE batch_id=?", (victim,))
            rec["facts"]["deleted_row_of"] = len(bs)
        con.commit()
        con.close()
        os.chmod(db, 0o600)
        if kind.startswith("ledger-"):
            r = lab.vtop("audit-coverage", "--config", cfg)
            j = r["json"] or {}
            holes = [h for u in j.get("units", []) for h in (u.get("unexplained_holes") or []) + (u.get("failed_holes") or [])]
            rec["verdicts"]["coverage"] = {"verdict": "hole" if holes else "no_hole", "rc": r["rc"], "holes": holes[:3]}
        else:
            v = audit_verify(lab, c, snap, root, (db, cfg))
            rec["verdicts"]["audit_verify"] = v
            ctl = base["control"]
            same = (v.get("rows"), v.get("last_seq"), v.get("last_hash")) == (ctl["rows"], ctl["last_seq"], ctl["last_hash"])
            rec["verdicts"]["anchor"] = {"verdict": "same" if same else "differs",
                                         "kept": [ctl["rows"], ctl["last_seq"], (ctl["last_hash"] or "")[:16]],
                                         "now": [v.get("rows"), v.get("last_seq"), (v.get("last_hash") or "")[:16]]}
    except Exception as e:  # noqa: BLE001
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["traceback"] = traceback.format_exc()[-1500:]
    judge(rec, spec["expect"], spec.get("limit"))
    rec["finished_at"] = now()
    return rec


# ------------------------------------------------------------------------------------------------ records
def save_logs(lab: Lab, c: Case):
    d = lab.out / "logs" / c.id
    d.mkdir(parents=True, exist_ok=True)
    for f in list(c.logs.glob("*.jsonl")) + list(c.logs.glob("*.json")):
        (d / (f.name + ".gz")).write_bytes(gzip.compress(f.read_bytes()))
    if c.db.exists():
        con = sqlite3.connect(c.db)
        dst = sqlite3.connect(d / "ledger.db")
        con.backup(dst)
        con.close()
        dst.close()


def reimpl_check(lab: Lab) -> dict:
    """Before any case: archive once (MAC on, lock bucket if available, else plain) and check that this file's
    re-implementation reproduces the engine's manifest self-hash and MAC, receipt hash and MAC, and audit links."""
    spec = {"id": "REIMPL-CHECK", "hyp": "-", "group": "check", "profile": "plain-mac", "action_text": "none",
            "expect": {}}
    c = Case(lab, spec)
    c.archive()
    out = {"manifests": [], "receipts": []}
    for r in [x for x in rows(c.db) if x["state"] == "source_committed"]:
        m = json.loads(c.get(r["manifest_uri"]))
        s = seal(m, "manifest", "k1")
        out["manifests"].append(s["manifest"]["sha256"] == m["manifest"]["sha256"] and s["manifest"]["mac"] == m["manifest"]["mac"])
        rc = json.loads(c.get(c.receipt_uri(r)))
        s = seal(rc, "receipt", "k1")
        out["receipts"].append(s["receipt"]["sha256"] == rc["receipt"]["sha256"] and s["receipt"]["mac"] == rc["receipt"]["mac"])
    sh([lab.vtopctl, "replay", "--config", c.cfg], env=lab.env)
    con = sqlite3.connect(c.db)
    prev, links = None, []
    for rec, ph, h, chain in con.execute("SELECT record, prev_hash, hash, chain FROM audit_events ORDER BY seq"):
        links.append(ph == prev and audit_link(prev, rec, chain.split(":", 1)[1] if ":" in chain else None) == h)
        prev = h
    con.close()
    out["audit_links"] = links
    out["ok"] = all(out["manifests"]) and all(out["receipts"]) and all(links) and bool(links) and bool(out["manifests"])
    c.cleanup()
    return out


def preflight(lab: Lab) -> dict:
    out = {}
    for role, b in lab.buckets.items():
        if not b:
            continue
        d = {}
        try:
            d["versioning"] = lab.s3.get_bucket_versioning(Bucket=b).get("Status")
        except ClientError as e:
            d["versioning"] = client_error(e)
        try:
            d["object_lock"] = lab.s3.get_object_lock_configuration(Bucket=b).get("ObjectLockConfiguration")
        except ClientError as e:
            d["object_lock"] = client_error(e)
        out[role] = {"bucket": b, **d}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vtopctl", required=True)
    ap.add_argument("--input", required=True, help="newline-terminated input file (the only *.log in its directory)")
    ap.add_argument("--plain-bucket", required=True, help="bucket without versioning or Object Lock")
    ap.add_argument("--lock-bucket", required=True, help="bucket created with Object Lock (versioned), no default retention")
    ap.add_argument("--susp-bucket", default="", help="local only: a non-locked bucket whose versioning may be suspended")
    ap.add_argument("--prefix", required=True, help="per-run data prefix, e.g. tamper/data/<run_id>")
    ap.add_argument("--out", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--endpoint", default="", help="lab only: S3 endpoint (MinIO). Never set on EC2")
    ap.add_argument("--cases", default="", help="comma-separated case ids to run (default: all)")
    ap.add_argument("--results-uri", default="")
    ap.add_argument("--deadline-epoch", type=float, default=0)
    ap.add_argument("--optional-permissions", default="",
                    help="comma list of optional grants the author approved: bypass (s3:BypassGovernanceRetention), "
                         "suspend (s3:PutBucketVersioning on the lock bucket). Cases needing one that is missing are "
                         "skipped: on AWS an IAM denial and an Object Lock refusal are both AccessDenied. Lab: all")
    ap.add_argument("--meta", default="{}", help="JSON merged into run.json (engine pin, instance, ...)")
    a = ap.parse_args()
    lab = Lab(a)
    lab.out.mkdir(parents=True, exist_ok=True)
    meta = {"experiment": "E-RQ5-TAMPER", "hypotheses": ["H2.5", "H2.6", "H2.7"], "started_at": now(),
            "vtopctl": str(lab.vtopctl), "vtopctl_sha256": hashlib.sha256(lab.vtopctl.read_bytes()).hexdigest(),
            "vtopctl_version": sh([lab.vtopctl, "--version"])["stdout"].strip(),
            "input": {"path": str(lab.input), "bytes": len(lab.src), "sha256": hashlib.sha256(lab.src).hexdigest()},
            "batching_max_bytes": MAX_BYTES, "buckets": lab.buckets, "endpoint": a.endpoint or "aws",
            "prefix": a.prefix, "platform": platform.platform(), "python": sys.version.split()[0],
            "boto3": boto3.__version__, "lab_keys": "k1=6b*32, k2=6c*32, mallory=a7*32 (lab-only, published)",
            "object_lock": "governance, retain_days 1 (engine minimum)",
            "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), **json.loads(a.meta)}
    meta["preflight"] = pf = preflight(lab)
    if pf["plain"].get("versioning") == "Enabled":
        raise SystemExit(f"plain bucket {a.plain_bucket} is versioned: the unversioned arms would not be unversioned")
    if (pf["lock"].get("versioning") != "Enabled"
            or not str(pf["lock"].get("object_lock") or "").count("Enabled")):
        raise SystemExit(f"lock bucket {a.lock_bucket} lacks versioning or Object Lock: {pf['lock']}")
    meta["reimpl_check"] = reimpl_check(lab)
    (lab.out / "run.json").write_text(json.dumps(meta, indent=1, default=str))
    if not meta["reimpl_check"]["ok"]:
        raise SystemExit("re-implementation check failed: tamper cases would not be valid")
    want = set(filter(None, a.cases.split(",")))
    sel = lambda i: not want or i in want  # noqa: E731
    out = open(lab.out / "cases.jsonl", "a")

    def emit(rec):
        out.write(json.dumps(rec, default=str) + "\n")
        out.flush()
        tag = "skip" if rec.get("skipped") else ("ok  " if rec.get("as_expected") else "DIFF")
        print(f"[{tag}] {rec['id']:24s} " + "  ".join(f"{x['verifier']}={x['observed']}" for x in rec.get("checks", []))
              + (f"  ERROR {rec['error']}" if rec.get("error") else ""), flush=True)
        if a.results_uri:
            sh(["aws", "s3", "sync", "--only-show-errors", lab.out, a.results_uri], timeout=300)

    def time_left():
        return not a.deadline_epoch or time.time() < a.deadline_epoch

    for spec in STARTUP:
        if sel(spec["id"]) and time_left():
            emit(run_startup(lab, spec))
    if sel("VER-SUSPEND-RUNNING") and time_left():
        emit(run_suspend_running(lab))
    granted = {"bypass", "suspend"} if a.endpoint else set(filter(None, a.optional_permissions.split(",")))
    meta["optional_permissions"] = sorted(granted)
    for spec in CASES:
        if not (sel(spec["id"]) and time_left()):
            continue
        if spec.get("optional_permission") and spec["optional_permission"] not in granted:
            rec = {"id": spec["id"], "hypothesis": spec["hyp"], "group": spec["group"], "action": spec["action_text"],
                   "verdicts": {}, "skipped": f"optional permission '{spec['optional_permission']}' not granted"}
            judge(rec, {}, [])
            emit(rec)
            continue
        emit(run_case(lab, spec))
    if any(sel(s["id"]) for s in AUDIT) and time_left():
        bases = {}
        for keyed in (True, False):
            try:
                bases[keyed] = audit_base(lab, keyed)
            except Exception as e:  # noqa: BLE001
                bases[keyed] = {"error": f"{type(e).__name__}: {e}"}
        for spec in AUDIT:
            if not sel(spec["id"]):
                continue
            base = bases[spec["keyed"]]
            if "error" in base:
                rec = {"id": spec["id"], "hypothesis": "H2.7", "error": base["error"], "verdicts": {}}
                judge(rec, spec["expect"], spec.get("limit"))
                emit(rec)
                continue
            emit(run_audit_case(lab, base, spec))
        for b in bases.values():
            if "case" in b:
                b["case"].cleanup()
    meta["finished_at"] = now()
    (lab.out / "run.json").write_text(json.dumps(meta, indent=1, default=str))
    out.close()
    if a.results_uri:
        sh(["aws", "s3", "sync", "--only-show-errors", lab.out, a.results_uri], timeout=300)


if __name__ == "__main__":
    main()
