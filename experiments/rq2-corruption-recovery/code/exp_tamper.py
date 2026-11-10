#!/usr/bin/env python3
"""E-RQ2-R: tampering with archived evidence while the engine is down.

Procedure per trial:
 1. Archive a Loghub file completely with ``vtopctl run``.
 2. Fault injection by state injection: set the ledger row of the batch with
    the highest end byte back to ``verified``. This is exactly the durable
    state VTOP leaves after a crash between verification and source commit
    (fault F4): the row is VERIFIED and the committed cursor, which the file
    adapter seeds from the newest committed row, stops before it.
 3. Apply one mutation to that batch's object or manifest.
 4. Restart ``vtopctl run`` and let recovery decide.

Outcome: ``accepted`` (the mutated batch was committed as is) or
``detected`` (recovery refused it and the range was archived again), plus a
final oracle check that the archive is complete and byte-exact.

Mutations (no secret key is used for any of them):
  M0 none (control)              M5 manifest field edited, stale self-hash
  M1 one object byte flipped     M6 manifest field edited, self-hash recomputed
  M2 object truncated            M7 consistent forgery: new object, manifest
  M3 object deleted                 digest/size updated, self-hash recomputed
  M4 object replaced by another  M8 manifest deleted
     batch's valid object
Variants: strong (SHA-256), strong+MAC (keyed BLAKE3 manifest code),
size-only (checksum disabled, require_strong_verification: false).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from vtoplab import DATASET_FILES, VTOP_COMMIT, Run, Variant, host_info, oracle  # noqa: E402

VARIANTS = {
    "strong": Variant("strong"),
    "strong+mac": Variant("strong+mac", mac=True),
    "size-only": Variant("size-only", checksum="none", strong=False),
}
MUTATIONS = ["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8"]


def self_hash(manifest: dict) -> str:
    """Re-implementation of VtopManifest::self_hash (compact serde_json, hash/MAC blanked)."""
    m = json.loads(json.dumps(manifest))
    m["manifest"]["sha256"] = ""
    if m["manifest"].get("mac") is not None:
        m["manifest"]["mac"] = ""
    return hashlib.sha256(json.dumps(m, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def target_batch(run: Run):
    con = sqlite3.connect(run.db)
    row = con.execute("SELECT batch_id, manifest_uri, object_uri FROM batches WHERE state='source_committed' "
                      "ORDER BY CAST(json_extract(progress_end_json,'$.end_byte') AS INTEGER) DESC LIMIT 1").fetchone()
    other = con.execute("SELECT object_uri FROM batches WHERE state='source_committed' "
                        "ORDER BY CAST(json_extract(progress_end_json,'$.end_byte') AS INTEGER) ASC LIMIT 1").fetchone()
    con.close()
    return row, other[0]


def local(run: Run, uri: str) -> Path:
    return run.objects / uri[len("s3://"):]


def mutate(run: Run, kind: str, man_p: Path, obj_p: Path, other_obj: Path):
    m = json.loads(man_p.read_bytes())
    if kind == "M0":
        return
    if kind == "M1":
        b = bytearray(obj_p.read_bytes()); b[len(b) // 2] ^= 0x01; obj_p.write_bytes(bytes(b))
    elif kind == "M2":
        b = obj_p.read_bytes(); obj_p.write_bytes(b[: len(b) // 2])
    elif kind == "M3":
        obj_p.unlink()
    elif kind == "M4":
        obj_p.write_bytes(other_obj.read_bytes())
    elif kind == "M5":
        m["record_count"] += 1
        man_p.write_bytes(json.dumps(m, indent=2).encode())
    elif kind == "M6":
        m["record_count"] += 1
        m["manifest"]["sha256"] = self_hash(m)
        man_p.write_bytes(json.dumps(m, indent=2).encode())
    elif kind == "M7":
        plain = zstandard.ZstdDecompressor().decompress(obj_p.read_bytes(), max_output_size=1 << 31)
        forged_plain = plain.replace(b"Failed password", b"Accepted passwd", 1) if b"Failed password" in plain \
            else plain[:-1] + b"X"
        forged = zstandard.ZstdCompressor(level=3).compress(forged_plain)
        obj_p.write_bytes(forged)
        if m["object"]["checksum_algorithm"] == "sha256":
            m["object"]["checksum"] = hashlib.sha256(forged).hexdigest()
        m["object"]["size_bytes"] = len(forged)
        m["manifest"]["sha256"] = self_hash(m)
        man_p.write_bytes(json.dumps(m, indent=2).encode())
    elif kind == "M8":
        man_p.unlink()
    for side in (Path(str(obj_p) + ".vtopck"), Path(str(man_p) + ".vtopck")):
        pass  # sidecars are inventory hints only; VTOP never trusts them, so leave them


def trial(out: Path, ds: str, vname: str, kind: str, rep: int) -> dict:
    v = VARIANTS[vname]
    v = Variant(v.name, checksum=v.checksum, strong=v.strong, mac=v.mac, max_bytes=(2 if ds == "ssh" else 1) * 2**20 // (1 if ds == "ssh" else 4), max_age_s=1)
    root = out / "raw" / f"{ds}-{vname}-{kind}-r{rep}"
    shutil.rmtree(root, ignore_errors=True)
    run = Run(root, v, {ds: DATASET_FILES[ds]})
    run.start(); ok = run.wait_drained(300); run.stop()
    assert ok, "initial archive did not drain"
    (bid, muri, ouri), other = target_batch(run)
    man_p, obj_p, other_p = local(run, muri), local(run, ouri), local(run, other)
    # sanity: our self-hash reimplementation matches the stored manifest
    sh_ok = self_hash(json.loads(man_p.read_bytes())) == json.loads(man_p.read_bytes())["manifest"]["sha256"]
    con = sqlite3.connect(run.db)
    con.execute("UPDATE batches SET state='verified' WHERE batch_id=?", (bid,))
    con.commit(); con.close()
    committed_before = run.committed()[str(run.sources[ds])]
    mutate(run, kind, man_p, obj_p, other_p)
    run.start(); t0 = time.time()
    ok = run.wait_drained(120)
    elapsed = time.time() - t0
    time.sleep(0.5)
    run.stop()
    con = sqlite3.connect(run.db)
    final_state, err = con.execute("SELECT state, error_message FROM batches WHERE batch_id=?", (bid,)).fetchone()
    replays = con.execute("SELECT COUNT(*) FROM batches WHERE batch_id<>? AND state='source_committed' AND "
                          "CAST(json_extract(progress_end_json,'$.end_byte') AS INTEGER) > ?", (bid, committed_before)).fetchone()[0]
    con.close()
    reasons = set()
    for lf in sorted(run.logs.glob("engine-*.jsonl"))[1:]:  # logs of the restart only
        for line in lf.read_text(errors="replace").splitlines():
            if "re-check" in line or "recovery" in line and "WARN" in line:
                try:
                    msg = json.loads(line)["fields"]["message"]
                except (ValueError, KeyError):
                    continue
                if "re-check" in msg or "failed storage re-check" in msg:
                    reasons.add(msg)
    o = oracle(run)
    s = o["sources"][ds]
    rec = {"dataset": ds, "variant": vname, "mutation": kind, "rep": rep, "batch_id": bid,
           "selfhash_reimpl_ok": sh_ok, "final_state": final_state, "error": err,
           "outcome": "accepted" if final_state == "source_committed" else ("detected" if final_state in ("failed", "replay_required") else final_state),
           "rearchived_batches": replays, "drained": ok, "recovery_to_drain_s": round(elapsed, 3),
           "archive_complete": s["complete"], "bad_objects": len(o["bad_objects"]),
           "bad_object_problems": sorted({b["problem"] for b in o["bad_objects"]}),
           "unsafe_commit": s["unsafe_commit"], "recheck_reasons": sorted(reasons)}
    shutil.rmtree(root / "raw", ignore_errors=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--datasets", default="ssh,linux")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--seed", type=int, default=20261110)
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    meta = {"experiment": "E-RQ2-R", "vtop_commit": VTOP_COMMIT, "host": host_info(), "seed": a.seed,
            "fault_injection": "ledger state injection (F4) + storage mutation while down", "backend": "localfs",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    cells = [(d, v, k, r) for d in a.datasets.split(",") for v in VARIANTS for k in MUTATIONS for r in range(a.reps)]
    random.Random(a.seed).shuffle(cells)
    with open(out / "trials.jsonl", "a") as fh:
        for i, (d, v, k, r) in enumerate(cells, 1):
            rec = trial(out, d, v, k, r)
            fh.write(json.dumps(rec) + "\n"); fh.flush()
            print(f"[{i}/{len(cells)}] {d:5s} {v:10s} {k} r{r}: {rec['outcome']:9s} complete={rec['archive_complete']} "
                  f"bad={rec['bad_object_problems']} err={(rec['error'] or '')[:70]}", flush=True)
    meta["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (out / "run.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
