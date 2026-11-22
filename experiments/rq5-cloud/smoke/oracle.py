#!/usr/bin/env python3
"""Independent S3 oracle for the RQ5 cloud smoke test (vtop-engine v0.10.1).

Adapted from s3_oracle() in research/experiments/rq3-cost-ablation/code/exp_minio_cost.py
and oracle() in research/experiments/lib/vtoplab.py. It uses no VTOP code. For one run it:

  1. lists the S3 prefix (data objects, *.manifest.json, *.receipt.json);
  2. for every ledger row, downloads the manifest and the object, re-hashes the object
     (SHA-256 and BLAKE3), decompresses it, and compares the bytes with the source file at
     the ledger's byte range;
  3. HEADs every object with ChecksumMode=ENABLED and records what S3 itself stored
     (ChecksumSHA256 / CRC32 / CRC64NVME, ChecksumType, ETag part suffix), so the path the
     engine could take (store checksum vs read-back) is observed independently;
  4. checks byte-exact coverage: the union of valid committed ranges must reach the
     source size from byte 0, and no committed row may name a bad object.

Object keys are stable across replays in v0.10.x (#626; build_object_name, batch.rs:311);
manifests are per attempt. The oracle maps objects only through the ledger's object_uri and
the manifest, counts distinct object keys, and never assumes one object key per attempt.

Usage (standalone re-check on the VM):
  python oracle.py --db STATE.db --source INPUT.log --bucket B --prefix P/ --mode sha256-store
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sqlite3
import sys

import blake3
import boto3
import io
import zstandard

# What each mode must show. "method" is what the receipt (and the HEAD evidence) must say for
# a single-PUT object; multipart objects keep no whole-object SHA-256 on S3, so the engine
# reads them back (s3_native.rs:1389-1420 at v0.10.1).
EXPECT = {
    "size-only":        {"algorithm": "none",   "class": "size_only", "single_put": "size_only"},
    "sha256-store":     {"algorithm": "sha256", "class": "strong",    "single_put": "store_checksum",
                         "require_single_put": True},
    "blake3-readback":  {"algorithm": "blake3", "class": "strong",    "single_put": "read_back"},
    "sha256-multipart": {"algorithm": "sha256", "class": "strong",    "single_put": "store_checksum",
                         "require_multipart": True},
}


def split_uri(uri: str) -> tuple[str, str]:
    assert uri.startswith("s3://"), uri
    bucket, key = uri[5:].split("/", 1)
    return bucket, key


def b64_to_hex(value: str | None) -> str | None:
    if not value or "-" in value:  # composite multipart checksums end in -<parts>
        return None
    raw = base64.b64decode(value)
    return raw.hex() if len(raw) == 32 else None


def decompress(data: bytes, codec: str) -> bytes:
    if codec == "zstd":
        # Stream it: decompress(..., max_output_size=2 GiB) preallocates the whole limit when the frame
        # has no content size, which raised MemoryError on a 2 GiB VM (smoke attempt 2, 2026-11-20).
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data), read_across_frames=True) as r:
            return r.readall()
    if codec == "gzip":
        import gzip
        return gzip.decompress(data)
    return data


def list_prefix(s3, bucket: str, prefix: str) -> list[dict]:
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def head(s3, bucket: str, key: str) -> dict:
    h = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    etag = h.get("ETag", "").strip('"')
    return {
        "size": h["ContentLength"], "etag": etag, "multipart": "-" in etag,
        "checksum_type": h.get("ChecksumType"),
        "checksum_sha256": h.get("ChecksumSHA256"), "checksum_crc32": h.get("ChecksumCRC32"),
        "checksum_crc64nvme": h.get("ChecksumCRC64NVME"),
        "store_sha256_hex": b64_to_hex(h.get("ChecksumSHA256")),
        "meta_vtop_checksum": h.get("Metadata", {}).get("vtop-checksum"),
    }


def ledger_rows(db: str) -> list[dict]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    cols = {r[1] for r in con.execute("PRAGMA table_info(batches)")}
    want = ["batch_id", "state", "progress_start_json", "progress_end_json", "object_uri",
            "manifest_uri", "object_size_bytes", "verification_class"]
    rows = [dict(r) for r in con.execute(f"SELECT {', '.join(c for c in want if c in cols)} FROM batches")]
    con.close()
    return rows


def check(db: str, source: str, bucket: str, prefix: str, mode: str, s3=None) -> dict:
    if s3 is None:  # default credential chain: the instance role on EC2
        lab = os.environ.get("SMOKE_LAB_ENDPOINT")  # local MinIO dry run only
        s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=lab or None)
    exp = EXPECT[mode]
    listing = list_prefix(s3, bucket, prefix)
    keys = {o["Key"]: o["Size"] for o in listing}
    manifests = [k for k in keys if k.endswith(".manifest.json")]
    receipts = [k for k in keys if k.endswith(".receipt.json")]
    data_keys = [k for k in keys if k not in manifests and k not in receipts]

    with open(source, "rb") as fh:
        src = fh.read()
    rows = ledger_rows(db)
    per_batch, coverage, problems, warnings, uncommitted_notes = [], [], [], [], []
    referenced = set()
    for row in rows:
        b = {"batch_id": row["batch_id"], "state": row["state"],
             "ledger_class": row.get("verification_class")}
        problems_here = []
        try:
            s0 = int(json.loads(row["progress_start_json"])["start_byte"])
            e0 = int(json.loads(row["progress_end_json"])["end_byte"])
            b["range"] = [s0, e0]
            mbucket, mkey = split_uri(row["manifest_uri"])
            manifest = json.loads(s3.get_object(Bucket=mbucket, Key=mkey)["Body"].read())
            obj = manifest["object"]
            if obj["uri"] != row["object_uri"]:
                problems_here.append("manifest object uri differs from ledger")
            obucket, okey = split_uri(obj["uri"])
            referenced.add(okey)
            h = head(s3, obucket, okey)
            data = s3.get_object(Bucket=obucket, Key=okey)["Body"].read()
            sha, b3 = hashlib.sha256(data).hexdigest(), blake3.blake3(data).hexdigest()
            algo = obj.get("checksum_algorithm")
            b.update({"object_key": okey, "object_bytes": len(data), "algorithm": algo,
                      "compression": manifest["compression"], "head": h,
                      "manifest_head": head(s3, mbucket, mkey)})
            if algo != exp["algorithm"]:
                problems_here.append(f"manifest algorithm {algo}, expected {exp['algorithm']}")
            if algo == "sha256" and obj["checksum"] != sha:
                problems_here.append("object SHA-256 differs from manifest")
            if algo == "blake3" and obj["checksum"] != b3:
                problems_here.append("object BLAKE3 differs from manifest")
            if len(data) != obj["size_bytes"] or h["size"] != len(data):
                problems_here.append("object size differs from manifest or HEAD")
            # What S3 stored, checked against the bytes we downloaded (any mode).
            b["store_sha256_matches_bytes"] = (h["store_sha256_hex"] == sha) if h["store_sha256_hex"] else None
            if h["store_sha256_hex"] and h["store_sha256_hex"] != sha:
                problems_here.append("S3 ChecksumSHA256 differs from downloaded bytes")
            raw = decompress(data, manifest["compression"])
            if raw != src[s0:e0]:
                problems_here.append("decompressed bytes differ from source range")
            b["manifest_start_matches"] = int(manifest["source_progress"].get("start_byte", -1)) == s0
            b["end_minus_len_matches_start"] = (e0 - len(raw)) == s0
            # Path the engine must have taken, inferred from S3's own HEAD answer
            # (s3_native.rs:1389-1420): service SHA-256 present -> store_checksum.
            if algo == "none":
                inferred = "size_only"
            elif algo == "sha256" and h["store_sha256_hex"] == obj["checksum"]:
                inferred = "store_checksum"
            else:
                inferred = "read_back"
            b["inferred_method"] = inferred
            rkey = mkey[: -len(".manifest.json")] + ".receipt.json"
            if rkey in keys:
                receipt = json.loads(s3.get_object(Bucket=mbucket, Key=rkey)["Body"].read())
                b["receipt"] = receipt.get("verification")
                if b["receipt"].get("method") != inferred:
                    problems_here.append(f"receipt method {b['receipt'].get('method')} != inferred {inferred}")
            elif row["state"] == "source_committed":
                # A receipt is best effort and never part of the commit (docs/operate/
                # verification-receipts.md:74-92), so a missing one is a warning only.
                warnings.append(f"{row['batch_id']}: no receipt for committed batch")
            if exp.get("require_single_put") and h["multipart"]:
                problems_here.append("object is multipart in a mode that must stay single-PUT")
            if not h["multipart"] and inferred != exp["single_put"]:
                problems_here.append(f"single-PUT object verified by {inferred}, expected {exp['single_put']}")
            if row["state"] == "source_committed" and b["ledger_class"] not in (None, exp["class"]):
                problems_here.append(f"ledger class {b['ledger_class']}, expected {exp['class']}")
        except Exception as e:  # noqa: BLE001
            problems_here.append(f"check raised {type(e).__name__}: {e}")
        b["problems"] = problems_here
        b["valid"] = not problems_here
        per_batch.append(b)
        if row["state"] == "source_committed" and "range" in b:
            coverage.append((b["range"][0], b["range"][1], b["valid"]))
        # Rows that never committed (failed attempts, replays) are recorded but do not fail the
        # check: only a committed row that names a bad object is unsafe.
        sink = problems if row["state"] == "source_committed" else uncommitted_notes
        sink.extend(f"{row['batch_id']} ({row['state']}): {p}" for p in problems_here)

    position, dup_bytes = 0, 0
    for s0, e0, valid in sorted(coverage):
        if valid and s0 <= position:
            dup_bytes += max(0, position - s0)
            position = max(position, e0)
    committed = [b for b in per_batch if b["state"] == "source_committed"]
    multipart = [b for b in per_batch if b.get("head", {}).get("multipart")]
    if exp.get("require_multipart") and not multipart:
        problems.append("no multipart object: this mode must exercise the read-back fallback")
    result = {
        "mode": mode, "bucket": bucket, "prefix": prefix, "source_bytes": len(src),
        "ledger_rows": len(rows), "committed_rows": len(committed),
        "listed": {"data_objects": len(data_keys), "manifests": len(manifests), "receipts": len(receipts)},
        "distinct_object_keys_referenced": len(referenced),
        "unreferenced_data_objects": sorted(set(data_keys) - referenced),
        "multipart_objects": len(multipart),
        "bad_objects": sum(1 for b in committed if not b["valid"]),
        "unsafe_commits": sum(1 for b in committed if not b["valid"]),
        "covered_prefix_bytes": position, "duplicate_bytes": dup_bytes,
        "complete": position >= len(src),
        "inferred_methods": sorted({b.get("inferred_method") for b in per_batch} - {None}),
        "receipt_methods": sorted({(b.get("receipt") or {}).get("method") for b in per_batch} - {None}),
        "problems": problems, "warnings": warnings, "uncommitted_row_notes": uncommitted_notes, "batches": per_batch,
    }
    result["pass"] = bool(committed) and result["complete"] and not problems and result["unsafe_commits"] == 0
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--mode", required=True, choices=sorted(EXPECT))
    a = ap.parse_args()
    r = check(a.db, a.source, a.bucket, a.prefix, a.mode)
    json.dump(r, sys.stdout, indent=1, default=str)
    print()
    sys.exit(0 if r["pass"] else 1)


if __name__ == "__main__":
    main()
