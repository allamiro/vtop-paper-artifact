#!/usr/bin/env python3
"""Request-counting S3 proxy for the LOCAL dry run only (never used on EC2).

Wraps RQ4's s3_fault_proxy.py (imported unchanged, mode "none": it forwards every request unchanged, Host header
included, so SigV4 still verifies at MinIO) and appends one JSON line per request: time, method, S3 operation
class, bucket, key, status, request and response body bytes. This gives a measured per-object request profile
of each system's S3 client against MinIO, which E-RQ5-PERF cannot measure on Amazon S3 without new permissions
(README "S3 requests").

  reqcount_proxy.py --listen 19401 --upstream 127.0.0.1:19400 --log requests.jsonl --access-key K --secret-key S
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "rq4-baselines" / "code"))
import s3_fault_proxy  # noqa: E402  (unchanged)

LOCK = threading.Lock()


def op_class(method: str, path: str, q: dict) -> str:
    parts = unquote(path).lstrip("/").split("/", 1)
    has_key = len(parts) == 2 and parts[1] != ""
    if method == "PUT":
        if "partNumber" in q:
            return "UploadPart"
        return "PutObject" if has_key else "PutBucket"
    if method == "POST":
        if "uploads" in q:
            return "CreateMultipartUpload"
        if "uploadId" in q:
            return "CompleteMultipartUpload"
        if "delete" in q:
            return "DeleteObjects"
        return "POST"
    if method == "GET":
        if not has_key:
            return "ListObjects" if ("list-type" in q or "prefix" in q or not q) else "GetBucket?" + ",".join(q)
        if "uploadId" in q:
            return "ListParts"
        return "GetObject"
    if method == "HEAD":
        return "HeadObject" if has_key else "HeadBucket"
    if method == "DELETE":
        return "AbortMultipartUpload" if "uploadId" in q else "DeleteObject"
    return method


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    a = ap.parse_args()
    a.mode, a.nth = "none", 0
    state = s3_fault_proxy.State(argparse.Namespace(mode="none", nth=0, log=a.log + ".faults", upstream=a.upstream,
                                                    access_key=a.access_key, secret_key=a.secret_key))
    Base = s3_fault_proxy.make_handler(state, a.upstream)

    class H(Base):
        def _forward(self):
            t0 = time.time()
            n_in = int(self.headers.get("Content-Length") or 0)
            u = urlsplit(self.path)
            q = parse_qs(u.query, keep_blank_values=True)
            status = {"code": None}
            orig = self.send_response_only

            def spy(code, msg=None):
                status["code"] = code
                return orig(code, msg)

            self.send_response_only = spy
            try:
                super()._forward()
            finally:
                parts = unquote(u.path).lstrip("/").split("/", 1)
                ev = {"t": round(t0, 4), "dt": round(time.time() - t0, 4), "method": self.command,
                      "op": op_class(self.command, u.path, q), "bucket": parts[0], "key": parts[1] if len(parts) > 1 else "",
                      "status": status["code"], "req_bytes": n_in,
                      "aws_chunked": "aws-chunked" in (self.headers.get("Content-Encoding") or "")
                      or self.headers.get("x-amz-decoded-content-length") is not None,
                      "ua": (self.headers.get("User-Agent") or "")[:80]}
                with LOCK, open(a.log, "a") as fh:
                    fh.write(json.dumps(ev) + "\n")

        do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _forward

    srv = ThreadingHTTPServer(("0.0.0.0", a.listen), H)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
