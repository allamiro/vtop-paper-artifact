#!/usr/bin/env python3
"""S3 fault proxy for E-RQ4-B: an object store that acknowledges a write and then loses or corrupts it.

The proxy sits between a client (VTOP's native S3 backend, or the Confluent S3 Sink) and MinIO.
It forwards every request unchanged, including the Host header, so SigV4 signatures still verify
at MinIO. It counts *data-object* writes that MinIO acknowledged (a PUT without ``partNumber`` /
``uploadId``, or a CompleteMultipartUpload); manifests (``*.manifest.json``) are never faulted.
On the N-th such write it alters the stored object directly in MinIO, before it relays MinIO's
200 to the client:

* ``corrupt``: flip one byte in the middle of the stored object (same size);
* ``lost``:    delete the stored object.

So the client sees a normal acknowledgement for data the store no longer holds intact: the
"acknowledged but corrupted" and "acknowledged but lost" storage faults. Every fault is appended
to ``--log`` as JSON.

Run: s3_fault_proxy.py --listen 9100 --upstream 127.0.0.1:9000 --mode corrupt --nth 5 --log f.jsonl
"""
from __future__ import annotations

import argparse
import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

import boto3
from botocore.config import Config

HOP = {"connection", "keep-alive", "proxy-connection", "te", "trailers", "upgrade"}


class State:
    def __init__(self, a):
        self.mode, self.nth, self.log = a.mode, a.nth, a.log
        self.count, self.lock = 0, threading.Lock()
        self.s3 = boto3.client("s3", endpoint_url=f"http://{a.upstream}", region_name="us-east-1",
                               aws_access_key_id=a.access_key, aws_secret_access_key=a.secret_key,
                               config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))

    def record(self, ev: dict):
        with open(self.log, "a") as fh:
            fh.write(json.dumps(ev) + "\n")

    def maybe_fault(self, bucket: str, key: str):
        with self.lock:
            self.count += 1
            n = self.count
        ev = {"t": round(time.time(), 3), "n": n, "bucket": bucket, "key": key}
        if self.mode == "none" or n != self.nth:
            return
        if self.mode == "lost":
            self.s3.delete_object(Bucket=bucket, Key=key)
            ev.update(fault="lost")
        elif self.mode == "corrupt":
            body = bytearray(self.s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            i = len(body) // 2
            body[i] ^= 0x01
            self.s3.put_object(Bucket=bucket, Key=key, Body=bytes(body))
            ev.update(fault="corrupt", offset=i, size=len(body))
        self.record(ev)


def is_data_write(method: str, path: str, query: dict, status: int, body: bytes) -> tuple[str, str] | None:
    if status != 200 or b"<Error>" in body[:4096]:
        return None
    parts = unquote(path).lstrip("/").split("/", 1)
    if len(parts) != 2 or not parts[1]:
        return None
    bucket, key = parts
    if key.endswith(".manifest.json"):
        return None
    if method == "PUT" and "partNumber" not in query and "uploadId" not in query \
            and not any(k in query for k in ("tagging", "acl", "retention", "legal-hold")):
        return bucket, key
    if method == "POST" and "uploadId" in query:
        return bucket, key
    return None


def make_handler(state: State, upstream: str):
    host, port = upstream.split(":")

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # quiet
            pass

        def _body(self) -> bytes:
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                out = bytearray()
                while True:
                    size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                    if size == 0:
                        while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                            pass
                        return bytes(out)
                    out += self.rfile.read(size)
                    self.rfile.readline()
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def _forward(self):
            body = self._body()
            conn = http.client.HTTPConnection(host, int(port), timeout=600)
            conn.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for k, v in self.headers.items():
                if k.lower() in HOP or k.lower() == "transfer-encoding":
                    continue
                if k.lower() == "content-length":
                    continue
                conn.putheader(k, v)
            conn.putheader("Content-Length", str(len(body)))
            conn.endheaders()
            if body:
                conn.send(body)
            resp = conn.getresponse()
            data = resp.read()
            u = urlsplit(self.path)
            q = parse_qs(u.query, keep_blank_values=True)
            hit = is_data_write(self.command, u.path, q, resp.status, data)
            if hit:
                try:
                    state.maybe_fault(*hit)
                except Exception as e:  # a fault that could not be applied is logged, never hidden
                    state.record({"t": round(time.time(), 3), "error": repr(e), "bucket": hit[0], "key": hit[1]})
            self.send_response_only(resp.status, resp.reason)
            for k, v in resp.getheaders():
                if k.lower() in HOP or k.lower() == "transfer-encoding":
                    continue
                if k.lower() == "content-length" and self.command != "HEAD":
                    continue  # re-sent below from the bytes actually relayed
                self.send_header(k, v)
            if self.command != "HEAD":
                self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)
            conn.close()

        do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _forward

    return H


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=9100)
    ap.add_argument("--upstream", default="127.0.0.1:9000")
    ap.add_argument("--mode", choices=["none", "corrupt", "lost"], default="none")
    ap.add_argument("--nth", type=int, default=0)
    ap.add_argument("--log", required=True)
    ap.add_argument("--access-key", default="rq4lab")
    ap.add_argument("--secret-key", default="rq4lab-secret")
    a = ap.parse_args()
    srv = ThreadingHTTPServer(("0.0.0.0", a.listen), make_handler(State(a), a.upstream))
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
