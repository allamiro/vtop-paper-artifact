#!/usr/bin/env python3
"""E-RQ2-R V-norecheck: causal ablation of restart-time evidence revalidation.

v0.7.0: a separate build whose storage_still_verified returns true without reading the manifest or object
(norecheck.patch). v0.10.x: the tag built with norecheck-v0.10.patch (build_norecheck.sh <tag>), whose ablation is
active only with VTOP_ABLATION_NORECHECK set, which this script sets. The trial procedure and independent oracle are
identical to exp_tamper.py. The build is an experiment-only counterfactual.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp_tamper  # noqa: E402
import vtoplab  # noqa: E402
from vtoplab import Variant, host_info  # noqa: E402

REPO = Path(__file__).resolve().parents[4]
if vtoplab.VTOP_BUILD == "v0.7.0":
    ABLATION = REPO / ".tmp/vtop-engine-v070-norecheck"
    PATCH = Path(__file__).resolve().parent / "norecheck.patch"
else:
    ABLATION = REPO / f".tmp/vtop-engine-norecheck-{vtoplab.VTOP_BUILD}"
    PATCH = Path(__file__).resolve().parents[1] / "norecheck-v0.10.patch"
    os.environ["VTOP_ABLATION_NORECHECK"] = "1"  # Run.env() copies os.environ into the engine's environment
vtoplab.VTOPCTL = ABLATION / "target/release/vtopctl"
if not vtoplab.VTOPCTL.exists():
    raise SystemExit(f"{vtoplab.VTOPCTL} missing: build it with build_norecheck.sh {vtoplab.VTOP_BUILD}")
exp_tamper.VARIANTS = {"norecheck": Variant("norecheck")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--datasets", default="ssh,linux")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--seed", type=int, default=20261111)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    engine = ABLATION / "crates/vtop-cli/src/engine.rs"
    meta = {
        "experiment": "E-RQ2-R V-norecheck (restart revalidation disabled)",
        "vtop_commit": exp_tamper.VTOP_COMMIT,
        "vtop_build": vtoplab.VTOP_BUILD,
        "ablation": "storage_still_verified returns a passing re-check without reading evidence",
        "ablation_engine_sha256": hashlib.sha256(engine.read_bytes()).hexdigest(),
        "ablation_patch_sha256": hashlib.sha256(PATCH.read_bytes()).hexdigest(),
        "ablation_vtopctl_sha256": hashlib.sha256(vtoplab.VTOPCTL.read_bytes()).hexdigest(),
        "backend": "localfs",
        "host": host_info(),
        "seed": a.seed,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    (out / "run.json").write_text(json.dumps(meta, indent=1))
    cells = [(d, "norecheck", k, r) for d in a.datasets.split(",") for k in exp_tamper.MUTATIONS for r in range(a.reps)]
    random.Random(a.seed).shuffle(cells)
    with open(out / "trials.jsonl", "a") as fh:
        for i, (d, v, k, r) in enumerate(cells, 1):
            rec = exp_tamper.trial(out, d, v, k, r)
            rec["ablation"] = meta["ablation"]
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            print(f"[{i}/{len(cells)}] {d:5s} {k} r{r}: {rec['outcome']:9s} unsafe={rec['unsafe_commit']} "
                  f"complete={rec['archive_complete']} bad={rec['bad_object_problems']}", flush=True)
    meta["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (out / "run.json").write_text(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
