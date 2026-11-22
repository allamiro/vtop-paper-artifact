"""Re-judge one E-RQ5-PERF throughput run offline from its downloaded S3 data objects.

Uses the same pieces as perf.py (TruthView over the lines file, HashIndex, decompress) and RQ4's locate()/judge()
as they are now in rq4-baselines/code/exp_baseline.py. Written for 106-tput-vector-r4 of session
20261122T041404Z-use1, whose on-VM verdict failed on a tie-break in locate() (see LESSONS.md).

  research/.venv/bin/python research/experiments/rq5-cloud/perf/rejudge.py \
      --run-dir research/experiments/rq5-cloud/perf/results/<session>-broker-<id> --run 106-tput-vector-r4 \
      --objects .tmp/perf-diag-106/106-tput-vector-r4 --lines-file <hdfs.log> --out <json>
"""
from __future__ import annotations

import argparse
import gzip
import json
import mmap
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import perf  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--objects", required=True, help="local copy of the run's data prefix")
    ap.add_argument("--lines-file", required=True, help="HDFS.log as the VM used it (unzip -p HDFS_v1.zip HDFS.log)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    run_dir = Path(a.run_dir)
    rec = next(json.loads(x) for x in open(run_dir / "results" / "runs.jsonl") if json.loads(x)["run"] == a.run)
    meta = json.load(gzip.open(run_dir / "results" / "objects" / f"{a.run}.json.gz"))
    order = [o["key"] for o in meta["data"]]            # the order the VM judged them in (first_seen)
    committed = {int(k): v for k, v in rec["committed_final"].items()}
    target = {int(k): v for k, v in rec["target"].items()}
    P = len(target)
    n = P * max(target.values())

    off = perf.line_offsets(a.lines_file, n)
    fh = open(a.lines_file, "rb")
    mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
    tv = perf.TruthView(mm, off, n, P)
    eb = perf.import_rq4()
    eb._INDEX[id(tv)] = perf.HashIndex(tv)

    intervals, per_obj, placements = [], [], []
    for key in order:
        raw = perf.decompress((Path(a.objects) / key).read_bytes())
        ivs, stt = eb.locate(raw, tv)
        intervals += ivs
        placements += stt.get("ambiguous_placements", [])
        per_obj.append({"key": key, **stt, "intact": all(iv[3] for iv in ivs)})
    intervals, moves = eb.reconcile(intervals, placements)
    j = eb.judge(intervals, tv, committed, target)
    ps = j["partitions"].values()
    o = {"unsafe_final": j["unsafe_partitions"], "complete": all(q["complete"] for q in ps),
         "missing_records": sum(max(0, q["target"] - q["intact_prefix"]) for q in ps),
         "lost_committed_records": sum(q["lost_committed_records"] for q in ps),
         "duplicate_objects": sum(q["duplicate_objects"] for q in ps),
         "duplicate_records": sum(q["duplicate_records"] or 0 for q in ps),
         "bad_intervals": j["bad_objects"], "intervals": j["objects"], "s3_data_objects": len(per_obj),
         "unmatched_lines": sum(q["unmatched_lines"] for q in per_obj),
         "ambiguous_lines": sum(q["ambiguous_lines"] for q in per_obj), "reconcile_moves": moves,
         "ambiguous_placements": placements, "partitions": j["partitions"]}
    res = {"run": a.run, "session_dir": run_dir.name, "locate_rule": "two-pass placement (exp_baseline.locate, 2026-11-22)",
           "objects_judged": len(order), "on_vm_oracle": {k: v for k, v in rec["oracle"].items() if k != "partitions"},
           "rejudged_oracle": o}
    Path(a.out).write_text(json.dumps(res, indent=1))
    print(json.dumps({k: v for k, v in o.items() if k not in ("partitions", "ambiguous_placements")}))


if __name__ == "__main__":
    main()
