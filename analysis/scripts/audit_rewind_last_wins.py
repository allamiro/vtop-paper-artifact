#!/usr/bin/env python3
"""Audit: did the v0.10.2 "rewind last wins" defect fire in any v0.10.2 run?

Defect (vtop-engine v0.10.2, 0abf116, crates/vtop-cli/src/engine.rs run_cycle l.3976-4128): every window that
did not reach VERIFIED in a cycle pushes its start marker onto `rewinds`, in read order; the adapter then applies
`replay_from_markers(&rewinds)`, which for file/spool assigns the read head per marker and for Kafka assigns the
cursor per marker (kafka_source.rs l.3560-3640), so with >=2 failed windows of ONE unit in ONE cycle the highest
start wins and the lower failed range is never re-read.

Read-only on every result directory: each ledger (with its -wal/-shm) is copied to a temporary directory before
it is opened; logs are read as files. Output: JSON on stdout (or --out FILE).

Three independent checks per run:
  L  (logs, exact cycle attribution). The engine logs one WARN "batch did not reach VERIFIED; its range was handed
     back ..." (or ERROR "could not hand a failed range back ...") per entry of `rewinds`, all in one loop at the
     end of run_cycle, after every window of that cycle was sealed. Every window is logged "batch_started" when it
     is sealed (engine.rs flush loop), so two hand-backs belong to the same cycle iff no "batch_started" or
     "read_cycle_profile" line lies between them in the same log file. Groups of >=2 hand-backs of the same unit
     in the same cycle are the defect trigger.
  D  (ledger, loss check, threshold-free). Per unit, the union of SOURCE_COMMITTED ranges must be contiguous from
     the unit's first start; every non-committed row's range (REPLAY_REQUIRED, SUPERSEDED, FAILED, in-flight) must
     lie inside that union. A lower failed range skipped by the defect would show as a gap / an uncovered row.
  G  (ledger, candidate groups, superset). The ledger has no cycle id, and created_at is written inside the
     concurrent per-window pipeline (engine.rs l.678-740), not at a cycle boundary, so no time threshold separates
     cycles exactly. G therefore takes EVERY pair of non-committed rows of the same unit with different,
     non-overlapping ranges (a retry of one window in a later cycle repeats its range), whatever their created_at
     distance - a superset of every same-cycle group - and checks the lower range of each pair against the committed
     union. The created_at distance is reported per pair, with the count under 1 s, for information only.
Plus: the run's own oracle verdict, read from the result files.
"""
from __future__ import annotations

import argparse
import collections
import gzip
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

EXP = Path(__file__).resolve().parents[2] / "experiments"
HANDBACK = "batch did not reach VERIFIED; its range was handed back"
REFUSED = "could not hand a failed range back"
BOUNDARY = ("batch_started", "read_cycle_profile")


# ------------------------------------------------------------------ run inventory

def _jsonl(p: Path):
    with open(p) as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _gz_json(p: Path):
    with gzip.open(p, "rt") as fh:
        return json.load(fh)


def inventory():
    """[(experiment, run_id, ledger, [logs], oracle: dict|None)] for every v0.10.2 run with a ledger."""
    runs = []
    # E-RQ1 crash (file, spool), local
    d = EXP / "rq1-source-safety/results/20261121-v0102-crash"
    tr = {f"{t['scenario']}-t{t['trial']:02d}": t for t in _jsonl(d / "trials.jsonl")}
    for rd in sorted((d / "raw").iterdir()):
        t = tr.get(rd.name)
        runs.append(("E-RQ1 crash (local)", str(rd.relative_to(EXP)), rd / "raw/state/vtop.db",
                     sorted((rd / "raw/logs").glob("engine-*.jsonl")),
                     t and {"complete": t["complete"], "lost": t["lost_bytes"], "unsafe_final": t["unsafe_commits_final"],
                            "oracle": "vtoplab.oracle (prefix from byte 0 vs source size)"}))
    # E-RQ1 Kafka, local
    d = EXP / "rq1-source-safety/results/20261121-v0102-kafka"
    tr = {f"{t['scenario'][len('kafka-'):]}-t{t['trial']:02d}": t for t in _jsonl(d / "trials.jsonl")}
    for rd in sorted(p for p in (d / "raw").iterdir() if p.is_dir()):
        t = tr.get(rd.name)
        runs.append(("E-RQ1 Kafka (local)", str(rd.relative_to(EXP)), rd / "raw/state/vtop.db",
                     sorted((rd / "raw/logs").glob("engine-*.jsonl")),
                     t and {"complete": t["complete"], "lost": t["lost_records"], "unsafe_final": t["unsafe_commits_final"],
                            "oracle": "exp_kafka.oracle_kafka (prefix vs topic records)"}))
    # E-RQ4 VTOP side, local
    d = EXP / "rq4-baselines/results/20261121-v0102-rq4"
    tr = {f"vtop-{t['topic']}-{t['fault']}-t{t['trial']:02d}": t for t in _jsonl(d / "trials.jsonl") if t["system"] == "vtop"}
    for rd in sorted(p for p in (d / "raw").iterdir() if p.is_dir()):
        t = tr.get(rd.name.rsplit("-", 1)[0])
        runs.append(("E-RQ4 VTOP (local)", str(rd.relative_to(EXP)), rd / "raw/state/vtop.db",
                     sorted((rd / "raw/logs").glob("engine-*.jsonl")),
                     t and {"complete": t["complete"], "lost": t["missing_records"], "unsafe_final": t["unsafe_final"],
                            "lost_committed": t["lost_committed_records"],
                            "oracle": "exp_baseline.judge (intact prefix vs topic records)"}))
    # E-RQ5 cost / WAN / scale (AWS): oracle.py per run
    for exp, sub in (("E-RQ5 cost (AWS)", "cost"), ("E-RQ5 WAN (AWS)", "wan"), ("E-RQ5 scale (AWS)", "scale")):
        for db in sorted((EXP / f"rq5-cloud/{sub}/results").rglob("ledger/*.db")):
            host = db.parent.parent
            orc = host / "oracle" / (db.stem + ".json.gz")
            o = _gz_json(orc) if orc.exists() else None
            runs.append((exp, str(db.relative_to(EXP)), db, [host / "logs" / (db.stem + ".jsonl.gz")],
                         o and {"complete": o["complete"], "pass": o["pass"], "unsafe_final": o["unsafe_commits"],
                                "lost": o["source_bytes"] - min(o["covered_prefix_bytes"], o["source_bytes"]),
                                "oracle": "rq5-cloud oracle.py (committed coverage from byte 0 vs source size)"}))
    # E-RQ5 tamper (AWS + the three local repetitions)
    for host in sorted((EXP / "rq5-cloud/tamper/results").iterdir()):
        cases = {c["id"]: c for c in _jsonl(host / "cases.jsonl")} if (host / "cases.jsonl").exists() else {}
        for db in sorted(host.glob("logs/*/ledger.db")):
            c = cases.get(db.parent.name)
            orcs = {k: v["oracle"] for k, v in (c or {}).get("verdicts", {}).items()
                    if isinstance(v, dict) and isinstance(v.get("oracle"), dict)}
            o = None
            if orcs:
                o = {"complete": all(v["complete_and_exact"] for v in orcs.values()),
                     "per_verifier": {k: v["complete_and_exact"] for k, v in orcs.items()},
                     "oracle": "tamper.py Case.oracle (committed objects vs source, prefix from byte 0)"}
            exp = "E-RQ5 tamper (AWS)" if "Z-i-" in host.name else "E-RQ5 tamper (local MinIO/AL2023 reps)"
            runs.append((exp, str(db.relative_to(EXP)), db, sorted(db.parent.glob("engine*.jsonl.gz")), o))
    # E-RQ5 perf (AWS): ledger on the sink host, oracle on the broker host
    brokers = {}
    for b in (EXP / "rq5-cloud/perf/results").glob("*-broker-*/results/runs.jsonl"):
        leg = b.parts[-3].split("-")[1]
        for r in _jsonl(b):
            brokers[(leg, r["run"])] = r
    for db in sorted((EXP / "rq5-cloud/perf/results").glob("*-sink-*/agent/runs/*/ledger.db")):
        leg = db.parts[-5].split("-")[1]
        r = brokers.get((leg, db.parent.name))
        o = r and r.get("oracle")
        runs.append(("E-RQ5 perf (AWS)", str(db.relative_to(EXP)), db, [db.parent / "engine.jsonl.gz"],
                     o and {"complete": o["complete"], "lost": o["missing_records"], "unsafe_final": o["unsafe_final"],
                            "lost_committed": o["lost_committed_records"],
                            "oracle": "perf.py (RQ4 judge: intact prefix vs topic records)"}))
    return runs


def ledgerless():
    """v0.10.2 local sets whose ledgers and logs were not kept: only what their result records say."""
    out = {}
    for key, rel in (("E-RQ3 cost (local)", "rq3-cost-ablation/results/20261121-v0102-cost/runs.jsonl"),
                     ("E-RQ3 cost-run (local)", "rq3-cost-ablation/results/20261121-v0102-cost-run/runs.jsonl")):
        recs = list(_jsonl(EXP / rel))
        states = collections.Counter()
        for r in recs:
            states.update(r.get("states") or {})
        orc = [r["oracle"] for r in recs if r.get("oracle")]
        out[key] = {"runs": len(recs), "final_ledger_states_summed": dict(states),
                    "oracle_recorded": len(orc), "oracle_complete": sum(bool(o.get("complete")) for o in orc)}
    for key, rel in (("E-RQ2 tamper (local)", "rq2-corruption-recovery/results/20261121-v0102-tamper/trials.jsonl"),
                     ("E-RQ2 norecheck (local)", "rq2-corruption-recovery/results/20261121-v0102-norecheck/trials.jsonl")):
        recs = list(_jsonl(EXP / rel))
        out[key] = {"runs": len(recs), "archive_complete": sum(bool(r["archive_complete"]) for r in recs),
                    "incomplete_cells": dict(collections.Counter(
                        f"{r['dataset']}/{r['variant']}/{r['mutation']}/{r['outcome']}" for r in recs
                        if not r["archive_complete"]))}
    return out


# ------------------------------------------------------------------ ledger

def _ns(ts: str) -> int:
    """RFC 3339 with up to 9 fractional digits -> integer ns since the epoch."""
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$", ts)
    base = datetime.fromisoformat(m.group(1) + ("+00:00" if m.group(3) == "Z" else m.group(3)))
    frac = (m.group(2) or "0").ljust(9, "0")[:9]
    return int(base.astimezone(timezone.utc).timestamp()) * 10**9 + int(frac)


def unit_and_range(ps: dict, pe: dict):
    st = ps.get("source_type")
    if st == "kafka":
        return (("kafka", ps["topic"], int(ps["partition"]), ps.get("consumer_group")),
                int(ps["start_offset"]), int(pe["end_offset"]) + 1)        # end_offset is inclusive
    path = ps.get("path") or ps.get("spool_path") or ps.get("spool_id")
    return ((st, path, ps.get("inode")), int(ps["start_byte"]), int(pe["end_byte"]))


def read_ledger(db: Path, tmp: Path):
    d = tmp / "l"
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir()
    for suf in ("", "-wal", "-shm"):
        q = Path(str(db) + suf)
        if q.exists():
            shutil.copy2(q, d / ("l.db" + suf))
    con = sqlite3.connect(d / "l.db")
    try:
        rows = con.execute("SELECT batch_id, state, progress_start_json, progress_end_json, created_at, updated_at, "
                           "error_message FROM batches").fetchall()
    finally:
        con.close()
    out = []
    for bid, state, psj, pej, ca, ua, err in rows:
        unit, s, e = unit_and_range(json.loads(psj), json.loads(pej))
        out.append({"batch_id": bid, "state": state, "unit": unit, "start": s, "end": e,
                    "created_ns": _ns(ca), "created_at": ca, "error": (err or "")[:120]})
    return out


def union(iv):
    out = []
    for s, e in sorted(iv):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


def uncovered(s, e, u):
    """Length of [s, e) not covered by the sorted disjoint union u."""
    left, cur = 0, s
    for a, b in u:
        if b <= cur:
            continue
        if a >= e:
            break
        if a > cur:
            left += a - cur
        cur = max(cur, b)
        if cur >= e:
            break
    return left + max(0, e - cur)


def ledger_checks(rows):
    units = collections.defaultdict(list)
    for r in rows:
        units[r["unit"]].append(r)
    gaps, uncov, pairs, same_unit_gaps = [], [], [], []
    for unit, rs in units.items():
        com = union([(r["start"], r["end"]) for r in rs if r["state"] == "source_committed"])
        first = min(r["start"] for r in rs)
        if com:
            if com[0][0] > first:
                gaps.append({"unit": list(unit), "gap": [first, com[0][0]]})
            for (a0, a1), (b0, b1) in zip(com, com[1:]):
                gaps.append({"unit": list(unit), "gap": [a1, b0]})
        bad = [r for r in rs if r["state"] != "source_committed"]
        for r in bad:
            left = uncovered(r["start"], r["end"], com)
            if left and r["end"] > r["start"]:
                uncov.append({"unit": list(unit), "batch_id": r["batch_id"], "state": r["state"],
                              "range": [r["start"], r["end"]], "uncovered": left, "error": r["error"]})
        rs_sorted = sorted(rs, key=lambda r: r["created_ns"])
        same_unit_gaps += [b["created_ns"] - a["created_ns"] for a, b in zip(rs_sorted, rs_sorted[1:])]
        bad.sort(key=lambda r: r["created_ns"])
        for i, a in enumerate(bad):
            for b in bad[i + 1:]:
                dt = b["created_ns"] - a["created_ns"]
                distinct = a["end"] <= b["start"] or b["end"] <= a["start"]
                if distinct:
                    lo = a if a["start"] < b["start"] else b
                    pairs.append({"unit": list(unit), "dt_ms": dt / 1e6, "within_1s": dt < 10**9,
                                  "lower_range_uncovered": uncovered(lo["start"], lo["end"], com),
                                  "a": {"batch_id": a["batch_id"], "state": a["state"], "range": [a["start"], a["end"]],
                                        "created_at": a["created_at"], "error": a["error"]},
                                  "b": {"batch_id": b["batch_id"], "state": b["state"], "range": [b["start"], b["end"]],
                                        "created_at": b["created_at"], "error": b["error"]}})
    return {"units": len(units), "rows": len(rows),
            "noncommitted": sum(r["state"] != "source_committed" for r in rows),
            "states": dict(collections.Counter(r["state"] for r in rows)),
            "committed_gaps": gaps, "uncovered_noncommitted": uncov, "distinct_pairs": pairs,
            "same_unit_created_gaps_ns": same_unit_gaps}


# ------------------------------------------------------------------ logs

_FILE = re.compile(r'path: "([^"]*)", inode: (Some\((\d+)\)|None), start_byte: (\d+)')
_SPOOL = re.compile(r'spool_id: "([^"]*)".*?path: "([^"]*)", inode: (Some\((\d+)\)|None), start_byte: (\d+)')
_KAFKA = re.compile(r'topic: "([^"]*)", partition: (\d+), start_offset: (-?\d+)')
_GROUP = re.compile(r'consumer_group: (?:Some\()?"([^"]*)"')


def parse_marker(s: str):
    if s.startswith("Kafka"):
        m = _KAFKA.search(s)
        g = _GROUP.search(s)
        return ("kafka", m.group(1), int(m.group(2)), g and g.group(1)), int(m.group(3))
    if s.startswith("SyslogSpool"):
        m = _SPOOL.search(s)
        if m:
            return ("syslog_spool", m.group(2), m.group(4) and int(m.group(4))), int(m.group(5))
    m = _FILE.search(s)
    if m:
        return ("file", m.group(1), m.group(3) and int(m.group(3))), int(m.group(4))
    return ("unparsed", s[:80], None), None


def _open(p: Path):
    return gzip.open(p, "rt", errors="replace") if p.suffix == ".gz" else open(p, errors="replace")


def log_checks(logs, rows):
    """Hand-backs grouped by (log file, cycle, unit)."""
    found, handbacks, refused, lines, missing = [], 0, 0, 0, []
    boundaries, multi_any_unit, same_unit_dt_ms, unmatched = 0, 0, [], []
    starts = {(r["unit"], r["start"]) for r in rows}
    for lp in logs:
        if not lp.exists():
            missing.append(str(lp))
            continue
        cycle = 0
        groups = collections.defaultdict(list)
        with _open(lp) as fh:
            for i, line in enumerate(fh):
                lines += 1
                if '"message"' not in line:
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                f = ev.get("fields", {})
                msg = f.get("message", "")
                if msg in BOUNDARY:
                    cycle += 1
                    boundaries += 1
                    continue
                if msg.startswith(HANDBACK) or msg.startswith(REFUSED):
                    kind = "handed_back" if msg.startswith(HANDBACK) else "refused"
                    handbacks += kind == "handed_back"
                    refused += kind == "refused"
                    unit, start = parse_marker(str(f.get("start", "")))
                    groups[(cycle, unit)].append({"line": i + 1, "ts": ev.get("timestamp"), "start": start, "kind": kind})
        # Positive control: cycles whose rewind loop handed back >=2 ranges of ANY units (shows the grouping does
        # put same-cycle hand-backs together when they exist).
        per_cycle = collections.Counter()
        for (cyc, _unit), hb in groups.items():
            per_cycle[cyc] += len(hb)
        multi_any_unit += sum(1 for n in per_cycle.values() if n >= 2)
        by_unit = collections.defaultdict(list)
        for (cyc, unit), hb in groups.items():
            for h in hb:
                by_unit[unit].append(h["ts"])
                if (unit, h["start"]) not in starts:
                    unmatched.append({"log": lp.name, "unit": list(unit), "start": h["start"]})
        for ts in by_unit.values():
            ns = sorted(_ns(t) for t in ts if t)
            same_unit_dt_ms += [(b - c) / 1e6 for c, b in zip(ns, ns[1:])]
        for (cyc, unit), hb in groups.items():
            if len(hb) >= 2:
                found.append({"log": str(lp.relative_to(EXP)), "unit": list(unit), "handbacks": hb,
                              "starts": sorted(h["start"] for h in hb)})
    return {"handbacks": handbacks, "refused": refused, "log_lines": lines, "missing_logs": missing,
            "boundary_events": boundaries, "cycles_with_ge2_handbacks_any_unit": multi_any_unit,
            "same_unit_handback_dt_ms": same_unit_dt_ms, "handbacks_without_ledger_row": unmatched,
            "multi_failed_same_cycle": found}


# ------------------------------------------------------------------ main

def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1)))] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    a = ap.parse_args()
    runs = inventory()
    tmp = Path(tempfile.mkdtemp(prefix="audit-rewind-"))
    per_run, all_gaps, hb_dt = [], collections.defaultdict(list), collections.defaultdict(list)
    try:
        for exp, rid, db, logs, orc in runs:
            rows = read_ledger(db, tmp)
            lc = ledger_checks(rows)
            lg = log_checks(logs, rows)
            # created_at distance between consecutive rows of one unit (any state): exposure information only
            all_gaps[exp] += lc.pop("same_unit_created_gaps_ns")
            hb_dt[exp] += lg.pop("same_unit_handback_dt_ms")
            per_run.append({"experiment": exp, "run": rid, "ledger": lc, "logs": lg, "oracle": orc})
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    summary = collections.OrderedDict()
    for r in per_run:
        s = summary.setdefault(r["experiment"], collections.Counter())
        s["runs"] += 1
        s["ledger_rows"] += r["ledger"]["rows"]
        s["noncommitted_rows"] += r["ledger"]["noncommitted"]
        s["handback_events"] += r["logs"]["handbacks"]
        s["refused_rewinds"] += r["logs"]["refused"]
        s["runs_missing_logs"] += bool(r["logs"]["missing_logs"])
        s["L_multi_failed_same_cycle_groups"] += len(r["logs"]["multi_failed_same_cycle"])
        s["G_distinct_range_pairs"] += len(r["ledger"]["distinct_pairs"])
        s["G_distinct_range_pairs_within_1s"] += sum(p["within_1s"] for p in r["ledger"]["distinct_pairs"])
        s["G_pairs_lower_range_uncovered"] += sum(p["lower_range_uncovered"] > 0 for p in r["ledger"]["distinct_pairs"])
        s["D_committed_gaps"] += len(r["ledger"]["committed_gaps"])
        s["D_uncovered_noncommitted_rows"] += len(r["ledger"]["uncovered_noncommitted"])
        s["L_boundary_events"] += r["logs"]["boundary_events"]
        s["L_control_cycles_with_ge2_handbacks_any_unit"] += r["logs"]["cycles_with_ge2_handbacks_any_unit"]
        s["L_handbacks_without_ledger_row"] += len(r["logs"]["handbacks_without_ledger_row"])
        o = r["oracle"]
        s["oracle_recorded"] += o is not None
        s["oracle_complete"] += bool(o and o.get("complete"))
    gap_stats = {exp: {"n": len(g), "min_ms": pct(g, 0) / 1e6 if g else None, "p01_ms": pct(g, 0.01) / 1e6 if g else None,
                       "p50_ms": pct(g, 0.5) / 1e6 if g else None, "n_below_1ms": sum(x < 1e6 for x in g)}
                 for exp, g in all_gaps.items()}
    hb_stats = {exp: {"n": len(g), "min_ms": pct(g, 0), "p50_ms": pct(g, 0.5)} for exp, g in hb_dt.items() if g}
    out = {"summary": {k: dict(v) for k, v in summary.items()},
           "same_unit_created_gap_stats": gap_stats, "same_unit_consecutive_handback_dt": hb_stats,
           "ledgerless_sets": ledgerless(), "runs": per_run}
    text = json.dumps(out, indent=1, default=str)
    if a.out:
        Path(a.out).write_text(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
