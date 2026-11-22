"""Tests for locate()'s tie-break on lines whose content occurs at several (partition, offset) places.

  research/.venv/bin/python research/experiments/rq4-baselines/code/test_locate.py

Case 106 rebuilds the pattern of PERF run 20261122T041404Z-use1 106-tput-vector-r4 (one record identical at p1 and p2,
same offset; the sink interleaves partitions). Case greedy rebuilds the pattern of 013-kill-vector-r0 (an open run
consumed the other partition's copy first). The property test compares the old rule (next line only) with the
current one on random interleavings with many identical cross-partition records: a correct locator reports every
partition complete with no duplicates, since every record is written exactly once.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp_baseline as eb  # noqa: E402


def old_locate(blob, truth):
    """locate() as it was until 2026-11-22 (tie-break on the very next line only), for comparison."""
    idx = eb.content_index(truth)
    lines = blob.split(b"\n")
    if lines and lines[-1] == b"":
        lines = lines[:-1]
    runs, open_ = [], {}
    for i, x in enumerate(lines):
        hit = False
        for p, ri in open_.items():
            r = runs[ri]
            if r[2] < len(truth[p]) and truth[p][r[2]] == x:
                r[2] += 1
                hit = True
                break
        if hit:
            continue
        cands = idx.get(x)
        p, o = cands[0]
        if len(cands) > 1:
            nxt = lines[i + 1] if i + 1 < len(lines) else None
            for q, oo in cands:
                if nxt is not None and oo + 1 < len(truth[q]) and truth[q][oo + 1] == nxt:
                    p, o = q, oo
                    break
        runs.append([p, o, o + 1])
        open_[p] = len(runs) - 1
    return [(p, s, e, True) for p, s, e in runs]


def verdict(objects, truth, loc):
    ivs, pls = [], []
    for blob in objects:
        r = loc(blob, truth)
        if isinstance(r, tuple):          # current locate(): intervals, stats with ambiguous placements
            ivs += r[0]
            pls += r[1]["ambiguous_placements"]
        else:
            ivs += r
    if pls:
        ivs, _ = eb.reconcile(ivs, pls)
    target = {p: len(v) for p, v in truth.items()}
    j = eb.judge(ivs, truth, target, target)
    ps = j["partitions"].values()
    return all(q["complete"] for q in ps), sum(q["duplicate_objects"] for q in ps)


def obj(pairs, truth):
    return b"\n".join(truth[p][o] for p, o in pairs) + b"\n"


def case_106():
    X = b"same record"
    truth = {0: [b"a0", b"a1", X, b"a3", b"a4", b"a5", b"a6", b"a7"],
             1: [b"b0", b"b1", X, b"b3", b"b4", b"b5", b"b6", b"b7"]}
    o1 = obj([(0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (0, 3), (0, 4)], truth)        # p0 0..4, p1 0..1
    o2 = obj([(1, 2), (0, 5), (1, 3), (0, 6), (1, 4), (0, 7), (1, 5), (1, 6), (1, 7)], truth)  # p1's X, interleaved
    return [o1, o2], truth


def case_greedy():
    X = b"same record"
    truth = {0: [b"a0", b"a1", X, b"a3", b"a4"], 1: [b"b0", b"b1", X, b"b3", b"b4"]}
    # p0's run is open and expects X when p1's X arrives, so p1's copy extends p0; p0's own X comes later.
    o = obj([(0, 0), (0, 1), (1, 0), (1, 1), (1, 2), (1, 3), (0, 2), (0, 3), (1, 4), (0, 4)], truth)
    return [o], truth


REPEAT = 0.01


def random_case(rng):
    # Like PERF's topic: line i of an input file goes to partition i % P at offset i // P, and the file repeats
    # its previous line now and then (HDFS: 2,085 of the first 11.04 M lines, 0.019 %; here REPEAT, ~50x more).
    P, n = 4, 400
    file, k = [], 0
    for i in range(P * n):
        if file and rng.random() < REPEAT:
            file.append(file[-1])
        else:
            file.append(b"line-%d" % k)
            k += 1
    truth = {p: [file[i] for i in range(p, P * n, P)] for p in range(P)}
    # each partition's stream is split into chunks; chunks go to objects; inside an object the streams interleave
    pos = {p: 0 for p in range(P)}
    objects = []
    while any(pos[p] < n for p in range(P)):
        queues = []
        for p in range(P):
            k = min(n - pos[p], rng.randint(0, 60))
            queues.append([(p, o) for o in range(pos[p], pos[p] + k)])
            pos[p] += k
        seq = []
        while any(queues):
            q = rng.choice([q for q in queues if q])
            seq.append(q.pop(0))
        if seq:
            objects.append(obj(seq, truth))
    return objects, truth


def main():
    eb._INDEX.clear()
    fails = 0
    for name, build in (("106", case_106), ("greedy", case_greedy)):
        eb._INDEX.clear(); eb._DUPS.clear()
        objects, truth = build()
        new = verdict(objects, truth, eb.locate)
        old = verdict(objects, truth, old_locate)
        ok = new == (True, 0)
        fails += not ok
        print(f"case {name:7s} old={old} new={new} {'PASS' if ok else 'FAIL'}")
    rng = random.Random(20261122)
    N = 2000
    old_bad = new_bad = 0
    for _ in range(N):
        objects, truth = random_case(rng)
        old_bad += verdict(objects, truth, old_locate) != (True, 0)
        new_bad += verdict(objects, truth, eb.locate) != (True, 0)
        eb._INDEX.clear(); eb._DUPS.clear()
    print(f"random {N} cases: old rule wrong verdict in {old_bad}, current rule in {new_bad}")
    fails += new_bad > 0
    # the oracle must still see real faults: one line dropped (-> incomplete) or one line replayed (-> duplicate)
    missed_drop = missed_dup = 0
    for _ in range(N):
        objects, truth = random_case(rng)
        k = rng.randrange(len(objects))
        ls = objects[k].split(b"\n")[:-1]
        j = rng.randrange(len(ls))
        dropped = objects[:k] + [b"\n".join(ls[:j] + ls[j + 1:]) + b"\n"] + objects[k + 1:]
        eb._INDEX.clear(); eb._DUPS.clear()
        missed_drop += verdict(dropped, truth, eb.locate)[0]
        replay = objects + [ls[j] + b"\n"]
        eb._INDEX.clear(); eb._DUPS.clear()
        missed_dup += verdict(replay, truth, eb.locate) != (True, 1)
    print(f"faults {N}+{N}: dropped line judged complete {missed_drop}, replayed line not one duplicate {missed_dup}")
    fails += missed_drop + missed_dup > 0
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
