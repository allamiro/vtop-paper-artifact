#!/usr/bin/env python3
"""Explicit-state model checker for VTOP's commit and recovery logic (E-RQ1-M).

Models one source unit split into N consecutive ranges (one range = one
batch's worth of records) and explores every interleaving of:

  * reading, sealing, uploading, manifest upload, verification;
  * concurrent verification of up to B batches of the unit per cycle;
  * verification/upload failures and adapter-commit failures (bounded);
  * engine crashes at any step (bounded), optionally with loss of the
    newest ledger write (SQLite WAL + synchronous=NORMAL on power loss);
  * corruption of a stored object while the engine is down (bounded);
  * restart recovery.

Two variants of the engine logic are encoded from source:

  v070  VTOP v0.7.0 (tag v0.7.0 = 6c8f386; logic unchanged since a8bce8e): read-order commits, failed ranges
        handed back to the source after the commit loop, recovery that
        collects rewinds (lowest start wins), re-checks VERIFIED rows
        against storage, and holds back a VERIFIED row while a lower range
        of the unit is re-read.
  v060  VTOP v0.6.0 (commit b1469be): completion-order commits, no rewind
        of failed ranges in the normal path, recovery that re-checks
        VERIFIED rows but applies rewinds row by row (last row visited
        wins) and commits without holdback.

Checked properties (research/state-machine/README.md §7):

  I1  commit rule: the adapter commit for a batch happens only while its
      ledger row is VERIFIED.
  I2  source-progress safety: whenever committed progress moves from C to
      c > C, every newly covered range C <= r < c has a stored object that
      passed verification. A violation means the engine committed past data
      it never archived and verified (an engine fault).
  D2  commit over damaged evidence: the range was verified, but every
      verified object for it was damaged by storage after its first commit
      (e.g. a Kafka offset moved backward by a stale recovery commit and
      then forward again). Counted, not an engine fault.
  D1  detected-but-unrepairable: recovery finds a VERIFIED row whose object
      fails the re-check although the source has already committed past
      its range (crash between adapter commit and ledger update, then
      damage while down). The rewind is clamped, so the range cannot be
      re-read; the engine logs it. Counted, not a violation of I2.
  I5  at-least-once: in every terminal state where all ranges are
      committed, every range has >= 1 verified intact object. Duplicates
      (more than one object per range) are counted.

Adapter rewinds are clamped to the committed point, as both the Kafka and
file adapters do in v0.7.0 (kafka_source.rs clamp_rewind; file_source.rs
replay_from_marker). Kafka commit semantics: committing range r sets
C = r + 1 (end offset inclusive, commit writes end + 1).

Usage:
  vtop_model.py --variant v070 --ranges 3 --per-cycle 1 --fails 1 --crashes 1
  vtop_model.py --sweep --out results.json
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Iterator, Optional

# Ledger states (subset relevant to the commit path; SEALED stands for the
# DISCOVERED..CHECKSUMMED prefix, which recovery treats identically).
SEALED, OBJ, MAN, VER, COMMITTED, FAILED = "SEALED", "OBJECT_UPLOADED", "MANIFEST_UPLOADED", "VERIFIED", "SOURCE_COMMITTED", "FAILED"
PRE_VERIFIED = (SEALED, OBJ, MAN)


@dataclass(frozen=True)
class Params:
    variant: str
    ranges: int
    per_cycle: int
    fails: int
    crashes: int
    ledger_loss: int
    mutations: int


@dataclass(frozen=True)
class State:
    up: bool                      # engine running
    H: int                        # adapter read head (next range to read)
    C: int                        # committed progress (ranges < C claimed archived)
    ledger: tuple                 # ((aid, r, state), ...) sorted by aid
    storage: tuple                # ((aid, r, intact, verified), ...) sorted by aid
    cycle: Optional[tuple]        # (aids in read order, phase, ptr, rewinds, pending_commit)
    recq: Optional[tuple]         # recovery commit queue: (aids...) or None
    rec_pending: Optional[int]    # aid whose adapter commit is done but ledger not yet updated (recovery)
    last_write: Optional[tuple]   # (aid, previous_state or None) for ledger-tail loss
    fails: int
    crashes: int
    losses: int
    muts: int
    next_aid: int
    unrepairable: int = 0         # D1 events on this path
    damaged_commits: int = 0      # D2 events on this path


# ---------------------------------------------------------------- helpers

def lget(s: State, aid: int):
    for a, r, st in s.ledger:
        if a == aid:
            return r, st
    return None


def lset(s: State, aid: int, new: str, r: Optional[int] = None) -> State:
    prev = None
    rows = []
    found = False
    for a, rr, st in s.ledger:
        if a == aid:
            prev = st
            rows.append((a, rr, new))
            found = True
        else:
            rows.append((a, rr, st))
    if not found:
        rows.append((aid, r, new))
        rows.sort()
    return replace(s, ledger=tuple(rows), last_write=(aid, prev, r))


def sput(s: State, aid: int, r: int) -> State:
    return replace(s, storage=tuple(sorted(s.storage + ((aid, r, True, False),))))


def sverify(s: State, aid: int) -> Optional[State]:
    """Mark the object of aid verified if it is intact; None if verification fails."""
    rows = []
    ok = False
    for a, r, intact, ver in s.storage:
        if a == aid:
            if not intact:
                return None
            ok = True
            rows.append((a, r, intact, True))
        else:
            rows.append((a, r, intact, ver))
    return replace(s, storage=tuple(rows)) if ok else None


def ever_verified(s: State, r: int) -> bool:
    return any(rr == r and ver for _, rr, _, ver in s.storage)


def evidenced(s: State, r: int) -> bool:
    return any(rr == r and intact and ver for _, rr, intact, ver in s.storage)


def obj_intact_verified(s: State, aid: int) -> bool:
    return any(a == aid and intact and ver for a, _, intact, ver in s.storage)


class Violation(Exception):
    def __init__(self, prop: str, detail: str):
        super().__init__(f"{prop}: {detail}")
        self.prop, self.detail = prop, detail


def adapter_commit(s: State, r: int, aid: int) -> State:
    """Kafka-style commit of range r: C := r + 1. Checks I1 and I2."""
    row = lget(s, aid)
    if row is None or row[1] != VER:
        raise Violation("I1", f"adapter commit for aid {aid} while ledger row is {row}")
    c = r + 1
    damaged = 0
    for q in range(s.C, c):
        if not ever_verified(s, q):
            raise Violation("I2", f"commit sets C={c} but range {q} was never archived and verified")
        if not evidenced(s, q):
            damaged += 1
    return replace(s, C=c, damaged_commits=s.damaged_commits + damaged)


# ---------------------------------------------------------------- transitions

def successors(s: State, p: Params) -> Iterator[tuple[str, State]]:
    if not s.up:
        yield from recover(s, p)
        # storage mutation while down: corrupt an object of a VERIFIED row
        if s.muts < p.mutations:
            for a, r, st in s.ledger:
                if st == VER and obj_intact_verified(s, a):
                    rows = tuple((aa, rr, False if aa == a else i, v) for aa, rr, i, v in s.storage)
                    yield f"corrupt(aid{a})", replace(s, storage=rows, muts=s.muts + 1)
        return

    # crash at any point
    if s.crashes < p.crashes:
        crashed = replace(s, up=False, cycle=None, recq=None, rec_pending=None, crashes=s.crashes + 1)
        yield "crash", crashed
        if s.losses < p.ledger_loss and s.last_write is not None:
            aid, prev, r = s.last_write
            if prev is None:
                ledger = tuple(x for x in crashed.ledger if x[0] != aid)
            else:
                ledger = tuple((a, rr, prev if a == aid else st) for a, rr, st in crashed.ledger)
            yield "crash+ledger_tail_loss", replace(crashed, ledger=ledger, losses=s.losses + 1, last_write=None)

    # recovery commit queue (after restart)
    if s.rec_pending is not None:
        yield "rec_ledger_commit", replace(lset(s, s.rec_pending, COMMITTED), rec_pending=None)
        return
    if s.recq:
        aid, rest = s.recq[0], s.recq[1:]
        r, st = lget(s, aid)
        ns = adapter_commit(s, r, aid)
        yield f"rec_adapter_commit(aid{aid},r{r})", replace(ns, recq=rest or None, rec_pending=aid)
        if s.fails < p.fails:  # adapter commit fails: row stays VERIFIED
            yield f"rec_adapter_commit_fail(aid{aid})", replace(s, recq=rest or None, fails=s.fails + 1)
        return
    if s.recq is not None and not s.recq:
        s = replace(s, recq=None)

    if s.cycle is None:
        if s.H < p.ranges:
            k = min(p.per_cycle, p.ranges - s.H)
            ns = s
            aids = []
            for i in range(k):
                aid = ns.next_aid
                ns = lset(ns, aid, SEALED, s.H + i)
                ns = replace(ns, next_aid=aid + 1)
                aids.append(aid)
            yield f"read_seal(r{s.H}..r{s.H + k - 1})", replace(ns, H=s.H + k, cycle=(tuple(aids), "verify", 0, (), None))
        return

    aids, phase, ptr, rewinds, pending = s.cycle
    if phase == "verify":
        all_done = True
        for aid in aids:
            r, st = lget(s, aid)
            if st in PRE_VERIFIED:
                all_done = False
                if st == SEALED:
                    yield f"upload_obj(aid{aid})", sput(lset(s, aid, OBJ), aid, r)
                elif st == OBJ:
                    yield f"upload_manifest(aid{aid})", lset(s, aid, MAN)
                elif st == MAN:
                    vs = sverify(s, aid)
                    if vs is not None:
                        yield f"verify(aid{aid})", lset(vs, aid, VER)
                if s.fails < p.fails:
                    yield f"fail(aid{aid})", replace(lset(s, aid, FAILED), fails=s.fails + 1)
        if all_done:
            if p.variant == "v070":
                yield "commit_phase", replace(s, cycle=(aids, "commit", 0, (), None))
            else:
                yield "commit_phase", replace(s, cycle=(aids, "commit060", 0, (), None))
        return

    if phase == "commit":  # v0.7.0: read order, then rewind failed ranges
        if pending is not None:
            yield "ledger_commit", replace(lset(s, pending, COMMITTED), cycle=(aids, phase, ptr, rewinds, None))
            return
        if ptr < len(aids):
            aid = aids[ptr]
            r, st = lget(s, aid)
            if st == VER:
                ns = adapter_commit(s, r, aid)
                yield f"adapter_commit(aid{aid},r{r})", replace(ns, cycle=(aids, phase, ptr + 1, rewinds, aid))
                if s.fails < p.fails:
                    yield f"adapter_commit_fail(aid{aid})", replace(s, fails=s.fails + 1, cycle=(aids, phase, ptr + 1, rewinds, None))
            else:  # FAILED: hand range back after the loop
                yield f"queue_rewind(r{r})", replace(s, cycle=(aids, phase, ptr + 1, rewinds + (r,), None))
            return
        H = s.H
        if rewinds:
            H = max(min(rewinds), s.C)  # clamp to committed point
        yield "rewind_failed_ranges", replace(s, H=H, cycle=None)
        return

    if phase == "commit060":  # v0.6.0: completion order, failed ranges not rewound
        if pending is not None:
            yield "ledger_commit", replace(lset(s, pending, COMMITTED), cycle=(aids, phase, ptr, rewinds, None))
            return
        done = set(rewinds)  # reuse field: aids already handled
        left = [a for a in aids if a not in done and lget(s, a)[1] == VER]
        if not left:
            yield "end_cycle", replace(s, cycle=None)
            return
        for aid in left:
            r, _ = lget(s, aid)
            ns = adapter_commit(s, r, aid)
            yield f"adapter_commit(aid{aid},r{r})", replace(ns, cycle=(aids, phase, ptr, rewinds + (aid,), aid))
            if s.fails < p.fails:
                yield f"adapter_commit_fail(aid{aid})", replace(s, fails=s.fails + 1, cycle=(aids, phase, ptr, rewinds + (aid,), None))
        return


def recover(s: State, p: Params) -> Iterator[tuple[str, State]]:
    """Restart: seed read head from committed progress, then recovery."""
    base = replace(s, up=True, H=s.C, cycle=None, recq=None, rec_pending=None, last_write=None)
    incomplete = [(a, r, st) for a, r, st in base.ledger if st != COMMITTED]
    if p.variant == "v070":
        ns = base
        rewinds = []
        verified = []
        for a, r, st in incomplete:
            if st == VER and obj_intact_verified(ns, a):
                verified.append((a, r))
            else:
                if st == VER and r < ns.C:
                    ns = replace(ns, unrepairable=ns.unrepairable + 1)
                ns = lset(ns, a, FAILED)  # FAILED -> REPLAY_REQUIRED (same effect here)
                rewinds.append(r)
        landed = max(min(rewinds), ns.C) if rewinds else None
        queue = []
        for a, r in sorted(verified, key=lambda x: x[1]):
            if landed is not None and landed < r:  # holds_back: lower range re-read
                continue
            queue.append(a)
        H = landed if landed is not None else ns.C
        ns = replace(ns, H=H, last_write=None)
        # Second-pass rewind (H := max(H, C)) is applied by the read step,
        # which reads from H; commits below only raise C, and the clamp
        # below keeps H >= C after them.
        yield "recover", replace(ns, recq=tuple(queue) if queue else None)
    else:  # v0.6.0: visit rows in any order; rewinds applied row by row
        for order in itertools.permutations(incomplete):
            ns = base
            trace_ok = True
            for a, r, st in order:
                if st == VER and obj_intact_verified(ns, a):
                    try:
                        ns = adapter_commit(ns, r, a)
                    except Violation:
                        trace_ok = False
                        raise
                    ns = lset(ns, a, COMMITTED)
                else:
                    if st == VER and r < ns.C:
                        ns = replace(ns, unrepairable=ns.unrepairable + 1)
                    ns = lset(ns, a, FAILED)
                    ns = replace(ns, H=max(r, ns.C))  # last row visited wins
            if trace_ok:
                yield "recover(" + ",".join(f"aid{a}" for a, _, _ in order) + ")", replace(ns, last_write=None)


def clamp(s: State) -> State:
    return replace(s, H=max(s.H, s.C)) if s.up and s.recq is None and s.cycle is None else s


# ---------------------------------------------------------------- search

@dataclass
class Result:
    params: dict
    states: int = 0
    transitions: int = 0
    terminal: int = 0
    violation: Optional[dict] = None
    max_duplicates: int = 0
    terminal_with_duplicates: int = 0
    stalled: int = 0  # engine idle with H>=N but C<N (a range never committed)
    unrepairable_states: int = 0
    damaged_commit_states: int = 0
    terminal_with_damaged_range: int = 0
    unrepairable_example: Optional[list] = None
    stalled_example: Optional[list] = None
    seconds: float = 0.0


def check(p: Params, limit: int = 5_000_000) -> Result:
    t0 = time.time()
    init = State(up=True, H=0, C=0, ledger=(), storage=(), cycle=None, recq=None, rec_pending=None,
                 last_write=None, fails=0, crashes=0, losses=0, muts=0, next_aid=0)
    res = Result(params=p.__dict__.copy())
    parent: dict[State, tuple[Optional[State], str]] = {init: (None, "init")}
    q = deque([init])
    while q:
        s = q.popleft()
        res.states += 1
        if res.states > limit:
            raise RuntimeError("state limit reached")
        n_succ = 0
        try:
            for label, ns in successors(s, p):
                ns = clamp(ns)
                n_succ += 1
                res.transitions += 1
                if ns not in parent:
                    parent[ns] = (s, label)
                    q.append(ns)
        except Violation as v:
            res.violation = {"property": v.prop, "detail": v.detail, "trace": trace(parent, s) + ["(next step violates)"]}
            break
        if s.up and s.cycle is None and s.recq is None and s.rec_pending is None and s.H >= p.ranges and s.C < p.ranges:
            res.stalled += 1
            if res.stalled_example is None:
                res.stalled_example = trace(parent, s)
        if s.damaged_commits:
            res.damaged_commit_states += 1
        if s.unrepairable:
            res.unrepairable_states += 1
            if res.unrepairable_example is None:
                res.unrepairable_example = trace(parent, s)
        if n_succ == 0 or (s.up and s.cycle is None and s.recq is None and s.H >= p.ranges and s.C >= p.ranges):
            if s.up and s.C >= p.ranges:
                res.terminal += 1
                for r in range(p.ranges):
                    if not ever_verified(s, r):
                        res.violation = {"property": "I5", "detail": f"terminal state: range {r} never archived", "trace": trace(parent, s)}
                        break
                if any(not evidenced(s, r) for r in range(p.ranges)):
                    res.terminal_with_damaged_range += 1
                counts = [sum(1 for _, rr, i, v in s.storage if rr == r) for r in range(p.ranges)]
                dup = sum(c - 1 for c in counts if c > 1)
                res.max_duplicates = max(res.max_duplicates, dup)
                if dup:
                    res.terminal_with_duplicates += 1
                if res.violation:
                    break
    res.seconds = round(time.time() - t0, 2)
    return res


def trace(parent, s) -> list[str]:
    out = []
    while s is not None:
        prev, label = parent[s]
        out.append(f"{label:<34} H={s.H} C={s.C} up={s.up} ledger={[(a, r, st) for a, r, st in s.ledger]}")
        s = prev
    return list(reversed(out))


# ---------------------------------------------------------------- CLI

SWEEP = [
    # (variant, ranges, per_cycle, fails, crashes, ledger_loss, mutations)
    *[(v, 3, 1, f, c, l, m) for v in ("v070", "v060") for f, c, l, m in
      [(0, 0, 0, 0), (1, 0, 0, 0), (0, 1, 0, 0), (1, 1, 0, 0), (1, 1, 1, 0), (0, 1, 0, 1), (1, 2, 1, 1)]],
    *[(v, 4, 1, 2, 2, 1, 1) for v in ("v070",)],
    *[(v, 3, 2, f, c, l, m) for v in ("v070",) for f, c, l, m in [(0, 1, 0, 0), (1, 0, 0, 0), (1, 1, 1, 1)]],
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variant", choices=["v070", "v060"], default="v070")
    ap.add_argument("--ranges", type=int, default=3)
    ap.add_argument("--per-cycle", type=int, default=1)
    ap.add_argument("--fails", type=int, default=1)
    ap.add_argument("--crashes", type=int, default=1)
    ap.add_argument("--ledger-loss", type=int, default=0)
    ap.add_argument("--mutations", type=int, default=0)
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--out")
    a = ap.parse_args()
    runs = SWEEP if a.sweep else [(a.variant, a.ranges, a.per_cycle, a.fails, a.crashes, a.ledger_loss, a.mutations)]
    results = []
    for v, n, b, f, c, l, m in runs:
        r = check(Params(v, n, b, f, c, l, m))
        results.append(r.__dict__)
        verdict = "OK" if r.violation is None else f"VIOLATION {r.violation['property']}"
        print(f"{v} N={n} B={b} fails={f} crashes={c} loss={l} muts={m}: {verdict:<14} "
              f"states={r.states} terminal={r.terminal} stalled={r.stalled} D1={r.unrepairable_states} D2={r.damaged_commit_states} max_dup={r.max_duplicates} ({r.seconds}s)")
        if r.violation:
            print("   " + r.violation["detail"])
    if a.out:
        with open(a.out, "w") as fh:
            json.dump(results, fh, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
