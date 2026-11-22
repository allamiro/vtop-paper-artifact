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
  v010  VTOP v0.10.1 (44f6619); the commit/hold-back code is unchanged on main
        after #824 (5623428), so this is also the v0.10.2 logic. Read from
        crates/vtop-cli/src/engine.rs at v0.10.1 (see
        research/state-machine/v0101-changes.md §11-12):
          * per-cycle hold-back (:3455-3500): a VERIFIED window above the
            lowest window of its unit that FAILED in the same cycle is held
            (stays VERIFIED) instead of committed;
          * a held row settles in a later pass only when the unit's committed
            point is >= its end AND has moved up since the hold
            (held_row_may_settle :5791); it never advances the commit itself;
          * a VERIFIED row whose source commit failed is retried by a later
            pass (#624, settle_verified :2833), not only at the next restart;
          * recovery: a non-VERIFIED row whose unit committed past it becomes
            SUPERSEDED (#567) instead of being replayed; held VERIFIED rows
            and recovered rows whose commit fails are settled later;
          * stable object keys (#626, build_object_name): a replay of a range
            reuses its intact stored object; a damaged one falls back to a
            per-attempt key (engine.rs:953-970);
          * VERIFIED and SOURCE_COMMITTED ledger writes are fsynced
            (synchronous=FULL, #626/PR #642): a power loss can drop only a
            newer write of another state;
          * the committed point is monotone per unit (progress resumes from the
            ledger's newest committed row).
        Not modelled: held ranges whose replay decision could not be recorded
        (#639/#782), Object Lock, more than one unit.
  v010-nohold  ablation: v010 without the per-cycle hold-back.

Fault added for v010: --refusals K lets the source refuse up to K hand-backs
(replay_from_markers returns Err: a rotated-away spool file, a copytruncate).
The engine counts and logs it (source_replay_refused_total) but does not stop
a later commit past the range, so this fault is expected to violate I2: it
shows the replayable-source assumption is necessary.

--rewind-last-wins models the v0.10.2 running-path hand-back as implemented
(0abf116): every failed window's start goes to replay_from_markers in read
order and each per-marker rewind assigns the read position, so the unit
resumes at its HIGHEST failed start (clamped to C), not the lowest. Recovery
is unchanged. Results: results/20261121-v010-rewind-last-wins.md.

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
SUPERSEDED = "SUPERSEDED"  # v010 only (#567): terminal, a handed-back range its unit has committed past
PRE_VERIFIED = (SEALED, OBJ, MAN)
V010 = ("v010", "v010-nohold", "v010-nohandback", "v010-noorder")
MONOTONE = True  # v010 committed point monotone per unit; --no-monotone checks the result without it
RESUME_NEWEST = False  # v010: at restart, C := end of the NEWEST-created committed row (Kafka #617,
                       # sqlite_store.rs:520-545 committed_kafka_offsets), not the adapter's max
REWIND_LAST_WINS = False  # v010 running path as v0.10.2 implements it (0abf116): run_cycle passes EVERY failed
                          # window's start to replay_from_markers in read order (engine.rs:4038, :4105, :4118);
                          # each per-marker rewind ASSIGNS the read position (file_source.rs:4179, spool :913;
                          # Kafka :3626, whose clamp also rises to the previous rewind's landing), so the
                          # HIGHEST failed start of the unit wins. Default (False): the lowest failed start.
                          # Recovery is unchanged either way (RewindPlan keeps the lowest start per unit).


@dataclass(frozen=True)
class Params:
    variant: str
    ranges: int
    per_cycle: int
    fails: int
    crashes: int
    ledger_loss: int
    mutations: int
    refusals: int = 0


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
    unsettled: tuple = ()         # v010: ((aid, held, committed_at_hold), ...) settled by a later pass
    objkey: tuple = ()            # v010: ((aid, object key), ...); absent = per-attempt key (= aid)
    refusals: int = 0             # v010: hand-backs the source refused on this path


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


def keyof(s: State, aid: int) -> int:
    """Object key of attempt aid: a stable key -(r+1) (v010), else the per-attempt key aid."""
    for a, k in s.objkey:
        if a == aid:
            return k
    return aid


def sput(s: State, aid: int, r: int) -> State:
    return replace(s, storage=tuple(sorted(s.storage + ((aid, r, True, False),))))


def sput010(s: State, aid: int, r: int) -> State:
    """v010 upload with stable object keys (#626): reuse an intact object of the range, fall back to a
    per-attempt key when the stable key holds damaged bytes, else create the stable object."""
    stable = -(r + 1)
    row = next((x for x in s.storage if x[0] == stable), None)
    if row is not None and row[2]:
        key, storage = stable, s.storage
    elif row is not None:
        key, storage = aid, tuple(sorted(s.storage + ((aid, r, True, False),)))
    else:
        key, storage = stable, tuple(sorted(s.storage + ((stable, r, True, False),)))
    return replace(s, storage=storage, objkey=tuple(sorted(s.objkey + ((aid, key),))))


def sverify(s: State, aid: int) -> Optional[State]:
    """Mark the object of aid verified if it is intact; None if verification fails."""
    rows = []
    ok = False
    key = keyof(s, aid)
    for a, r, intact, ver in s.storage:
        if a == key:
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
    key = keyof(s, aid)
    return any(a == key and intact and ver for a, _, intact, ver in s.storage)


class Violation(Exception):
    def __init__(self, prop: str, detail: str):
        super().__init__(f"{prop}: {detail}")
        self.prop, self.detail = prop, detail


def adapter_commit(s: State, r: int, aid: int, mono: bool = False) -> State:
    """Kafka-style commit of range r: C := r + 1 (v010: C := max(C, r + 1)). Checks I1 and I2."""
    row = lget(s, aid)
    if row is None or row[1] != VER:
        raise Violation("I1", f"adapter commit for aid {aid} while ledger row is {row}")
    c = max(r + 1, s.C) if (mono and MONOTONE) else r + 1
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
                    k = keyof(s, a)
                    rows = tuple((aa, rr, False if aa == k else i, v) for aa, rr, i, v in s.storage)
                    yield f"corrupt(aid{a})", replace(s, storage=rows, muts=s.muts + 1)
        return

    # crash at any point
    if s.crashes < p.crashes:
        crashed = replace(s, up=False, cycle=None, recq=None, rec_pending=None, crashes=s.crashes + 1, unsettled=())
        yield "crash", crashed
        synced = (p.variant in V010 and s.last_write is not None
                  and (lget(s, s.last_write[0]) or (None, None))[1] in (VER, COMMITTED))
        if s.losses < p.ledger_loss and s.last_write is not None and not synced:
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
        v010 = p.variant in V010
        ns = adapter_commit(s, r, aid, mono=v010)
        yield f"rec_adapter_commit(aid{aid},r{r})", replace(ns, recq=rest or None, rec_pending=aid)
        if s.fails < p.fails:  # adapter commit fails: row stays VERIFIED (v010: settled by a later pass)
            uns = tuple(sorted(s.unsettled + ((aid, False, -1),))) if v010 else s.unsettled
            yield f"rec_adapter_commit_fail(aid{aid})", replace(s, recq=rest or None, fails=s.fails + 1, unsettled=uns)
        return
    if s.recq is not None and not s.recq:
        s = replace(s, recq=None)

    if p.variant in V010 and s.cycle is None and s.unsettled:
        yield from settle(s, p)

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
                    put = sput010 if p.variant in V010 else sput
                    yield f"upload_obj(aid{aid})", put(lset(s, aid, OBJ), aid, r)
                elif st == OBJ:
                    yield f"upload_manifest(aid{aid})", lset(s, aid, MAN)
                elif st == MAN:
                    vs = sverify(s, aid)
                    if vs is not None:
                        yield f"verify(aid{aid})", lset(vs, aid, VER)
                if s.fails < p.fails:
                    yield f"fail(aid{aid})", replace(lset(s, aid, FAILED), fails=s.fails + 1)
        if all_done:
            if p.variant in ("v070", "nohandback"):
                yield "commit_phase", replace(s, cycle=(aids, "commit", 0, (), None))
            elif p.variant == "noorder":
                yield "commit_phase", replace(s, cycle=(aids, "commitany", (), (), None))
            elif p.variant in V010:
                start = () if p.variant == "v010-noorder" else 0  # noorder: ptr = tuple of aids already handled
                yield "commit_phase", replace(s, cycle=(aids, "commit010", start, (), None))
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
        if rewinds and p.variant != "nohandback":  # nohandback: failed ranges not handed back
            H = max(min(rewinds), s.C)  # clamp to committed point
        yield "rewind_failed_ranges", replace(s, H=H, cycle=None)
        return

    if phase == "commit010":  # v0.10.x: read order, per-cycle hold-back, then hand back failed ranges
        if pending is not None:
            yield "ledger_commit", replace(lset(s, pending, COMMITTED), cycle=(aids, phase, ptr, rewinds, None))
            return
        anyorder = p.variant == "v010-noorder"  # ablation: commits in any order (hold-back and hand-back kept)
        if anyorder:
            choices = [(a, ptr + (a,)) for a in aids if a not in ptr]
        else:
            choices = [(aids[ptr], ptr + 1)] if ptr < len(aids) else []
        for aid, nxt in choices:
            r, st = lget(s, aid)
            if st == VER:
                failed = [lget(s, a)[0] for a in aids if lget(s, a)[1] == FAILED]
                if p.variant != "v010-nohold" and failed and r > min(failed):  # engine.rs:3474-3500
                    uns = tuple(sorted(s.unsettled + ((aid, True, s.C),)))
                    yield f"hold(aid{aid},r{r})", replace(s, unsettled=uns, cycle=(aids, phase, nxt, rewinds, None))
                    continue
                ns = adapter_commit(s, r, aid, mono=True)
                yield f"adapter_commit(aid{aid},r{r})", replace(ns, cycle=(aids, phase, nxt, rewinds, aid))
                if s.fails < p.fails:  # source commit fails: row stays VERIFIED, retried by a later pass (#624)
                    uns = tuple(sorted(s.unsettled + ((aid, False, -1),)))
                    yield f"adapter_commit_fail(aid{aid})", replace(s, fails=s.fails + 1, unsettled=uns,
                                                                     cycle=(aids, phase, nxt, rewinds, None))
            else:
                yield f"queue_rewind(r{r})", replace(s, cycle=(aids, phase, nxt, rewinds + (r,), None))
        if choices:
            return
        if p.variant == "v010-nohandback":  # ablation: failed ranges not handed back during the run
            H = s.H
        elif rewinds and REWIND_LAST_WINS:  # v0.10.2: the last (= highest, read order) failed start wins
            H = max(max(rewinds), s.C)
        else:
            H = max(min(rewinds), s.C) if rewinds else s.H
        yield "rewind_failed_ranges", replace(s, H=H, cycle=None)
        if rewinds and s.refusals < p.refusals:  # replay_from_markers -> Err: counted, not re-read (:3600-3616)
            yield "replay_refused", replace(s, cycle=None, refusals=s.refusals + 1)
        return

    if phase == "commitany":  # ablation: hand-back kept, commits in any order
        if pending is not None:
            yield "ledger_commit", replace(lset(s, pending, COMMITTED), cycle=(aids, phase, ptr, rewinds, None))
            return
        left = [a for a in aids if a not in ptr]
        if not left:
            H = max(min(rewinds), s.C) if rewinds else s.H
            yield "rewind_failed_ranges", replace(s, H=H, cycle=None)
            return
        for aid in left:
            r, st = lget(s, aid)
            if st == VER:
                ns = adapter_commit(s, r, aid)
                yield f"adapter_commit(aid{aid},r{r})", replace(ns, cycle=(aids, phase, ptr + (aid,), rewinds, aid))
                if s.fails < p.fails:
                    yield f"adapter_commit_fail(aid{aid})", replace(s, fails=s.fails + 1, cycle=(aids, phase, ptr + (aid,), rewinds, None))
            else:
                yield f"queue_rewind(r{r})", replace(s, cycle=(aids, phase, ptr + (aid,), rewinds + (r,), None))
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


def settle_enabled(s: State) -> bool:
    for aid, held, at in s.unsettled:
        row = lget(s, aid)
        if row is None or row[1] != VER:
            return True
        if not held or (s.C >= row[0] + 1 and s.C > at):
            return True
    return False


def settle(s: State, p: Params) -> Iterator[tuple[str, State]]:
    """v010 settle_verified/settle_one (engine.rs:2833-2960), run by a later pass."""
    for u in s.unsettled:
        aid, held, at = u
        rest = tuple(x for x in s.unsettled if x != u)
        row = lget(s, aid)
        if row is None or row[1] != VER:
            yield f"settle_drop(aid{aid})", replace(s, unsettled=rest)
            continue
        r = row[0]
        if held:
            # held_row_may_settle: committed point >= end AND moved up since the hold; the
            # source commit is then a no-op (C already past the end), the ledger row commits.
            if s.C >= r + 1 and s.C > at:
                yield f"settle_held(aid{aid},r{r})", replace(lset(s, aid, COMMITTED), unsettled=rest)
        else:
            ns = adapter_commit(s, r, aid, mono=True)
            yield f"settle_commit(aid{aid},r{r})", replace(ns, unsettled=rest, rec_pending=aid)
            if s.fails < p.fails:
                yield f"settle_commit_fail(aid{aid})", replace(s, fails=s.fails + 1)


def recover010(s: State, p: Params) -> Iterator[tuple[str, State]]:
    """v0.10.1 recover() (engine.rs:4031-4330)."""
    C = s.C
    if RESUME_NEWEST:  # aids grow with row creation, so the newest-created committed row has the largest aid
        done = [(a, r) for a, r, st in s.ledger if st == COMMITTED]
        C = max(done)[1] + 1 if done else 0
    ns = replace(s, up=True, C=C, H=C, cycle=None, recq=None, rec_pending=None, last_write=None, unsettled=())
    rewinds, verified = [], []
    for a, r, st in [x for x in ns.ledger if x[2] not in (COMMITTED, SUPERSEDED)]:
        if st == VER:
            if obj_intact_verified(ns, a):
                verified.append((a, r))
                continue
            if r < ns.C:  # D1: detected, but the rewind is clamped
                ns = replace(ns, unrepairable=ns.unrepairable + 1)
            ns = lset(ns, a, FAILED)  # FAILED -> REPLAY_REQUIRED
            rewinds.append(r)
        elif r < ns.C:  # committed_past (#567): retired, no rewind
            ns = lset(ns, a, SUPERSEDED)
        else:
            ns = lset(ns, a, FAILED)
            rewinds.append(r)
    landed = max(min(rewinds), ns.C) if rewinds else None
    queue, held = [], []
    for a, r in sorted(verified, key=lambda x: x[1]):
        if landed is not None and landed < r:  # holds_back: settled later (Unsettled::held_back)
            held.append((a, True, ns.C))
        else:
            queue.append(a)
    H = landed if landed is not None else ns.C
    if p.variant == "v010-noorder":  # ablation: recovery commits in any order too
        for q in sorted(set(itertools.permutations(queue))):
            yield "recover", replace(ns, H=H, last_write=None, recq=q if q else None, unsettled=tuple(sorted(held)))
        return
    yield "recover", replace(ns, H=H, last_write=None, recq=tuple(queue) if queue else None,
                             unsettled=tuple(sorted(held)))


def recover(s: State, p: Params) -> Iterator[tuple[str, State]]:
    """Restart: seed read head from committed progress, then recovery."""
    if p.variant in V010:
        yield from recover010(s, p)
        return
    base = replace(s, up=True, H=s.C, cycle=None, recq=None, rec_pending=None, last_write=None)
    incomplete = [(a, r, st) for a, r, st in base.ledger if st != COMMITTED]
    if p.variant in ("v070", "noorder", "nohandback"):
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
        if p.variant == "noorder":  # ablation: recovery commits in any order
            for q in set(itertools.permutations(queue)):
                yield "recover", replace(ns, recq=q if q else None)
        else:
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
        if (s.up and s.cycle is None and s.recq is None and s.rec_pending is None and s.H >= p.ranges and s.C < p.ranges
                and not (p.variant in V010 and settle_enabled(s))):
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


# v0.10.x: (variant, ranges, per_cycle, fails, crashes, ledger_loss, mutations, refusals).
# Same grid as SWEEP for B = 1 so v070 and v010 compare row by row; then B = 2 (two windows of a unit per
# cycle, which v0.10.x produces by a rebuild, a rotation/truncation, and from #824 a size overflow); then the
# hold-back ablation and the replayable-source fault.
SWEEP010 = [
    *[("v010", 3, 1, f, c, l, m, 0) for f, c, l, m in
      [(0, 0, 0, 0), (1, 0, 0, 0), (0, 1, 0, 0), (1, 1, 0, 0), (1, 1, 1, 0), (0, 1, 0, 1), (1, 2, 1, 1)]],
    ("v010", 4, 1, 2, 2, 1, 1, 0),
    *[("v010", 3, 2, f, c, l, m, 0) for f, c, l, m in [(0, 1, 0, 0), (1, 0, 0, 0), (1, 1, 1, 1), (2, 2, 1, 1)]],
    ("v010", 4, 2, 2, 1, 1, 1, 0),
    ("v010", 4, 3, 2, 1, 0, 0, 0),
    *[("v010-nohold", 3, 2, f, c, l, m, 0) for f, c, l, m in [(1, 0, 0, 0), (1, 1, 1, 1)]],
    ("v010-nohold", 3, 1, 1, 1, 1, 1, 0),
    *[("v010", 3, b, 1, 0, 0, 0, 1) for b in (1, 2)],
]

# Single-rule ablations on the v0.10.x logic (B7): the B = 1 grid of the 2026-11-11 v0.7.0 ablation, then B = 2.
SWEEP010_ABL = [
    *[(v, 3, 1, f, c, l, m, 0) for v in ("v010-nohandback", "v010-noorder") for f, c, l, m in
      [(0, 0, 0, 0), (1, 0, 0, 0), (0, 1, 0, 0), (1, 1, 0, 0), (1, 1, 1, 0), (0, 1, 0, 1), (1, 2, 1, 1)]],
    *[(v, 4, 1, 2, 2, 1, 1, 0) for v in ("v010-nohandback", "v010-noorder")],
    *[(v, 3, 2, f, c, l, m, 0) for v in ("v010-nohandback", "v010-noorder") for f, c, l, m in
      [(0, 1, 0, 0), (1, 0, 0, 0), (1, 1, 1, 1)]],
]

# v0.10.2 rewind defect (--rewind-last-wins): boundary configurations beyond SWEEP010 — the smallest unit
# (N = 2), two and three failures at B = 1 (no violation expected), one failure at B = 3, and more failures at
# B = 2 and B = 3.
SWEEP010_LASTWINS_EXTRA = [
    ("v010", 2, 2, 2, 0, 0, 0, 0),
    ("v010", 3, 2, 2, 0, 0, 0, 0),
    ("v010", 3, 2, 3, 0, 0, 0, 0),
    ("v010", 3, 1, 2, 0, 0, 0, 0),
    ("v010", 4, 1, 3, 0, 0, 0, 0),
    ("v010", 3, 1, 2, 1, 1, 1, 0),
    ("v010", 4, 3, 1, 0, 0, 0, 0),
    ("v010", 4, 3, 1, 1, 1, 1, 0),
    ("v010", 4, 3, 2, 0, 0, 0, 0),
    ("v010", 4, 3, 3, 0, 0, 0, 0),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variant", choices=["v070", "v060", "noorder", "nohandback", "v010", "v010-nohold", "v010-nohandback", "v010-noorder"], default="v070")
    ap.add_argument("--ranges", type=int, default=3)
    ap.add_argument("--per-cycle", type=int, default=1)
    ap.add_argument("--fails", type=int, default=1)
    ap.add_argument("--crashes", type=int, default=1)
    ap.add_argument("--ledger-loss", type=int, default=0)
    ap.add_argument("--mutations", type=int, default=0)
    ap.add_argument("--refusals", type=int, default=0, help="v010: hand-backs the source may refuse")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--sweep010", action="store_true", help="v0.10.x sweep and ablations (SWEEP010)")
    ap.add_argument("--sweep010-ablation", action="store_true", help="single-rule ablations on v0.10.x (SWEEP010_ABL)")
    ap.add_argument("--no-monotone", action="store_true",
                    help="v010: let a later commit of a lower range move the committed point back (robustness check)")
    ap.add_argument("--resume-newest", action="store_true",
                    help="v010: restart resumes at the newest-created committed row's end (Kafka ledger resume)")
    ap.add_argument("--rewind-last-wins", action="store_true",
                    help="v010: the running path's hand-back lands at the HIGHEST failed start of the cycle, not the "
                         "lowest (v0.10.2 run_cycle + per-marker replay_from_marker)")
    ap.add_argument("--sweep010-lastwins", action="store_true",
                    help="SWEEP010 plus the boundary grid SWEEP010_LASTWINS_EXTRA (use with --rewind-last-wins)")
    ap.add_argument("--out")
    a = ap.parse_args()
    global MONOTONE, RESUME_NEWEST, REWIND_LAST_WINS
    MONOTONE = not a.no_monotone
    RESUME_NEWEST = a.resume_newest
    REWIND_LAST_WINS = a.rewind_last_wins
    if a.sweep:
        runs = [x + (0,) for x in SWEEP]
    elif a.sweep010:
        runs = SWEEP010
    elif a.sweep010_lastwins:
        runs = SWEEP010 + SWEEP010_LASTWINS_EXTRA
    elif a.sweep010_ablation:
        runs = SWEEP010_ABL
    else:
        runs = [(a.variant, a.ranges, a.per_cycle, a.fails, a.crashes, a.ledger_loss, a.mutations, a.refusals)]
    results = []
    for v, n, b, f, c, l, m, k in runs:
        r = check(Params(v, n, b, f, c, l, m, k))
        results.append(r.__dict__)
        verdict = "OK" if r.violation is None else f"VIOLATION {r.violation['property']}"
        print(f"{v} N={n} B={b} fails={f} crashes={c} loss={l} muts={m}{f' refusals={k}' if k else ''}: {verdict:<14} "
              f"states={r.states} terminal={r.terminal} stalled={r.stalled} D1={r.unrepairable_states} D2={r.damaged_commit_states} max_dup={r.max_duplicates} ({r.seconds}s)")
        if r.violation:
            print("   " + r.violation["detail"])
    if a.out:
        with open(a.out, "w") as fh:
            json.dump(results, fh, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
