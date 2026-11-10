# Batch state machine and recovery: extracted from VTOP v0.7.0

**Snapshot described by the paper: VTOP v0.7.0**, tag `v0.7.0` =
commit `6c8f386` (author decision, 2026-11-10; annotated tag on `main`).
Local checkout: `.tmp/vtop-engine-v0.7.0/` (checked out at the tag).
This extraction was first made at `a8bce8e` (branch `release/0.7.0`, the
version-bump commit of PR allamiro/vtop-engine#605); §11 lists the five
commits between `a8bce8e` and the tag and what they change here.
Line numbers below are for `a8bce8e` and are approximate anchors
(`engine.rs` lines after ≈ L650 shift by a few lines at the tag).

v0.6.0 (`b1469be`, `.tmp/vtop-engine/`) is kept for comparison; §9 lists
what changed.

Everything here is read from source or the protocol draft. Items not yet
settled are marked **open question (OQ)**.

## 1. States and legal transitions

Source: `crates/vtop-core/src/state_machine.rs` (unchanged since v0.6.0).

```text
DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED →
OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED

any state except SOURCE_COMMITTED → FAILED → REPLAY_REQUIRED → BATCHING
```

- `can_transition(from, to)` allows exactly the forward chain, the
  failure edge from every state except `SOURCE_COMMITTED`, and
  `FAILED → REPLAY_REQUIRED → BATCHING`.
- `transition()` has a dedicated guard: `to == SOURCE_COMMITTED` with
  `from != VERIFIED` returns `CommitBeforeVerified`. This is the
  **commit rule** in code.
- `SOURCE_COMMITTED` is terminal.
- Protocol draft `docs/VTOP_PROTOCOL_DRAFT.md` (v0.2, unchanged): §12
  states, §13 commit rule (9 ordered steps), §14 recovery.

## 2. Source units and progress markers

| Source | Unit | Marker | Committed progress stored in |
|---|---|---|---|
| Kafka | topic partition (consumer group, auto-commit off) | start/end offset, **end inclusive**; commit writes `end_offset + 1` ("next to read"), `CommitMode::Sync` (`kafka_source.rs` ≈ L1162–1200) | Kafka broker |
| File | path + inode (generation) | start/end byte; partial trailing line never committed | VTOP state store |
| Syslog spool | spool file | spool_id, start/end byte | VTOP state store |

- A Kafka batch read under a different consumer group than the engine's
  current one is **not** committed; it stays `VERIFIED` (≈ L1175–1190).
- Startup seeding (`engine.rs` `seed_committed_offsets` ≈ L2503) uses a
  generation-aware `CommittedCursor` (path, inode, end byte, observed
  size; `crates/vtop-state/src/models.rs`), so a rotated or
  `copytruncate`d file is not resumed at the old file's high-water mark.
- Adapters advance their **read** head when they read, not when a batch
  commits.

## 3. Normal-path commit (`engine.rs` ≈ L1864–2060)

1. Buffers are keyed by `(source type, source name)`. Each cycle flushes
   buffers whose seal threshold fired, plus buffers **sealed early**
   because the next read did not continue them (a rotated or truncated
   file).
2. **Verify phase:** up to `limit` batches run
   `process_until_verified` concurrently (`buffer_unordered`, ≈ L1946).
   `limit` is the upload width, optionally adaptive (halve on throttle,
   +1 otherwise).
3. **Read-order commit:** results are sorted by the order their windows
   were read (≈ L1956), then committed serially via `commit_verified`
   (≈ L902 → `adapter.commit_progress(progress_end)`). On adapter error
   the batch stays `VERIFIED` and recovery retries.
4. **Failed-range rewind:** a batch that did not reach `VERIFIED` (or
   errored before it) has its start marker collected; after the commit
   loop, `adapter.replay_from_markers(rewinds)` hands those ranges back so
   the next read starts at the first unarchived byte (≈ L2029). If a
   rewind fails (e.g. the spool file was rotated away), the engine logs
   that the range "will not be re-read" — an explicit violation of the
   replayability assumption.

## 4. Verification classes (protocol §17; README)

| Class | Basis | Default |
|---|---|---|
| Strong | digest of the stored body (read back by VTOP, or service-computed SHA-256) equals manifest digest | **on** |
| Backend-limited | existence + size | refused unless explicit opt-out |
| Disabled checksum | size only | explicit opt-in |

Sidecars, ETags, uploader metadata are never accepted. Manifest:
reproducible self-hash; optional keyed BLAKE3 MAC; missing MAC under a
MAC policy does not downgrade silently. Hardened profile records
`manifest_version_id` and pins recovery to it; the ledger has no
`object_version_id`.

## 5. Recovery (`engine.rs` `recover()` ≈ L2195)

1. Seed committed cursors (§2).
2. **Claim** incomplete batches with an owner lease
   (`claim_incomplete_batches`, `LEASE_TTL_SECS = 600`).
3. **Scan** each claimed row (decisions are made durable *before* any
   rewind; ledger writes are fenced against another engine's claim,
   issue #557):
   - `VERIFIED` → `storage_still_verified`: re-download manifest (pinned
     version if recorded), check self-hash/MAC and ledger binding, check
     manifest-vs-ledger object metadata, re-verify object content.
     Pass → queue for commit. Fail → `FAILED` → `REPLAY_REQUIRED`, add
     start marker to the `RewindPlan`.
   - any state before `VERIFIED`, `FAILED`, `REPLAY_REQUIRED` →
     `FAILED` → `REPLAY_REQUIRED`, add start marker to the `RewindPlan`.
4. **Rewind, first pass:** `RewindPlan` keeps the lowest pending start per
   source unit (per inode for files); adapters rewind and report where
   each read head *lands* (a rewind never goes below the committed
   point). `rewinds.rebase(landed)`.
5. **Commit with holdback:** each queued `VERIFIED` row commits unless
   `rewinds.holds_back(progress_start)` — i.e. a lower range of the same
   unit is about to be re-read. Held-back rows stay `VERIFIED` and commit
   in a later pass.
6. **Rewind, second pass,** so ranges just committed are not re-read.

## 6. Durability of the ledger

`crates/vtop-state/src/sqlite_store.rs` ≈ L630–640: SQLite **WAL,
`synchronous=NORMAL`** (unchanged). Power loss can drop the newest ledger
transactions (no corruption): rows roll back to earlier states, whose
recovery actions are replay (duplicate at worst) or idempotent re-commit.
Asymmetric case: a Kafka offset committed externally whose ledger tail
is lost leaves a verified, **orphaned** object. PostgreSQL store behind
the `postgres` feature (not reviewed).

## 7. Candidate invariants (to formalize and check)

For source unit *u*, `C(u)` = committed progress. A range is
**evidenced** if its batch's object and manifest passed verification
(at commit time, or re-checked at recovery).

| ID | Invariant | Where enforced / status |
|---|---|---|
| I1 Commit rule | `SOURCE_COMMITTED` only from `VERIFIED` | `transition()`; check in model |
| I2 Source-progress safety | every position below `C(u)` lies in an evidenced range | read-order commit + rewinds + recovery holdback; **check in model and by fault injection** (OQ1) |
| I3 Replayability | uncommitted ranges stay readable | assumption; rewind failure is logged |
| I4 Recovery revalidation | no recovery commit without a fresh storage re-check | `storage_still_verified`; check |
| I5 At-least-once | every record below `C(u)` is in ≥1 verified object; duplicates allowed | random `batch_id` suffix (`crates/vtop-core/src/batch.rs` ≈ L253) ⇒ duplicates on replay |
| I6 Single owner | one engine acts on a batch at a time | lease + fenced ledger writes; adapter-commit fencing unknown (OQ3) |

## 8. Open questions

- **OQ1 (narrowed for v0.7.0).** In the normal path, commits within a
  cycle are applied in read order, but a *failed* earlier batch is only
  rewound **after** the commit loop. If two batches of the same unit are
  in one cycle and the earlier fails, the later one commits first. For a
  file, this occurs with early-sealed (rotated/truncated) windows, which
  are different generations (inodes) — probably safe because cursors are
  per generation. For Kafka, buffers are keyed by source name: can one
  cycle carry two batches of the **same partition**? If yes, the later
  commit moves the broker offset past the failed range before the rewind;
  a crash at that point would lose the range. **Settle by model (E-RQ1-M)
  and fault F9 (E-RQ1-F).**
- **OQ3: ownership.** Ledger transitions are fenced (#557); is
  `adapter.commit_progress` fenced against a stale owner (beyond Kafka's
  own group-generation checks)? Lease = 600 s, clock-based.
- **OQ4:** PostgreSQL durability profile (only if in scope).
- ~~OQ2~~ resolved in v0.7.0 by `RewindPlan` holdback (§5).
- ~~OQ5~~ resolved: Kafka end offset inclusive, commit at `end + 1`,
  synchronous.

## 9. What changed from v0.6.0

| Area | v0.6.0 | v0.7.0 |
|---|---|---|
| Commit order in a cycle | completion order | read order |
| Failed batch in normal path | marked failed; read head not rewound (next read continues past it) | range handed back to the source in the same cycle |
| Recovery rewinds | applied row by row (last row wins, #552) | collected; lowest start per unit; applied once |
| Recovery commit | commit any re-checked `VERIFIED` row | held back if a lower range of the unit is re-read |
| File cursor seeding | `MAX(end_byte)` per path | generation-aware `CommittedCursor` |
| Ledger replay writes | unfenced | fenced against another engine's claim |

These were fixes for real ordering hazards in v0.6.0
(VTOP issues #552, #558, and #563). The paper can use them as motivation for the
protocol's ordering rules.

## Formal model

`model/` (to create): one source unit with N ranges, upload width W,
crash at every step, ledger-tail loss, storage mutation while down, and
recovery. Encode **v0.7.0** logic (§3, §5) and, as a contrast, **v0.6.0**
logic (§9). Check I1, I2, I4, I5. Small bounds (N ≤ 4, W ≤ 2, ≤ 2
crashes) suffice to find ordering counterexamples. TLC is not installed
on the Mac; options: install the TLA+ tools (needs a JDK) or write a
Python explicit-state checker.

## 10. Findings from the experiments (2026-11-10)

- **F1: manifest under-states the covered range (protocol §11.1 gap).**
  `engine.rs` ≈ L659 builds the manifest with
  `source_progress: read.progress_end`, the marker of the *last read* in
  the batch window. When a batch accumulates several reads before
  sealing, the object holds bytes `[first read start, end)` but the
  manifest's `start_byte` (or `start_offset`) is the last read's start.
  The ledger's `progress_start_json` is correct, so ledger-driven
  recovery is unaffected. Consequence: an auditor (or a recovery after
  ledger loss) that relies on manifests alone sees gaps that are not
  real, and the manifest does not bind the full source range it
  archives. Observed in 36 of 38 manifests at 1 MiB batches (Linux+SSH)
  and in multi-read batches throughout the crash and Kafka runs; see
  `manifest_start_understated` in the result files. Not a data-loss bug;
  a chain-of-custody fidelity bug. Candidate VTOP issue. Still present at
  the tag (≈ L666, `read.progress_end.for_manifest()`): 3,955 of 4,056
  manifests in the tag's crash and Kafka runs; batch identifiers carry the
  same under-stated start.
- **F2: power-loss rollback is safe but costly.** Deleting the SQLite WAL
  after a kill (worst case of `synchronous=NORMAL`) rolled the ledger and
  file cursors back to the last checkpoint: 0 unsafe commits, 0 lost
  bytes, but 101–148 duplicate objects per trial (replay amplification
  1.02–1.49× of the source; tag round: 50–128 objects, 0.50–1.29×).
- **F3: the ledger is the integrity anchor.** Forged manifests with a
  recomputed self-hash are rejected without the MAC because the ledger
  holds the object and manifest digests (E-RQ2-R, M6/M7).
- **F4: size-only verification commits same-size corruption** (E-RQ2-R,
  M1, 4/4) — the configuration opt-out is a real integrity loss, not a
  cosmetic one.
- **F5: intermittent Kafka partition stall (liveness).** Tag round,
  trial `20261110-v070-kafka/stall-hdfs-p4-t02` (4 partitions): the final
  process read partition 0 contiguously to offset 447,960, committed
  447,961, and then got no further records from that partition for 900 s
  (`read_cycle_profile`: `empty_sources: 1`, no WARN/ERROR); partitions
  1–3 completed. Ledger: 54 rows, all `SOURCE_COMMITTED`. Broker
  committed offset = verified prefix, so I1/I2 hold; 80,181 records
  stayed readable, and a restart on the same ledger and group committed
  them in 2 s. `kafka_source.rs` is unchanged between `a8bce8e` and the
  tag (first round 0/14), so it is intermittent. The adapter keeps sticky
  assignments, per-pass pauses of full topics, and a `delivered` map to
  decide when to `seek`; a partition left paused or positioned past its
  delivered offset would look exactly like this, but the cause was not
  confirmed. Candidate VTOP issue: the engine has no stall detector for a
  partition whose log end is ahead of its committed offset.
- **OQ1 resolved in the model** (E-RQ1-M): v0.7.0 is safe if at most one
  batch per source unit is in flight per cycle, and unsafe otherwise.

## 11. From `a8bce8e` to the `v0.7.0` tag (`6c8f386`)

`git log a8bce8e..v0.7.0`: #605 (version bump), #608 and #609 (release
workflow), #604 (benchmark harness), and **#606 = issue #578**, the only
change to engine or adapter code (14 files under `crates/`).

#578: a read position was tied to a file by path, inode and length, so a
replacement that reused the inode and was at least as long resumed
mid-file. Every file and spool marker now carries
`content_fingerprint` = SHA-256 of the first min(length, 4096) bytes
(spool markers also gain `file_size`). The continuity check,
`commit_is_current`, the `delete_after_commit` guard,
`replay_from_marker`, and `read_position` compare it, and a mismatch reads
the file from byte 0. `committed_cursors` returns the newest committed
row's fingerprint, so a replacement made while the engine is down is
caught at restart. Recovery groups rewinds by `rewind_key` = unit plus
fingerprint (`engine.rs` ≈ L2992), i.e. per *generation* of a unit, where
`a8bce8e` grouped by unit only.

Effect on this document:

- §1 states, §3 normal path, §4 verification, §6 ledger: unchanged.
- §2 markers: file and spool generations are now identified by path,
  inode, **and first-page content**; markers gain optional JSON fields
  (no DDL). Manifests never carry them (`ProgressMarker::for_manifest`).
- §5 recovery: the rewind plan is computed per generation. For a unit
  that is never replaced the key is constant once the file exceeds
  4 KiB, so the plan is the one modelled; the formal model (one unit, no
  replacement) is unaffected.
- F1 (manifest under-states its start) is **still present** at the tag:
  `source_progress: read.progress_end.for_manifest()` (≈ L666).
- Experiments: every binary-dependent campaign was re-run on the tag
  (`results/20261110-v070-*`); the `a8bce8e` runs stay in
  `results/20261110-*` for comparison.

## 12. OQ3 resolved: the adapter commit is not fenced (2026-11-10)

Verified against the code at tag `6c8f386`:
`commit_verified` (`engine.rs` ≈ L929) and `commit_recovered` (≈ L2457) call
`adapter.commit_progress` first and `store.mark_source_committed` afterwards. Ledger state updates are
compare-and-swap fenced (`sqlite_store.rs` `update_batch_state`: `WHERE batch_id = ? AND state = ? AND
claim_gen = ?`), but nothing checks the lease or claim generation before the adapter call. A stale owner
can therefore advance source progress before its ledger write is rejected. For Kafka, the broker's group
generation offers partial protection; files and spools have none. The paper keeps single ownership as
Assumption 5 and states this gap (Sections III, V, VIII).

