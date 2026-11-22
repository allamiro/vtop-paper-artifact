# vtop-engine v0.7.0 → v0.10.1 (44f6619): changes that affect the paper

Source: the vtop-engine maintainer, 2026-11-20, in reply to the paper study (decision D4).
File:line references are at tag v0.10.1. **Status: REPORTED, not yet verified by the paper study.**
Per `research/README.md`, each item must be checked against the v0.10.1 source before it changes the
model, a proof, a figure or a sentence of the paper; tick the box and cite the line when verified.

**Verification status (2026-11-20).** §1–10 and §13 were checked against the code in a separate code review:
**38 verified, 11 corrected, 1 unsure** — evidence and corrections in `v0101-verification.md` (read it before
using any item here; its "Claims that were wrong" list overrides the text below). §11–12 were checked by the
paper study itself (below). Unchecked boxes in this file are superseded by that report.

## 1. Batch state machine (`crates/vtop-core/src/state_machine.rs`)

- [ ] New terminal state **SUPERSEDED** (:41-49; #567, PR #655, v0.8.0): a handed-back FAILED or
  REPLAY_REQUIRED range whose unit has since committed past its end. New edges FAILED→SUPERSEDED and
  REPLAY_REQUIRED→SUPERSEDED; `can_transition` :138; "any non-terminal state may fail" is
  `!from.is_terminal()`, so SUPERSEDED cannot fail. No states removed.
- [ ] The only edge into SOURCE_COMMITTED is still from VERIFIED.
- [ ] A run-time failure now writes FAILED then REPLAY_REQUIRED (`fail!`, `crates/vtop-cli/src/engine.rs`
  :519-528; #627, PR #638, v0.8.0). The range is handed back whether or not the writes land; if the
  REPLAY_REQUIRED write fails the row stays FAILED and the next recovery replays it. (v0.7.0: FAILED only.)
- [ ] Ledger rows are born only as BATCHING, in code and in the DB schema (#625, PR #637, v0.8.0).

## 2. Recovery and restart (`crates/vtop-cli/src/engine.rs`)

- [ ] `recover()` :4031; recovery's REPLAY_REQUIRED writes :4161, :4285.
- [ ] Hold-back (#639, PR #645, v0.8.0): a range whose replay decision cannot be recorded is HELD, not
  rewound; no VERIFIED range above it commits past it (`fn hold` :5484). Since v0.10.0 (#782, PR #785) a
  rewind above a held range is lowered to the held start (`fn starting_at` :5656) instead of being
  withheld; at worst a range is read and archived twice.
- [ ] Failed ranges of rotated files are handed back and re-read from the rotated file (#621, PR #647,
  v0.8.0; `crates/vtop-adapters/src/rotated.rs`, field in `file_source.rs`).
- [ ] A VERIFIED row left uncommitted is settled by a later pass, not only at the next restart (#624,
  PR #659, v0.8.0): `hold_verified` :1672, event `commit_held_back` :3488, `settle_verified` :2833. Same
  VERIFIED→SOURCE_COMMITTED edge, possibly later and in another pass.
- [ ] VERIFIED and SOURCE_COMMITTED writes sync the SQLite WAL (#626, PR #642, v0.8.0): after power loss
  only in-flight batches are re-archived.

## 3. Kafka commit fencing (`crates/vtop-adapters/src/kafka_source.rs`)

- [ ] The Kafka offset commit is still **not** fenced (assign(), no group generation; `commit_progress`
  :3425; doc comment :82-100).
- [ ] Since #614/PR #617 (v0.8.0) the engine resumes and clamps rewinds from the **ledger** (:1343 ff.),
  whose writes are fenced by the claim generation (#557). A stale broker commit can no longer skip or
  replay a range; the broker offset is written for lag monitoring only.

## 4. Manifest (`crates/vtop-core/src/manifest.rs`)

- [ ] Written version still "0.3" by default (`VTOP_VERSION` :36); "0.4" opt-in via
  `manifest.write_version` (`config.rs`:59); readers accept ≤ 0.4 (`MAX_READABLE_VERSION` :388; #671/#674).
  0.4 planned as default at v0.12.0 (`docs/reference/compatibility.md`).
- [ ] A multi-read batch's manifest names its whole range (#622, PR #630; `window_marker`
  engine.rs:5876) — the paper's manifest-start fidelity defect is fixed.
- [ ] `verification_status` still never updated (`set_verification` :1122, no non-test caller).
- [ ] Opt-in since v0.10.0: manifest MAC with key rotation (`manifest.mac`, needs 0.4), verification
  receipts (`manifest.verification_receipts`).

## 5. Verification on real S3 (`crates/vtop-upload/src/s3_native.rs`)

- [ ] SHA-256 PutObject carries `x-amz-checksum-sha256` (:881); S3 recomputes and rejects a mismatch
  (BadDigest); hex digest also kept as user metadata.
- [ ] Verify: HeadObject with `ChecksumMode::Enabled` (:1334), `verify_object` :1352.
- [ ] Multipart objects have only a composite checksum, which is rejected; verification then streams and
  hashes the stored body (`digest_stored_body` :931, used :1406, :1427). **Batches above the multipart
  threshold are therefore read back even in store-SHA-256 mode.**
- [ ] SDK default `AWS_REQUEST_CHECKSUM_CALCULATION=when_supported`: the backend sends a whole-object
  SHA-256 header instead of the SDK's aws-chunked CRC32 trailer (:218-226, :736-742; #778, v0.10.0). With
  `when_required` and no Object Lock profile, no checksum is sent. UNSURE: UploadPart behaviour.
- [ ] Per-stream checksum choice with strong verification as a floor (#669, PR #673). Size-only is
  `upload.require_strong_verification: false`, announced rather than refused (#651).

## 6. Upload path and configuration

- [ ] Per-request deadline grows with body size (#663, PR #666; `body_timeouts` s3_native.rs:765,
  `request_min_bytes_per_second`) — the policed-uplink failure in the paper (§VII-D) is fixed.
- [ ] Defaults (`crates/vtop-core/src/config.rs`:528-545): max_records 10,000; max_bytes 100 MiB;
  max_age 60 s; max_concurrent_batches 8; source_poll_wait 250 ms; idle_poll_interval 2,000 ms.
- [ ] Keys added since v0.7.0 (spellings UNSURE; `docs/reference/config.md` at v0.10.1 is authoritative):
  per-stream checksum algorithm, operator egress ceiling (#481), request min bytes/s (#663),
  manifest.write_version/mac/verification_receipts, upload.object_lock (#684), upload.require_object_versioning,
  upload.multipart_part_attempts, opentelemetry section (#683). No renamed key known.

## 7. Open at v0.10.1 (`docs/ROADMAP.md` :1264-1268, :900-908)

#798 (OTel test flake), #729 (mutation gate on verify/recovery/file-continuity not done), #724 (platform
matrix), #709 (reused object after Object Lock expiry fails every replay; fixed on main by PR #757 after
v0.10.1), #773/#774 (#626 follow-ups), #706, #752/#751 (cluster/group commit), #743 (formats registry),
#733 (world-readable key/state files not refused; fixed on main by PR #775 after v0.10.1), #714 (prototype).
**#675 is closed.**

## 8. Changes to the safety argument (v0.9.0, v0.10.x)

- [ ] **Stable object keys** (#626 parts 1-3; PRs #755, #793, #796): a later replay, or one over a grown
  source, resolves to the already archived object or is rebuilt as the first cut; replays are bounded.
  The commit edge is unchanged, but the paper's "a replay writes a new random-key object" is no longer
  true; duplicate-object claims and the RQ1 duplicate counts must be re-measured.
- [ ] Held-range rewind lowering (#782/#785), above.
- [ ] Opt-in, off by default: Object Lock profile (stored-retention check before VERIFIED), manifest MAC,
  verification receipts.
- [ ] File identity adds ctime and the page below the read position (#620, #619); `delete_after_commit`
  waits until the file stops being written (#619).
- [ ] PostgreSQL remains out of scope for multi-host engines: only in a `--features postgres` source build
  (release.yml :212, :260 build without it); the engine is single-instance only, enforced at startup
  (`docs/PRODUCTION_HA.md`:1615); multi-writer recovery is [PROPOSED] (:543). The claim generation (#557)
  makes a recovery handover between engines safe at the ledger, not concurrent operation.
- [ ] v0.10.1 Linux archives need glibc ≥ 2.28 (#808, PR #809).

## 9. Harness compatibility (engine maintainer, second reply, 2026-11-20; REPORTED)

- [ ] **Config keys** (`docs/configure/reference.md`): `checksum.algorithm` global (:29, default sha256)
  and per stream (sha256 | blake3 | none; `docs/configure/streams.md`:41); `upload.require_strong_verification`
  (default true; false = size-only, accepts same-size corruption, :228);
  `upload.max_egress_bytes_per_second` (s3_native only, min 1024, :225); `upload.request_min_bytes_per_second`
  (default 4096, :224); `manifest.write_version` (default "0.3", must be quoted, :102); `manifest.mac` (:103);
  `manifest.verification_receipts` (default false, :104); `upload.object_lock` (s3_native only, :230);
  `upload.require_object_versioning` (default false, :229); `upload.multipart_part_attempts` (default 3, :241).
- [ ] **Multipart:** `upload.multipart_threshold_bytes` default **8 MiB** (:238); part size 8 MiB (:237),
  parallelism 4 (:239), abandon after 86400 s (:240). With max_bytes 100 MiB, any compressed object ≥ 8 MiB is
  multipart → verified by read-back even in store-SHA-256 mode. UploadPart carries no vtop checksum
  (s3_native.rs:1554-1585); UNSURE whether the SDK adds per-part CRC32 under `when_supported`.
- [ ] **CLI:** `vtopctl run --config` and `vtopctl process-once --source <kafka|file|syslog> --config`
  unchanged (`crates/vtop-cli/src/commands.rs`:38, :48); global `--json`, `--log-level`. `run` shutdown:
  `shutdown flush complete` → exit 0, `shutdown flush incomplete` → exit 1 (`docs/reference/cli.md`:63-71).
  UNSURE: process-once drain details.
- [ ] **Ledger schema** (`crates/vtop-state/src/sqlite_store.rs`): table `batches` (:48-72); new columns
  `verification_class`, `sealed_at` (migrations :1069, :1074); `PRAGMA user_version` still 1; lower-case
  state strings incl. `superseded` (`state_machine.rs`:56-67). **Trigger `trg_batches_insert_as_batching`
  (:1095) aborts any INSERT whose state ≠ 'batching'** — injectors must insert as batching, then UPDATE.
  New append-only table `audit_events` (:98; UPDATE/DELETE refused by triggers :1106, :1109).
- [ ] **Power-loss emulation:** VERIFIED and SOURCE_COMMITTED are written on a `synchronous=FULL` connection
  (:279, :334). **Deleting the whole WAL after SIGKILL is now stronger than a power loss** (it discards
  fsynced commits). Faithful method: `crates/vtop-cli/tests/ledger_power_loss.rs` (header :1-35): record the
  WAL length after every ledger write; for a crash after write k, truncate the WAL to its length after the
  last FULL write at or before k; stay below the auto-checkpoint size. Alternatives: LazyFS or dm-log-writes
  (UNSURE on AWS). No failpoints in release builds (#818 merged after v0.10.1).
- [ ] **No-recheck ablation:** `storage_still_verified`, engine.rs:3866 (called at :4077), now returns
  `(recheck, checked)`.
- [ ] **Object keys:** batch id `vtop-<UTCstamp>-<source>-<range>-<8 hex uuid4>` names one attempt
  (`crates/vtop-core/src/batch.rs`:271); manifests are per attempt. Objects are named by
  `build_object_name` (:311) with no random part (file/spool `vtop-<engine>-<source>-<range>[-i<inode>]-<digest>`,
  Kafka `vtop-<source>-<range>-<digest>`, digest = first 32 hex of the object checksum); a replay of the same
  bytes reuses the object after verifying it. Falls back to the per-attempt key when the backend cannot tell a
  missing key from a refused request, the derived key holds bytes that do not verify, or under the Object
  Lock fallback (engine.rs:953-970, event `object_key_per_attempt`). Rows written before v0.10.0 have no
  stable seal hour. **Count duplicate objects by object URI, not by manifest.**
- [ ] **Events kept:** `batch_started`, `batch_sealed`, `manifest_uploaded` (:1317), `verification_passed`
  (:1559; variant `verification_passed (backend_limited: size-only)` :1551), `source_committed`. Added:
  commit_held_back, object_key_derived/reused/per_attempt/fallback/probe_throttled, multipart_session_aborted/kept,
  multipart_sweep(_failed), upload_throttled, verification_receipt_failed, bucket_versioning_refused,
  object_lock_bucket_refused. UNSURE: exact replay-decision message texts — grep the parser's strings.
- [ ] **File identity** (`crates/vtop-adapters/src/file_source.rs`): rename + new file → `OtherFile { inode }`
  (:165-175, `same_file` :788); truncation → `Shrank`; new: same inode and length with a different first page
  or page below the archived end → rewritten in place (#620; `ArchivedEnd` :905-917). Rotated-away failed
  range re-read from the rotated file (#621, `rotated.rs`). UNSURE: rename-then-append to the old path within
  one poll.
- [ ] **Kafka:** `enable.auto.commit=false` (kafka_source.rs:241); consumer keys :238-258; still `assign`
  (:3041) / `incremental_assign` (:1712), no group membership; resume from the ledger (:1343).
- [ ] **Also breaks a v0.7.0 harness:** `require_strong_verification` true by default — a backend with no
  store checksum and no read-back path is refused, not committed size-only; unquoted `manifest.write_version`
  fails config load; `upload.max_egress_bytes_per_second` and `upload.object_lock` refused off s3_native.

## 10. Corrections from the engine maintainer to the paper study's summary

1. "All six known problems are fixed, including the Kafka stall" was the paper study's inference, not the
   maintainer's statement. Earlier the paper study verified only that the six **fix PRs** are contained in
   v0.8.0. Each defect must be checked against its own issue (closed, and behaviour at v0.10.1) before the
   paper says it is fixed. #675 was a CI test flake, not an engine defect.
2. The claim-generation fence on ledger writes (#557) **predates v0.7.0**. New in v0.8.0 is that Kafka progress
   resumes from that fenced ledger instead of the broker group offset (#614/#617). The Kafka commit is still
   unfenced. Assumption 5 should credit resume-from-ledger.
3. "Same object name on replay" holds except for the B5 fallbacks and for rows written before v0.10.0; say
   "usually", with the exceptions.

## 11. The theorem's second condition no longer holds literally (engine maintainer, 2026-11-20; REPORTED)

- [x] Since v0.10.0 (#626, stable object keys) one cycle can hold **two windows of the same unit**: a rebuilt
  (replayed) window and the bytes after it (engine.rs:3455-3458).
- [x] Safety comes from an **ordering rule with hold-back** (engine.rs:3459-3500): if an earlier range of the
  unit failed in this cycle, a later window that reached VERIFIED is HELD (stays VERIFIED, event
  `commit_held_back`) and is committed only after the source has committed through the failed range's re-read
  (`Unsettled::held_in_process`; `settle_verified` :2833). Recovery uses the same hold for a row above a range
  being re-read (#624/PR #659; #639 for undecidable ranges).
- [ ] Maintainer's suggested condition: *"a batch's source commit is issued only when no lower range of the
  same source unit is pending or failed"* (ordered commit with hold-back); the v0.7.0 "at most one pending
  batch per unit per cycle" is a special case.
- [ ] UNSURE (maintainer): whether any path other than the rebuild case puts two windows of one unit in a
  cycle (verify runs up to `max_concurrent_batches` = 8 batches of different sources concurrently). **Must be
  checked in the code before the proof relies on it.**
- Model consequence: the guard-only counterexample stands. The submitted paper's model showed the protocol
  with B = 2 is unsafe *without* hold-back and that read-order commit was not shown necessary with B = 1;
  v0.10.x is the B = 2 case **with** ordered hold-back, so the model must add the hold-back rule and check
  B = 2 (expected safe), and the single-rule ablation should then show hold-back necessary at B = 2.

## 12. Batch-size fix and the three two-window paths (engine maintainer, 2026-11-20; REPORTED)

- [x] The run-mode batch-size defect (item X1) is fixed by **PR #824** (merged to main as 5623428 on 2026-11-20; NOT in
  v0.10.1): a read that would take the buffered window past max_records or max_bytes seals the window first,
  so a run-mode batch holds at most max_bytes; covers files and Kafka. Cite #824 for the defect and its fix.
- [x] Two windows of one source unit in one cycle arise on three paths: (1) a rebuilt window (#626);
  (2) rotation or truncation — a read that does not continue the buffered window seals it (**present at
  v0.10.1**); (3) from #824 on, a size overflow. The per-unit hold-back (engine.rs:3455-3500 at v0.10.1)
  orders all three. **The model must not assume two windows arise only from a rebuild.**

## Paper impact (to plan after verification)

| Paper element | Change needed |
|---|---|
| Fig. 2, §IV-B state machine | add SUPERSEDED; run-time failure → FAILED → REPLAY_REQUIRED |
| Model (`state-machine/model/vtop_model.py`), Table 3 | add SUPERSEDED, later-pass settle of VERIFIED, held-range lowering; re-run sweep and ablations |
| Assumption 2 / power-loss failure mode, RQ1 power-loss trials | WAL sync at VERIFIED and SOURCE_COMMITTED; re-run |
| Assumption 5 / Theorem 1 scope | Kafka progress resumed from the fenced ledger; broker commit unfenced but no longer authoritative |
| §IV-D manifest fidelity, §III last paragraph, §VI defect list | start-marker defect fixed; most listed defects fixed |
| At-least-once / duplicates (§III-F, Table 4, §VIII-C) | stable object keys: re-measure duplicates; rewrite claims |
| §VII-D long-distance policer result | #663 fixed: re-measure or state as a v0.7.0 history note |
| Store SHA-256 cost (RQ3, Fig. 4) | multipart objects are read back: choose batch size below the multipart threshold or report per path |

### §11–12 checked against the code (2026-11-20)

Read at v0.10.1 (`.tmp/vtop-engine-v0.10.1`, 44f6619) and on main after #824 (`.tmp/vtop-engine-post824`,
aa928ea0). The hold-back block (`failed_from` … `hold_verified`) is **byte-identical** on main; #824
(5623428) adds only `PendingBuffer::would_overflow` and a seal-before-append in the read loop
(engine.rs:2298-2323, :3714-3728 on main), never splitting a rebuilt window. Its regression test
`run_mode_batches_stay_within_max_bytes_across_passes` quotes our smoke numbers (73,417,507 B at 16 MiB →
33,554,257 B batches).

What the code does in one cycle (v0.10.1 `engine.rs`):
1. All windows of the cycle run `process_until_verified` concurrently (`buffer_unordered(limit)`, :3397) and
   are **all awaited** before any commit; then sorted by read order (:3407).
2. `failed_from[unit]` = lowest start of a window of that unit that did **not** reach VERIFIED this cycle
   (FAILED outcome or an error), :3455-3470. `unit_key` = source key + inode (:5703), so a rotated file is a
   different unit; a truncated file (same inode) is the same unit.
3. Commits are issued serially in read order. A VERIFIED window whose start is above `failed_from[unit]` is
   **held**: stays VERIFIED, event `commit_held_back`, pushed as `Unsettled::held_in_process` with the
   adapter's `committed_through` at the hold (:3474-3500).
4. Every other VERIFIED window is committed (`commit_verified`). If its source commit fails, the row stays
   VERIFIED and is retried later (`Unsettled::commit_failed`), and **later windows of the same unit still
   commit** (:3538-3560). This is safe: the skipped-over bytes are verified in storage.
5. Each non-VERIFIED window's range is handed back (`replay_from_markers`) and buffered windows of that unit at
   or above it are dropped (`drop_pending_above`, :5613), so the next cycle re-reads from the failed start.
6. A held row settles (`settle_one`, :2858) only when `held_row_may_settle`: the unit's committed point is ≥ the
   row's end **and** has moved up since the hold (:5791). So a held row never advances the commit itself; its
   claim is renewed (fenced) before the source is touched.

**Exact condition, as code:** the committed point of a unit advances to the end of a window only when that
window is VERIFIED and no lower window of the same unit **failed in the same cycle**; lower windows of earlier
cycles that failed were handed back and are re-read below it. The maintainer's wording ("no lower range of the
same unit is pending or failed") is right in effect; "pending" never happens inside a cycle because all windows
are awaited before commits. A lower window that is VERIFIED but whose source commit failed does **not** block.

**The theorem relies on the replay assumption** (already in the paper as Assumption *Replayability*, `asm:replay`, 03_system_model.tex:120; correction 2026-11-20: not new). If `replay_from_markers` refuses (a spool file
rotated away; a truncated file whose old bytes are gone), the range is not re-read, `count_refused_replay`
increments the `source_replay_refused_total` metric (:4559-4566) and an ERROR is logged (:3600-3616, "it is the one outcome
that leaves the archive short of the source"). In the **next** cycle `failed_from` is empty, so a later window of
that unit commits past the unarchived range. Safety therefore holds only under: *every range handed back can be
read again from the source* (source retention). The v0.7.0 paper's assumption on source retention must be
checked to cover rotation and truncation explicitly.

**Confirmed by the engine maintainer (2026-11-20, reply to the paper study):** (1) no guard stops the later
commit after a refused replay; the refused range is also forgotten by the rebuild bookkeeping
(`self.recorded.forget`, :3611); recovery clamps its rewind to the committed point (#627, :3575-3578), so it is
never archived. Loud and counted (`vtop_source_replay_refused_total`, alert `VtopSourceReplayRefused`, #720/#772),
not prevented. (2) copytruncate keeps the inode, so it is the same unit; the new generation's window starts below
`failed_from` and commits; the old range's rewind is refused by the file adapter (`replay_from_marker`,
file_source.rs:4091 ff., "…truncated; it was N when this range was read"). `Shrank`/`ArchivedEnd` (#620) stop
mis-archiving replacement bytes but neither create a unit nor stop the commit; #621's rotated re-read needs the old
inode to be reachable (rename rotation with a held descriptor). **UNSURE (maintainer):** a copytruncate after a
rewind succeeded but before the re-read — whether the read loop re-validates identity (`PendingBuffer::continues`,
engine.rs:2305) must be checked before the proof relies on it.

**§III:** keep Assumption *Replayability* (`asm:replay`), which already covers deleted, truncated and rewritten files and spools; add one sentence that v0.10.x counts a refused hand-back (`vtop_source_replay_refused_total`) but does not prevent a later commit past it. Earlier draft, superseded: *Assumption (replayable sources). Every range the engine hands back to its source
can be read again with the same bytes until the source has committed past it. A refused hand-back (a rotated-away
spool file, a copytruncate) violates the assumption; VTOP v0.10.x reports it (`vtop_source_replay_refused_total`)
but does not prevent a later commit past it.*

**Model consequence:** model the per-cycle rule exactly as steps 1–6 with B ≥ 2 windows per unit; add a
`replay_refused` fault that violates the assumption (expected: counterexample, i.e. the assumption is
necessary); ablate the hold (expected: counterexample at B = 2); ablate "await all before commit" is not
needed (it is structural).

## 13. Next release: v0.10.2 (engine maintainer, third reply, 2026-11-20; REPORTED)

Target pin under decision D5. Not tagged yet; the maintainer will message the tag and re-confirm this list
against it. Main at the time of the reply: `aa928ea0` (22 commits after v0.10.1; worktree
`.tmp/vtop-engine-post824`).

- [ ] **Contents planned:** #824 (batch size ≤ max_bytes in run mode); #830 (OTel span flush, fixes #798);
  #827 (localfs fsyncs file and directory before a write is reported done); **run-mode per-stage timings**
  (fix just started — makes `run` report stage timings like `process-once`; the smoke test saw zeros). Possibly
  glibc 2.17 floor + static musl archives; otherwise glibc 2.28 (fine on AL2023). Date not set: "a few days",
  UNSURE; the maintainer decides the cut.
- [ ] **Batch states and ledger schema unchanged** since v0.10.1 (state_machine.rs untouched; no column change).
- [ ] **#775 (merged): startup refusal of loose permissions.** The engine and `vtopctl validate` refuse to start
  when the manifest MAC key, the state-store/ledger file or the audit file is readable by other local users. New
  SQLite ledgers are created 0600; an existing 0644 ledger is refused until `chmod 0600`. Override
  `security.allow_loose_secret_permissions` (default false; WARN + audit record + metric). **Harness: chmod 0600
  every ledger it creates or copies** (do not use the override in measured runs).
- [ ] **#757 (merged), Object Lock only:** a replay over an unlocked or expired-lock object writes a fresh locked
  object under the per-attempt key (`object_key_fallback`, alert VtopObjectLockFallback). Irrelevant unless
  `upload.object_lock` is set.
- [ ] **#824 adds the third two-window path** (size overflow); ordering by the per-unit hold-back unchanged.
- [ ] No change to verification-method selection, commit ordering or hold-back; no new archive-path config keys.
  Cluster-plane only: #780, #758, #763, #832 (unmerged). #818 failpoints are test-only, excluded from release builds.
- [x] **Resume point: NOT changed in v0.10.2 (engine maintainer correction, 2026-11-20).** Prompted by the paper
  model's trace, PR #844 tried MAX(end_offset) per (topic, partition, group) and was found to open a **loss** case: a
  deleted and recreated topic restarts offsets at 0, so the old incarnation's higher end sits above the new rows and a
  restart resumes (and moves the group offset) past records never archived; the guard that distrusts the ledger when
  the group has no committed offset covers only the first restart. v0.10.2 keeps the **newest-row** resume (duplicates
  possible, no loss); a proper fix needs the topic id in the progress marker (format change, parked). File and spool
  resume also stay newest-row: a lower newer row on the same inode cannot be told from a copytruncate without a
  generation marker. **Paper:** state the newest-row trace as a duplicate source; do not claim a max-end resume.
- [ ] **Tamper/encryption (engine maintainer, 2026-11-20):** v0.10.2 already has `manifest.mac`, Object Lock,
  versioning (pinned manifest version), receipts and the hash-chained audit trail; the engine's end-to-end tamper suite
  and SSE (PR #856; `upload.server_side_encryption` is a block: `mode`, optional `kms_key_id`, `bucket_key`) target
  **v0.10.3**. No-ledger check: `vtopctl verify-manifest`. No
  supported ledger rebuild from manifests (`rebuild.rs` is the #626 window rebuild).
- [ ] **Object names bounded to 255 bytes (fix in progress for v0.10.2; engine maintainer 2026-11-20).** Found by the
  paper harness port: at v0.10.1 `build_object_name` embeds the full sanitized source path with no bound; over
  NAME_MAX the localfs upload fails (ENAMETOOLONG) on every pass and the run never drains (safe, but a permanent
  liveness failure). Fix: only the source token is shortened, to a readable prefix plus a short stable hash of the full
  path, only when needed; names that fit are unchanged (stable keys preserved); e2e localfs test. PR number: TBD.
  Harness workaround: `vtoplab.Run` moves inputs to `.tmp/in/<hash>/` when the predicted name exceeds 240 bytes.

## 14. v0.10.2 as tagged (2026-11-21)

- Tag `v0.10.2` (tag object 90e15fa3) → commit **0abf11696e47f4db871bd4c2cda882756c114390** (squash of #853), checked
  locally with `git rev-parse v0.10.2^{commit}`. Contains #824 (5623428 is an ancestor) and #842 (Linux archives with
  a glibc 2.17 floor and static musl builds). Release workflow started 23:11Z on 2026-11-20; archives, SHA256SUMS
  and cosign bundles pending — pin the archive digest only after they are published and verified.
- **Not in v0.10.2** (maintainer; checked for the name fix: no bound in `batch.rs` at the tag): run-mode stage timings
  (#843; use `VTOP_METRICS_ADDR` + `vtop_stage_duration_seconds` meanwhile), the engine's tamper suite, SSE, the
  localfs fsync fix (#827), and the 255-byte object-name bound (harness keeps its short-path workaround; cloud runs
  use short paths). These target v0.10.3.
- Resume point unchanged (newest committed row; #844 not merged).

## 15. Engine tamper suite (PR #859, v0.10.3; engine maintainer 2026-11-21; REPORTED, MinIO only)

12 live MinIO cases (`crates/vtop-cli/tests/live_s3_tamper.rs`, governance Object Lock) and 10 ledger/audit cases
(`tamper_audit_trail.rs`); `docs/reference/tamper-evidence.md` table is test-checked against the tests; every NOT CAUGHT
row asserts the wrong verdict (a measured limit). Summary in `research/hypotheses.md` H2.5–H2.7. Paper rule: a
"detects" claim cites the test names and keeps the NOT CAUGHT rows as stated limits; reproduce the key rows on real S3
in E-RQ5-TAMPER (compliance mode and real AWS were not exercised by the engine).

## 16. Engine bugs found by the paper's tamper tests (2026-11-21; accepted by the engine maintainer)

Found by `research/experiments/rq5-cloud/tamper/tamper.py` on v0.10.2 (MinIO, 3 identical runs):
1. **`vtopctl verify-manifest` does not bind a manifest to the key it was read from**: a swapped genuine manifest of
   another batch passes (MAC on or off, with or without a ledger); scrub and recovery catch it through the ledger
   binding. Fix in progress: the key derived from the manifest's own fields must equal the URI's key (authenticated
   under the MAC); new live tamper test; docs to state which manifest versions are bound.
2. **Versioning suspended under a running engine keeps writing manifests** (10 objects in 25 s, nothing committed).
   Fix in progress: treat "no object version to pin" as a bucket-state condition (no re-put per retry, bounded
   backoff on GetBucketVersioning, rate-limited ERROR, metric).
Target v0.10.3 if green before the SSE PR (#856) and the bump, else v0.10.4; PR numbers to follow. **Paper wording
at v0.10.2:** the swap is flagged by scrub and recovery (ledger binding) but not by standalone verify-manifest;
suspended versioning commits nothing but keeps retrying writes — both as observed behaviour, not claims.
- Fix PRs (engine maintainer, 2026-11-21; target v0.10.3 if green): **#882** verify-manifest requires the recorded
  `manifest.uri` to equal the URI read (all manifest versions 0.1–0.4; covered by the self-hash, and by the MAC when
  signed; `--json` gains `manifest_key_ok`; live test `live_verify_manifest_fails_another_batchs_valid_manifest_at_this_key`).
  **#883** suspended versioning: 1 object, 0 manifests over ten retries (was 10 manifests), GetBucketVersioning backoff
  5→60 s, resumes when Enabled; ERROR `bucket_versioning_paused`, gauge `vtop_bucket_versioning_paused`, alert
  `VtopBucketVersioningPaused`; live test `live_suspended_versioning_under_a_running_engine_writes_one_object_and_resumes`.

## 17. v0.10.3 released (engine maintainer, 2026-11-21; REPORTED, not adopted)

Commit 8c8349421526c08ee2669fb2bb5c9a9a5910ec92 (squash of #879); SHA256SUMS e99e4124…4335; aarch64 gnu archive
9d5c5c63…7abe; image ghcr.io/allamiro/vtop-engine:0.10.3 @ sha256:89bdbe17…a9f6; cosign and provenance verified by the
maintainer. Contents: run-mode per-stage timings (#843), S3 SSE `upload.server_side_encryption` (#856), tamper suite
(#859), Kafka resume keyed by topic id (#855), librdkafka pause fix (#877), localfs fsync (#827), client-cert CN binding
(#854). **Correction (maintainer, same day; checked against the GitHub v0.10.3 release notes, line 46):** #882 (verify-manifest binds a manifest to its key) **is in v0.10.3**; #883 (suspended versioning) is not (→ v0.10.4). Paper wording: the stand-alone swap passes in "v0.10.2 and earlier", fixed in 0.10.3 (#882); suspended-versioning retries in "v0.10.3 and earlier" (#883, v0.10.4).
**Paper stays pinned to v0.10.2** (D5): every reported result (C29 local re-runs, E-RQ5 cost/WAN/scale/tamper/perf) is on
v0.10.2; adopting v0.10.3 would mean re-running all of them and the author has asked to close the AWS work (D15).
Encryption at rest (H2.8) therefore stays untested in this paper.
- [x] At the next paper update: change 'fixed after 0.10.2, \#882' to 'fixed in 0.10.3, \#882' in both versions (07_experiments.tex, supplemental.tex); #883 stays 'fixed after 0.10.3'. Done 2026-11-21 in research/drafts/manuscript-tnsm-{12p,10p} (C34); paper/ not yet.

## 18. Running-path rewind: the highest failed start wins (v0.10.2 defect; fix targeted at v0.10.4 (PR #891, opened 2026-11-21))

Found by the engine maintainers (2026-11-21); checked by the paper study against tag v0.10.2 (0abf116); the same code
is at v0.10.0 (ac37936) and v0.10.1 (44f6619) (engine.rs :3512, :3579, :3592; file_source.rs :4179; kafka_source.rs
:3626), so the v010 model's source (v0.10.1) has the defect too. v0.10.3 was not checked (tag not in the local clone).

- `Engine::run_cycle` (crates/vtop-cli/src/engine.rs) pushes **every** failed window's start to `rewinds`, in read order
  (`verified.sort_by_key(|(order, _, _)| *order)` :3934; `rewinds.push(started_at)` :4038 for a FAILED outcome and :4105
  for an error before VERIFIED), and hands them all to `adapter.replay_from_markers(&rewinds)` (:4116-4118).
- The default `replay_from_markers` (crates/vtop-adapters/src/base.rs:229-238) calls `replay_from_marker` once per
  marker, and each call **assigns** the read position: file `c.read_byte = (*start_byte).max(c.committed_byte)`
  (file_source.rs:4179), spool `c.read_byte = replay_byte` (syslog_spool_source.rs:895, :913). Kafka overrides it
  (kafka_source.rs:3560) but also assigns, `cur.committed_offset = Some(resume_at)` (:3626), and its clamp frontier
  includes the previous rewind's landing (`in_memory` :3617, `rewind_frontier` :105), so in Kafka the highest start wins
  in any order. Result: when two or more windows of one unit FAIL in the same cycle, the unit resumes at the
  **highest** failed start; the lower failed range is never re-read.
- The per-cycle hold-back (`failed_from`, :3985-3999, :4001-4030) uses the **lowest** failed start, but holds only
  VERIFIED windows. The re-read of the higher failed window is a new window in a later cycle with no failure below it,
  so it commits past the lower failed range: data loss (I2).
- Recovery is not affected: `RewindPlan` keeps one rewind per unit and content generation at the lowest start
  (`add_start` :5964, `if position < start_position(&generation.marker)` :5981; `markers()` :6145, applied :4879-4897).
  With at most one failed window per unit per cycle there is no defect (the lowest and the highest start coincide).
- Model: `--rewind-last-wins` in `model/vtop_model.py` (the running-path hand-back lands at max of the failed starts,
  clamped to C; recovery unchanged). Results in `results/20261121-v010-rewind-last-wins.{md,txt,json}`: **I2 violated
  in every configuration with B ≥ 2 and ≥ 2 failures** (smallest: N = 2, B = 2, 2 failures, no crash, 249 states);
  **no violation with B = 1 or with one failure**, where results equal the default model's row for row. The default
  model (lowest start wins) is unchanged and still finds no violation.
- **Paper consequence:** Theorem 1's Definition "Ordered commit with hold-back" (research/drafts/05_correctness_v010.tex)
  assumes the lowest failed start is handed back; v0.10.2 as built does not meet it when one unit has ≥ 2 failed
  windows in a cycle. The theorem describes the protocol (and the fixed engine), not v0.10.2's running path; the paper
  must say so (author decision).

### §18 addendum (2026-11-22): affected range is v0.7.0–v0.10.3
The maintainer traced the defect to #563 (first released in v0.7.0). Checked here at v0.7.0 (6c8f386): engine.rs:2005-2041
collects every failed start into `rewinds` and calls `replay_from_markers` (:2041); base.rs:201-209 applies markers one
by one; file_source.rs:1182 assigns `c.read_byte = (*start_byte).max(c.committed_byte)`. For the v0.7.0 results the
trigger (two failed windows of one unit in one cycle) lies outside that paper's stated condition (one batch per unit
and cycle). Fix #891: 42 checks green, independent review clean on cce3b549 (one earlier finding, a same-inode replacement whose old
content failed alongside it, fixed with a regression test); queued to merge for v0.10.4. Pipelining PRs #892, #895,
#896 are held until after the v0.10.4 tag.
