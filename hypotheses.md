# Hypotheses and answers

Each research question is split into testable hypotheses. A hypothesis is
**OPEN** until an experiment answers it; then record the answer, the
evidence files, and the analysis script, and set the status to
**SUPPORTED**, **REJECTED**, or **MIXED**. Only answered hypotheses may be
written into the paper.

Experiment IDs refer to folders in [experiments/](experiments/).
Invariants (I1–I6) and open questions (OQ1–OQ5) refer to
[state-machine/README.md](state-machine/README.md).

---

## RQ1: Can one verification-bound commit protocol enforce source-progress safety across Kafka, files, and syslog spools?

### H1.1 Commit rule holds in the model
**Claim:** In a formal model of the v0.7.0 state machine and recovery,
no reachable state commits a batch that has not reached `VERIFIED` (I1),
including across crashes at every step.
**Prediction:** zero violations of I1 within the checked bounds.
**Test:** E-RQ1-M (model checking).
**Status:** SUPPORTED (within bounds). **Answer:** No I1 violation in any
of the 18 configurations, up to N=4 ranges, 2 failures, 2 crashes, one
ledger-tail loss, and one storage mutation (793,353 states for v0.7.0).
I1 is checked on every adapter commit in the model. Evidence:
`experiments/rq1-source-safety/results/20261110-model-sweep/sweep.json`,
`state-machine/model/vtop_model.py`. Caveat: bounded explicit-state
check of an abstraction of the code, not a proof.

### H1.2 Source-progress safety (no gap below the committed marker)
**Claim:** For every source unit, every position below the committed
marker is covered by a verified object (I2), across crashes and
recovery.
**Prediction:** holds for v0.7.0 (read-order commits, failed-range
rewinds, recovery holdback), with one case to settle: a failed earlier
and a verified later batch of the same Kafka partition in one cycle
(OQ1). The v0.6.0 logic is expected to violate I2 (contrast model).
**Test:** E-RQ1-M (model, v0.7.0 vs v0.6.0 logic) and E-RQ1-F (fault
injection, oracle counts unsafe commits).
**Status (model part):** SUPPORTED for v0.7.0 with one batch per source
unit per cycle; REJECTED for v0.6.0. **Answer (model):**
- v0.7.0, B=1: I2 holds in all 8 bounded configurations, including
  crashes, ledger-tail loss, and storage mutation.
- v0.6.0: I2 is violated by a **single verification failure with no
  crash** (the failed range is not handed back, the next range commits
  past it; 32 states). Also violated in every configuration with a
  failure. Matches the v0.6.0 → v0.7.0 fix (VTOP #552).
- v0.7.0 with B=2 (two batches of one unit in one cycle): I2 violated by
  one failure. So v0.7.0's safety relies on at most one in-flight batch
  per source unit per cycle. In the code this holds because pending
  buffers are per Kafka topic-partition and per file path; early-sealed
  rotation windows belong to a different file generation (inode). This
  settles OQ1 for the model; E-RQ1-F checks it empirically.
- Two non-violations reported separately: **D1** (crash between adapter
  commit and ledger update, then storage damage while down: recovery
  detects the damage but cannot re-read, since rewinds are clamped to the
  committed point) and **D2** (Kafka offset moved backward by a stale
  recovery commit, then forward over a range whose object was damaged
  after its first commit). Both are storage damage after commit, not
  engine faults.
Evidence: `experiments/rq1-source-safety/results/20261110-model-sweep/`
(sweep.json, counterexample traces). **Fault-injection part
(E-RQ1-F): SUPPORTED.** On the v0.7.0 tag (`6c8f386`): 49 trials on
real Loghub data, 285 SIGKILLs at random times, 42 storage or broker
outages, and 80 power-loss emulations (WAL deleted after the kill). The
independent oracle found **0 unsafe commits** at every crash snapshot and
at the end, for file, syslog-spool, and Kafka sources, single- and
multi-partition. The first round on pre-tag `a8bce8e` (49 trials, 281
kills, 43 outages, 80 WAL deletions) gave the same safety result.
Evidence: `experiments/rq1-source-safety/results/20261110-v070-crash/`
and `.../20261110-v070-kafka/` (first round: `20261110-crash/`,
`20261110-kafka/`); table `analysis/tables/rq1_crash.tex`.
**Note:** a counterexample is a reportable finding, not a failure of the
study.

**v0.10.2 re-run (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** model variant v010 (ordered commit with hold-back) holds I2 at B = 1, 2, 3 up to 2,356,152 states; hold-back necessary at B = 2; one refused hand-back violates I2 (Assumption *Replayability* necessary). Runs: file/spool/power-loss/HDFS crash campaign 228 kills, 0 unsafe at every crash and final, 35/35 complete; Kafka 14/14 complete, 0 unsafe. The v0.7.0 condition "one in-flight batch per unit" is replaced by ordered commit with hold-back (T1). Single-rule ablations on v010 (`state-machine/results/20261121-v010-rule-ablation.md`): without the hand-back I2 fails whenever a failure is allowed (B = 1 and 2); without read-order commit (hold-back kept) no violation up to 657,353 states.
### H1.3 One rule, three source types
**Claim:** The same commit and recovery logic gives I2 for Kafka offsets,
file byte ranges, and syslog spool ranges, given each adapter's marker
semantics (rotation, truncation, partial lines, broker-side commits).
**Prediction:** zero unsafe commits per source type across the RQ1 fault
matrix; adapter-specific edge cases (file rotation, `copytruncate`,
Kafka rebalance) identified and either handled or listed as assumptions.
**Test:** E-RQ1-F, all three source types.
**Status:** SUPPORTED for safety in the tested cases; one liveness
failure (Kafka). **Answer:** Zero unsafe commits for files (Linux, SSH,
BGL, HDFS; up to three concurrent sources), syslog spools (Linux, SSH),
and Kafka (SSH on 1 partition, 2 M HDFS records on 4 partitions),
including broker outages that make offset commits fail. Every file and
spool trial and 13 of 14 Kafka trials ended with a complete, byte-exact
archive. **Liveness failure (F5):** in one 4-partition Kafka trial on the
tag (`20261110-v070-kafka/stall-hdfs-p4-t02`), the final engine process
read partition 0 contiguously to offset 447,960, committed it, and then
received no further records for that partition for the whole 900 s
drain, polling empty every 250 ms with no warning or error; partitions
1–3 completed. Nothing was committed past the archive (broker offset
447,961 = verified prefix), so the 80,181 remaining records stayed in
Kafka; restarting the engine on the same ledger and consumer group
archived and committed them within 2 s. The Kafka adapter is identical
in `a8bce8e` and the tag, and the first round had 0 of 14 such stalls, so
this is an intermittent fault in the running adapter, not a regression;
its root cause was not identified. Not tested: file rotation and
`copytruncate` during a run, Kafka rebalance with several consumers,
retention expiry before replay (assumption A4). These stay listed as
assumptions or future experiments.

**v0.10.2 re-run (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** files, spools and Kafka all 0 unsafe; the Kafka stall of v0.7.0 (80,181 records unarchived) did not recur: 14/14 complete; RQ4 VTOP side 25/25 complete (v0.7.0: 4 of 27 stalled).
### H1.4 At-least-once, with measurable duplicates
**Claim:** After any single crash, every committed record exists in at
least one verified object (I5); replay creates duplicate objects because
`batch_id` has a random suffix.
**Prediction:** zero lost records; duplicate objects > 0 for crashes
between upload and commit, = 0 for crashes before upload.
**Test:** E-RQ1-F (duplicate counter per fault point).
**Status:** SUPPORTED, with a nuance. **Answer:** No committed record
was missing from the archive in any of the 49 trials. (In the one stalled
Kafka trial, 80,181 records were not yet archived but also not committed;
see H1.3.) Under random SIGKILL, duplicates were rare: two spool-mix
trials produced 1 and 2 duplicate objects; files, HDFS, and Kafka none
(first round: one file-mix trial with 2, two HDFS trials with 1), because
a duplicate requires the kill to fall in the few-millisecond window
between upload and commit. Under power-loss emulation the ledger rolls
back to its last checkpoint and duplicates dominate: median 108 duplicate
objects per trial (95% CI 94–119), replay amplification median 109% of
the source bytes (range 50–129%; first round: median 119%). At-least-once is confirmed; its cost depends on the failure type,
not only on crash frequency.

---

**v0.10.2 re-run (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** 0 duplicate objects in every local scenario incl. power loss (v0.7.0: median 108) and in every cloud run; duplicates remain possible in the model (stored object damaged before replay; Kafka newest-row resume). The power-loss drop combines two changes no run separates: the emulation (whole-WAL deletion → truncation after the last fsynced commit) and the engine (fsynced VERIFIED/SOURCE_COMMITTED, stable object keys). Still claim at-least-once, never exactly-once.
## RQ2: Does manifest-bound verification improve the detection and handling of corrupted or replaced archival objects during recovery?

### H2.1 Strong verification detects stored-object corruption before commit
**Claim:** With strong verification, a corrupted, truncated, or replaced
object body is detected before the source commit, so the batch replays
instead of committing.
**Prediction:** 100% detection for each injected mutation class under
strong verification; backend-limited (size-only) misses same-size
mutations.
**Test:** E-RQ2-C (corruption between upload and verify).
**Status:** PARTIAL. **Answer:** E-RQ2-C (mutation in the
milliseconds between upload and verification) was not run; it needs a
storage-side hook. The same digest comparison was exercised at restart
(E-RQ2-R): strong verification detected a same-size byte flip (M1),
truncation (M2), deletion (M3), and replacement (M4) in 8/8 trials each
across SSH and Linux; size-only verification missed M1 in 4/4 trials and
**committed the corrupted batch** (oracle: archive no longer byte-exact),
while catching M2–M4 through size. Evidence:
`experiments/rq2-corruption-recovery/results/20261110-v070-tamper/`
(tag); the first round on `a8bce8e` (`20261110-tamper/`) gave the same
outcome in every cell.

### H2.2 Restart revalidation catches post-verification tampering
**Claim:** If an object or manifest is deleted or modified while the
engine is down with the batch `VERIFIED`, recovery re-check refuses the
commit and replays (I4).
**Prediction:** detection for delete, body change, manifest change;
ablation without revalidation commits stale evidence.
**Test:** E-RQ2-R (mutation during downtime) plus ablation V-norecheck.
**Status:** SUPPORTED, including the causal ablation. **Answer:** On the tag,
and identically on `a8bce8e`, in 108 trials
(2 datasets × 3 verification variants × 9 mutations × 2 reps), every
object or manifest mutation M1–M8 under strong verification was refused
at restart (64/64 with and without MAC), the row went to
`REPLAY_REQUIRED`, the range was re-read and re-archived, and the final
archive was complete and byte-exact. The unmutated control (M0) was
committed without replay in 12/12 trials. VTOP's own log gives the
reason per class (content mismatch, content verification failed,
manifest authentication or ledger binding failed, manifest download
failed). In the separate V-norecheck build (`results/20261111-v070-norecheck`),
all 32 mutations (plus 4 unmutated controls) were accepted, with 24 unsafe commits across
M1–M4, M7, and M8. Thus the counterfactual is now measured: disabling
restart revalidation commits stale or corrupted evidence.

**v0.10.2 re-run (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** identical outcome: strong and strong+MAC detected 64/64 mutations; size-only accepted the 4 same-size bit flips; the V-norecheck ablation (env-gated patch on the v0.10.2 tag) accepted 32/32 with 24 unsafe commits.
### H2.3 Manifest authentication closes the rewrite gap
**Claim:** Self-hash alone does not detect a consistent rewrite of
manifest + object by an actor with write access; the keyed MAC does;
version pinning additionally rejects rollback to an older valid
manifest.
**Prediction:** self-hash-only accepts consistent rewrite; MAC rejects;
rollback is rejected only with pinned versions.
**Test:** E-RQ2-R (adversarial rewrite cases).
**Status:** REJECTED as stated (in VTOP's design). **Answer:** A
consistent rewrite of object + manifest with a recomputed self-hash (M7)
and a manifest edit with a recomputed self-hash (M6) were **detected
without the MAC** (4/4 each, strong and even size-only), because the
ledger stores the object and manifest digests and recovery checks the
stored manifest against them ("ledger binding"). The local ledger, not
the self-hash, is the trust anchor against an actor who can write only
to object storage. The MAC's separate contribution could not be isolated:
VTOP reports one combined reason, and the MAC matters only when the
ledger is unavailable or also writable by the attacker (not modeled).
Rollback to an older valid manifest version was not tested (the LocalFS
backend has no object versions).

### H2.4 Comparison with response-based acknowledgment
**Claim:** Baselines that commit on the upload response (no read-back)
commit offsets for objects corrupted after upload.
**Prediction:** baselines commit; VTOP strong does not.
**Test:** E-RQ4-B, acknowledged corruption and acknowledged object loss on
Confluent S3 Sink 12.1.15 and VTOP.
**Status:** SUPPORTED for the tested Confluent configuration. **Answer:** In all
20 storage-fault trials (10 corruption, 10 deletion), the Confluent connector
committed past the affected object, leaving 40,000 committed records per fault
class without an intact copy. VTOP had no unsafe commits and completed 9/10
trials in each class after replaying. Vector, Fluent Bit, and Flink were not
run, and elapsed times are excluded from performance claims.

---

**v0.10.2 VTOP side (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** 25/25 complete, 0 unsafe (corrupt 10, lost 10, kill 5). Baselines not re-run: Confluent 12.1.15, Vector 0.61.0 and Fluent Bit 5.2.0 (C23, `rq4-baselines/results/20261111-rq4-{vector,fluentbit}`) each committed past the damaged object in 20/20 storage-fault trials (40,000 committed records without an intact copy per trial); their trials ran against a MinIO image of the same release but a different build digest — disclose.
### H2.5 Signed manifests make forgery evident without the ledger (v0.10.2)
**Claim:** With `manifest.mac` on (write version "0.4", key ring), an actor who can write to the bucket but
does not hold the key cannot forge an object + manifest pair, or substitute another batch's manifest, without
`vtopctl scrub` or recovery reporting it **even when the ledger is not the trust anchor** (ledger lost and
rebuilt from storage, or a storage-only audit); with signing off, the same forgery passes such an audit.
Motivated by H2.3: with the ledger present, ledger binding already caught consistent rewrites at v0.7.0, so
the MAC's own contribution must be tested where the ledger cannot help.
**Prediction:** MAC on: every forgery and manifest swap flagged by scrub/audit and refused by recovery;
MAC off: storage-only audit accepts the consistent forgery (the honest limit); ledger present: caught either way.
**Test:** E-RQ5-TAMPER (real S3, v0.10.2), reusing `rq2-corruption-recovery/code/exp_tamper.py` (M6, M7, M8,
manifest swap) and the env-gated no-recheck build. **No-ledger arm:** `vtopctl verify-manifest --manifest <s3 uri>
--config <cfg> --json` (commands.rs `Command::VerifyManifest`, `verify_manifest_deep`) runs standalone with no
ledger row: self-hash, then the MAC through the configured key ring (unsigned or pre-key manifests rejected when a
key is configured), then the object against the manifest's digest. Scenario: ledger lost/restored from an old
backup, or an auditor with only config + key, or an insider who can rewrite bucket and ledger but lacks the key.
Expected: key absent → consistent forgery passes; key configured → fails authentication. `scrub` and
`audit-coverage` need ledger rows, so they are the ledger-present arm (engine maintainer, 2026-11-20). **Status:** OPEN (proposed 2026-11-20 by the engine
maintainer's assessment; author asked to add it). **Engine evidence (REPORTED, MinIO only; PR #859, v0.10.3,
`live_s3_tamper.rs`, `docs/reference/tamper-evidence.md`):** MAC on → a forged object + recomputed manifest is
`unauthenticated` with or without a rewritten ledger, a stripped MAC is refused, recovery replays; MAC off → caught
against a bucket-only attacker (ledger digest), **NOT CAUGHT** against a bucket + ledger attacker; a swapped valid
manifest of another batch is caught either way; a foreign key id and a retired key are refused; a leaked key before
retirement with the ledger rewritten is **NOT CAUGHT**. So the MAC's contribution is exactly the bucket + ledger and
no-ledger (`verify-manifest`) cases — what our test must reproduce on real S3.

### H2.6 Object Lock and versioning resist or expose overwrite and rollback (v0.10.2)
**Claim:** With `upload.object_lock` (bucket with Object Lock, retention on objects and manifests) an
overwrite or delete of an archived object or manifest is refused by the store; with
`upload.require_object_versioning` an overwrite creates a new version that does not silently replace the
archived one, and a rollback to an older valid manifest version is detected (H2.3's untested rollback case,
testable now that the store has versions).
**Prediction:** locked: PUT/DELETE refused by S3 within retention; versioned: the ledger records the manifest's
version id (`manifest_version_id`) and scrub/recovery read that **pinned** version (`get_manifest_pinned`,
scrub.rs ≈285-300), so an overwrite or rollback of the current version does not change what is verified, a replaced
object fails against the pinned manifest's digest, and a deleted pinned version fails closed (#135). UNSURE (engine
maintainer): whether scrub also *reports* that the current version differs from the pinned one — record whether a
rollback is "flagged" or only "not followed"; the paper must say which. **Engine evidence (REPORTED, MinIO,
governance mode; PR #859):** scrub **reports** a versioned overwrite of manifest and object ("the current key no
longer holds version V", `mismatched`) and keeps the originals; with versioning off the originals are gone. Object
Lock refuses deleting a locked version or shortening retention, but **accepts** an overwrite (new version) and a plain
DELETE (delete marker) — the locked version keeps its bytes; scrub reports `mismatched`/`missing`;
`scrub --check-retention` reports a bypass-cleared retention. Versioning suspended: a new engine refuses the bucket.
Not exercised: compliance mode, real AWS.
**Test:** E-RQ5-TAMPER on a separate bucket with Object Lock (GOVERNANCE mode, short retention, so the study
can clean up) and versioning. Needs new S3 permissions for the operator and the instance role (author approval).
**Status:** OPEN.

**Paper tamper tests (local MinIO, v0.10.2, 3 identical runs; `rq5-cloud/tamper/`):** standalone `verify-manifest` accepts a swapped genuine manifest (scrub and recovery catch it via the ledger); versioning suspended under a running engine commits nothing but keeps retrying writes. Both accepted as engine bugs (fixes for v0.10.3/v0.10.4); at v0.10.2 state them as observed limits. AWS run done (see below).

### H2.7 The audit trail is tamper-evident
**Claim:** Editing or deleting a record of the append-only, hash-chained audit trail breaks the chain, and
verification reports where.
**Prediction:** every edit/delete/reorder of a record is reported; an untouched trail verifies.
**Test:** E-RQ5-TAMPER (local file mutation of the audit table/log, then the engine's audit verification).
**Status:** OPEN. **Engine evidence (REPORTED; PR #859, `tamper_audit_trail.rs`):** UPDATE/DELETE refused by triggers;
with triggers dropped, an edit, mid-chain delete or swap → `audit verify` BROKEN; **NOT CAUGHT:** a truncated tail
(only against a kept anchor), an unkeyed chain recomputed after an edit, a keyed chain with a leaked key; a committed
ledger row deleted from the end. Receipts with MAC off: edits to `verified_by`/`method` NOT CAUGHT.

**E-RQ5-TAMPER on Amazon S3 (2026-11-21, v0.10.2, run `research/experiments/rq5-cloud/tamper/results/20261121T143301Z-i-0938589e4d715ff8d`; 55 cases, our own tamper tool, re-implementation of manifest/receipt hash+MAC and audit links checked against the engine's):** every case's verdict matches the three local runs (53/53 comparable; the 2 suspended-versioning cases need an unlocked versioned bucket and stay local-only). **Supported with measured limits:** with `manifest.mac` on, a forged object + manifest is caught by scrub, standalone `verify-manifest` and recovery, also when the ledger is rewritten; with the MAC off it is caught only while the ledger is intact; a leaked signing key, receipt fields the ledger does not hold (MAC off), a truncated audit tail, an unkeyed chain recomputed after an edit and the last ledger row deleted are NOT CAUGHT; standalone `verify-manifest` accepts a swapped genuine manifest (scrub/recovery catch it). Versioning: overwrite and rollback are reported by scrub and not followed by recovery. Object Lock (governance): deleting a locked version or shortening retention is refused by S3; an overwrite or plain delete creates a new version/marker that scrub reports. Audit trail: edits, deletes and swaps break the chain. Results: `research/experiments/rq5-cloud/tamper/results/20261121T143301Z-i-0938589e4d715ff8d/ANALYSIS.md`.

### H2.8 Encryption at rest keeps verification intact (engine feature in progress)
**Claim:** With S3 server-side encryption (SSE-S3, and SSE-KMS with a key id) configured through the new
`upload` key, S3 reports every object and manifest as encrypted, and store-SHA-256 and BLAKE3 read-back
verification still pass, at a small cost.
**Prediction:** HeadObject shows `ServerSideEncryption` (AES256 / aws:kms with the key id) on every object;
the oracle passes; throughput difference against no encryption within the H3.5 noise.
**Test:** E-RQ5-TAMPER (or a small E-RQ5-COST cell) on **v0.10.3** (the feature and the engine's own tamper
suite target v0.10.3, not v0.10.2). Config (engine PR #856, a **block**, not a scalar):
`upload.server_side_encryption: {mode: none|sse-s3|sse-kms, kms_key_id: <id or ARN, optional>, bucket_key: <bool,
sse-kms only>}`; a bare `server_side_encryption: sse-s3` is a config error. Every object/manifest/receipt PUT and
CreateMultipartUpload sends `x-amz-server-side-encryption`; `sse-kms` without a key id uses `aws/s3` (no monthly key
charge). HeadObject verification fails (batch replayed, never committed) if S3 reports no encryption, another
algorithm, or another key (id/ARN; an alias only checks the algorithm, with a startup WARN); gauge
`vtop_server_side_encryption_mode`, failures in `vtop_verification_failures_total`; refused off `s3_native`;
pre-SSE objects report as scrub mismatches. Engine tests passed on MinIO+KMS only; **the paper's us-east-1 run would
be its earliest test on real AWS** (SSE-KMS with aws/s3, single-PUT and multipart). Out of scope: SSE-C, DSSE-KMS,
client-side encryption. Note: SSE-KMS with a customer-managed key has a monthly key charge;
check price before creating one. **Status:** OPEN (feature not released).

## RQ3: What are the latency, throughput, storage, and CPU costs of verification-bound commits?

### H3.1 Verification cost depends on method, not just on enabling it
**Claim:** Commit latency overhead of strong verification is dominated
by read-back bandwidth for client-side digests and is lower with
service-computed checksums.
**Prediction:** overhead(read-back BLAKE3/SHA-256) > overhead(service
SHA-256) > overhead(size-only); gap grows with object size.
**Test:** E-RQ3-A, variants V-none, V-size, V-svc, V-readback.
**Status:** NOT RESOLVED for the read-back-versus-service method comparison;
SUPPORTED for the stored-object-size effect. **Answer (tag):** With LocalFS
read-back verification, checksum + verify take 0.1–1.4 ms per 16 MiB batch
when objects are zstd-compressed (compression itself: 13.5–24.6 ms), and
12.8–15.1 ms without compression. Stage costs are stable across both rounds;
compressed end-to-end effects vary with the host. The clean S3-over-local-
Docker dataset (90 unexposed originals + 60 quiet replacements, 10 reps per
cell) has no resolved throughput penalty for service-computed SHA-256 or for
BLAKE3 read-back against size-only on any dataset: every CI includes zero.
The earlier overlap-affected BGL/HDFS BLAKE3 claims are excluded. Without
compression, throughput drops 62.86%, 65.81%, and 66.72% on SSH, BGL, and
HDFS, with every CI below zero. The predicted method/bandwidth ordering is
therefore not established on this setup, while the compression effect is
clear. HTTP request counts and public-cloud S3 costs remain unmeasured; the
unsafe no-verification ablation (V-none) was not run.

**v0.10.2 re-run (2026-11-21) (local campaign `experiments/run_v0102.sh`; numbers recomputed in `analysis/v0102/summary.json`, `NUMBERS.md`, 183 checks):** local (macOS, localfs) cost of compressed verified objects now resolved on all three datasets at 4.3–6.6% (SSH with MAC unresolved); uncompressed 13.7 / 7.5 / 11.3% (SSH / BGL / HDFS). Not like-for-like with v0.7.0: v0.7.0 run-mode batches were ~32 MiB (X1), v0.10.2 batches ≤ 16 MiB. Real-S3 method comparison: H3.5, H3.6.
### H3.2 Manifest authentication is cheap
**Claim:** Keyed BLAKE3 MAC adds negligible CPU and latency relative to
object hashing and upload.
**Prediction:** < a small fraction of per-batch time (threshold to be
fixed in the E-RQ3-A spec before running).
**Test:** E-RQ3-A, V-mac vs V-svc.
**Status:** SUPPORTED (no threshold was pre-registered). **Answer:** The
keyed BLAKE3 manifest MAC changed per-batch stage time by under 1 ms in
both rounds. Median throughput relative to SHA-256 without MAC: +1.1%
(SSH), +1.6% (BGL), +10.1% (HDFS) on the tag; +1.6%, −2.9%, +0.1% in the
first round; all inside the run-to-run spread. CPU per GiB with and
without the MAC differs by at most 0.17 s (tag: 2.93–3.16 s vs
3.05–3.21 s).

### H3.3 Recovery cost scales with verified-but-uncommitted backlog
**Claim:** Restart time grows with the number of `VERIFIED` rows that
must be re-checked against storage.
**Prediction:** roughly linear in re-checked bytes for read-back
verification.
**Test:** E-RQ3-R (crash with controlled backlog sizes).
**Status:** OPEN (not run). Related observation from E-RQ1-F (tag):
median restart-to-first-commit 0.05 s (files), 0.16 s (HDFS, 16 MiB
batches), 1.04 s (spool).

### H3.4 Storage overhead of manifests and duplicates
**Claim:** Manifest objects add a fixed per-batch cost; replay
duplicates add cost proportional to crash frequency.
**Prediction:** manifest bytes/object bytes falls with batch size.
**Test:** E-RQ3-A (no faults) and E-RQ1-F (duplicates).
**Status:** SUPPORTED. **Answer:** Manifest bytes relative to stored
object bytes (BGL, tag): 2.41% at 1 MiB batches, 0.59% at 4 MiB, 0.149%
at 16 MiB, 0.040% at 64 MiB — inversely proportional to batch size.
Throughput rises with batch size (BGL median 163 → 298 → 340 → 344 MiB/s
for 1/4/16/64 MiB; HDFS 158 → 293 → 334 → 348; first round BGL 210 → 323
→ 362 → 377). Duplicate storage depends on the failure type: ~0 under
random kills, 50–129% of the source under power-loss rollback (RQ1).

---

### H3.5 Verification cost on a public cloud (real S3, separate hosts)
**Claim:** On Amazon S3 with the engine on a separate EC2 host, the
method ordering of H3.1 holds: the store-computed SHA-256 adds no
measurable throughput cost against size-only, whereas BLAKE3 read-back,
which downloads every object again, does.
**Prediction:** CI on the difference of medians includes zero for store
SHA-256 vs size-only, and lies below zero for BLAKE3 read-back, on each
dataset; every run drains and passes the independent oracle.
**Test:** E-RQ5-SMOKE (feasibility, one run per mode), then E-RQ5-COST
(AWS us-east-1, EC2 + S3; experiments/rq5-cloud/).
**Status:** PARTLY SUPPORTED — ordering holds, "no measurable cost" does not hold for the largest dataset.
**HDFS re-check (D9, t4g.large = twice the baseline network allowance, run `20261121T012945Z-i-072d88074e58c7845`,
30/30 passing):** size-only 49.21 MiB/s [48.62, 49.64]; store SHA-256 46.71 [45.84, 47.80], **−5.07% [−7.06, −2.62]**;
BLAKE3 read-back 38.82 [38.50, 39.57], **−21.11% [−22.19, −19.23]**. Network-allowance events now balanced between
size-only and SHA-256 (medians 75.5 / 74.5; read-back 92), so the remaining SHA-256 cost is not the shaping confound;
per-batch `total_ms` 233 → 252 ms (+18 ms: the service-checksum HeadObject per batch). The t4g.medium −11.9% included
unbalanced shaping and noise. **Revised claim for the paper:** on S3, the store-computed SHA-256 costs at most about 5%
(resolved only on the 1.5 GB dataset; unresolved on SSH and BGL), and BLAKE3 read-back costs 14–21% on every dataset —
three to four times more. First run on t4g.medium follows. **Answer (E-RQ5-COST, v0.10.2,
2026-11-21, run `20261121T002644Z-i-07a97d2f7a612888e`, t4g.medium us-east-1a, 90 runs, 90 passing: drained,
oracle byte-exact, log/receipt/ledger/HEAD agree, all single-PUT, every batch ≤ 16 MiB):** BLAKE3 read-back is
resolved slower on every dataset: SSH −14.2% [−20.0, −5.5], BGL −18.7% [−31.0, −11.4], HDFS −19.8% [−32.5, −16.4]
(steady MiB/s, difference of medians, bootstrap 95% CI). Store-computed SHA-256 has no resolved cost on SSH
(+4.9% [−3.7, 13.7]) and BGL (−4.5% [−13.1, 6.5]), but **a resolved cost on HDFS: −11.9% [−23.2, −5.3]** — the
prediction fails for HDFS as stated. Confound under check: the instance's outbound network allowance was exceeded
on BGL and HDFS (t4g.medium baseline 0.256 Gbps; runs at ~45 MiB/s exceed it), with more events in the verified modes
(HDFS medians 52 / 75 / 82) and a moderate negative correlation with throughput (−0.32 on HDFS); no time drift
(order–throughput correlation −0.04), no disk effect (writes ≈ 1% of input, iowait < 1%). Per-batch `total_ms` on
HDFS: 245 / 292 / 325 ms. Absolute rates (≈ 36–47 MiB/s) are bounded by the instance's network allowance and are not
an engine maximum. (Motivated by the gap that all earlier runs were on a single laptop.)
Feasibility (not an answer): E-RQ5-SMOKE passed on 2026-11-20 — on Amazon S3 v0.10.1 verified
single-request objects from S3's own SHA-256 and read back multipart objects; cost not yet measured.

### H3.6 Verification over real long-distance and constrained links (E-RQ5-WAN)
**Claim:** With the engine in a far region writing to an S3 bucket in us-east-1, source-progress safety holds
under bandwidth limits, packet loss, jitter, a network partition and SIGKILL; the per-method cost ordering of H3.5
holds, and a cost that needs an extra round trip per batch grows with distance.
**Prediction:** every run complete, byte-exact, 0 unsafe; store SHA-256 ≤ read-back cost in every region;
read-back cost increases with RTT.
**Test:** E-RQ5-WAN (D8, D9), v0.10.2, one t4g.medium per region, SSH, 68 runs per region.
**Status:** SUPPORTED. **Answer (2026-11-21; results `experiments/rq5-cloud/wan/results/`, combined analysis
`combined/ANALYSIS.md`):** **272/272 runs pass, 0 unsafe commits.** Measured RTT to the bucket: eu-west-1 66.5 ms,
sa-east-1 110.8 ms, ap-northeast-1 147.9 ms, ap-southeast-2 196.1 ms. Native SSH throughput (size-only) 11.57 /
9.57 / 7.99 / 6.66 MiB/s (−42% to −66% against us-east-1). Store SHA-256 vs size-only: −1.0% [−4.8, 1.4] (Ireland,
unresolved), −13.9% [−15.4, −11.3] (São Paulo), −6.3% [−8.1, −3.5] (Tokyo), −7.2% [−15.5, −1.3] (Sydney); BLAKE3
read-back −23.8% / −30.3% / −32.5% / −35.3%, growing with RTT, and larger than store SHA-256 in every region.
Low bandwidth: with kernel shaping at 2 Mbit/s uploads reach 0.67–0.82 of the cap; VTOP's own egress cap gives the
same upload rate, but read-back keeps its download speed (the cap limits uploads only) and drains 1.7–1.8× faster
than under two-way shaping. Loss: 5% loss slows the drain 9.3–12.3×, with no failed batch. Partition (60 s, all
egress): 0 commits during the cut, first commit 2.0–3.5 s after restore, complete, 0 unsafe in all four regions.
SIGKILL mid-upload: restart drained in 6.3–15.5 s with 0 duplicate bytes and 0 unreferenced objects. Policed
1 Mbit/s uplink (#663 regression check): the shipped per-request deadline completed in every region; only in Sydney
did a batch need > 300 s from seal to commit (308 s), so **Sydney is the one valid #663 check** (Tokyo 299.5 s just
below); the fixed-120 s control committed nothing in 480 s everywhere. Limits: one VM and one time window per region;
loss and bandwidth are emulated on the VM, the distance is real; faults ran once per region.

### H3.7 Horizontal scale-out on a public cloud (E-RQ5-SCALE)
**Claim:** Independent VTOP engines (one ledger and one source set each) writing to one S3 bucket scale linearly: for
N = 2, 4, 8 engines on separate EC2 hosts, aggregate throughput is N times one engine's, without S3 throttling, and
every engine keeps source-progress safety.
**Prediction:** efficiency CI lower bound ≥ 0.90 at N = 2, 4, 8; 0 throttling; every engine-run passes the oracle with
0 unsafe commits.
**Test:** E-RQ5-SCALE (D13), v0.10.2, 8 × t4g.large in us-east-1a, HDFS (1.58 GB) per engine, store SHA-256, 16 MiB
batches, 10 steps per N, randomized, S3 barrier start.
**Status:** SUPPORTED. **Answer (2026-11-21, run `research/experiments/rq5-cloud/scale/results/20261121T153823Z-scale`):** 46/46 steps, **198/198 engine-runs pass, 0 unsafe**, 0 S3
retries, 0 throttle verdicts. Aggregate 46.61 [45.95, 47.02] (N = 1), 92.96 [91.59, 93.76] (2), 188.14 [185.46,
190.12] (4), **373.09 [368.91, 377.68] MiB/s (8)**; efficiency 0.997 [0.980, 1.013], 1.009 [0.990, 1.029],
**1.001 [0.985, 1.021]**; per-engine change vs N = 1 within ±1.3%. Peak 157 S3 request attempts/s at N = 8 (far below
S3's per-prefix limits, so this shows scale-out at a moderate fleet, not S3's ceiling). Store SHA-256 vs size-only at
N = 8: −5.38% [−6.69, −4.10] aggregate (consistent with H3.5's −5.07%). Limits: one region and AZ, one time window,
t4g burstable instances; horizontal scale-out of independent engines, not a shared-ledger cluster.

## RQ4: How does VTOP compare with existing telemetry-to-object-storage solutions under equivalent workloads and failures?

### H4.1 Safety under the shared fault matrix
**Claim:** Under identical crash points, VTOP commits no unverified
range; baselines differ by mechanism (Confluent S3 Sink exactly-once
config, Vector end-to-end acks, Fluent Bit buffering, Flink checkpoints).
**Prediction:** per-system table of lost / duplicated / corrupted-accepted
outcomes per fault point; no single system dominates on every column.
**Test:** E-RQ4-B.
**Status:** ANSWERED for one baseline (2026-11-11). **Answer:** Under acknowledged-but-corrupted and acknowledged-but-lost writes, the Confluent S3 Sink (exactly-once config) committed past the damaged object in 20/20 trials (40,000 committed records without an intact copy each); VTOP in 0/20. Under SIGKILL neither lost data; Confluent wrote no duplicates, VTOP a median of 2 objects; VTOP also stalled on Kafka in 4/27 trials (liveness). No system dominates every column, as predicted. Later (C23) Vector 0.61.0 and Fluent Bit 5.2.0 ran the same matrix: each committed past the damaged object in 20/20 storage-fault trials. Flink not run. v0.10.2 VTOP side: 25/25 complete, 0 unsafe, no stall (see H2.4).

### H4.2 Performance trade-off
*(Gap in the earlier evaluation: the paper had safety comparisons with the three sinks but no throughput/latency/cost comparison. Until E-RQ5-PERF is done, say "no performance comparison with prior work yet".)*
**Claim:** VTOP's verification gating costs throughput or latency
relative to response-based baselines on equal hardware and batch sizes.
**Prediction:** VTOP slower than at least one baseline in steady state;
the cost is the price of H2.x detection.
**Test:** E-RQ4-B (no-fault runs); E-RQ5-PERF (same workload, VTOP vs the three sinks on EC2 + S3, experiments/rq5-cloud/).
**Status:** SUPPORTED, with a measured cause. **Answer (E-RQ5-PERF, 2026-11-21, v0.10.2, 2 × t4g.large per leg, HDFS on Kafka, 16 MiB objects; us-east-1 105/105 runs pass, Sydney 38/38; results `experiments/rq5-cloud/perf/results/*/results/analysis/ANALYSIS.md`):**
- **Throughput (done = archived intact and committed, MiB/s).** us-east-1 (RTT 0.6 ms): VTOP store-SHA-256 19.61 [19.30, 20.51], size-only 19.38, read-back 18.89 (verification differences unresolved), gzip-6 14.86; Confluent 37.48 [34.17, 40.53] (1.9×), Vector 9.61, Fluent Bit 4.83. Sydney → us-east-1 bucket (RTT 196 ms): VTOP 6.05 [6.03, 6.16] (size-only 6.98, read-back 5.00); Confluent 24.89 (4.1×), Vector 9.24 (1.5×), Fluent Bit 4.83.
- **CPU per GiB:** VTOP 22.0 (us-east-1) / 21.5 (Sydney) s — lowest; Confluent 54.2 / 57.8, Vector 202.1 / 207.8, Fluent Bit 26.5 / 26.2. Peak memory VTOP ≈ 230 MiB, Confluent ≈ 1.2 GiB.
- **Latency p50 (produce → archived and committed):** us-east-1 VTOP 2.75 s, Confluent 2.49, Fluent Bit 3.15, Vector 5.15; Sydney VTOP 4.72, Confluent 4.17, Fluent Bit 4.46, Vector 6.14.
- **Recovery after SIGKILL (first commit):** VTOP 2.75 s (us-east-1) / 3.87 s (Sydney), 0 duplicates; Confluent ≈ 50 s, Fluent Bit ≈ 45 s, both 0 duplicates; Vector 10.5–15.6 s with 120,000–240,000 duplicate records per kill.
- **Cause of the throughput gap (measured part):** VTOP without verification (size-only) is within 1.1% (us-east-1) and 15% (Sydney) of store-SHA-256, while Confluent is 1.9×/3.6× faster than size-only, so most of the gap is not verification. Engine diagnosis (maintainer, not measured here): `run` works in stop-and-wait cycles (read, upload/verify, wait, commit), serialising round trips and idling connections (TCP slow start after idle); pipelining is planned. VTOP used 0.13–0.43 cores.
- **Storage faults on real S3 (D13; native overwrite/delete after the sink's acknowledgement, no proxy):** Confluent, Vector and Fluent Bit committed past the damaged object in every RQ4-comparable trial (≈ 40,000–42,000 committed records lost per trial). For VTOP the native injector raced its verification: damage landed after VTOP's logged verification in 9/10 trials and within the verification round trip (42 ms, two host clocks) in 1/10; VTOP's `scrub` flagged the damage in 10/10. Post-verification damage is outside VTOP's guarantee; the controlled pre-verification case remains RQ4's proxy experiment (0 unsafe). No S3 server-side access log, so the 1/10 order cannot be proven either way.

### H4.3 Heterogeneity
**Claim:** VTOP applies one commit rule to Kafka, files, and spools,
whereas Kafka-only sinks (Confluent) cannot ingest files/spools, and
log shippers (Vector, Fluent Bit) use per-source mechanisms.
**Prediction:** capability matrix (qualitative) plus per-source fault
outcomes.
**Test:** E-RQ4-B and documentation review (versioned docs only).
**Status:** PARTLY ANSWERED. **Answer:** Confluent's S3 Sink is a Kafka Connect sink and takes only Kafka; VTOP applied one commit rule to Kafka (E-RQ4-B), files and spools (E-RQ1-F). Vector and Fluent Bit (log shippers) were run with Kafka input in the RQ4 matrix (C23); per-source (file/spool) fault outcomes for them were not measured.

## Re-pin to vtop-engine v0.10.4 (decision D16, 2026-11-22): answers re-checked

Every local and cloud campaign was re-run on v0.10.4 (be5b419) with the same scripts, seeds and plans. Sources:
`analysis/v0104/summary.json` (`analyze.py --set v0104`), checked by `verify_v0104_data.py` (345 checks);
v0.10.2 → v0.10.4 per number: `analysis/v0104/NUMBERS.md`; every changed leaf: `analysis/v0104/DIFF-v0102.md`.
Answers above that name v0.10.2 stay as the record of that build; these are the v0.10.4 answers.

- **H1.1/H1.2 (model):** unchanged results. v0.10.4 rewinds each unit once to its lowest failed start (#891), which is
  the model's default rule; v0.7.0–v0.10.3 rewound to the highest (the `--rewind-last-wins` violation from 249
  states). `state-machine/v0104-repin-notes.md`.
- **H1.2/H1.3 (crash, outage, power loss, Kafka):** SUPPORTED. 49 trials, 323 kills, 47 outages, 0 unsafe, 49/49
  complete, 0 duplicate objects, 0 of 5,825 manifests understated; power loss 80 truncations (47 dropped commits, 408
  in all; 1 empty WAL). Kafka SSH trials drained within 2–3 lives (median final drain 0.0 s, HDFS 6.4–13.5 s).
  Kafka resumes from the highest committed end of the topic id since v0.10.3 (#855); newest row only without an id.
- **H2.1/H2.2 (RQ2):** unchanged: 108 trials, 64/64 refused, size-only M1 0/4; ablation 32/32 accepted, 24 unsafe.
- **H2.5 (signed manifests):** SUPPORTED, one limit removed: the M4 manifest swap is now also caught by the
  stand-alone verify-manifest (#882, v0.10.3: a manifest must name the key it was read from). Without a MAC key a
  forged manifest naming the new key still passes it. 15 measured limit cases (v0.10.2: 17). AWS = local 53/53 ×3.
- **H2.6 (versioning, Object Lock):** SUPPORTED; local-only VER-SUSPEND-RUNNING: nothing committed, no manifest, 1
  object (the batch in flight; at most the upload width), v0.10.2: 10 objects (#883).
- **H3.1/H3.2 (local cost):** compressed 6.9–11.1%, all resolved (v0.10.2: 4.3–6.6%, SSH+MAC unresolved);
  uncompressed 18.2–23.5%; MAC vs SHA-256 unresolved everywhere. Local absolute throughput fell ~40% because the
  localfs backend fsyncs file and directory since v0.10.3 (#827): object/manifest upload 10–20 / 7–11 ms per batch
  (0.0 before); interleaved A/B 0.61–0.81 (`analysis/v0104/RQ3-AB.md`). Not comparable with v0.10.2 numbers.
- **H3.5 (S3 cost):** read-back −16.3/−19.3/−17.8% (SSH/BGL/HDFS), all resolved; store SHA-256 −3.5/−3.2/−3.3%,
  resolved on BGL and HDFS (v0.10.2: HDFS only, −11.9%); t4g.large HDFS −3.9% / −18.9%.
- **H3.6 (long links):** store SHA-256 −2.9 to −10.0% (unresolved only in Ireland); read-back −23.0 to −36.4%, larger
  on the three longer links than at 66 ms and largest at 196 ms, but no longer monotonic in RTT (São Paulo −33.9 vs
  Tokyo −31.8; CIs overlap): "grows with RTT" is withdrawn. 5% loss 7.9–12.6× slower, one failed batch (Tokyo,
  replayed, run passed); partition first commit 2.0–4.7 s; SIGKILL drain 6.4–16.2 s, 0 duplicates; 272/272 pass.
- **H3.7 (scale-out):** 45.3 → 369.3 MiB/s at N = 8, efficiency 1.02 [0.988, 1.045], per engine within 2.4%, 0
  retries/throttling, peak 154 attempts/s; 198/198 pass.
- **H4.2 (performance):** SUPPORTED. VTOP store SHA-256 23.1 / 7.1 MiB/s (us-east-1 / Sydney); Confluent 1.64× /
  3.65×; Vector 0.41× / 1.32×; size-only +6.4% (unresolved) / +22.7% vs store; CPU 24.0 s/GiB, lowest of the four
  (VTOP size-only 23.9 from Sydney); live p50 2.70 / 4.70 s (Confluent 2.68 / 4.18); first commit after SIGKILL
  1.7 / 5.6 s, 0 duplicates. Faults: sinks 30/30 unsafe (26/26 RQ4-comparable); VTOP damage after verification
  8/10, before it 2/10 (6.3, 18.1 ms); scrub flagged 10/10. One Vector drain failed only on the oracle's tie-break
  for identical records and was re-judged complete (`rq5-cloud/perf/LESSONS.md`).
