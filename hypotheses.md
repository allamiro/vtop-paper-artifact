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
**Status:** SUPPORTED (without the ablation). **Answer:** On the tag,
and identically on `a8bce8e`, in 108 trials
(2 datasets × 3 verification variants × 9 mutations × 2 reps), every
object or manifest mutation M1–M8 under strong verification was refused
at restart (64/64 with and without MAC), the row went to
`REPLAY_REQUIRED`, the range was re-read and re-archived, and the final
archive was complete and byte-exact. The unmutated control (M0) was
committed without replay in 12/12 trials. VTOP's own log gives the
reason per class (content mismatch, content verification failed,
manifest authentication or ledger binding failed, manifest download
failed). The V-norecheck ablation (a patched build that skips the
re-check) was not built, so the counterfactual "without revalidation the
stale evidence is committed" is argued from code, not measured.

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
**Test:** E-RQ2-C on baselines from E-RQ4.
**Status:** OPEN (baselines not run).

---

## RQ3: What are the latency, throughput, storage, and CPU costs of verification-bound commits?

### H3.1 Verification cost depends on method, not just on enabling it
**Claim:** Commit latency overhead of strong verification is dominated
by read-back bandwidth for client-side digests and is lower with
service-computed checksums.
**Prediction:** overhead(read-back BLAKE3/SHA-256) > overhead(service
SHA-256) > overhead(size-only); gap grows with object size.
**Test:** E-RQ3-A, variants V-none, V-size, V-svc, V-readback.
**Status:** PARTIAL (read-back only). **Answer (tag):** With LocalFS
read-back verification, checksum + verify take 0.1–1.4 ms per 16 MiB
batch when objects are zstd-compressed (compression itself: 13.5–24.6
ms), and 12.8–15.1 ms without compression, because VTOP hashes the
**stored** bytes and these logs compress about 6–15×. Stage costs are
stable across both rounds (first round: 1.6–3.9 ms vs 24–37 ms
compression). End-to-end throughput is not: continuous-engine medians
(10 reps) put strong SHA-256 2.2% (SSH), 8.0% (BGL), and 14.1% (HDFS)
below size-only on the tag, with 95% CIs overlapping size-only for SSH
and HDFS, against 3.4%, 4.2%, and −2.6% in the first round on a code
path that did not change between the two builds. BLAKE3 and SHA-256 do
not differ consistently. Uncompressed objects cost 20.4% (SSH), 13.9%
(BGL), and 12.3% (HDFS) on the tag (first round 14.0%, 7.2%, 13.1%), the
one throughput effect that is large in both rounds. So the cost of
strong verification is governed by the stored object size, i.e. by
compression, more than by the digest; at the throughput level the
compressed-object effect is within the run-to-run spread of this host. Not
tested: service-computed checksums on S3/MinIO (V-svc) and the unsafe
no-verification ablation (V-none).

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

## RQ4: How does VTOP compare with existing telemetry-to-object-storage solutions under equivalent workloads and failures?

### H4.1 Safety under the shared fault matrix
**Claim:** Under identical crash points, VTOP commits no unverified
range; baselines differ by mechanism (Confluent S3 Sink exactly-once
config, Vector end-to-end acks, Fluent Bit buffering, Flink checkpoints).
**Prediction:** per-system table of lost / duplicated / corrupted-accepted
outcomes per fault point; no single system dominates on every column.
**Test:** E-RQ4-B.
**Status:** OPEN. **Answer:** —

### H4.2 Performance trade-off
**Claim:** VTOP's verification gating costs throughput or latency
relative to response-based baselines on equal hardware and batch sizes.
**Prediction:** VTOP slower than at least one baseline in steady state;
the cost is the price of H2.x detection.
**Test:** E-RQ4-B (no-fault runs).
**Status:** OPEN. **Answer:** —

### H4.3 Heterogeneity
**Claim:** VTOP applies one commit rule to Kafka, files, and spools,
whereas Kafka-only sinks (Confluent) cannot ingest files/spools, and
log shippers (Vector, Fluent Bit) use per-source mechanisms.
**Prediction:** capability matrix (qualitative) plus per-source fault
outcomes.
**Test:** E-RQ4-B and documentation review (versioned docs only).
**Status:** OPEN. **Answer:** —
