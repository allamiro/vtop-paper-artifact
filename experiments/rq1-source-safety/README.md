# RQ1: Source-progress safety across Kafka, files, and syslog spools

Hypotheses: H1.1–H1.4 ([../../hypotheses.md](../../hypotheses.md)).

## E-RQ1-M: model checking

**Goal:** check invariants I1, I2, I4, I5 of
[state-machine/README.md](../../state-machine/README.md) and resolve OQ1.

- Model one source unit with N ∈ {2,3,4} consecutive ranges, upload
  width W ∈ {1,2}, crash at any step (≤ 2 crashes), ledger-tail loss
  (models SQLite `synchronous=NORMAL` on power loss), storage mutation
  while down, and recovery.
- Two variants of the logic: **v0.7.0** (read-order commit, failed-range
  rewinds, `RewindPlan` holdback) — the system under study — and
  **v0.6.0** (completion-order commit, no holdback) as a contrast that
  should produce counterexamples.
- Tool: TLA+/TLC preferred; a Python explicit-state checker is acceptable
  if TLC is unavailable. Model code lives in
  `../../state-machine/model/`.
- **Output:** for each variant, invariant results with state counts and
  bounds; any counterexample trace saved to `results/`.
- **Pass rule:** H1.1 supported if I1 holds in both. H1.2 answered per
  variant (a counterexample is a valid, reportable answer).

## E-RQ1-F: fault injection with an independent oracle

**Systems:** VTOP v0.7.0 (tag, `6c8f386`; first round on pre-tag `a8bce8e`). Optionally
v0.6.0 (`b1469be`) for F9 only, to show the fixed hazard.

**Sources:** Kafka (single partition and multi-partition), file
(append, rotation by rename, `copytruncate`), syslog spool.

**Fault points** (process kill `SIGKILL` unless noted):

| # | Point | Expected (per spec) |
|---|---|---|
| F1 | before object upload | replay; no commit; no duplicate |
| F2 | after object upload, before manifest upload | replay; orphan object (duplicate) |
| F3 | after manifest upload, before verification | replay; duplicate |
| F4 | after `VERIFIED`, before adapter commit | recovery re-check then commit; no duplicate |
| F5 | after adapter commit, before ledger `SOURCE_COMMITTED` | idempotent re-commit (file/spool) or orphan (Kafka, with ledger-tail loss) |
| F6 | adapter commit error (injected) | stays `VERIFIED`; retried |
| F7 | storage unavailable during verify | `FAILED`; replay |
| F8 | power-loss emulation (VM/container freeze + ledger tail truncation) | rolls back to earlier state; safe actions only |
| F9 | same-unit out-of-order completion (delay earliest range's upload, fail it) | **OQ1 probe**: does committed marker pass the failed range? |
| F10 | crash during recovery re-check | no commit until re-check completes |

**Oracle (independent of VTOP code):** every generated record gets a
logical ID and its source position. After each run and restart, the
oracle reads the committed marker per unit (broker offset or VTOP ledger)
and the object store, decodes all objects, and reports:

- `unsafe_commits`: positions below the committed marker missing from
  every verified object;
- `lost_records`: generated records in no object after drain;
- `duplicate_objects` and `duplicate_records` per fault point;
- `replay_bytes` / committed bytes (replay amplification).

**Workload:** synthetic generator (see
[../../data-requirements.md](../../data-requirements.md)), small
(≈10^4 records) for Stage 1.

**Pass rule:** H1.2/H1.3 supported per source type iff
`unsafe_commits = 0` and `lost_records = 0` across all valid runs of
F1–F10. H1.4 answered by the duplicate table.

## As run (2026-11-10)

- E-RQ1-M: `state-machine/model/vtop_model.py --sweep` (Python
  explicit-state checker; TLC not installed). Results in
  `results/20261110-model-sweep/`.
- E-RQ1-F files/spools: `code/exp_crash.py` — scenarios file-mix,
  spool-mix, power-loss (WAL deletion = F8), hdfs; random SIGKILL lives
  of 0.2–1.2 s (8 per trial), one storage outage per trial (object
  store made read-only). LocalFS backend. Results in
  `results/20261110-crash/`.
- E-RQ1-F Kafka: `code/exp_kafka.py` — Kafka 3.9.1 in Docker, topics
  ssh-p1 and hdfs-p4, broker outage by `docker pause`. Results in
  `results/20261110-kafka/`.
- Deviations from the plan: fault points F1–F5 are hit by random kills,
  not targeted; F9 (same-unit out-of-order failure) is covered by the
  model only; file rotation, `copytruncate`, and Kafka rebalance were
  not exercised; v0.6.0 was not run (model only).

**Tag round (v0.7.0, `6c8f386`), same day.** Every binary-dependent
campaign was re-run on the release tag with the same seeds and settings
(`experiments/run_v070.sh`, log `experiments/run_v070.log`); results in
`results/20261110-v070-*`. These are the results the paper reports; the
rounds above (`a8bce8e`) are kept as a replication.
Tag outcome: 49 trials, 285 kills, 0 unsafe; one 4-partition Kafka trial
(`hdfs-p4-t02`) stalled on one partition and ended incomplete without any
unsafe commit (finding F5 in `state-machine/README.md`). The harness's
`lost_records`/`lost_bytes` field counts data missing from the archive at
the end of the drain, whether or not it was committed; the analysis
reports it as "complete trials" and counts committed-but-unarchived data
only through the unsafe-commit check.
