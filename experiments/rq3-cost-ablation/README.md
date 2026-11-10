# RQ3: Cost of verification-bound commits

Hypotheses: H3.1–H3.4 ([../../hypotheses.md](../../hypotheses.md)).

## Variants (controlled ablations of one VTOP build)

| ID | Verification | Manifest auth | Restart re-check |
|---|---|---|---|
| V-none | none: commit after upload response (ablation patch, never deployable) | off | off |
| V-size | size only (backend-limited) | off | on |
| V-svc | service-computed SHA-256 | off | on |
| V-readback | read-back BLAKE3 / SHA-256 | off | on |
| V-mac | V-svc + keyed BLAKE3 MAC | on | on |

V-none exists only to measure the cost of gating; it is labelled as an
unsafe ablation everywhere.

## E-RQ3-A: steady state, no faults

Factors (staged, not full factorial):

- record size: 256 B, 1 KiB, 16 KiB
- batch target: 1, 16, 64 MiB
- compression: zstd, none
- upload width: 1, 4, 8 (adaptive off)
- storage RTT: LAN baseline, +20 ms (Toxiproxy)

Metrics: records/s and MiB/s; per-stage latency (compress, checksum,
put object, put manifest, verify, commit) p50/p95/p99; commit latency
(seal → `SOURCE_COMMITTED`); CPU time and peak RSS of the engine;
verification read bytes / uploaded bytes; storage request count per
batch; manifest bytes / object bytes; ledger writes per batch.

H3.2 threshold (fix before running): TODO (author) — e.g. MAC adds
< X% to per-batch time.

## E-RQ3-R: recovery cost

Create a backlog of K ∈ {0, 10, 100, 1000} `VERIFIED` batches (adapter
commit blocked), kill, restart. Measure time to first safe commit and
time to drain the backlog, re-checked bytes, and storage requests.

## Analysis

Medians with bootstrap 95% CIs; paired comparisons against V-svc;
overhead reported as absolute and relative. Scripts in
`../../analysis/scripts/`.

## As run (2026-11-10)

- Per-stage costs: `code/exp_cost.py` (process-once, per-batch metrics),
  4 datasets × {size-only, sha256, blake3, sha256+mac, sha256-nocomp} ×
  3 reps at 16 MiB, plus BGL batch sweep {1, 4, 16, 64} MiB. Results in
  `results/20261110-cost/`. Throughput from process-once includes a
  process start per batch and is not used for throughput claims.
- Throughput: `code/exp_cost.py --mode run` (continuous engine),
  steady-state = bytes committed by the last size-sealed batch / time of
  that commit; 10 reps per cell (SSH, BGL, HDFS) and a continuous-engine
  batch sweep on BGL and HDFS. Results in `results/20261110-cost-run/`.
- Backend: LocalFS only (read-back verification). V-svc (service-computed
  SHA-256 on S3/MinIO) and V-none (unsafe ablation patch) not run, so
  H3.1's method comparison is partial. E-RQ3-R (recovery backlog) not run.

**Tag round (v0.7.0, `6c8f386`), same day.** Every binary-dependent
campaign was re-run on the release tag with the same seeds and settings
(`experiments/run_v070.sh`, log `experiments/run_v070.log`); results in
`results/20261110-v070-*`. These are the results the paper reports; the
rounds above (`a8bce8e`) are kept as a replication.
Tag outcome: per-stage costs reproduce; end-to-end throughput deltas for
compressed objects do not (see RESULTS.md, RQ3), so stage times are the
primary cost measure.
