# RQ2: Detection and handling of corrupted or replaced archival objects

Hypotheses: H2.1–H2.4 ([../../hypotheses.md](../../hypotheses.md)).

Storage: MinIO (or equivalent) with versioning on, so mutations and
version pinning can be controlled. A storage-side mutation hook runs
between pipeline steps (E-RQ2-C) or while the engine is stopped
(E-RQ2-R).

## Mutation classes

| ID | Mutation | Target |
|---|---|---|
| M1 | flip one byte (same size) | object |
| M2 | truncate | object |
| M3 | replace with different valid object of same size | object |
| M4 | delete | object / manifest |
| M5 | edit a bound field, keep stale self-hash | manifest |
| M6 | edit a bound field and recompute self-hash (no key) | manifest |
| M7 | consistent rewrite: new object + manifest with recomputed self-hash | both |
| M8 | roll back to an older, validly authenticated manifest version | manifest |

## E-RQ2-C: mutation between upload and verification

Variants: strong read-back (SHA-256, BLAKE3), service-computed SHA-256,
backend-limited (size only, explicit opt-out), checksum disabled.

Outcome per (variant, mutation): `detected_before_commit`
(yes/no), resulting state, replay performed, committed marker correct.

## E-RQ2-R: mutation while the engine is down with batches `VERIFIED`

Procedure: drive N batches to `VERIFIED`, block the adapter commit
(fault F4), stop the engine, apply mutation, restart, observe recovery.

Variants: default (re-check on), **V-norecheck** ablation (patch that
skips `storage_still_verified`; built only for this experiment),
MAC on/off, version pinning on/off.

## Baselines (H2.4)

Apply M1–M4 after upload for the RQ4 baselines that support it and
record whether the source offset is committed.

## Pass rules

- H2.1: strong variants detect M1–M3 in 100% of valid runs;
  size-only results reported as their own (weaker) contract.
- H2.2: default detects M1–M5 at restart; V-norecheck commits.
- H2.3: matrix of M6–M8 × {self-hash, MAC, MAC+pinning}.

## As run (2026-11-10)

- E-RQ2-R only: `code/exp_tamper.py`. The VERIFIED-before-commit state
  (F4) is reached by **ledger state injection** after a complete
  archive, then M0–M8 are applied and the engine restarted. LocalFS
  backend; datasets SSH and Linux; variants strong, strong+MAC,
  size-only; 2 reps → 108 trials. Results in `results/20261110-tamper/`.
- Not run: E-RQ2-C (mutation between upload and verify), the
  V-norecheck ablation, version pinning / rollback (M8-rollback needs a
  versioned store), baselines (H2.4).

**Tag round (v0.7.0, `6c8f386`), same day.** Every binary-dependent
campaign was re-run on the release tag with the same seeds and settings
(`experiments/run_v070.sh`, log `experiments/run_v070.log`); results in
`results/20261110-v070-*`. These are the results the paper reports; the
rounds above (`a8bce8e`) are kept as a replication.
Tag outcome: identical to `a8bce8e` in every cell.
