# Audit 2026-11-21: did the v0.10.2 "rewind last wins" defect fire in any v0.10.2 run?

**Answer: no.** No v0.10.2 run, local or on AWS, ever had two or more windows of the same unit fail in the same
engine cycle. No range was skipped, and no result is affected. The engine defect is still real. Our campaigns simply
never produced the condition that triggers it (see "What this does and does not show").

Script: `research/analysis/scripts/audit_rewind_last_wins.py` (sha256 `ad727317…fc1f`). Run from the repo root:

```bash
research/.venv/bin/python -I research/analysis/scripts/audit_rewind_last_wins.py --out <scratch>/audit.json
```

The script runs in about 8 s. It never writes into a result directory. Each ledger is copied together with its `-wal`
and `-shm` files to a temporary directory before it is opened, and the logs are only read. All numbers below come
from that output (sha256 of the JSON: `6eec9fc3…be9`).

## The defect, as it is in the code (vtop-engine 0abf116, tag v0.10.2)

- `run_cycle` (`crates/vtop-cli/src/engine.rs` l.3976–4128) pushes the start marker of every window that did not reach
  VERIFIED onto `rewinds`, in read order (l.4038 and l.4105). It then calls `adapter.replay_from_markers(&rewinds)`
  (l.4116–4118).
- The default `replay_from_markers` (`vtop-adapters/src/base.rs` l.229–238) calls `replay_from_marker` once per marker.
  For files, that call sets the read head (`file_source.rs` l.4091 ff.). Kafka sets `cur.committed_offset =
  resume_at` per marker (`kafka_source.rs` l.3560–3640).
- So with two or more failed windows of one unit (a file, a spool, or a Kafka topic-partition) in one cycle, the
  highest start wins.
- The recovery path is separate. It uses `RewindPlan` with hold and lower logic (engine.rs l.4583–4960) and is not
  affected.

## Method

Each run gets three independent checks plus its own oracle verdict.

**L: logs, exact cycle attribution.** `run_cycle` logs one line for each entry of `rewinds`. The line is either WARN
`batch did not reach VERIFIED; its range was handed back to the source and will be re-read` or ERROR `could not hand a
failed range back…`. These lines are emitted in one loop at the end of the cycle (l.4116–4150), after every window of
that cycle has finished its pipeline. Every window logs `batch_started` inside the pipeline (l.741), and
`read_cycle_profile` is logged once per cycle when the read phase is longer than 0 ms (l.3776–3797).

- Two hand-backs therefore belong to the same cycle exactly when no `batch_started` or `read_cycle_profile` line lies
  between them in the same log file.
- `run_cycle` takes `&mut self`, so two cycles never interleave.
- When `read_cycle_profile` is skipped, the rule can only merge two cycles. It can never split one, so any error makes
  the count of same-cycle groups too high, not too low.
- Groups of two or more hand-backs of one unit in one cycle are the trigger.

The method was validated in three ways:

- Every hand-back's (unit, start) matches a ledger row: 0 unmatched.
- The cycle boundaries are present in every log set: the `L_boundary_events` counts are below.
- Positive control: the grouping did find cycles with two or more hand-backs of *different* units. There were 7, in
  file-mix-t01 (1), file-mix-t05 (2), power-loss-t02 (3) and power-loss-t03 (1). Each is bgl.log plus ssh.log failing
  together during the injected storage outage. The engine handles that case correctly, because each unit has its own
  read head.

**D: ledger, loss check, no threshold.** For each unit:

- File and spool units are keyed by `(source_type, path, inode)`. A Kafka unit is `(topic, partition,
  consumer_group)`.
- A row covers `[progress_start.start_byte, progress_end.end_byte)`. For Kafka it covers `[start_offset,
  end_offset + 1)`, because end_offset is inclusive: consecutive committed rows meet at `end_offset + 1`, checked on
  the ledgers.
- The union of SOURCE_COMMITTED ranges must be contiguous from the unit's first start.
- Every non-committed row must lie inside that union: REPLAY_REQUIRED, SUPERSEDED, and in-flight rows such as
  CHECKSUMMED or MANIFEST_UPLOADED. (No row ends in FAILED in v0.10.2, because `fail!` writes FAILED and then
  REPLAY_REQUIRED.)
- A lower range skipped by the defect would show up as a gap in the union, or as a row the union does not cover.
- Ledger pruning is off in every config: `engine.ledger_retention_days` is unset, and pruning only runs when it is set
  (engine.rs l.5400–5440). Every ledger therefore holds the run's full history.

**G: ledger, candidate groups (a superset of the real ones).** The ledger has no cycle id. `created_at` is written
inside the concurrent per-window pipeline (engine.rs l.678–740), not at a cycle boundary, so no time threshold can
split cycles exactly. G therefore takes **every** pair of non-committed rows of one unit with different,
non-overlapping ranges, however far apart their `created_at` values are. (A retry of the same window in a later cycle
repeats the range, so it is not a pair.) G then checks the lower range of each pair against the committed union. This
contains every possible same-cycle group, and no threshold is needed.

For reference, the data on timing:

| Quantity | Value |
|---|---|
| Smallest gap in `created_at` between two consecutive windows of one unit | 4.4 ms (E-RQ1 crash, median 14.5 ms); ≥ 175 ms in every Kafka and AWS set |
| Pairs under 1 ms | 10, in E-RQ5 perf only (see Exposure) |
| Smallest gap between two consecutive hand-backs of one unit | 166 ms (median 211 ms): retries in later cycles |
| Smallest `created_at` gap of any G pair | 307.6 ms |

**Oracle.** Each run's own recorded verdict was read from its result files. Whether each oracle checks completeness
against the source:

| Set | Oracle (code) | Checks completeness against the source? |
|---|---|---|
| E-RQ1 crash | `lib/vtoplab.py` `oracle()` l.290–370 | **Yes.** Decodes every object, compares it byte for byte with the source file (l.333–336), and builds the contiguous prefix from byte 0 (l.351). `complete = prefix >= size` (l.366). |
| E-RQ1 Kafka | `rq1-source-safety/code/exp_kafka.py` `oracle_kafka` l.166–210 | **Yes.** Compares the records with the topic's ground truth (l.186). `complete = prefix >= len(values)` (l.206). |
| E-RQ4 VTOP | `rq4-baselines/code/exp_baseline.py` `Vtop.intervals` l.306–330 and `judge` l.616–646 | **Yes.** Intact prefix against the topic truth (l.328). `complete = prefix >= goal` (l.641). |
| E-RQ5 cost, WAN, scale | `rq5-cloud/*/results/*/oracle.py` (one version, md5 807d63e1) `check()` l.108–224 | **Yes.** Ranges come from the committed ledger rows, but every object is decompressed and compared with `src[s0:e0]` (l.159). Valid committed coverage must reach the source size from byte 0 (l.219), so a skipped range stops `position`. |
| E-RQ5 tamper | `rq5-cloud/tamper/tamper.py` `Case.oracle` l.513–539 | **Yes.** Every committed object is checked against the source (l.524), and `complete_and_exact = pos >= len(src)` (l.538). It runs only in the `recovery` verifier, so it is recorded for 66 of 114 local cases and 22 of 38 AWS cases. |
| E-RQ5 perf | `rq5-cloud/perf/perf.py` l.1515–1555, which imports `exp_baseline.locate/judge` unchanged | **Yes.** Every S3 object is located in the topic truth by content, then `judge` builds the intact prefix (l.1543–1545). |

## Results per experiment

Hand-backs, same-cycle groups and boundaries come from L. Pairs come from G. Gaps and uncovered rows come from D.

| Experiment (v0.10.2) | Runs scanned (ledger + logs) | Ledger rows | Non-committed rows | Hand-backs (L) | **Same-cycle groups of ≥2 failures of one unit (L)** | G pairs (under 1 s) | **Groups or pairs with an unarchived lower range** | Committed gaps (D) | Uncovered non-committed rows (D) | Oracle complete / recorded | Boundaries (L) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E-RQ1 crash, local (file, spool, HDFS, power loss) | 35 | 5,219 | 64 | 45 | **0** | 85 (18) | **0** | 0 | 0 | 35 / 35 | 10,242 |
| E-RQ1 Kafka, local | 14 | 641 | 0 | 0 | **0** | 0 | **0** | 0 | 0 | 14 / 14 | 1,490 |
| E-RQ4 VTOP side, local (Kafka; kill, corrupt, lost) | 25 | 781 | 20 | 20 | **0** | 0 | **0** | 0 | 0 | 25 / 25 | 2,282 |
| E-RQ5 cost, AWS (including the HDFS re-check) | 120 | 7,200 | 0 | 0 | **0** | 0 | **0** | 0 | 0 | 120 / 120 | 14,403 |
| E-RQ5 WAN, AWS (4 regions) | 272 | 1,344 | 16 | 4 | **0** | 0 | **0** | 0 | 4 ¹ | 268 / 272 ¹ | 2,707 |
| E-RQ5 scale, AWS | 198 | 18,810 | 0 | 0 | **0** | 0 | **0** | 0 | 0 | 198 / 198 | 37,625 |
| E-RQ5 tamper, AWS | 38 | 130 | 16 | 0 | **0** | 0 | **0** | 0 | 0 | 19 / 22 ² | 261 |
| E-RQ5 tamper, local reps (MinIO ×2, AL2023) | 114 | 390 | 48 | 0 | **0** | 0 | **0** | 0 | 0 | 57 / 66 ² | 789 |
| E-RQ5 perf, AWS (VTOP runs; us-east-1 and Sydney) | 61 | 4,487 | 1 | 0 | **0** | 0 | **0** | 0 | 0 | 51 / 61 ³ | 13,074 |
| **Total** | **877** | 39,002 | 165 | 69 | **0** | 85 | **0** | 0 | 4 | | |

Notes on the runs the oracle marks incomplete. None of them is a defect group.

1. **WAN, 4 incomplete runs.** These are `policed-fixed-sha256-raw-mp-r0`, one per region. That cell is the control
   designed to commit nothing (`rq5-cloud/wan/README.md` l.37, l.318). Each ledger holds one in-flight `checksummed`
   row covering [0, 16777073), left by the driver's SIGKILL. Its logs have 0 hand-backs and 0 committed rows. Those
   4 rows are the 4 "uncovered" rows in column D. In the other 12 WAN runs with a non-committed row, that row is a
   single REPLAY_REQUIRED row: 4 `partition` runs with 1 hand-back each, and 8 `sigkill` runs where recovery claimed
   the row at restart. Every one of those 12 rows is covered by committed rows.
2. **Tamper, 3 AWS and 9 local incomplete runs.** These are MAC-FORGE-OFF-BL, MAC-FORGE-OFF-BL-RCPT and MAC-LEAKED-BL,
   in each of the 4 runs. Forged content was committed by design ("NOT CAUGHT, measured", `tamper/README.md`
   l.97–105). Each ledger has 3 rows, all SOURCE_COMMITTED, and the logs have 0 hand-backs.
3. **Perf, 10 incomplete runs.** These are the 10 us-east-1 storage-fault runs (corrupt and lost: 025, 029, 030, 031,
   066, 076, 082, 092, 095, 100). This is the known H4.2 finding: commits stand over objects damaged after or during
   verification. Each ledger has 18 rows, all SOURCE_COMMITTED, so no window ever failed, and the logs have 0
   hand-backs. The single non-committed perf row (Sydney `016-kill-vtop-sha256-store-r0`) is REPLAY_REQUIRED after a
   kill and is covered.

The hand-backs line up with the non-committed rows: E-RQ4 has 20 and 20, and WAN has 4 hand-backs for the 4
partition rows. In E-RQ1, four power-loss runs logged hand-backs (t02: 6, t03: 3, t05: 3, t09: 2) but kept fewer or
no non-committed rows. This is consistent with the emulated power loss rolling back writes that were not fsynced. In
v0.10.x only VERIFIED and SOURCE_COMMITTED are fsynced (`lib/vtoplab.py` docstring, `power_loss_truncate`). For that
reason the logs, not the ledger, are the primary source for check L. In those runs every handed-back start has a
ledger row at the same start, and D finds no gap.

All 85 G pairs are in E-RQ1 crash: hdfs-t01 27, hdfs-t00 20, file-mix-t06 12, hdfs-t04 9, hdfs-t03 5, file-mix-t02 3,
file-mix-t07 2, hdfs-t02 2, and 1 each in file-mix-t01, t03, t04, t08 and t09.

- Most of them are rows claimed by recovery after a kill (`recovered: replay required`), or failures in different
  cycles of the storage outage (`object upload failed: io error: Permission denied`).
- Their `created_at` values are 307.6 ms or more apart. L places every pair in different cycles.
- Every lower range is fully covered by SOURCE_COMMITTED rows (uncovered bytes = 0 for all 85).
- The 18 pairs under 1 s are listed in the script's JSON output (`runs[].ledger.distinct_pairs`).

### Sets whose ledgers and logs were not kept (cannot be scanned)

| Set | Runs | What the records show |
|---|---|---|
| E-RQ3 cost, local | 72 | The final ledger states recorded in `runs.jsonl`: 5,022 rows, all `source_committed`. No window failed, so the defect cannot fire. The oracle was run on 24 of them (rep 0 only), and all 24 are complete. |
| E-RQ3 cost-run, local | 190 | 21,975 rows, all `source_committed`, and all runs drained. No window failed. |
| E-RQ2 tamper, local | 108 | 104 of 108 have `archive_complete`. The 4 incomplete ones are size-only/M1 "accepted": a byte flip that size-only cannot see, with a bad object recorded. |
| E-RQ2 norecheck, local | 36 | 12 of 36 complete. The 24 incomplete ones are the ablation accepting mutations: 20 with a bad object recorded, and 4 M8 manifest rollbacks with 0 bad objects (`rearchived_batches = 0`). |

In RQ2, each mutation is applied while the engine is down (`rq2-corruption-recovery/README.md` l.31–34, l.56), and the
replay goes through the recovery `RewindPlan`, not through `run_cycle`. No fault is injected on the running path, so
these sets cannot be checked here. Nothing points to a trigger in them either.

## Exposure: was the precondition reachable at all?

Two or more windows of one unit sealed in one cycle shows in the ledger as same-unit rows created less than 1 ms apart.
That is a lower bound, because `created_at` is written inside the pipeline. It occurred only in the 10 perf
storage-fault runs. Each has the pair ssh-p1/0 [529984, 566989) and [566989, 604003), created 0.08–0.15 ms apart,
and both rows are SOURCE_COMMITTED. In every other set, windows of one unit are at least 4.4 ms apart. The faults we
injected caused at most one failed window per unit per cycle. The 69 hand-backs were all single per unit per cycle,
including the 7 control cycles where two units failed together.

## Conclusion

Across 877 scanned v0.10.2 runs (49 local RQ1, 25 local RQ4, and 803 RQ5 runs on AWS or on the local tamper
repetitions), there were zero groups of two or more failed windows of the same unit (file, spool or Kafka partition) in
the same cycle. No range was left unarchived, and every oracle that checks completeness against the source agrees,
except the by-design cells explained above. No paper result is affected.

What this does and does not show:

- The campaigns never created the defect's precondition. They are not evidence that v0.10.2 is safe under it.
- The precondition needs two windows of one unit in one cycle, and both must fail.
- The only runs with two windows of one unit in a cycle are the perf storage-fault runs, and none of their windows
  failed.
- Any claim of "no loss" for v0.10.2 should be scoped to one failed window per unit per cycle, or the defect should be
  cited once it has an issue number. Its fix version is \todo.
