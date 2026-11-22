# v0.10.2 running-path rewind: the highest failed start wins (2026-11-21)

**v0.10.2 defect; fix targeted at v0.10.4 (PR number pending).** Found by the engine maintainers; checked against the
code and modelled for the paper study. Details and line references also in `../v0101-changes.md` §18.

Question: the v010 model (`20261120-v010-sweep.md`) abstracts the running path's hand-back as "the unit resumes at
or below F(u)", i.e. at the **lowest** start of the windows that failed in the cycle. v0.10.2 resumes at the
**highest** one. Does source-progress safety (I2) still hold, and where is the boundary?

## The code (tag v0.10.2 = 0abf116; same code at v0.10.0 ac37936 and v0.10.1 44f6619)

- `crates/vtop-cli/src/engine.rs`, `Engine::run_cycle`: windows are processed in read order
  (`verified.sort_by_key(|(order, _, _)| *order);` :3934). Every window that does not reach VERIFIED pushes its start:
  `rewinds.push(started_at);` at :4038 (FAILED outcome) and :4105 (error before VERIFIED). Then all of them go to the
  adapter in one call: `.zip(adapter.replay_from_markers(&rewinds).await)` :4116-4118.
- `crates/vtop-adapters/src/base.rs:229-238`, default `replay_from_markers`: `for marker in markers {
  results.push(self.replay_from_marker(marker).await); }` (one rewind per marker, in order).
- Each rewind **assigns** the read position:
  - file `crates/vtop-adapters/src/file_source.rs:4179`: `c.read_byte = (*start_byte).max(c.committed_byte);`
  - spool `syslog_spool_source.rs:895`, `:913`: `let replay_byte = (*start_byte).max(c.committed_byte);` …
    `c.read_byte = replay_byte;`
  - Kafka overrides `replay_from_markers` (`kafka_source.rs:3560`) but also assigns: `cur.committed_offset =
    Some(resume_at);` (:3626), with `resume_at = clamp_rewind(*start_offset, frontier)` (:3623) and a frontier that
    includes the cursor's previous position `in_memory` (:3617, `rewind_frontier` :105). So for Kafka the highest start
    wins in any order; for file and spool the last one in read order, which is the highest.
- The hold-back `failed_from` (:3985-3999) records the **lowest** failed start per unit, but only holds **VERIFIED**
  windows above it (:4001-4030). It does not affect where the source resumes.
- Recovery is different and not affected: `RewindPlan::add_start` (:5964) keeps the lowest start per unit and content
  generation (`if position < start_position(&generation.marker)` :5981).

Consequence: with two or more FAILED windows of one unit in one cycle, the lower failed range is never re-read; the
re-read of the higher one starts a new cycle with no failure below it and commits past the lower range.

## Model change

`model/vtop_model.py --rewind-last-wins` (global `REWIND_LAST_WINS`, default off): in the v010 running-path commit
phase, the hand-back sets `H := max(max(rewinds), C)` instead of `H := max(min(rewinds), C)`. Nothing else changes
(recovery keeps the lowest start; the hold-back keeps the lowest failed start). `--sweep010-lastwins` runs `SWEEP010`
followed by `SWEEP010_LASTWINS_EXTRA` (the smallest unit, N = 2; B = 1 with two and three failures; B = 3 with one to
three failures).

Reproduce: `python3 model/vtop_model.py --sweep010-lastwins --rewind-last-wins --out results/20261121-v010-rewind-last-wins.json`
(stdout = `20261121-v010-rewind-last-wins.txt`). The default-behaviour column below comes from the same grid without
the flag, `20261121-v010-rewind-last-wins-baseline.{txt,json}`.

Regression (2026-11-21): with the flag off, `--sweep010` reproduces `20261120-v010-sweep.txt` line for line and
`20261120-v010-sweep.json` field for field, apart from the per-run wall-clock time (`(…s)`, `"seconds"`), which
differs between any two runs, including two runs of the unmodified model. `--sweep010-ablation` reproduces
`20261121-v010-rule-ablation.{txt,json}` the same way.

## Result (N ranges, B windows per unit per cycle, f failures, c crashes, l ledger-tail losses, m storage mutations, k refused hand-backs)

States: for a "holds" row, all reachable states; for a violation row, the states explored (breadth-first) before the
first violation was found, so its trace is a shortest one.

| variant | N,B,f,c,l,m,k | default (lowest failed start) | **--rewind-last-wins (v0.10.2)** | last-wins stalled / D1 / D2 / max dup |
|---|---|---|---|---|
| v010 | 3,1,0,0,0,0,0 | 25 / holds | 25 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,1,0,0,0,0 | 213 / holds | 213 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,0,1,0,0,0 | 183 / holds | 183 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,1,1,0,0,0 | 1,688 / holds | 1,688 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,1,1,1,0,0 | 2,905 / holds | 2,905 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,0,1,0,1,0 | 267 / holds | 267 / holds | 0 / 27 / 0 / 1 |
| v010 | 3,1,1,2,1,1,0 | 36,504 / holds | 36,504 / holds | 0 / 4561 / 0 / 3 |
| v010 | 4,1,2,2,1,1,0 | 646,783 / holds | 646,783 / holds | 0 / 95372 / 0 / 4 |
| v010 | 3,2,0,1,0,0,0 | 498 / holds | 498 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,2,1,0,0,0,0 | 353 / holds | 353 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,2,1,1,1,1,0 | 14,837 / holds | 14,837 / holds | 0 / 473 / 0 / 2 |
| v010 | 3,2,2,2,1,1,0 | 2,356,152 / holds | 38,237 / **I2 violated** ← | 0 / 797 / 0 / 0 |
| v010 | 4,2,2,1,1,1,0 | 357,211 / holds | 8,905 / **I2 violated** ← | 0 / 203 / 0 / 0 |
| v010 | 4,3,2,1,0,0,0 | 608,972 / holds | 47,033 / **I2 violated** ← | 0 / 0 / 0 / 0 |
| v010-nohold | 3,2,1,0,0,0,0 | 73 / **I2 violated** | 73 / **I2 violated** | 0 / 0 / 0 / 0 |
| v010-nohold | 3,2,1,1,1,1,0 | 393 / **I2 violated** | 393 / **I2 violated** | 0 / 0 / 0 / 0 |
| v010-nohold | 3,1,1,1,1,1,0 | 4,056 / holds | 4,056 / holds | 0 / 288 / 0 / 2 |
| v010 | 3,1,1,0,0,0,1 | 53 / **I2 violated** | 53 / **I2 violated** | 0 / 0 / 0 / 0 |
| v010 | 3,2,1,0,0,0,1 | 198 / **I2 violated** | 198 / **I2 violated** | 0 / 0 / 0 / 0 |
| v010 | 2,2,2,0,0,0,0 | 888 / holds | 249 / **I2 violated** ← | 0 / 0 / 0 / 0 |
| v010 | 3,2,2,0,0,0,0 | 2,148 / holds | 585 / **I2 violated** ← | 0 / 0 / 0 / 0 |
| v010 | 3,2,3,0,0,0,0 | 11,520 / holds | 548 / **I2 violated** ← | 0 / 0 / 0 / 0 |
| v010 | 3,1,2,0,0,0,0 | 1,117 / holds | 1,117 / holds | 0 / 0 / 0 / 0 |
| v010 | 4,1,3,0,0,0,0 | 10,656 / holds | 10,656 / holds | 0 / 0 / 0 / 0 |
| v010 | 3,1,2,1,1,1,0 | 26,299 / holds | 26,299 / holds | 0 / 1838 / 0 / 3 |
| v010 | 4,3,1,0,0,0,0 | 1,402 / holds | 1,402 / holds | 0 / 0 / 0 / 0 |
| v010 | 4,3,1,1,1,1,0 | 227,964 / holds | 227,964 / holds | 0 / 2054 / 0 / 2 |
| v010 | 4,3,2,0,0,0,0 | 10,916 / holds | 2,653 / **I2 violated** ← | 0 / 0 / 0 / 0 |
| v010 | 4,3,3,0,0,0,0 | 85,541 / holds | 2,904 / **I2 violated** ← | 0 / 0 / 0 / 0 |

← = differs from the default; every other row is identical in all fields.

## Minimal counterexample (N = 2, B = 2, 2 failures, no crash, no loss, no mutation; 249 states)

```
init                               H=0 C=0 up=True ledger=[]
read_seal(r0..r1)                  H=2 C=0 up=True ledger=[(0, 0, 'SEALED'), (1, 1, 'SEALED')]
fail(aid0)                         H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'SEALED')]
fail(aid1)                         H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED')]
commit_phase                       H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED')]
queue_rewind(r0)                   H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED')]
queue_rewind(r1)                   H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED')]
rewind_failed_ranges               H=1 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED')]
read_seal(r1..r1)                  H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED'), (2, 1, 'SEALED')]
upload_obj(aid2)                   H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED'), (2, 1, 'OBJECT_UPLOADED')]
upload_manifest(aid2)              H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED'), (2, 1, 'MANIFEST_UPLOADED')]
verify(aid2)                       H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED'), (2, 1, 'VERIFIED')]
commit_phase                       H=2 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'FAILED'), (2, 1, 'VERIFIED')]
(next step violates)
```

The next step, `adapter_commit(aid2,r1)`, sets C = 2: "commit sets C=2 but range 0 was never archived and verified".
Both windows of the first cycle fail; both starts are handed back in read order, and the second (r1) overwrites the
first (r0), so `rewind_failed_ranges` lands at H = 1. The default model lands at H = 0 here. The re-read of r1 is
the only window of its cycle, nothing is held, and its commit covers r0. The N = 3 shortest trace is the same with
r2 read alongside r1 (585 states).

With B = 3 the hold-back is visible but does not help (N = 4, B = 3, 2 failures, no crash; 2,653 states; abridged, the full trace is in the JSON):

```
read_seal(r0..r2)                  H=3 C=0 up=True ledger=[(0, 0, 'SEALED'), (1, 1, 'SEALED'), (2, 2, 'SEALED')]
fail(aid0)                         ...
verify(aid1)                       ...
fail(aid2)                         H=3 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'VERIFIED'), (2, 2, 'FAILED')]
commit_phase                       H=3 C=0 ...
queue_rewind(r0)                   H=3 C=0 ...
hold(aid1,r1)                      H=3 C=0 ...      (r1 is VERIFIED above the failed r0: held, correctly)
queue_rewind(r2)                   H=3 C=0 ...
rewind_failed_ranges               H=2 C=0 ...      (r2 overwrites r0)
read_seal(r2..r3)                  H=4 C=0 ...
...                                                 (aid3 = r2 and aid4 = r3 verify)
commit_phase                       H=4 C=0 up=True ledger=[(0, 0, 'FAILED'), (1, 1, 'VERIFIED'), (2, 2, 'FAILED'), (3, 2, 'VERIFIED'), (4, 3, 'VERIFIED')]
(next step violates: adapter_commit(aid3,r2) sets C=3 but range 0 was never archived and verified)
```

Full traces of every violation are in the JSON (`violation.trace`).

## Answer, within these bounds

1. **The defect is real in the model of v0.10.2's running path:** I2 is violated in every configuration with B ≥ 2
   and at least two failures (N = 2 to 4, B = 2 and 3, 2 or 3 failures, with or without crashes, ledger-tail loss
   and mutation). The smallest: N = 2, B = 2, two failures, 249 states.
2. **No violation with B = 1 or with one failure.** Every such row is identical (states, terminal, stalled, D1, D2,
   max dup) to the default model: with one failed window per unit per cycle the lowest and highest failed start
   coincide. This includes B = 1 with two and three failures (up to 646,783 states) and B = 3 with one failure (up to
   227,964 states).
3. **The default model (lowest failed start) is unchanged** and still finds no violation in these configurations.
4. The rows that do not depend on the rewind rule are unchanged: the hold-back ablation (`v010-nohold`) and the
   refused hand-back (k = 1) violate I2 as before, with the same counts.

## Not modelled

One unit only, so the per-unit grouping of markers across units, rotation (a new inode is a new unit) and Kafka's
order-independence are not exercised; the model's hand-back is the max of the failed starts, which is what both the
file/spool (read order) and Kafka (clamp to the previous landing) paths produce for one unit. `drop_pending_above`
(buffered windows above a rewind) is not modelled. v0.10.3 was not checked.

## Consequence for the paper

Theorem 1 (with Definition "Ordered commit with hold-back", `research/drafts/05_correctness_v010.tex`) and proof step
(iii) ("a batch of an earlier cycle that did not reach VERIFIED was handed back … Every cycle's first read of u starts
at or below F(u)") hold for the protocol with the lowest failed start handed back, which the default model checks.
v0.10.2 as built does not meet that rule when one unit has two or more failed windows in one cycle. The paper must
not state that v0.10.2's running path satisfies the theorem without this qualification (author decision on wording;
the fix is targeted at v0.10.4).
