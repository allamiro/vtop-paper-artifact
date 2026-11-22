# Rule ablation in the bounded model (2026-11-11)

Question: guard-only recovery (v0.6.0) drops two rules at once, the run-time hand-back of failed ranges
and read-order commit. Which one carries the safety result (Eq. 2, model property I2)?

Variants added to `model/vtop_model.py`; each changes one rule and is otherwise v0.7.0:
- `noorder`: hand-back kept; commits in any order, in the cycle and in the recovery queue (all orders explored).
- `nohandback`: read-order commit kept; failed ranges are not handed back during a run (recovery unchanged).

Same B=1 configurations as the paper's v0.7.0 sweep (N ranges, B batches per unit per cycle, f failures,
c crashes, l ledger-tail losses, m storage mutations):

| N,B,f,c,l,m | v070 states / result | noorder | nohandback |
|---|---|---|---|
| 3,1,0,0,0,0 | 25 / holds | 25 / holds | 25 / holds |
| 3,1,1,0,0,0 | 187 / holds | 187 / holds | 38 / **I2 violated** |
| 3,1,0,1,0,0 | 183 / holds | 183 / holds | 183 / holds |
| 3,1,1,1,0,0 | 1,398 / holds | 1,454 / holds | 171 / **I2 violated** |
| 3,1,1,1,1,0 | 3,525 / holds | 3,601 / holds | 279 / **I2 violated** |
| 3,1,0,1,0,1 | 267 / holds | 267 / holds | 267 / holds |
| 3,1,1,2,1,1 | 46,885 / holds | 47,870 / holds | 714 / **I2 violated** |
| 4,1,2,2,1,1 | 793,353 / holds | 833,440 / holds | 877 / **I2 violated** |

Max duplicates are identical for v070 and noorder in every configuration.

Answer, within these bounds:
- Dropping only the hand-back violates source-progress safety whenever one failure is allowed,
  with or without a crash.
- Dropping only read-order commit violates nothing, up to 833,440 states. With one batch per
  unit per cycle, its only effect is the order of recovery commits.

Not shown: the model has one source unit, so it cannot show the engine's stated reason for read
order. That reason is file rotation, where old and new generations are separate units and the
restart seed is taken from the newest committed row. That remains a code-comment rationale
(v0.7.0 engine.rs above `verified.sort_by_key`), not a model result.

The v070 control reproduces the paper's Table 3 counts (793,353), so the model change does not
alter v0.7.0 behaviour.
