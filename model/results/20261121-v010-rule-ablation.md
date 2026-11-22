# Single-rule ablation on the v0.10.x model (2026-11-21; B7 in the reframing plans)

Same question as `20261111-rule-ablation.md`, asked of the v0.10.x logic (variant `v010`, ordered commit with
per-cycle hold-back). Each variant removes one rule and keeps the rest:
- `v010-nohandback`: failed ranges are not handed back to the source during a run (recovery unchanged).
- `v010-noorder`: commits within a cycle, and the recovery commit queue, may happen in any order (all orders explored);
  hold-back and hand-back kept.

Reproduce: `python3 model/vtop_model.py --sweep010-ablation` (raw output `20261121-v010-rule-ablation.{txt,json}`).
Regression: `--sweep010` reproduces `20261120-v010-sweep.txt`, and `--sweep` reproduces the v0.7.0/v0.6.0 output of
the previously committed model, line for line (diffed 2026-11-21).

| N,B,f,c,l,m | v010 (protocol) | v010-nohandback | v010-noorder |
|---|---|---|---|
| 3,1,0,0,0,0 | 25 / holds | 25 / holds | 25 / holds |
| 3,1,1,0,0,0 | 213 / holds | 41 / **I2 violated** | 213 / holds |
| 3,1,0,1,0,0 | 183 / holds | 183 / holds | 183 / holds |
| 3,1,1,1,0,0 | 1,688 / holds | 179 / **I2 violated** | 1,706 / holds |
| 3,1,1,1,1,0 | 2,905 / holds | 261 / **I2 violated** | 2,923 / holds |
| 3,1,0,1,0,1 | 267 / holds | 267 / holds | 267 / holds |
| 3,1,1,2,1,1 | 36,504 / holds | 663 / **I2 violated** | 36,730 / holds |
| 4,1,2,2,1,1 | 646,783 / holds | 846 / **I2 violated** | 657,353 / holds |
| 3,2,0,1,0,0 | 498 / holds | 498 / holds | 577 / holds |
| 3,2,1,0,0,0 | 353 / holds | 139 / **I2 violated** | 411 / holds |
| 3,2,1,1,1,1 | 14,837 / holds | 2,902 / **I2 violated** | 17,229 / holds |

Answer, within these bounds:
- **The hand-back is necessary** on v0.10.x too: without it, source-progress safety fails whenever one failure is
  allowed, with one or two batches per unit and cycle.
- **Read-order commit is not shown necessary**: with the hold-back in place, committing in any order violated nothing
  up to 657,353 states, including two batches per unit and cycle. (The engine keeps read order for the restart seed
  across file rotations; the one-unit model does not test that.)
- Together with `20261120-v010-sweep.md`: the hold-back is necessary at B = 2 and the replayability assumption is
  necessary; the rule set {hand-back, hold-back} plus the commit guard is what the safety result rests on.
