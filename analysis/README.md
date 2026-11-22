# Analysis

Scripts here read `experiments/*/results/**/run.json` and raw outputs and
produce the statistics, figures, and tables that the paper uses.

```
analysis/
├── scripts/   one script per output; deterministic; no hand-edited numbers
├── figures/   generated PDFs (vector) → copied to paper/figures/ as suliman-figN.pdf
└── tables/    generated LaTeX table bodies → \input into paper/tables/
```

Rules:

- Every figure/table header comment names the script, the run IDs, and
  the git commit it was generated from.
- Skip runs with `"valid": false`, and report how many were skipped.
- Statistics: medians, p95/p99, bootstrap 95% CIs, paired effect sizes;
  raw counts (never only rates) for unsafe commits, lost records,
  duplicates, and detections.
- Figures: vector PDF, sized for one column (`\columnwidth`) or two
  (`\textwidth`); IEEE CS requires ≥300 dpi for any raster.

When an analysis answers a hypothesis, update its entry in
[../hypotheses.md](../hypotheses.md) with the answer and the file paths.

## Result sets (builds)

`scripts/analyze.py --set v070` (default) reproduces the submitted paper's `summary.json`, `tables/` and
`figures/` (v0.7.0, 6c8f386); `--set v0102` writes `v0102/summary.json`, `v0102/tables/`, `v0102/figures/` from the
vtop-engine v0.10.2 runs, the cloud campaigns (COST, WAN, TAMPER, SCALE, PERF; blocks `rq5_cost`, `rq5_wan`,
`rq5_tamper`, `rq5_scale`, `rq5_perf`, tables `rq5_*.tex`, each campaign's own analyzer re-run and checked against its committed summary) and
the v010 model. `--out DIR` redirects the outputs (reproduction
diffs), `--stamp SHA` fixes `generated_from_commit`. For v0.10.2, then run
`scripts/make_protocol_figures.py --set v0102` (package under `v0102/package/`, never `paper/`),
`scripts/make_numbers_v0102.py` (`v0102/NUMBERS.md`: every ledger number, v0.7.0 vs v0.10.2) and
`scripts/verify_v0102_data.py` (recomputes every v0.10.2 number from the run files and re-checks the v070
reproduction byte for byte).
