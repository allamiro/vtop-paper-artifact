# Analysis

Scripts here read `experiments/*/results/**/run.json` and raw outputs and
produce the statistics, figures, and tables that the paper uses.

```
analysis/
├── scripts/   one script per output; deterministic; no hand-edited numbers
├── figures/   generated PDFs (vector) → copied to VTOP-IEEE-TCC/figures/ as suliman-figN.pdf
└── tables/    generated LaTeX table bodies → \input into VTOP-IEEE-TCC/tables/
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
