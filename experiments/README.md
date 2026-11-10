# Experiments

One folder per research question. Each has:

```
<rq>/
├── README.md   spec: hypotheses tested, variants, fault points, metrics, procedure, pass/fail rule
├── code/       harness, fault injectors, oracle, workload generators
├── configs/    VTOP and baseline configs, one file per variant/cell
└── results/    one subfolder per run: <YYYYMMDD>-<cell>-<seed>/
```

| Folder | Experiments | Hypotheses |
|---|---|---|
| [rq1-source-safety/](rq1-source-safety/) | E-RQ1-M model checking; E-RQ1-F crash/fault injection with oracle | H1.1–H1.4 |
| [rq2-corruption-recovery/](rq2-corruption-recovery/) | E-RQ2-C corruption before verify; E-RQ2-R mutation while down | H2.1–H2.4 |
| [rq3-cost-ablation/](rq3-cost-ablation/) | E-RQ3-A verification ablations; E-RQ3-R recovery cost | H3.1–H3.4 |
| [rq4-baselines/](rq4-baselines/) | E-RQ4-B comparison with Confluent S3 Sink, Vector, Fluent Bit, Flink | H4.1–H4.3 |

## Rules for every run

Each run folder must contain a `run.json`:

```json
{
  "experiment": "E-RQ1-F",
  "cell": "kafka-crash-after-object-upload",
  "vtop_commit": "6c8f386",
  "baseline_versions": {},
  "config_files": ["configs/..."],
  "seed": 0,
  "container_digests": {},
  "host": {"cpu": "", "cores": 0, "memory_gb": 0, "os": "", "disk": ""},
  "storage": {"backend": "minio", "version": "", "versioning": true},
  "started_at": "", "finished_at": "",
  "valid": true, "invalid_reason": null
}
```

plus raw outputs (engine logs, Prometheus samples, ledger dump, object
listing with versions, source offset history, oracle report).

- A run with a harness or oracle failure is marked `"valid": false` and
  kept; it is never silently dropped.
- Pilot: 3 repetitions per cell to estimate variance; final cells ≥ 10
  repetitions where practical, randomized order, paired seeds across
  systems.
- Large raw artifacts go under `results/<run>/raw/` (git-ignored);
  commit `run.json`, summaries, and oracle reports.

## Reuse before building

VTOP's own `benchmarks/` (run_matrix.py, long-lived engine runner,
result schemas) and `tests/integration_replay.rs`,
`tests/integration_state_recovery.rs` already cover measurement and some
recovery cases. Extend them; don't rebuild. What is missing (per
`data/papers/analysis/repository_ingest_review.md`): an independent
coverage oracle, an unsafe-commit counter, same-unit ordering control,
and evidence-invalidation scenarios.

The broader ECCF plan in `data/papers/analysis/experimental_plan.md`
(fault matrix, metrics definitions, statistics) is the reference for
definitions; the specs here are the subset needed for RQ1–RQ4 of the
current paper.
