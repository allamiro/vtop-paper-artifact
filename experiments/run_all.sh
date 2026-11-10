#!/usr/bin/env bash
# Runs the remaining experiments one after another (exclusive CPU for timing runs).
set -u
cd "$(dirname "$0")/.."
PY=.venv/bin/python
D=20261110
echo "== RQ3 throughput (continuous engine) $(date)"
$PY experiments/rq3-cost-ablation/code/exp_cost.py --mode run --out experiments/rq3-cost-ablation/results/$D-cost-run --datasets ssh,bgl,hdfs --reps 3
echo "== RQ1 crash campaign $(date)"
$PY experiments/rq1-source-safety/code/exp_crash.py --out experiments/rq1-source-safety/results/$D-crash --trials 10 --kills 8
echo "== RQ2 tampering $(date)"
$PY experiments/rq2-corruption-recovery/code/exp_tamper.py --out experiments/rq2-corruption-recovery/results/$D-tamper --datasets ssh,linux --reps 2
echo "== done $(date)"
