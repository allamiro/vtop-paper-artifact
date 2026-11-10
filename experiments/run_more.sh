#!/usr/bin/env bash
# Follow-up throughput runs: more repetitions and a continuous-engine batch-size sweep.
set -u
cd "$(dirname "$0")/.."
PY=.venv/bin/python
OUT=experiments/rq3-cost-ablation/results/20261110-cost-run
echo "== extra reps $(date)"
$PY experiments/rq3-cost-ablation/code/exp_cost.py --mode run --out $OUT --datasets ssh,bgl,hdfs --reps 7 --rep-offset 3
echo "== batch sweep $(date)"
$PY experiments/rq3-cost-ablation/code/exp_cost.py --mode run --batch-sweep --out $OUT --datasets bgl,hdfs --reps 5
echo "== done $(date)"
