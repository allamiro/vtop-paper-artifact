#!/usr/bin/env bash
# Re-runs every binary-dependent experiment on the VTOP v0.7.0 tag (6c8f386).
# Earlier 20261110-* results (no -v070) came from a8bce8e and are kept for comparison.
# Runs one after another so timing runs get the CPU to themselves.
set -u
cd "$(dirname "$0")/.."
PY=.venv/bin/python
R=experiments
D=20261110-v070
echo "== vtopctl $(../.tmp/vtop-engine-v0.7.0/target/release/vtopctl --version) $(git -C ../.tmp/vtop-engine-v0.7.0 describe --tags) $(date)"
echo "== RQ3 throughput, variants $(date)"
$PY $R/rq3-cost-ablation/code/exp_cost.py --mode run --out $R/rq3-cost-ablation/results/$D-cost-run --datasets ssh,bgl,hdfs --reps 10
echo "== RQ3 throughput, batch sweep $(date)"
$PY $R/rq3-cost-ablation/code/exp_cost.py --mode run --batch-sweep --out $R/rq3-cost-ablation/results/$D-cost-run --datasets bgl,hdfs --reps 5
echo "== RQ3 per-stage cost $(date)"
$PY $R/rq3-cost-ablation/code/exp_cost.py --out $R/rq3-cost-ablation/results/$D-cost
echo "== RQ1 crash campaign $(date)"
$PY $R/rq1-source-safety/code/exp_crash.py --out $R/rq1-source-safety/results/$D-crash --trials 10 --kills 8
echo "== RQ1 Kafka campaign $(date)"
$PY $R/rq1-source-safety/code/exp_kafka.py --out $R/rq1-source-safety/results/$D-kafka
echo "== RQ2 tampering $(date)"
$PY $R/rq2-corruption-recovery/code/exp_tamper.py --out $R/rq2-corruption-recovery/results/$D-tamper --datasets ssh,linux --reps 2
echo "== done $(date)"
