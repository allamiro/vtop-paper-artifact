#!/usr/bin/env bash
# Re-runs every binary-dependent local experiment on vtop-engine v0.10.2 (0abf116), decisions D5/D6/D9.
# Mirrors run_v070.sh (same datasets, reps, trials, seeds) so v0.7.0 and v0.10.2 results compare cell by cell;
# v0.7.0 results stay archived and are not overwritten. One experiment at a time, timing runs first.
# Progress goes to experiments/campaign-v0102.txt (*.log is git-ignored).
set -u
quiet() {  # timing runs wait for a quiet host (1-min load < 4 on 18 cores), at most 2 h; the load is recorded
  for _ in $(seq 1 120); do l=$(sysctl -n vm.loadavg | awk '{print $2}'); awk -v l="$l" 'BEGIN{exit !(l<4)}' && break; sleep 60; done
  echo "-- host load before timing step: $(sysctl -n vm.loadavg) at $(date)"
}
cd "$(dirname "$0")/.."
export VTOP_BUILD=v0.10.2
PY=.venv/bin/python
R=experiments
D=20261121-v0102
V=../.tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin/vtopctl
echo "== vtopctl $($V --version) (0abf116, release archive, SHA256SUMS + cosign checked) $(date)"
echo "-- order: correctness first, timing last (RQ3 waits for a quiet host)"
echo "== RQ1 crash campaign (power loss: WAL truncated to the last fsynced commit) $(date)"
echo "-- load $(sysctl -n vm.loadavg)"; $PY $R/rq1-source-safety/code/exp_crash.py --out $R/rq1-source-safety/results/$D-crash --trials 10 --kills 8
echo "== RQ1 Kafka campaign $(date)"
echo "-- load $(sysctl -n vm.loadavg)"; $PY $R/rq1-source-safety/code/exp_kafka.py --out $R/rq1-source-safety/results/$D-kafka
echo "== RQ2 tampering $(date)"
echo "-- load $(sysctl -n vm.loadavg)"; $PY $R/rq2-corruption-recovery/code/exp_tamper.py --out $R/rq2-corruption-recovery/results/$D-tamper --datasets ssh,linux --reps 2
echo "== RQ2 V-norecheck ablation (build_norecheck.sh v0.10.2) $(date)"
echo "-- load $(sysctl -n vm.loadavg)"; $PY $R/rq2-corruption-recovery/code/exp_norecheck.py --out $R/rq2-corruption-recovery/results/$D-norecheck --datasets ssh,linux --reps 2
echo "== RQ4 VTOP side (the three sinks are unchanged and not re-run) $(date)"
echo "-- load $(sysctl -n vm.loadavg)"; $PY $R/rq4-baselines/code/exp_baseline.py --out $R/rq4-baselines/results/$D-rq4 --systems vtop --cells ssh-p1:corrupt:10,ssh-p1:lost:10,hdfs-p4:kill:5
echo "== RQ3 throughput, variants $(date)"
quiet; $PY $R/rq3-cost-ablation/code/exp_cost.py --mode run --out $R/rq3-cost-ablation/results/$D-cost-run --datasets ssh,bgl,hdfs --reps 10
echo "== RQ3 throughput, batch sweep $(date)"
quiet; $PY $R/rq3-cost-ablation/code/exp_cost.py --mode run --batch-sweep --out $R/rq3-cost-ablation/results/$D-cost-run --datasets bgl,hdfs --reps 5
echo "== RQ3 per-stage cost $(date)"
quiet; $PY $R/rq3-cost-ablation/code/exp_cost.py --out $R/rq3-cost-ablation/results/$D-cost
echo "== done $(date)"
