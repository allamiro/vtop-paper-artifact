#!/usr/bin/env python3
"""Verify key manuscript claims against generated experiment summaries."""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]
SUMMARY = json.loads((ROOT / "research/analysis/summary.json").read_text())
TEXT = "\n".join((ROOT / "VTOP-IEEE-TCC" / p).read_text()
                 for p in ["main.tex", *["sections/" + f for f in [
                     "01_introduction.tex", "03_system_model.tex", "04_protocol.tex",
                     "05_correctness.tex", "06_implementation.tex", "07_experiments.tex",
                     "08_discussion.tex", "09_conclusion.tex"]]])
checks = []


def require(condition, label):
    if not condition:
        raise AssertionError(label)
    checks.append(label)


model = SUMMARY["rq1_model"]
largest = max(r["states"] for r in model if r["variant"] == "v070" and r["per_cycle"] == 1)
v060_bad = next(r for r in model if r["variant"] == "v060" and r["result"] == "I2 violated")
v070_b2_bad = next(r for r in model if r["variant"] == "v070" and r["per_cycle"] == 2 and r["result"] == "I2 violated")
require(largest == 793353, "largest v0.7.0 model has 793,353 states")
require(v060_bad["states"] == 32, "v0.6.0 counterexample has 32 states")
require(v070_b2_bad["states"] == 73, "v0.7.0 B=2 counterexample has 73 states")
for value in ("793{,}353", "32 states", "73 states"):
    require(value in TEXT, f"manuscript includes {value}")

crash = SUMMARY["rq1_crash"]
require(sum(v["trials"] for v in crash.values()) == 49, "49 trials match")
require(sum(v["kills"] for v in crash.values()) == 285, "285 kills match")
require(sum(v["outages"] for v in crash.values()) == 42, "42 outages match")
require(sum(v["wal_deleted"] for v in crash.values()) == 80, "80 WAL deletions match")
require(sum(v["unsafe_commits"] for v in crash.values()) == 0, "zero unsafe commits match")
require(sum(v["complete_trials"] for v in crash.values()) == 48, "48 complete trials match")
require(sum(v["manifest_start_understated"] for v in crash.values()) == 3955, "3,955 understated manifests match")
require(sum(v["manifests"] for v in crash.values()) == 4056, "4,056 total manifests match")
for value in ("49", "285", "42", "80", "3{,}955 of 4{,}056"):
    require(value in TEXT, f"manuscript includes {value}")
require(crash["kafka-hdfs-p4"]["unarchived_at_end_uncommitted"] == 80181,
        "Kafka stall leaves 80,181 records unarchived and uncommitted")

rq2 = SUMMARY["rq2"]["cells"]
strong_refused = sum(rq2[f"M{i}"][variant]["detected"]
                     for i in range(1, 9) for variant in ("strong", "strong+mac"))
require(strong_refused == 64, "64 strong-mode mutations refused")
require(rq2["M1"]["size-only"]["detected"] == 0, "size-only M1 not detected")
require(rq2["M1"]["size-only"]["trials"] == 4, "size-only M1 has four trials")
require("64" in TEXT and "all four trials" in TEXT, "RQ2 counts appear in manuscript")

power = crash["power-loss"]
require(power["dup_objects_median_ci"][0] == 108, "power-loss duplicate median is 108")
require(round(power["replay_amp_pct_median_ci"][0], 1) == 108.9, "power-loss replay median is 108.9%")
require("108" in TEXT and "108.9\\%" in TEXT, "power-loss figures appear in manuscript")

cost = SUMMARY["rq3"]["throughput_drop_vs_size_only_pct"]
require(cost["ssh"]["sha256"] == 2.2, "SSH strong-verification estimate is 2.2%")
require(cost["bgl"]["sha256"] == 8.0, "BGL strong-verification estimate is 8.0%")
require(cost["hdfs"]["sha256"] == 14.1, "HDFS strong-verification estimate is 14.1%")
require(cost["ssh"]["sha256-nocomp"] == 20.4, "SSH uncompressed cost is 20.4%")
require(cost["bgl"]["sha256-nocomp"] == 13.9, "BGL uncompressed cost is 13.9%")
require(cost["hdfs"]["sha256-nocomp"] == 12.3, "HDFS uncompressed cost is 12.3%")
require("2.2--14.1\\%" in TEXT and "12.3--20.4\\%" in TEXT,
        "RQ3 percentage ranges appear in manuscript")

for name in [f"suliman-fig{i}.pdf" for i in range(1, 7)]:
    require((ROOT / "VTOP-IEEE-TCC/figures" / name).exists(), f"{name} exists")
for name in ["rq1_model.tex", "rq1_crash.tex", "rq2_tamper.tex", "rq3_cost.tex",
             "fault_matrix.tex", "comparison.tex", "notation.tex"]:
    require((ROOT / "VTOP-IEEE-TCC/tables" / name).exists(), f"{name} exists")
require("alg:recovery" in TEXT, "conservative recovery algorithm is present")

print(f"Verified {len(checks)} manuscript data and artifact checks.")
