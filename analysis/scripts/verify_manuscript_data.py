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
require(cost["ssh"]["sha256+mac"] == 1.1, "smallest compressed estimate is 1.1% (SSH, MAC)")
require("1.1--14.1\\%" in TEXT, "RQ3 compressed range appears in manuscript")
ci = SUMMARY["rq3"]["throughput_drop_vs_size_only_ci"]
comp = ["sha256", "blake3", "sha256+mac"]
require(all(ci["bgl"][v][1] > 0 for v in comp), "every BGL compressed drop CI is above zero")
require(all(ci[d][v][1] <= 0 <= ci[d][v][2] for d in ["ssh", "hdfs"] for v in comp),
        "SSH and HDFS compressed drop CIs include zero")
require(min(ci["bgl"][v][0] for v in comp) == 6.6 and max(ci["bgl"][v][0] for v in comp) == 9.9,
        "BGL compressed drops are 6.6-9.9%")
require(ci["ssh"]["sha256-nocomp"][1] > 0 and ci["bgl"]["sha256-nocomp"][1] > 0
        and ci["hdfs"]["sha256-nocomp"][1] <= 0, "uncompressed drop resolved on SSH and BGL, not HDFS")
require("6.6--9.9\\%" in TEXT, "BGL resolved range appears in manuscript")
require(all(ci_[1] <= 0 <= ci_[2] for ci_ in SUMMARY["rq3"]["mac_drop_vs_sha256_ci"].values()),
        "MAC vs SHA-256 difference not resolved on any dataset")
ratio = SUMMARY["rq3"]["compression_ratio_median"]
require(min(ratio.values()) == 10.3 and max(ratio.values()) == 14.3, "compression ratios 10.3-14.3x")
require("10.3--14.3$\\times$" in TEXT, "compression ratio range appears in manuscript")

nr = SUMMARY["rq2_norecheck"]
require(nr["mutations"] == 32 and nr["mutations_accepted"] == 32 and nr["controls"] == 4, "ablation: 32/32 mutations accepted, 4 controls")
require(nr["unsafe_commits"] == 24 and all(nr["unsafe_by_mutation"][m] == 4 for m in ("M1", "M2", "M3", "M4", "M7", "M8"))
        and nr["unsafe_by_mutation"]["M5"] == 0 and nr["unsafe_by_mutation"]["M6"] == 0, "ablation: 24 unsafe (M1-M4, M7, M8)")
FLAT = " ".join(TEXT.split())   # whitespace-normalised, for phrases that wrap
rq4 = SUMMARY["rq4"]
c4 = rq4["cells"]
for f in ("corrupt", "lost"):
    conf, vt = c4[f"{f}|confluent"], c4[f"{f}|vtop"]
    require(conf["unsafe_trials"] == conf["trials"] > 0, f"Confluent committed past damage in every {f} trial")
    require(conf["lost_committed_records_total"] == 40_000 * conf["trials"], f"Confluent {f}: 40,000 lost per trial")
    require(vt["unsafe_trials"] == 0 and vt["lost_committed_records_total"] == 0, f"VTOP {f}: no unsafe commit")
require(c4["kill|vtop"]["unsafe_trials"] == 0 and c4["kill|confluent"]["unsafe_trials"] == 0
        and c4["kill|confluent"]["lost_committed_records_total"] == 0, "no loss under kills")
require(c4["kill|confluent"]["duplicate_objects_median"] == 0, "Confluent wrote no duplicate objects under kills")
require(rq4["vtop_incomplete_all_timed_out"], "every incomplete VTOP trial is a stall (drain timeout)")
require(f"in {rq4['vtop_incomplete']} of {rq4['vtop_trials_run']}" in FLAT, "RQ4 stall count matches the text")
require(f"the {['zero','one','two','three','four'][rq4['vtop_stalls_before_fault']]} stalls before the injected fault"
        in FLAT, "RQ4 pre-fault stall count matches the text")
require("accepted all 32 mutations. In 24 of them" in FLAT, "ablation numbers appear in the manuscript")
require("tab:rq4" in TEXT and "rq4_baselines" in TEXT, "RQ4 table is in the paper")

for name in [f"suliman-fig{i}.pdf" for i in range(1, 5)] + ["suliman-figS1.pdf", "suliman-figS2.pdf", "suliman-figS3.pdf"]:
    require((ROOT / "VTOP-IEEE-TCC/figures" / name).exists(), f"{name} exists")
for name in ["rq1_model.tex", "rq1_crash.tex", "rq2_tamper.tex", "rq3_cost.tex",
             "fault_matrix.tex", "comparison.tex", "notation.tex"]:
    require((ROOT / "VTOP-IEEE-TCC/tables" / name).exists(), f"{name} exists")
require("alg:recovery" in TEXT, "conservative recovery algorithm is present")
for sink in ("vector", "fluentbit"):
    for f in ("corrupt", "lost"):
        cc = c4[f"{f}|{sink}"]
        require(cc["trials"] == 10 and cc["unsafe_trials"] == 10, f"{sink} {f}: committed past damage in 10/10")
    require(c4[f"kill|{sink}"]["unsafe_trials"] == 0 and c4[f"kill|{sink}"]["lost_committed_records_total"] == 0,
            f"{sink} kill: no loss in the final state")
require(c4["kill|fluentbit"]["transient_at_kill_events"] == 6 and c4["kill|fluentbit"]["kills"] == 15 and
        c4["kill|fluentbit"]["transient_at_kill_trials"] == 5 and "6 of 15 kill events, across all five trials" in FLAT,
        "Fluent Bit transient gap at kills is reported")
require("Confluent S3 Sink, Vector, and Fluent Bit advanced progress" in FLAT, "abstract names the three sinks")
m3 = SUMMARY["rq3_minio"]; ch = m3["change_vs_size_only_pct"]
require(m3["matrix_runs"] == 150 and m3["oracle_runs"] == 15, "S3 cost campaign: 150 clean runs, 15 oracle cells")
require(all(ch[ds][v][1] <= 0 <= ch[ds][v][2] for ds in ch for v in ("sha256", "blake3")),
        "S3: store SHA-256 and read-back have no resolved cost on any dataset")
require(all(ch[ds]["sha256-nocomp"][2] < 0 for ds in ch), "S3: uncompressed drop resolved on every dataset")
lat = SUMMARY["rq3_latency"]
require(lat["20"]["matrix_runs"] == 90 and lat["50"]["matrix_runs"] == 45 and lat["20"]["all_drained"]
        and lat["50"]["all_drained"], "latency campaign: 135 runs, all drained")
for lvl, txt in (("20", "19--23"), ("50", "24--27")):
    c = lat[lvl]["change_vs_size_only_pct"]
    require(all(c[ds]["blake3"][2] < 0 for ds in c), f"+{lvl} ms: read-back cost resolved on every dataset")
    require(all(c[ds]["sha256"][1] <= 0 <= c[ds]["sha256"][2] for ds in c), f"+{lvl} ms: store SHA-256 cost unresolved")
    lo_, hi_ = round(min(-c[ds]["blake3"][0] for ds in c)), round(max(-c[ds]["blake3"][0] for ds in c))
    require(f"{lo_}--{hi_}" == txt and txt in FLAT, f"+{lvl} ms read-back range {lo_}--{hi_} matches the text")
lo = min(-ch[ds]["sha256-nocomp"][0] for ds in ch); hi = max(-ch[ds]["sha256-nocomp"][0] for ds in ch)
require(f"{lo:.0f}--{hi:.0f}" in FLAT, f"S3 uncompressed drop range {lo:.0f}--{hi:.0f} appears in the manuscript")
ABL = (ROOT / "research/state-machine/results/20261111-rule-ablation.md").read_text()
require("| 4,1,2,2,1,1 | 793,353 / holds | 833,440 / holds | 877 / **I2 violated** |" in ABL
        and "| 3,1,1,0,0,0 | 187 / holds | 187 / holds | 38 / **I2 violated** |" in ABL,
        "rule-ablation record has the reported counts")
grp = {(o["variant"], o["per_cycle"]): [] for o in SUMMARY["rq1_model"]}
for o in SUMMARY["rq1_model"]:
    grp[(o["variant"], o["per_cycle"])].append(o)
nh, no = grp[("nohandback", 1)], grp[("noorder", 1)]
require(len(nh) == 8 and sum(o["result"] != "holds" for o in nh) == 5
        and min(o["states"] for o in nh if o["result"] != "holds") == 38, "no-hand-back row matches the sweep")
require(len(no) == 8 and all(o["result"] == "holds" for o in no)
        and max(o["states"] for o in no) == 833440, "no-read-order row matches the sweep")
require("833{,}440 states" in FLAT and "No hand-back & 1 & 8 & 25--877 & 5 & 38" in (ROOT / "VTOP-IEEE-TCC/tables/rq1_model.tex").read_text(),
        "rule-ablation counts appear in the manuscript")

print(f"Verified {len(checks)} manuscript data and artifact checks.")
