#!/usr/bin/env python3
"""Write research/analysis/v0102/NUMBERS.md: every manuscript number in the change ledger
(research/drafts/v0101-paper-changes.md, rows E1-E12, X1, T1 and the model rows C3/C4) with its v0.7.0 value, its
v0.10.2 value and CI, the source, and whether the claim's direction changed.

Every value is read from research/analysis/summary.json (v0.7.0) and research/analysis/v0102/summary.json (v0.10.2),
or computed here from run files with analyze.py's own helpers (batch sizes, power-loss amplification range).
Numbers that no file supports are written as "no counterpart" or "not computable", never estimated.

  research/.venv/bin/python research/analysis/scripts/analyze.py --set v0102
  research/.venv/bin/python research/analysis/scripts/make_numbers_v0102.py
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze as A  # noqa: E402  (pure helpers only: log_batch_sizes, run_mode_batch_sizes, jl)

RES = Path(__file__).resolve().parents[2]
EXP = RES / "experiments"
O = json.loads((RES / "analysis/summary.json").read_text())
S = json.loads((RES / "analysis/v0102/summary.json").read_text())
OUT = RES / "analysis/v0102/NUMBERS.md"


def ci(v, nd=1, pct=False):
    u = "%" if pct else ""
    return f"{v[0]:.{nd}f}{u} [{v[1]:.{nd}f}, {v[2]:.{nd}f}]"


def rng(xs, nd=1, u=""):
    return f"{min(xs):.{nd}f}–{max(xs):.{nd}f}{u}"


DS = ["ssh", "bgl", "hdfs"]
COMP = ["sha256", "blake3", "sha256+mac"]
oc, sc = O["rq1_crash"], S["rq1_crash"]
ot = lambda k: sum(v[k] for v in oc.values())  # noqa: E731
st = lambda k: sum(v[k] for v in sc.values())  # noqa: E731
FILES = ["file-mix", "spool-mix", "power-loss", "hdfs"]

# ---- power loss: v0.7.0 amplification range from trials (not in summary.json)
pl070 = [r["replay_amplification"] * 100 for r in A.jl(EXP / "rq1-source-safety/results/20261110-v070-crash/trials.jsonl")
         if r["scenario"] == "power-loss"]
pl = sc["power-loss"]

# ---- RQ3 local
oq, sq = O["rq3"], S["rq3"]
o_ci, s_ci = oq["throughput_drop_vs_size_only_ci"], sq["throughput_drop_vs_size_only_ci"]


def stage_ranges(q):
    s = q["stage_mean_ms_per_batch"]
    cv = [s[f"{d}|{v}"]["checksum_ms"] + s[f"{d}|{v}"]["verify_ms"] for d in DS for v in COMP]
    cm = [s[f"{d}|{v}"]["compress_ms"] for d in DS for v in ["size-only"] + COMP]
    nc = [s[f"{d}|sha256-nocomp"]["checksum_ms"] + s[f"{d}|sha256-nocomp"]["verify_ms"] for d in DS]
    # largest per-stage change from adding the MAC, compression excluded (compression does not depend on the MAC)
    mac = [max(abs((s[f"{d}|sha256+mac"][k] or 0) - (s[f"{d}|sha256"][k] or 0)) for k in s[f"{d}|sha256"] if k != "compress_ms")
           for d in DS]
    return cv, cm, nc, mac


ocv, ocm, onc, omac = stage_ranges(oq)
scv, scm, snc, smac = stage_ranges(sq)
otp = [v[0] for k, v in oq["throughput_mib_s"].items() if k.split("|")[0] in DS and k.split("|")[1] in
       ["size-only"] + COMP + ["sha256-nocomp"]]
stp = [v[0] for k, v in sq["throughput_mib_s"].items() if k.split("|")[0] in DS and k.split("|")[1] in
       ["size-only"] + COMP + ["sha256-nocomp"]]
ocpu = [v[0] for k, v in oq["cpu_s_per_gib"].items() if k.split("|")[0] in DS and k.split("|")[1] in ["size-only"] + COMP + ["sha256-nocomp"]]
scpu = [v[0] for k, v in sq["cpu_s_per_gib"].items() if k.split("|")[0] in DS and k.split("|")[1] in ["size-only"] + COMP + ["sha256-nocomp"]]

# ---- batch sizes (X1), v0.7.0 computed now with the same helpers
o_run = A.run_mode_batch_sizes(EXP / "rq3-cost-ablation/results/20261110-v070-cost-run")
o_crash = A.log_batch_sizes(EXP / "rq1-source-safety/results/20261110-v070-crash")
o_k1 = A.log_batch_sizes(EXP / "rq1-source-safety/results/20261110-v070-kafka")
o_k4 = A.log_batch_sizes(EXP / "rq4-baselines/results/20261111-v070-rq4", only_prefix="vtop-")
bs = S["batch_sizes"]

# ---- RQ4, cloud, model
o4, s4 = O["rq4"], S["rq4"]
mc = S["rq5_cost"]["t4g.medium"]["cells"]
lc = S["rq5_cost"]["t4g.large"]["cells"]["hdfs"]
W, WC = S["rq5_wan"], S["rq5_wan"]["claims"]
# 5% loss slowdown from the drain medians (the stored x_native is rounded to 2 places first)
X5 = [W["regions"][g]["loss"]["loss5"]["drain_s"] / W["regions"][g]["native"]["sha256-store"]["drain_s"] for g in W["regions"]]
G, MC = S["rq1_model_grouped"], S["rq1_model_claims"]
om = O["rq1_model"]
o_b1 = max(r["states"] for r in om if r["variant"] == "v070" and r["per_cycle"] == 1)
o_stalled = None  # v0.7.0 stall counts are in state-machine/results/20261120-v010-sweep.md, not in summary.json
lat = O["rq3_latency"]
lat_rb = {lv: [-lat[lv]["change_vs_size_only_pct"][d]["blake3"][0] for d in DS] for lv in ("20", "50")}
mino = O["rq3_minio"]["change_vs_size_only_pct"]

CH, SAME, NEW, NONE = "**CHANGED**", "same", "new", "**NO v0.10.2 EVIDENCE**"
rows = []


def row(rid, where, claim, v070, v0102, src, direction):
    rows.append((rid, where, claim, v070, v0102, src, direction))


# ================================================================ T1, C3, C4: model (Table 3, §V-D, §I, abstract)
row("T1/C3", "Table 3, §V-D, §I contrib., §IX", "largest safe configuration of the protocol",
    f"{o_b1:,} states (B = 1)", f"{MC['largest_safe_states']:,} states (B = 2); B = 1: {MC['largest_safe_B1_states']:,}",
    "model v010 sweep (`state-machine/results/20261120-v010-sweep.json`)", SAME + " (safety holds; number changes)")
row("T1", "abstract, §I, §III-F, Thm. 1, §V-D, §IX, cover letter",
    "safety needs failed-range rewind and at most one pending batch per unit; two pending batches (B = 2) "
    "commit past a failed range",
    f"protocol at B = 2: {sum(1 for r in om if r['variant']=='v070' and r['per_cycle']==2 and r['result']!='holds')} of "
    f"{sum(1 for r in om if r['variant']=='v070' and r['per_cycle']==2)} configs violate I2, smallest 73 states",
    f"protocol at B = 2: 0 of {G['v010|B2']['configs']} violate ({G['v010|B2']['states_min']:,}–"
    f"{G['v010|B2']['states_max']:,} states); B = 3: 0 of {G['v010|B3']['configs']}; without hold-back B = 2: "
    f"{G['v010-nohold|B2']['violated']} of {G['v010-nohold|B2']['configs']} violate, smallest "
    f"{G['v010-nohold|B2']['smallest_violation']} states",
    "model v010 sweep", CH + ": the one-pending-batch condition is replaced by ordered commit with hold-back; "
    "the B = 2 violation now needs hold-back removed")
row("C4", "§V 05:181, §I", "guard-only (v0.6.0 logic) counterexample",
    "32 states", f"{MC['guard_only_smallest_violation']} states (reference row: v060 model path unchanged, reproduced "
    "line for line per 20261120-v010-sweep.md; not re-run in the v010 sweep)",
    "`rq1-source-safety/results/20261110-model-sweep/sweep.json`", SAME)
row("C3", "Table 3 row 'No hand-back', §I 'hand-back necessary'", "no hand-back violates I2 (5 of 8 configs, smallest 38)",
    "5/8, 38 states", "not ablated on v010. Closest v010 rows: one *refused* hand-back (the source refuses the "
    f"re-read) violates I2 at {MC['refusal_smallest_violation']['1']} (B = 1) and {MC['refusal_smallest_violation']['2']} "
    "states (B = 2); that models Assumption *Replayability*, not an engine without hand-back",
    "model v010 sweep", NONE + " for the engine ablation")
row("C3", "Table 3 row 'No read-order', §I, §V 05:186", "read-order commit not shown necessary (up to 833,440 states)",
    "0/8 violated, ≤ 833,440 states", "not ablated on v010 (\"read-order commit at B ≥ 2 was not ablated\", sweep.md)",
    "—", NONE)
row("C3/R5", "§V-D, §VIII-B", "states in which a range stays uncommitted until restart (stall)",
    "up to 6,747 stalled states (sweep.md; not in summary.json)", f"0 (protocol, monotone point); robustness without "
    f"monotonicity: up to {S['rq1_model_robustness']['nonmonotone']['protocol_stalled_max']:,}; Kafka newest-row resume: "
    f"{S['rq1_model_robustness']['resume_newest']['protocol_stalled_max']}",
    "v010 sweep + `-nonmonotone`, `-resume-newest`", CH + " (stall path removed in the model)")
row("C3", "§V-D", "commit over damaged evidence (D2) states",
    "up to 3,576 (sweep.md; not in summary.json)", f"{MC['protocol_D2_max']} (non-monotone robustness: up to "
    f"{S['rq1_model_robustness']['nonmonotone']['protocol_D2_max']:,})", "v010 sweeps", CH)
row("C3 (new)", "§III Asm. *Replayability*", "replayable source shown necessary", "—",
    f"one refused hand-back violates I2 ({MC['refusal_smallest_violation']['1']} / "
    f"{MC['refusal_smallest_violation']['2']} states at B = 1 / 2)", "v010 sweep", NEW)

# ================================================================ E1: RQ1 crash, Table 4
row("E1", "Table 4, §VII-C, abstract, §I, §IX", "crash and outage trials", f"{ot('trials')}", f"{st('trials')}",
    "`rq1-source-safety/results/20261121-v0102-{crash,kafka}/trials.jsonl`", SAME)
row("E1", "Table 4, §VII-C", "random kills", f"{ot('kills')} (files/spools {sum(oc[s]['kills'] for s in FILES)}, "
    f"Kafka {oc['kafka-ssh-p1']['kills'] + oc['kafka-hdfs-p4']['kills']})",
    f"{st('kills')} (files/spools {sum(sc[s]['kills'] for s in FILES)}, Kafka "
    f"{sc['kafka-ssh-p1']['kills'] + sc['kafka-hdfs-p4']['kills']})", "same", SAME + " (count differs; random kill times)")
row("E1", "§VII-C", "storage or broker outages", f"{ot('outages')}", f"{st('outages')}", "same", SAME)
row("E1", "Table 4, abstract, §I, §IX", "unsafe commits (at any crash snapshot or final)", f"{ot('unsafe_commits')}",
    f"{st('unsafe_commits')}", "same", SAME)
row("E1", "Table 4, §VII-C, §IX", "complete archives", f"{ot('complete_trials')}/{ot('trials')}",
    f"{st('complete_trials')}/{st('trials')}", "same", CH + " (the one incomplete v0.7.0 trial was the Kafka stall)")
row("E1", "§VII-C 'Kafka liveness', §I, §VIII-B, §IX", "Kafka partition stall; records left unarchived",
    f"1 of {oc['kafka-ssh-p1']['trials'] + oc['kafka-hdfs-p4']['trials']} Kafka trials; "
    f"{oc['kafka-hdfs-p4']['unarchived_at_end_uncommitted']:,} records", f"0 of "
    f"{sc['kafka-ssh-p1']['trials'] + sc['kafka-hdfs-p4']['trials']}; 0 records",
    "same", CH + " (negative finding no longer observed)")
row("E1", "§VII-C", "duplicate objects per trial under random kills (max)",
    f"≤ {max(oc[s]['dup_objects_max'] for s in oc if s != 'power-loss')} (spool-mix)",
    f"{max(sc[s]['dup_objects_max'] for s in sc)} in every trial", "same", CH + " (no duplicates observed)")
row("E1", "§VII-C 'Manifest fidelity', §I, §VIII-B, §IX", "manifests that understate their start (F1)",
    f"{ot('manifest_start_understated'):,} of {ot('manifests'):,}", f"{st('manifest_start_understated')} of {st('manifests'):,}",
    "same", CH + " (defect fixed, #622; negative finding gone)")
row("E1", "H3.3 (not in the paper text)", "restart to first commit, median [CI] s: files / spool / HDFS",
    " / ".join(f"{oc[s]['first_commit_after_restart_s_median_ci'][0]:.2f}" for s in ("file-mix", "spool-mix", "hdfs")),
    " / ".join(ci(sc[s]['first_commit_after_restart_s_median_ci'], 2) for s in ("file-mix", "spool-mix", "hdfs")),
    "same", SAME)
row("E1", "RESULTS.md", "Kafka final drain, median s", "—",
    f"{statistics.median(r['final_drain_s'] for r in A.jl(EXP / 'rq1-source-safety/results/20261121-v0102-kafka/trials.jsonl')):.1f}",
    "same", NEW)

# ================================================================ E2: power loss
row("E2", "§VII-A faults (iii), Table 4, §VIII-C", "power-loss emulations and method",
    f"{ot('wal_deleted')} whole-WAL deletions after a kill",
    f"{pl['power_cuts']} WAL truncations to the last fsynced VERIFIED/SOURCE_COMMITTED commit; "
    f"{pl['power_cuts_dropping_commits']} of them dropped ledger commits ({pl['power_cut_dropped_commits_total']} in all), "
    f"{pl['power_cuts_empty_wal']} found an empty WAL", "`20261121-v0102-crash/trials.jsonl` (`events[].power_cut`)",
    CH + " (method)")
row("E2", "Table 4, §VII-C, §VIII-C, H1.4", "duplicate objects per power-loss trial, median [95% CI]",
    ci(oc["power-loss"]["dup_objects_median_ci"], 0), ci(pl["dup_objects_median_ci"], 0), "same",
    CH + " (see the power-loss note: two causes, not separable)")
row("E2", "Table 4, §VII-C, §VIII-C", "replayed source bytes, median (range)",
    f"{oc['power-loss']['replay_amp_pct_median_ci'][0]:.1f}% ({min(pl070):.0f}–{max(pl070):.0f}%)",
    f"{pl['replay_amp_pct_median_ci'][0]:.1f}% (0–0%)", "same", CH)

# ================================================================ E3, E10: RQ2
c2 = S["rq2"]["cells"]
row("E3", "Table 5, §VII-C, abstract, §I, §IX", "strong-mode mutations refused at restart",
    "64/64", f"{sum(c2[f'M{i}'][v]['detected'] for i in range(1, 9) for v in ('strong', 'strong+mac'))}/64",
    "`rq2-corruption-recovery/results/20261121-v0102-tamper/trials.jsonl`", SAME)
row("E3", "Table 5, abstract, §I", "size-only accepted the same-size byte flip", "4/4 trials",
    f"{c2['M1']['size-only']['trials'] - c2['M1']['size-only']['detected']}/{c2['M1']['size-only']['trials']} trials", "same", SAME)
row("E3", "§VII-C", "tampering trials", "108", f"{S['rq2']['trials']}", "same", SAME)
nr = S["rq2_norecheck"]
row("E3/I2", "§VII-C, §VI variants", "no-recheck ablation: mutations accepted; unsafe commits",
    "32/32; 24", f"{nr['mutations_accepted']}/{nr['mutations']}; {nr['unsafe_commits']}",
    "`20261121-v0102-norecheck/trials.jsonl` (patch on the v0.10.2 tag)", SAME)
TT = S["rq5_tamper"]
row("E10", "§VII-C, §III-B, §VIII", "claim scope: detects corruption of stored objects", "RQ2 evidence",
    f"RQ2 evidence plus E-RQ5-TAMPER on Amazon S3 ({TT['cases']} cases, {len(TT['limit_cases'])} measured NOT-CAUGHT "
    "limits; see E13)", "`rq5_tamper`", SAME + "; tamper-evidence wording only as far as E13 (stated limits beside it)")
row("E13 (new)", "§VII-C, Table (rq5_tamper), Supp.", "E-RQ5-TAMPER: signed manifests, versioning, Object Lock, audit chain (H2.5–H2.7)",
    "—", f"{TT['cases']} cases ({', '.join(f'{h} {sum(v.values())}' for h, v in TT['cases_by_hypothesis'].items())}); "
    f"outcomes {TT['outcomes']}; forged content committed only in {', '.join(TT['forged_content_committed'])}; "
    f"S3 refused {len(TT['store_refusals'])} Object Lock / versioning requests; AWS = local in "
    f"{min(a['agree'] for a in TT['agreement_with_local'].values())}/"
    f"{min(a['comparable'] for a in TT['agreement_with_local'].values())} comparable cases (3 local runs)",
    "`rq5-cloud/tamper/results/20261121T143301Z-i-0938589e4d715ff8d/cases.jsonl`", NEW + " (one AWS repetition)")
SC = S["rq5_scale"]
row("E14 (new)", "§VII-D, Table (rq5_scale)", "E-RQ5-SCALE: independent engines on separate hosts, one bucket (H3.7)", "—",
    "aggregate " + " / ".join(f"N={n}: {SC['per_n'][n]['aggregate_mib_s'][0]:.1f}" for n in SC["per_n"]) + " MiB/s; "
    f"efficiency {SC['claims']['efficiency_point_range'][0]:.3f}–{SC['claims']['efficiency_point_range'][1]:.3f} "
    f"(CI lower bounds ≥ {SC['claims']['efficiency_ci_low_min']:.3f}); {SC['engine_runs_passing']}/{SC['engine_runs']} "
    f"engine-runs pass, {SC['unsafe']} unsafe, {SC['claims']['s3_retries_total']} retries, "
    f"{SC['claims']['s3_throttle_verdicts_total']} throttling; peak {SC['claims']['peak_s3_attempts_per_s_n8']} S3 attempts/s",
    "`rq5-cloud/scale/results/20261121T153823Z-scale` (engines.jsonl, steps.csv)", NEW + " (one region, one AZ, one window)")

# ================================================================ E4: RQ3 local
row("E4", "§VII-D, §I contrib.", "compressed-object verification cost vs size-only (range of medians)",
    rng([o_ci[d][v][0] for d in DS for v in COMP], 1, "%"), rng([s_ci[d][v][0] for d in DS for v in COMP], 1, "%"),
    "`summary.json` → `rq3.throughput_drop_vs_size_only_ci`", SAME + " (range narrows)")
res_o = [f"{d.upper()} {v}" for d in DS for v in COMP if o_ci[d][v][1] > 0]
unres_s = [f"{d.upper()} {v}" for d in DS for v in COMP if s_ci[d][v][1] <= 0]
row("E4", "§VII-D, §I contrib. ('resolved only on BGL'), §VIII-C, §IX", "where the compressed cost is resolved (CI on the "
    "difference of medians above 0)", f"BGL only ({rng([o_ci['bgl'][v][0] for v in COMP], 1, '%')}); SSH, HDFS CIs include 0",
    f"every dataset: SSH, BGL, HDFS ({rng([s_ci[d][v][0] for d in DS for v in COMP if s_ci[d][v][1] > 0], 1, '%')}); "
    f"unresolved only {', '.join(unres_s)} ({ci(s_ci['ssh']['sha256+mac'], 1, True)})",
    "same", CH + ": cost is now resolved on all three datasets (smaller, 4–7%); 'resolved only on BGL' no longer holds")
row("E4", "§VII-D, §VIII-C", "uncompressed cost vs size-only, SSH / BGL / HDFS [CI]",
    " / ".join(ci(o_ci[d]["sha256-nocomp"], 1, True) for d in DS) + " (HDFS unresolved)",
    " / ".join(ci(s_ci[d]["sha256-nocomp"], 1, True) for d in DS) + " (all resolved)", "same",
    CH + " (all resolved; SSH/BGL smaller)")
row("E4", "§VII-D", "MAC vs SHA-256 alone: difference resolved?", "no (all CIs include 0)",
    "no (" + "; ".join(f"{d.upper()} {ci(sq['mac_drop_vs_sha256_ci'][d], 1, True)}" for d in DS) + ")",
    "`rq3.mac_drop_vs_sha256_ci`", SAME)
row("E4", "§VII-D, H3.2", "MAC: largest change of any non-compression stage per 16 MiB batch, sha256+mac vs sha256",
    f"{max(omac):.2f} ms (text: under 1 ms)", f"{max(smac):.2f} ms", "`rq3.stage_mean_ms_per_batch`",
    SAME + (" (under 1 ms)" if max(smac) < 1 else " — **check**: not under 1 ms"))
row("E4", "§VII-D", "zstd size reduction per 16 MiB batch (median)",
    f"{min(oq['compression_ratio_median'].values())}–{max(oq['compression_ratio_median'].values())}×",
    f"{min(sq['compression_ratio_median'].values())}–{max(sq['compression_ratio_median'].values())}×",
    "`rq3.compression_ratio_median` (process-once batches.csv)", SAME)
row("E4", "§VII-D, Fig. S2", "checksum + verify per 16 MiB batch, compressed (ms)", rng(ocv, 1), rng(scv, 1) +
    " (engine logs whole ms; means of 0/1 ms values)", "`rq3.stage_mean_ms_per_batch`", SAME)
row("E4", "§VII-D, Fig. S2", "compression per 16 MiB batch (ms)", rng(ocm, 1), rng(scm, 1), "same", SAME)
row("E4", "§VII-D, Fig. S2", "checksum + verify, uncompressed (ms)", rng(onc, 1), rng(snc, 1), "same", SAME)
row("E4", "Table S1, Fig. S3, RESULTS.md", "median throughput, 15 cells (MiB/s)", rng(otp, 0), rng(stp, 0),
    "`rq3.throughput_mib_s` (run mode)", SAME + " (but see X1: v0.7.0 batches were ~31 MiB)")
row("E4", "Table S1", "CPU s per GiB (range of medians)", rng(ocpu, 2), rng(scpu, 2), "`rq3.cpu_s_per_gib`", SAME)
bo, bn = oq["batch_sweep"]["datasets"], sq["batch_sweep"]["datasets"]
row("E4", "§VII-D, Fig. S1, H3.4", "BGL throughput 1 → 64 MiB batches, median (MiB/s)",
    f"{bo['bgl']['1'][0]:.0f} → {bo['bgl']['64'][0]:.0f}", f"{ci(bn['bgl']['1'], 0)} → {ci(bn['bgl']['64'], 0)}",
    "`rq3.batch_sweep`", SAME + " (v0.7.0 'batch target' was ~2× the setting, X1)")
mo, mn = oq["batch_sweep"]["manifest_overhead_pct"], sq["batch_sweep"]["manifest_overhead_pct"]
row("E4", "Fig. S1, H3.4", "manifest bytes / stored bytes at 1 / 16 / 64 MiB",
    " / ".join(f"{mo[k][0]:.3f}%" for k in ("1", "16", "64")), " / ".join(f"{mn[k][0]:.3f}%" for k in ("1", "16", "64")),
    "`rq3.batch_sweep.manifest_overhead_pct` (process-once)", SAME)

# ================================================================ E5: S3 cost → real S3
row("E5", "§VII-D 'S3 object store'", "S3 over local Docker: store SHA-256 and read-back cost vs size-only",
    "no resolved cost for either on any dataset (150 runs)", "no local re-run. Real S3 (E-RQ5-COST, t4g.medium, 90/90 pass): "
    "read-back " + " / ".join(ci(mc[d]["blake3-readback"]["change_vs_size_only_pct"], 1, True) for d in DS) +
    " (SSH/BGL/HDFS, all resolved); store SHA-256 " + " / ".join(ci(mc[d]["sha256-store"]["change_vs_size_only_pct"], 1, True)
                                                              for d in DS) + " (resolved on HDFS only)",
    "`rq5_cost.t4g.medium` ← `rq5-cloud/cost/results/20261121T002644Z-*/runs.jsonl`",
    CH + ": on real S3 read-back has a resolved cost on every dataset; 'neither had a resolved cost' does not carry over")
row("E5", "§VII-D, H3.5", "HDFS re-check on t4g.large (D9): store SHA-256 / read-back", "—",
    f"{ci(lc['sha256-store']['change_vs_size_only_pct'], 2, True)} / {ci(lc['blake3-readback']['change_vs_size_only_pct'], 2, True)}"
    f" (30/30 pass); ENA allowance events, median size-only/SHA-256/read-back "
    f"{lc['size-only']['allowance_exceeded_median']:g}/{lc['sha256-store']['allowance_exceeded_median']:g}/"
    f"{lc['blake3-readback']['allowance_exceeded_median']:g} (t4g.medium HDFS: "
    f"{mc['hdfs']['size-only']['allowance_exceeded_median']:g}/{mc['hdfs']['sha256-store']['allowance_exceeded_median']:g}/"
    f"{mc['hdfs']['blake3-readback']['allowance_exceeded_median']:g})",
    "`rq5_cost.t4g.large` ← `20261121T012945Z-*`", NEW + ": store SHA-256 costs ~5% on HDFS once shaping is balanced")
row("E5", "§VII-D", "S3 over Docker, uncompressed drop", f"{rng([-mino[d]['sha256-nocomp'][0] for d in DS], 0, '%')}",
    "not run on v0.10.2 (no uncompressed variant in E-RQ5-COST)", "—", NONE)
row("E5", "Fig. 4, §VII-D, §VIII-D 'store's checksum stayed free', §IX '19–27%'", "read-back slowdown with emulated latency "
    "(+20 / +50 ms); store SHA-256", f"{rng(lat_rb['20'], 0)}% / {rng(lat_rb['50'], 0)}%; store SHA-256 unresolved",
    f"no emulated-latency re-run. Real links (E-RQ5-WAN, RTT {WC['rtt_ms_range'][0]:.0f}–{WC['rtt_ms_range'][1]:.0f} ms): "
    f"read-back {-WC['readback_change_pct_range'][1]:.1f}–{-WC['readback_change_pct_range'][0]:.1f}% (all resolved, grows "
    f"with RTT); store SHA-256 {-WC['store_change_pct_range'][1]:.1f}–{-WC['store_change_pct_range'][0]:.1f}%, resolved in "
    f"{len(WC['store_resolved_regions'])} of 4 regions", "`rq5_wan` ← `rq5-cloud/wan/results/*/runs.jsonl`",
    CH + ": read-back ordering holds and is larger; 'store checksum stays free' does not hold on real long links")

# ================================================================ E6: long distance
row("E6", "abstract ('96–97% of capacity'), §VII-D", "link use on an emulated 100 ms / 10 Mbit/s link",
    "96% (97% with 1% burst loss); external measurement of v0.7.0 [vtop2026egress], not in summary.json",
    "no counterpart. E-RQ5-WAN measures upload body bytes / drain time (includes verify and commit): at a 10 Mbit/s "
    f"tc limit {WC['tc10_store_of_cap_range'][0]:.2f}–{WC['tc10_store_of_cap_range'][1]:.2f} of the cap, at 2 Mbit/s "
    f"{WC['tc2_store_of_cap_range'][0]:.2f}–{WC['tc2_store_of_cap_range'][1]:.2f} (store SHA-256, 73 MB SSH in five "
    "16 MiB batches) — a different metric", "`rq5_wan.claims`", NONE + " (abstract number must go or be re-measured)")
row("E6", "§VII-D", "95th-percentile upload time 143 → 146 s", "external measurement", "not computable from v0.10.2 files",
    "—", NONE)
pol = W["regions"]
row("E6", "§VII-D, §VIII", "policed uplink: the default committed nothing (fixed 300 s per-request deadline)",
    "10 Mbit/s policer, 5,000 B bucket: nothing committed", "1 Mbit/s policer, 5,000 B bucket: the shipped deadline "
    f"(#663) committed everything in all {len(WC['policed_completed_regions'])} regions; only "
    f"{', '.join(WC['policed_seal_to_commit_over_300s_regions'])} needed > 300 s seal-to-commit "
    f"({pol['ap-southeast-2']['policed']['policed']['seal_to_commit_max_s']:.0f} s), so it is the one valid regression "
    f"check; the fixed-120 s control committed {WC['policed_fixed_committed_bytes']} bytes in 480 s",
    "`rq5_wan.regions.*.policed`", CH + " (fixed, as expected from #663)")
row("E6 (new)", "§VII", "WAN faults: partition, SIGKILL, loss", "—",
    f"60 s partition: {WC['partition_commits_during_cut']} commits during the cut, first commit "
    f"{WC['partition_first_commit_after_restore_s_range'][0]:.1f}–{WC['partition_first_commit_after_restore_s_range'][1]:.1f} s "
    f"after restore; SIGKILL mid-upload: restart drained in {WC['sigkill_restart_drain_s_range'][0]:.1f}–"
    f"{WC['sigkill_restart_drain_s_range'][1]:.1f} s, {WC['sigkill_duplicate_bytes']} duplicate bytes; 5% loss: drain "
    f"{min(X5):.1f}–{max(X5):.1f}× slower, {WC['loss_failed_batches']} failed batches; {W['passing_runs']}/{W['runs']} runs "
    f"pass, {W['unsafe_commits']} unsafe", "`rq5_wan`", NEW + " (one fault trial per region)")

# ================================================================ E7: RQ4
cv4 = s4["cells"]
row("E7", "Table 6, §VII-E, abstract, §I, §IX", "VTOP unsafe trials under ack.-corrupted / ack.-lost / kill",
    " / ".join(f"{o4['cells'][f'{f}|vtop']['unsafe_trials']}/{o4['cells'][f'{f}|vtop']['trials']}" for f in ("corrupt", "lost", "kill")),
    " / ".join(f"{cv4[f'{f}|vtop']['unsafe_trials']}/{cv4[f'{f}|vtop']['trials']}" for f in ("corrupt", "lost", "kill")),
    "`rq4-baselines/results/20261121-v0102-rq4/trials.jsonl`", SAME)
row("E7", "§VII-E, §I, §IX ('four of its 27 trials stalled')", "VTOP Kafka stalls",
    f"{o4['vtop_incomplete']} of {o4['vtop_trials_run']} ({o4['vtop_stalls_before_fault']} before the fault)",
    f"{s4['vtop_incomplete']} of {s4['vtop_trials_run']}; longest trial {s4['vtop_elapsed_s_max']:.0f} s", "same",
    CH + " (liveness finding gone)")
row("E7", "Table 6", "VTOP complete archives", f"{sum(o4['cells'][f'{f}|vtop']['complete'] for f in ('corrupt','lost','kill'))}/"
    f"{sum(o4['cells'][f'{f}|vtop']['trials'] for f in ('corrupt','lost','kill'))}",
    f"{sum(cv4[f'{f}|vtop']['complete'] for f in ('corrupt','lost','kill'))}/"
    f"{sum(cv4[f'{f}|vtop']['trials'] for f in ('corrupt','lost','kill'))}", "same", CH + " (better)")
row("E7", "Table 6 Dup.", "VTOP duplicate objects per kill trial (median)", f"{o4['cells']['kill|vtop']['duplicate_objects_median']:.0f}",
    f"{cv4['kill|vtop']['duplicate_objects_median']:.0f}", "same", CH)
row("E7", "Table 6, abstract", "Confluent / Vector / Fluent Bit committed past the damaged object",
    "10/10 each, both faults", "same trials (not re-run): 10/10 each; paired with the new VTOP side",
    "`20261111-v070-rq4` (Confluent rows), `20261111-rq4-{vector,fluentbit}`",
    SAME + " — baselines are v0.7.0-era runs against MinIO image "
    f"`{s4['minio_image_baselines'][7:19]}…`; the v0.10.2 VTOP side ran on a rebuilt image `{s4['minio_image_vtop'][7:19]}…` "
    "(same release and commit)")
row("E7", "§VII-E", "Fluent Bit transient gap at kills", "6 of 15 kill events, all 5 trials",
    f"{cv4['kill|fluentbit']['transient_at_kill_events']} of {cv4['kill|fluentbit']['kills']} (same trials)", "same", SAME)

# ================================================================ E8, E9, E11, E12
PF = S["rq5_perf"]
PC, PU, PS = PF["claims"], PF["legs"]["us-east-1"], PF["legs"]["ap-southeast-2"]
_d = lambda e, c: e["tput"][c]["done_mib_s"][0]  # noqa: E731
row("E8/E12", "§I, §VII-E, §VIII, abstract, cover letter", "performance comparison with the sinks (H4.2)", "not measured",
    f"E-RQ5-PERF, 2 × t4g.large per leg, HDFS on Kafka, 16 MiB objects: done MiB/s VTOP store SHA-256 "
    f"{ci(PU['tput']['vtop-sha256-store']['done_mib_s'], 2)} in-region / {ci(PS['tput']['vtop-sha256-store']['done_mib_s'], 2)} "
    f"from Sydney ({PC['rtt_ms']['ap-southeast-2']:.0f} ms); Confluent {PC['confluent_over_vtop_store']['us-east-1']:.2f}× / "
    f"{PC['confluent_over_vtop_store']['ap-southeast-2']:.2f}×, Vector {PC['vector_over_vtop_store']['us-east-1']:.2f}× / "
    f"{PC['vector_over_vtop_store']['ap-southeast-2']:.2f}×; size-only vs store SHA-256 "
    f"{ci(PC['size_only_vs_store_pct']['us-east-1'], 1, True)} / {ci(PC['size_only_vs_store_pct']['ap-southeast-2'], 1, True)}; "
    f"CPU s/GiB VTOP {PC['cpu_s_per_gib']['us-east-1']['vtop-sha256-store']:.1f} (lowest, resolved), Confluent "
    f"{PC['cpu_s_per_gib']['us-east-1']['confluent']:.1f}, Vector {PC['cpu_s_per_gib']['us-east-1']['vector']:.1f}, Fluent Bit "
    f"{PC['cpu_s_per_gib']['us-east-1']['fluentbit']:.1f}; first commit after SIGKILL VTOP "
    f"{PC['kill_first_commit_s']['us-east-1']['vtop-sha256-store']:.1f} s, 0 duplicates; Vector up to "
    f"{max(max(v['vector']) for v in PC['kill_duplicate_records'].values()):,} duplicate records",
    "`rq5_perf` ← `rq5-cloud/perf/results/*-broker-*/results/runs.jsonl` + `series/`", NEW + " (VTOP slower than Confluent; "
    "one instance pair per leg, one window)")
row("E7 (cloud)", "§VII-E", "storage faults on real S3 (D13): sinks commit past the damaged object", "—",
    f"sinks {PC['fault_sinks_unsafe']}/{PC['fault_sinks_trials']} trials ({PC['fault_sinks_unsafe_rq4_comparable']}/"
    f"{PC['fault_sinks_rq4_comparable']} RQ4-comparable), {PC['fault_sinks_lost_committed_per_trial_range'][0]:,}–"
    f"{PC['fault_sinks_lost_committed_per_trial_range'][1]:,} committed records lost per trial; VTOP: damage after its "
    f"verification in {PC['fault_vtop_vs_verification'].get('after_verification', 0)}/{PC['fault_vtop_trials']}, "
    f"{', '.join(str(v) for v in PC['fault_vtop_damage_before_verification_ms'])} ms before its logged verification in "
    f"{PC['fault_vtop_vs_verification'].get('before_verification', 0)}; scrub flagged {PC['fault_vtop_scrub_flagged']}/"
    f"{PC['fault_vtop_trials']}", "`rq5_perf.legs.us-east-1.faults`",
    NEW + " (RQ4 reproduced for the sinks; for VTOP outside the guarantee — RQ4's proxy test stays the pre-verification evidence)")
row("E9", "§VII-F threats", "single host, LocalFS, Docker on macOS", "all experiments", "RQ1–RQ4 still one host; cost and WAN "
    "on EC2 + S3 (one instance per run, one AZ / one VM per region, one time window)", "run.json of each campaign",
    CH + " (text only)")
row("E11", "§III-B, §VI, §VIII", "encryption at rest", "not stated", "not evaluated (feature not released, H2.8 OPEN)", "—", NONE)

# ================================================================ X1: batch sizes
def kmax(d):
    return max(e["max_mib"] for e in d.values()), max(e["max_over_setting"] for e in d.values())


row("X1", "§VII-A ('sealed at 16 MiB or after 2 s'), every run-mode result",
    "mean uncompressed MiB per committed batch at a 16 MiB setting, run mode (SSH / BGL / HDFS)",
    " / ".join(f"{o_run[f'{d}|16']['mean_mib'][0]:.1f}" for d in DS),
    " / ".join(f"{bs['run_mode_local'][f'{d}|16']['mean_mib'][0]:.2f}" for d in DS) + " (count = ceil(bytes / 16 MiB))",
    "`runs.jsonl` of `20261110-v070-cost-run` / `20261121-v0102-cost-run` (bytes / committed rows)",
    CH + " (correction, not a result: v0.7.0 run-mode batches were ~2× the stated size)")
row("X1", "Fig. S1 'batch target'", "run-mode mean MiB per batch at 1 / 4 / 64 MiB (BGL)",
    " / ".join(f"{o_run[f'bgl|{m}']['mean_mib'][0]:.2f}" for m in (1, 4, 64)),
    " / ".join(f"{bs['run_mode_local'][f'bgl|{m}']['mean_mib'][0]:.2f}" for m in (1, 4, 64)), "same", CH)
row("X1", "§VII-A, Table 4", "largest batch in the crash campaign vs its setting (files, spools, HDFS)",
    ", ".join(f"{k} {e['max_mib']:.1f} MiB ({e['max_over_setting']:.2f}×)" for k, e in o_crash.items()),
    ", ".join(f"{k} {e['max_mib']:.1f} MiB ({e['max_over_setting']:.2f}×)" for k, e in bs["crash"].items()),
    "engine `batch_metrics` in `raw/` (local only) + `config.yaml`", CH)
row("X1", "§VII-A, RQ1 Kafka, RQ4", "largest Kafka batch vs a 4 MiB setting (RQ1 / RQ4)",
    f"{kmax(o_k1)[0]:.2f} MiB ({kmax(o_k1)[1]:.2f}×) / {kmax(o_k4)[0]:.2f} MiB ({kmax(o_k4)[1]:.2f}×)",
    f"{kmax(bs['kafka_rq1'])[0]:.3f} MiB ({kmax(bs['kafka_rq1'])[1]:.3f}×) / {kmax(bs['kafka_rq4_vtop'])[0]:.3f} MiB "
    f"({kmax(bs['kafka_rq4_vtop'])[1]:.3f}×); "
    f"{sum(e['batches_over_setting'] for e in bs['kafka_rq1'].values())} of {sum(e['batches'] for e in bs['kafka_rq1'].values())} / "
    f"{sum(e['batches_over_setting'] for e in bs['kafka_rq4_vtop'].values())} of "
    f"{sum(e['batches'] for e in bs['kafka_rq4_vtop'].values())} batches slightly above 4 MiB", "same",
    CH + " (answers the open X1 question: Kafka was affected in v0.7.0 too)")
row("X1", "§VII-D cloud", "largest batch in E-RQ5-COST / E-RQ5-WAN (ledger)", "—",
    f"{bs['cloud_cost']['max_batch_uncompressed_bytes']:,} B / {bs['cloud_wan']['max_batch_uncompressed_bytes']:,} B "
    f"({bs['cloud_wan']['runs_with_batch_over_max_bytes']} policed WAN runs had one batch 49 B over 16 MiB)",
    "`ledger` field of the cloud runs.jsonl", NEW)

# ================================================================ abstract / conclusion statements without numbers
row("D2/D4", "abstract, §III-F, §VIII-C, §IX", "'recovery remains at-least-once and can duplicate objects'",
    "observed: median 108 duplicates under power loss, ≤ 2 under kills, VTOP median 2 in RQ4 kills",
    "0 duplicate objects in every v0.10.2 trial (RQ1 local, RQ4 VTOP, WAN SIGKILL: 0 duplicate bytes); duplicates "
    f"appear only in the model (max {G['v010|B1']['max_duplicates']} per state, only with a damaged stored object; and "
    "Kafka newest-row resume after a further restart)", "all of the above", CH + ": at-least-once wording stays "
    "(AGENTS.md), but duplicates now have model evidence only, no observed instance")

# ================================================================ write
hdr = """# Manuscript numbers: v0.7.0 (submitted) vs vtop-engine v0.10.2

Generated by `research/analysis/scripts/make_numbers_v0102.py` from `research/analysis/summary.json` (v0.7.0,
engine 6c8f386) and `research/analysis/v0102/summary.json` (v0.10.2, engine 0abf116; `analyze.py --set v0102`).
Checked by `research/analysis/scripts/verify_v0102_data.py`, which recomputes every v0.10.2 value from the run files.
Row ids follow the change ledger `research/drafts/v0101-paper-changes.md`. Do not edit by hand: re-run the script.

Statistics: local results are medians with a percentile bootstrap (10,000 resamples, RNG seeded from the data), as
in the submitted paper; a cost "drop" is positive when slower. Cloud results keep the fixed-seed bootstrap of their
campaign analyzers (`analyze_minio_cost.py` `ci`/`pct_ci`, B = 10,000), which `analyze.py` re-runs and checks
against the committed campaign summaries; a cloud "change" is negative when slower. Brackets are 95% CIs.

**Direction** says whether the claim the manuscript makes from the number still holds on v0.10.2:
`same` (holds; the value may differ), **CHANGED** (the claim must be rewritten), `new` (no v0.7.0 counterpart),
**NO v0.10.2 EVIDENCE** (the v0.7.0 claim has nothing to stand on for the new build).

## Read first

1. **Power loss changed twice at once.** (a) *Emulation*: v0.7.0 runs deleted the whole SQLite WAL after a kill
   ({wal} deletions), which is stronger than a power loss once VERIFIED and SOURCE_COMMITTED writes are fsynced;
   v0.10.2 runs truncate the WAL after the last fsynced VERIFIED/SOURCE_COMMITTED commit (`vtoplab.power_loss_truncate`,
   the engine's own `tests/ledger_power_loss.rs` method; {cuts} cuts, {drop} of which dropped ledger commits, {empty}
   found an empty WAL). (b) *Engine*: v0.10.x fsyncs those two states (#626/PR #642) and gives objects stable keys, so a
   replay of the same bytes reuses the verified object (#688, #755/#793/#796). The drop from a median of 108 duplicate
   objects to 0 is the **combined** effect of (a) and (b). No run isolates either one: there is no v0.10.2 run with
   whole-WAL deletion and no v0.7.0 run with truncation. Do not attribute the drop to the engine alone or to the
   emulation alone.
2. **Batch size (X1).** The submitted paper says batches close at 16 MiB; in `vtopctl run` mode v0.7.0 closed them at
   about twice the setting (mean {o_run16} MiB on SSH / BGL / HDFS; Kafka up to {o_kafka:.2f} MiB at a 4 MiB setting;
   the crash campaign up to {o_crash_x:.2f}× its setting). v0.10.2 (#824) keeps them at the setting: run-mode means
   {s_run16} MiB, process-once and crash batches never above it, cloud ledgers ≤ 16 MiB except one batch of
   {wan_max:,} B ({wan_over} B over) in each of {wan_runs} policed WAN runs, and Kafka batches up to {s_kafka_x:.3f}× of 4 MiB. Throughput and per-batch
   numbers are therefore not like-for-like between the builds (D6); compare directions, not values. Process-once stage
   times (Fig. S2) were already at 16 MiB in v0.7.0.
3. **RQ4 baselines were not re-run.** The Confluent, Vector and Fluent Bit cells are the v0.7.0-era trials; only the
   VTOP side is v0.10.2. The VTOP side ran against a rebuilt MinIO image (same release and commit, different digest).
4. **No v0.10.2 counterpart** exists for: the S3-over-Docker and emulated-latency campaigns (replaced by real S3 and
   real long-distance links, which measure something related but not the same), the no-hand-back and no-read-order
   model ablations, the 96–97% link-use figure and the 143 → 146 s upload percentile (external v0.7.0 measurement), the
   uncompressed variant on S3. (The performance comparison with the sinks, absent in v0.7.0, is now E-RQ5-PERF.)

## Claims whose direction changed

{changed}

## Claims without v0.10.2 evidence

{noev}

## All numbers

| # | Where (manuscript at 65bac0f) | Claim / number | v0.7.0 | v0.10.2 [95% CI] | Source (v0.10.2) | Direction |
|---|---|---|---|---|---|---|
"""
changed = "\n".join(f"- **{r[0]}** ({r[1]}): {r[2]} — v0.7.0 {r[3]}; v0.10.2 {r[4]}. {r[6].replace('**CHANGED**', '').lstrip(' :')}"
                    for r in rows if "CHANGED" in r[6])
noev = "\n".join(f"- **{r[0]}** ({r[1]}): {r[2]} — v0.7.0 {r[3]}; v0.10.2: {r[4]}." for r in rows if "NO v0.10.2" in r[6])
text = hdr.format(wal=ot("wal_deleted"), cuts=pl["power_cuts"], drop=pl["power_cuts_dropping_commits"],
                  empty=pl["power_cuts_empty_wal"], changed=changed, noev=noev,
                  o_run16=" / ".join(f"{o_run[f'{d}|16']['mean_mib'][0]:.1f}" for d in DS),
                  s_run16=" / ".join(f"{bs['run_mode_local'][f'{d}|16']['mean_mib'][0]:.2f}" for d in DS),
                  o_kafka=max(kmax(o_k1)[0], kmax(o_k4)[0]), o_crash_x=kmax(o_crash)[1],
                  s_kafka_x=max(kmax(bs["kafka_rq1"])[1], kmax(bs["kafka_rq4_vtop"])[1]),
                  wan_max=bs["cloud_wan"]["max_batch_uncompressed_bytes"],
                  wan_over=bs["cloud_wan"]["max_batch_uncompressed_bytes"] - 2**24,
                  wan_runs=bs["cloud_wan"]["runs_with_batch_over_max_bytes"])
esc = lambda x: str(x).replace("|", "\\|")  # noqa: E731
text += "\n".join("| " + " | ".join(esc(c) for c in r) + " |" for r in rows) + "\n"
text += """
## Not computable from the files

- v0.7.0 model stall and D2 state counts (6,747; 3,576) are quoted from `state-machine/results/20261120-v010-sweep.md`;
  the v0.7.0 `summary.json` does not carry them.
- The 96–97% link use and the 143 → 146 s upload percentile come from an external v0.7.0 measurement
  (`vtop2026egress`); no file here reproduces them for either build.
- Per-batch sizes of the local run-mode runs: their `raw/` holds only `config.yaml`, so only the mean per run
  (bytes / committed rows) is known; the per-batch maximum comes from the process-once, crash, Kafka and cloud logs.
- Which of the two power-loss changes removed the duplicates (see above).
"""
OUT.write_text(text)
print("wrote", OUT, len(rows), "rows;", sum("CHANGED" in r[6] for r in rows), "changed;",
      sum("NO v0.10.2" in r[6] for r in rows), "without evidence")
