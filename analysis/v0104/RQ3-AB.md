# RQ3 local timing: why v0.10.4 is slower on the local store (2026-11-22)

## Observation

In the v0.10.4 campaign (`rq3-cost-ablation/results/20261122-v0104-cost-run`), run-mode throughput on the local store
was about 40% lower than in the v0.10.2 campaign in every cell, size-only included (SSH size-only 199 vs 314 MiB/s).
CPU s/GiB was 12-58% higher. The host load before the timing step was the same in both campaigns (1-min load 3.74 vs
3.58; `experiments/campaign-v0104.txt` and `campaign-v0102.txt`, line 232).

## Interleaved A/B on one host

Setup:
- results in `rq3-cost-ablation/results/20261122-ab-v0102-v0104/`;
- `exp_cost.py --mode run --datasets ssh --reps 2`, run three times per build, alternating v0.10.2 and v0.10.4 (seeds
  20261123-25);
- 14:13-14:16 local time, with the host busy (1-min load 12.9-18.6; another session's containers were running).

The absolute values therefore do not compare with either campaign; the ratio between the builds does, since both
builds ran in the same minutes.

| Variant (SSH, run mode, 16 MiB) | v0.10.2 MiB/s (n) | v0.10.4 MiB/s (n) | v0.10.4 / v0.10.2 | CPU s/GiB v0.10.2 → v0.10.4 |
|---|---|---|---|---|
| size-only | 226.2 (6) | 169.6 (6) | 0.75 | 4.03 → 4.26 |
| sha256 | 203.9 (6) | 164.6 (6) | 0.81 | 4.33 → 4.03 |
| blake3 | 234.6 (6) | 143.2 (6) | 0.61 | 3.88 → 4.73 |
| sha256+mac | 243.5 (6) | 152.9 (6) | 0.63 | 3.77 → 4.50 |
| sha256-nocomp | 189.8 (6) | 135.0 (6) | 0.71 | 4.64 → 5.24 |

v0.10.4 is slower than v0.10.2 on the same host, in the same minutes. Most of the difference is therefore the engine,
not the host.

## Where the time goes (per-stage timings, process-once, 16 MiB batches; `summary.json` `rq3.stage_mean_ms_per_batch`)

| Cell | object upload ms | manifest upload ms | compress ms | checksum + verify ms |
|---|---|---|---|---|
| SSH sha256 | 0.0 → 12.9 | 0.0 → 8.7 | 12.7 → 13.3 | 0.0 → 0.0 |
| SSH sha256-nocomp | 0.0 → 20.3 | 0.0 → 10.5 | 7.6 → 8.1 | 11.4 → 11.0 |
| HDFS sha256 | 0.0 → 13.0 | 0.0 → 10.4 | 17.8 → 18.2 | 0.4 → 0.1 |
| HDFS sha256-nocomp | 0.0 → 19.5 | 0.0 → 10.3 | 8.9 → 8.5 | 13.0 → 12.0 |

The added time is in the local store's writes. Compression, checksum and verification are unchanged.

v0.10.3 #827 (commit fda0ce3e) is the matching change: "the localfs backend fsyncs the staging file, renames it, and
fsyncs the key's directory and every directory it created before a write is reported done, so a power loss after
SOURCE_COMMITTED cannot lose an object, manifest or receipt the ledger counts as archived".

The batch-size sweep fits a fixed cost per write. BGL at 1 MiB batches went from 159 to 37 MiB/s; at 64 MiB, from 362
to 311 MiB/s.

## Consequences

- Local-store throughput and CPU values of v0.10.4 are not comparable with v0.10.2: the local store now pays durable
  writes. This is a durability fix, not a verification cost.
- The verification costs relative to size-only, within the v0.10.4 campaign, are measured on that durable local
  store: compressed 6.9-11.1% (all resolved), uncompressed 18.2-23.5%.
- The real-S3 campaigns (E-RQ5-COST, WAN, SCALE, PERF) do not use localfs and are not affected.
