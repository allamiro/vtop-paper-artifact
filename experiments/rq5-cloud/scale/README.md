# E-RQ5-SCALE: N independent VTOP engines writing to one S3 bucket (proposed H3.7)

**Status: prepared, not launched (2026-11-21).** No AWS call that creates, launches, uploads or spends was made for
this stage. No AWS account call was made at all; prices came from the public price-list files, and the launcher was
tested against a fake `aws` on `PATH`. **Launch needs the author's "go"** (skill step 1; D13 says "launch within a few
dollars each, else ask"). It also needs the [decisions](#decisions-for-the-author) below.

**Why (D13a).** The earlier evaluation did not show the applicability on a large-scale
cloud platform. COST, WAN and PERF each run **one** engine. This stage runs **1, 2, 4 and 8 engines at the same
time**, each on its own EC2 host and all writing to the same bucket. It measures:

- what the fleet archives together and what each engine keeps;
- what S3 sees (request rate, retries, throttling);
- whether every engine stays safe.

## Proposed hypothesis H3.7 (for the author; not added to `hypotheses.md`)

> **H3.7 Horizontal scale-out on a public cloud (E-RQ5-SCALE).**
> **Claim:** independent VTOP engines on separate EC2 hosts (one ledger and one set of sources each) write to one
> Amazon S3 bucket. Their throughput adds up nearly linearly with the number of engines up to the tested size, S3
> does not throttle them, and each engine keeps the safety and verification evidence of H3.5.
> **Prediction:**
>
> - **Efficiency.** For N = 2, 4, 8 engines, the scaling efficiency E(N) = median aggregate MiB/s at N ÷ (N × median
>   at N = 1) has a bootstrap 95% CI lower bound ≥ 0.90.
> - **No throttling.** S3 throttles nothing: 0 throttling verdicts from the engines' SDK retry classifier and 0
>   `upload_throttled` / `object_key_probe_throttled` events.
> - **Safety.** Every engine-run drains and passes the independent oracle (byte-exact, complete, 0 unsafe commits),
>   and its log, receipt, ledger class and S3 HEAD agree on the verification method.
>
> **Test:** E-RQ5-SCALE, v0.10.2, 8 × t4g.large in us-east-1a, HDFS per engine, 16 MiB batches, store SHA-256.
> Also, as a description only: the store SHA-256 vs size-only per-engine difference at N = 8, set against H3.5's ≤ 5%.
> **Rejected** if E(N)'s CI lies below 0.90 for any N, or if any engine-run is unsafe.
> **Scope:** horizontal scale-out of independent engines, not a shared-ledger cluster. Multi-writer recovery is out
> of scope (`research/state-machine/v0101-verification.md` 8.5).

The prediction is not a guess about S3 limits. Each engine makes about 6 requests per 16 MiB batch (measured
locally: 3 PUT and 3 HEAD; size-only 3 PUT and 2 HEAD). That is about 18 requests/s per engine at COST's
≈ 48 MiB/s. Eight engines therefore make ≈ 150 requests/s. AWS documents "at least 3,500 PUT/COPY/POST/DELETE or
11,000 GET/HEAD requests per second per partitioned prefix" [8]. **This stage therefore shows that VTOP scales out
and stays safe at a moderate fleet size. It does not reach S3's limits.** Any statement beyond 8 engines is an
extrapolation and must be labelled as one.

## Design

| Factor | Value | Why |
|---|---|---|
| N | **1, 2, 4, 8** engines, one per host. Optional **N = 16** with 2 engines per host (`DENSE_REPS`) | D13 |
| Fleet | **8 × t4g.large** (2 Graviton2 vCPU, 8 GiB), one RunInstances call, one subnet (first AZ that offers the type, us-east-1a as in COST). The same 8 hosts serve every step, and a step with N < 8 uses a random N of them (seeded) | 8 × 2 = **16 vCPUs = the cap**. A fleet that lives through the session costs one bootstrap, not one per step. Randomised hosts keep host-to-host variation out of the N effect |
| Why t4g.large, not medium | 0.512 Gbps baseline network (medium 0.256), 8 GiB RAM, EBS baseline 86.88 MB/s [7] | COST saw ENA `bw_out_allowance_exceeded` events on t4g.medium. The large instance's HDFS re-check gives the N = 1 reference: size-only 49.2 MiB/s and store SHA-256 46.7 MiB/s (H3.5). One engine uploads only ≈ 4.7 MB/s on average (HDFS compresses ≈ 10×: 157 MB sent per 1.58 GB run in COST), about 7% of the baseline. ENA counters are still recorded per host and step, because COST saw micro-burst events far below the average |
| Sources | Each engine archives **its own file**: HDFS (1,577,982,906 B, 11.2 M lines), from `cost/input/HDFS_v1.zip` (MD5 → SHA-256 → size checked on the VM), at `/opt/s/in/<j>/hdfs.log` | It is the simplest source with no broker, and it is comparable with COST's HDFS cells (≈ 95 batches, ≈ 33 s per run). Each engine has its own path and inode, so object names and ledgers are distinct. **Not Kafka:** a broker would be a shared component the fleet would need to scale too, and it would mix broker limits into the S3 scale-out question. PERF covers Kafka |
| Engine | v0.10.2 `0abf116…`, gnu aarch64 archive SHA-256 `5872e5a3…df39` (pinned and cosign-checked 2026-11-21, LOG). Config = `cost.config_text` with the engine's own name: `max_bytes` 16 MiB (D6), 8 concurrent batches, age 2 s, zstd 3, receipts on, multipart threshold 64 MiB (every object a single PUT) | same as COST, so N = 1 can be compared with the HDFS re-check |
| Modes | **store SHA-256** in every scale step; **size-only control at N = 8** (5 reps) | D13. The control is needed to place H3.5's method cost at N = 8 |
| S3 layout | data `scale/data/<session>/<step>/<engine>/` (distinct per engine, deleted after the engine passes the oracle); results `scale/results/<session>/h<slot>-<instance>/`; coordination `scale/coord/<session>/`; code `scale/code/<session>/` | one bucket. A fresh prefix per engine-run, because v0.10.x reuses stably named objects |
| Reps and order | 1 warm-up step (N = 8, excluded), then **10 rounds**. Each round holds N = 1, 2, 4, 8 (store SHA-256) and, in rounds 0–4, the size-only N = 8 step, shuffled (`random.Random(20261124)`). **46 steps, 198 engine-runs** | 10 reps as COST and WAN. Rounds block out time drift |
| Optional sustained step | `SUSTAIN_S` s at N = 8, after all rounds. Each engine is fed one HDFS clone after another: a copy-on-write `cp --reflink=always` (no disk used), renamed into the watched directory, with 2 files waiting. The step reports per-minute rates, the trend, RSS over time and throttles | see [trade-off](#time-and-cost-estimate-prices-read-2026-11-21) |

### Coordination: a leaderless S3 barrier (`scale.py`)

The pattern is close to E-RQ5-PERF's (hosts announce themselves in `<stage>/coord/<session>/`), but no host leads.

1. **Prepare.** Before each step, every host prepares its engines (config, `vtopctl validate`, warm read of the
   input). It then writes `coord/<session>/<step>/h<slot>.json` holding its clock reading `t`.
2. **Wait.** Each host LISTs `coord/<session>/` every second until it holds all 8 markers.
3. **Start.** All hosts start their engines at **T = ceil(max t + 3 s)**. Every host reads the same markers, so every
   host computes the **same T**, the **same abort decision** and the **same deadline decision** (stop if T is past
   the session deadline). Hosts are synchronised by chrony against the Amazon Time Sync Service; `chronyc tracking`
   is recorded at the start and end on every host. Start skew and lateness are recorded per step.
4. **Stop.** A host whose engine fails to drain, commits unsafely, or reaches 3 failed runs writes `abort/h<slot>.json`.
   A host that cannot bootstrap (EXIT trap, exit ≠ 0, 3) does the same. Every other host stops at the next barrier.
   A host that never shows up stops the others after 1,500 s at the first barrier and after 900 s at the later ones.
   Every host also has its own failsafe `shutdown -h +130`.

**The launch fails cleanly.**

- `launch.sh` refuses to start when the pending and running instances in us-east-1 (any project; **E-RQ5-PERF's two
  hosts count**) plus the fleet would exceed 16 vCPUs.
- It launches with **MinCount = MaxCount = 8**, so a refusal (`VcpuLimitExceeded`, quota, permission) launches
  nothing.
- On `InsufficientInstanceCapacity` it tries the next AZ.
- If it receives fewer instances than it asked for, it terminates them and stops.

### What is measured

**Per engine-run** (`engines.jsonl`):

- **Throughput:** steady MiB/s, as COST (bytes at the second-to-last commit ÷ time since spawn), end-to-end MiB/s,
  records/s (exact, from a newline index), the commit series in epoch time, and start lateness.
- **Resources:** CPU s and cores, and **peak RSS from `/proc/<pid>/status` VmHWM** read before SIGTERM (see
  [Observations](#observations-for-other-stages)).
- **Engine evidence:** ledger evidence (measured batch sizes, classes) and the engine log summary.
- **S3 requests**, counted from the engine's own AWS SDK. `RUST_LOG` raises `aws_sdk_s3::operation` and the SDK
  orchestrator and retry modules to DEBUG, so every attempt is logged inside its `S3.<Operation>` span. From that the
  record holds:
  - requests, attempts, retries and the retry classifier's verdicts (a 503 SlowDown is a `ThrottlingError`), per
    operation;
  - per-operation SDK latency (p50/p90/p99);
  - attempts per second.

  This needs no new permission. CloudWatch request metrics, server access logs or CloudTrail would.
- **Oracle:** `smoke/oracle.py`, unchanged, with COST's mmap patch, and the pass checks.

**Per host and step** (`steps.jsonl`):

- barrier wait and number of polls;
- CPU busy and steal, NIC bytes, disk reads;
- **ENA `bw_in/bw_out/pps/conntrack/linklocal_allowance_exceeded` deltas**, and a 1 s time series of the same.

**Per step** (`analyze.py`):

- aggregate MiB/s (the sum of per-engine steady rates);
- **window MiB/s**: bytes committed by all engines inside the common window [last spawn, first engine's steady end],
  which proves the engines really overlapped. The window's overlap fraction is also reported;
- makespan MiB/s and start skew;
- S3 attempts/s and the peak second;
- retries, throttles and ENA events.

**Per N:**

- median aggregate MiB/s with its CI;
- **efficiency E(N) with a bootstrap CI** (steps resampled within each N, B = 10,000);
- the per-engine change against N = 1 (`pct_ci`, imported from the RQ3 analysis as in COST);
- request rates and SDK latency.

**Control:** store SHA-256 vs size-only at N = 8, per engine and in aggregate. **Sustained step:** rate per
60 s window, the trend in %/10 min, RSS and throttles.

**An engine-run passes** if it drained before SIGTERM, the oracle passes (byte-exact and complete), it has 0 unsafe
commits, its log `object_check` equals the mode's text, its ledger class is as expected, its receipts equal the
HEAD-inferred methods, and every object is a single PUT. These are COST's checks plus an explicit `no_unsafe`.
`analyze.py` refuses a session with any failed run unless `--allow-failures` is given, and reports unsafe commits
first.

## Files

| File | Role |
|---|---|
| `scale.py` | `plan` (operator, stdlib only) writes `schedule.json`. `worker` (each VM) runs the barrier, the engines, the feeder, the request accounting, the per-file oracle wrapper and the result upload. It imports `cost/cost.py` and `smoke/oracle.py` **unchanged** |
| `user-data.sh` | AL2023 arm64 (8.0 KB). It sets the failsafe and the EXIT trap, refuses an unfilled `SESSION`, takes the slot from the `ami-launch-index`, verifies the release (pinned SHA-256 + `SHA256SUMS`, then `vtopctl --version`), records chrony, builds the venv (boto3 1.44.0, zstandard 0.25.0, blake3 1.0.11, as COST), fetches and verifies HDFS, fetches the session code and runs `scale.py worker`. The trap deletes this host's leftover data unless `KEEP_DATA` exists, writes an abort marker on failure, writes `exit.json`, uploads and powers off |
| `launch.sh` | Checks identity and the type, runs the vCPU guard, plans, fills `SESSION`/`FAILSAFE_MIN`, checks the params and the 16 KB size, stages the code to `scale/code/<session>/`, launches the fleet (all or nothing), waits, downloads `scale/results/<session>/`, counts leftover data and lists leftover instances. `DRY=1` stops before RunInstances |
| `analyze.py` | `summary.json`, `steps.csv`, `engines.csv`, `ANALYSIS.md` (statistics as above) |
| `dryrun.sh` | the local dry run (below); also covers the abort, barrier-timeout and deadline paths |

## Launch (only after the author's go and the decisions below)

From the repo root, with the operator profile. **E-RQ5-PERF's us-east-1 leg was launched 14:43Z today** (2 ×
t4g.large = 4 vCPUs). The vCPU guard refuses SCALE until that leg has finished.

1. **Pre-flight (read-only).** The credit specification should be `unlimited`, and nothing should be left running in
   us-east-1.
   ```bash
   export AWS_PROFILE=vtop-op AWS_REGION=us-east-1
   aws ec2 get-default-credit-specification --instance-family t4g
   aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping \
     --query 'Reservations[].Instances[].[InstanceId,InstanceType,Tags[?Key==`Name`]|[0].Value]' --output text
   aws s3 ls s3://vtop-eval-XXXXXXXXXXXX/cost/input/HDFS_v1.zip
   ```
2. **Checks:**
   ```bash
   bash -n research/experiments/rq5-cloud/scale/user-data.sh && shellcheck research/experiments/rq5-cloud/scale/*.sh
   DRY=1 research/experiments/rq5-cloud/scale/launch.sh      # plans, fills and checks user-data; no launch
   ```
3. **Launch** (expected ≈ 50–60 min; the launcher waits up to failsafe + 15 min):
   ```bash
   research/experiments/rq5-cloud/scale/launch.sh 2>&1 | tee research/experiments/rq5-cloud/scale/launch-$(date -u +%Y%m%dT%H%MZ).log
   # optional parts (decisions 3 and 4):  DENSE_REPS=5 SUSTAIN_S=1200 research/experiments/rq5-cloud/scale/launch.sh ...
   ```
   Record the session and the 8 instance IDs in `../LOG.md` as the launcher prints them.
4. **While it runs:** each host uploads after every step, so you can watch progress:
   `aws s3 ls --recursive s3://vtop-eval-XXXXXXXXXXXX/scale/results/<session>/ | grep engines.jsonl`.
   To stop everything: `aws ec2 terminate-instances --instance-ids <the 8 ids>`.
5. **Analyze:**
   ```bash
   research/.venv/bin/python research/experiments/rq5-cloud/scale/analyze.py research/experiments/rq5-cloud/scale/results/<session>
   ```
   Then record the result in `../LOG.md`. Record it in `hypotheses.md` (as H3.7, once the author accepts the text
   above) only with the author.
6. **Clean up** (skill step 6). Every `exit.json` should show `data_objects_left` 0; data is kept on purpose after a
   failure.
   ```bash
   B=vtop-eval-XXXXXXXXXXXX
   aws s3 rm --recursive s3://$B/scale/data/<session>/
   aws s3 rm --recursive s3://$B/scale/coord/<session>/
   aws s3api list-multipart-uploads --bucket $B --prefix scale/ --query 'Uploads[].[Key,UploadId]' --output text   # expect none (single PUT)
   aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval Name=instance-state-name,Values=pending,running,stopping,stopped \
     --query 'Reservations[].Instances[].InstanceId'
   aws ce get-cost-and-usage --time-period Start=<day>,End=<day+1> --granularity DAILY --metrics UnblendedCost   # lags ≤ 1 day
   # keep scale/results/; scale/code/<session>/ may go
   ```

## Time and cost (estimate; prices read 2026-11-21)

**Time per step**, from the COST HDFS re-check on t4g.large (`cost/results/20261121T012945Z-…`):

| Part | Time |
|---|---|
| engine wall | 34.2 s |
| oracle | 19.6 s |
| validate and warm read | ≈ 1 s |
| record and upload | ≈ 2 s |
| barrier (lead + poll) | ≈ 4 s |
| **one step** | **≈ 60 s** |
| dense step (two oracles) | ≈ 85 s |

COST's bootstrap (dnf, venv, HDFS fetch and checks) took ≈ 1–2 min, plus ≈ 1 min for the instance to start.
**Default plan: 46 steps ≈ 46 min, plus ≈ 5 min boot and wrap-up → ≈ 0.85 h per host, ≈ 7 instance-hours.**
The hard limits are a failsafe of 130 min and no new step after 105 min.

**Requests**, measured locally per batch: store SHA-256 3 PUT + 3 HEAD; size-only 3 PUT + 2 HEAD. With 198
engine-runs × 95 batches = 18,810 batches, that gives:

| Source | Requests |
|---|---|
| engines | ≈ 56k PUT and ≈ 55k HEAD |
| oracle (2 GET + 2 HEAD + 1 GET receipt per batch, 1 LIST per run) | ≈ 94k GET/HEAD |
| coordination (LIST every 1 s while a host waits, mostly idle hosts) | ≈ 14k LIST |
| result uploads | ≈ 2k PUT |

| Item | Price used [source] | Expected (≈ 0.85–0.9 h) | Cap (130 min failsafe) |
|---|---|---:|---:|
| 8 × t4g.large on demand | USD 0.0672/h [1] | 0.48 | 1.16 |
| 8 × public IPv4 | USD 0.005/h [2] | 0.04 | 0.09 |
| 8 × gp3 16 GiB root | USD 0.08/GB-month [3] | 0.01 | 0.03 |
| CPU surplus credits (unlimited). Bound: 2 vCPU at 100% − 30% baseline = 1.4 vCPU-h/h per host. Expected ≈ 0: per host, the average load is ≈ 0.5 vCPU while an engine runs and ≈ 1 vCPU during the oracle, idle otherwise, so it stays below the 0.6 vCPU baseline | USD 0.04/vCPU-h [4] | 0 – 0.40 | 0.97 |
| S3 PUT/COPY/POST/LIST, ≈ 72k | USD 0.005 per 1,000 [5] | 0.36 | 0.54 |
| S3 GET/HEAD and other, ≈ 150k | USD 0.004 per 10,000 [5] | 0.06 | 0.09 |
| S3 storage (data deleted per step, ≤ 1.3 GB at a time; results ≈ 0.1 GB kept) | USD 0.023/GB-month [5] | < 0.01 | < 0.01 |
| EC2 ↔ S3 same region; DELETE | free [6] | 0 | 0 |
| **Default plan** | | **≈ 0.95 (≤ 1.35 with the credit bound)** | **≈ 2.9** |

The cap is the failsafe on every host, with S3 requests at 1.5× the plan.

**Optional parts** (each adds to the default plan):

| Option | What it adds | Expected USD |
|---|---|---:|
| `DENSE_REPS=5`: N = 16, 2 engines per host | 5 steps ≈ 7 min, ≈ 23k more PUT. Shows per-host density on the same fleet | ≈ 0.20 |
| `SUSTAIN_S=1200`: 20 min sustained at N = 8 | ≈ 56 GiB per engine (≈ 37 HDFS clones), ≈ 450 GiB in total. Oracle ≈ 12 min per host, so the step takes ≈ 33 min. ≈ 84k PUT; failsafe +40 min | ≈ 0.85 (≤ 1.1) |
| `SUSTAIN_S=3600`: one hour | the same, three times as long | ≈ 2.4 |

**The sustained step's trade-off.**

- **What it adds:** it shows that throughput and memory hold over time and that S3 stays unthrottled at a constant
  load. A short-step design cannot show this.
- **What it does not add:** a new scale point. It costs as much as or more than the whole default plan.
- **Disk:** it depends on reflink being available on the AL2023 root file system. If any host cannot clone, every
  host skips the step (tested locally).
- **A different regime:** cloned files are read from EBS, not from the page cache (≈ 50 MB/s per host against the
  86.88 MB/s EBS baseline), and `analyze.py` reports the disk reads.

**Totals:**

| Plan | Expected | Cap |
|---|---:|---:|
| default | ≈ USD 1.0–1.35 | ≈ 2.9 |
| default + dense + 20 min sustained | ≈ USD 2.0–2.6 | ≈ 4.5 |

**Budget:** the monthly budget is USD 10, and E-RQ5-PERF's estimate is USD 3.3 (cap 5.3) on top of COST, WAN and
TAMPER. The 50% alert is likely, and the 85% alert is possible.

Sources (read 2026-11-21):

1. AWS public price list, EC2 on-demand Linux us-east-1 (`b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/US East (N. Virginia)/Linux/index.json`, publication 2026-11-21T00:48:16Z, re-read 2026-11-21): t4g.large 0.0672, t4g.medium 0.0336, t3.large 0.0832 USD/h.
2. <https://aws.amazon.com/vpc/pricing/>: in-use public IPv4 USD 0.005/h (as `cost/README.md` source 2).
3. EBS price list, gp3 us-east-1 0.08 USD/GB-month (as `cost/README.md` source 3).
4. <https://aws.amazon.com/ec2/instance-types/t4/>: "$0.04 per vCPU-hour" (as `cost/README.md` source 4).
5. AWS Price List API, AmazonS3 us-east-1 offer, publication 2026-11-21T00:14:14Z (version 20261121001414), re-read
   2026-11-21:
   - Requests-Tier1 "$0.005 per 1,000 PUT, COPY, POST, or LIST requests";
   - Requests-Tier2 "$0.004 per 10,000 GET and all other requests";
   - Standard storage "$0.023 per GB - first 50 TB / month".
6. <https://aws.amazon.com/s3/pricing/>: DELETE is free, and transfer to AWS services in the same Region is free
   (as `cost/README.md` source 6).
7. <https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html>:
   - t4g.large: 0.512/5.0 Gbps network and 86.88/347.5 MB/s EBS;
   - t4g.medium: 0.256/5.0 Gbps network.

   These are as quoted in `perf/README.md` and `cost/README.md`; the page was not re-read in full here.
8. <https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html> (read 2026-11-21):
   - "at least 3,500 PUT/COPY/POST/DELETE or 11,000 GET/HEAD requests per second per partitioned prefix";
   - "you might see some 503 (Slow Down) errors" while S3 scales a prefix.

## Local dry run (2026-11-21) — noise, not evidence

This tested the code paths only. Every throughput number below comes from one laptop, where all "hosts" and one
MinIO share the same CPU, disk and loopback. That contention explains the sub-linear scaling; it says nothing about
EC2 or S3.

**Setup:**

- **Engine and store:** v0.10.2 `aarch64-apple-darwin` `vtopctl`, and MinIO `RELEASE.2025-09-07T16-13-09Z` in the
  container `rq5scale-minio` on 127.0.0.1:19510 (removed afterwards; no other container was touched).
- **Hosts:** 4 `scale.py worker` processes as 4 hosts, each with its own input tree in the session scratchpad.
- **Input:** a 160 MiB slice of HDFS cut at a newline (10 batches).
- **Command:**
  `KEEP=1 HOSTS=4 NS=1,2,4 REPS=3 CONTROL_REPS=2 DENSE_REPS=2 SUSTAIN_S=30 SLICE_MIB=160 scale/dryrun.sh <out>`,
  then `analyze.py`.

**Final run (`dry-20261121T144725Z`):** **15/15 steps and 53/53 engine-runs pass** (drained, oracle byte-exact and
complete, 0 unsafe, log/receipt/ledger/HEAD agree, single PUT). Every engine's data was deleted (0 objects left), and
the operator-side download and `analyze.py` worked.

| N (laptop) | Aggregate MiB/s | Efficiency | Per-engine vs N=1 | S3 attempts/s (peak) | Retries / throttles |
|---:|---:|---:|---:|---:|---:|
| 1 | 199.4 [169.1, 199.6] | — | — | 21.5 (54) | 0 / 0 |
| 2 | 328.3 [272.4, 338.5] | 0.82 [0.68, 1.00] | −17.7% [−31.7, −0.8] | 38.1 (108) | 0 / 0 |
| 4 | 494.6 [404.9, 520.3] | 0.62 [0.51, 0.77] | −38.1% [−48.8, −25.6] | 71.1 (176) | 0 / 0 |
| 8 (2 per "host") | 706.6 [680.8, 732.5] | 0.44 | −55.4% | — | 0 / 0 |

- **Size-only control (N = 4):** store SHA-256 vs size-only per engine +5.8% [−12.7, 9.5] (unresolved). Attempts
  per 10-batch run: 60 (store SHA-256) vs 50 (size-only), the per-batch profile used in the cost table.
- **Sustained step (30 s, 4 engines):** 18–20 clones fed per engine (APFS clonefile), 11.6 GiB in total, every file
  complete, oracle pass with per-file ledger slices.
- **Barrier:** start skew ≤ 8.9 ms and lateness ≤ 9.9 ms (one clock). Overlap fraction ≥ 0.94 of each engine's
  steady time.
- **Failure paths:**
  - `FAIL_SLOT=2` (0.5 s engine timeout on one host): that host wrote `abort/h2.json` and kept its data (6 objects);
    the other 3 stopped at the next barrier. `analyze.py` refused (rc 2) and summarised with `--allow-failures`.
    Notably the undrained engine still committed everything in its shutdown flush (oracle pass), which the
    pass rule counts as a failure, as in COST.
  - `DEAD_SLOT=3` (a host never starts): the others stopped after the boot timeout.
  - `DEADLINE_S=12`: all 4 hosts stopped at the same step (`deadline reached before step s003`).
- **Linux, in an `amazonlinux:2023` container** (glibc 2.34, the Linux aarch64 release, SHA-256 checked, 2
  workers):
  - all runs passed, and the `/proc` counters work;
  - **reflink is unavailable on overlayfs**, so the sustained step was skipped on every host. This is the designed
    behaviour;
  - the second input per host fell back to a full copy;
  - `VmHWM` peak RSS ≈ 68 MiB per engine.
- **Request logging overhead** (A/B, 1 host, 2 × 5 runs per arm): median 207.3 MiB/s with SDK DEBUG logging vs
  206.1 without. There is no visible cost on the laptop, but on EC2 it is still unmeasured (decision 6).
- **Shell and launcher:**
  - `bash -n` and `shellcheck` are clean on `user-data.sh` (8,029 B), `launch.sh` and `dryrun.sh`;
    `--check-params` refuses an unfilled `SESSION` (rc 2) and a failsafe over 300 min.
  - The AWS CLI turns `--count 8:8` into MinCount 8 / MaxCount 8 (checked offline against a dead local endpoint).
  - The trap's `aws s3 rm --exclude '*' --include "*/h2e*"` deleted only host 2's keys on MinIO, not `h12e0` or
    `h3e0`.
  - `launch.sh` against a fake `aws` (no network):
    - refuses when 4 vCPUs are already running (no RunInstances);
    - stops cleanly on `VcpuLimitExceeded` ("nothing was launched");
    - moves to the next AZ on `InsufficientInstanceCapacity`;
    - terminates a short fleet;
    - `DRY=1` plans 46 steps and 198 engine-runs with failsafe 130 min and deadline +105 min.

## Decisions for the author

1. **Go and budget:** default plan ≈ USD 1.0–1.35 (cap ≈ 2.9), ≈ 7 instance-hours. It launches after the E-RQ5-PERF
   us-east-1 leg ends, because the vCPU cap of 16 is otherwise exceeded.
2. **Instance type:** 8 × t4g.large (recommended: twice the network baseline of medium, and the HDFS re-check is the
   N = 1 reference) or 8 × t4g.medium (−USD 0.25, but COST saw more shaping events). t3 would need the x86_64
   archive digest pinned first; it is not done because t4g is recommended.
3. **N = 16 with 2 engines per host** (`DENSE_REPS=5`, + ≈ USD 0.2). It shows density on the same 16 vCPUs, but it is
   a different regime (two engines share a NIC and CPU) and is reported separately.
4. **Sustained step:** none (default), 20 min (+ ≈ USD 0.85), or 60 min (+ ≈ USD 2.4). See the trade-off above.
5. **Reps:** 10 per N and 5 for the size-only control (default), or 5 per N (≈ −40% cost, CIs ≈ 1.4× wider).
6. **Request logging:** SDK DEBUG on in every step (default). Without it there are no measured request rates,
   retries or throttles without new permissions. The overhead is invisible locally, and every N carries it equally,
   but N = 1 then differs from COST's HDFS re-check by this setting. Alternative: log only one step in five.
7. **Fallback if the vCPU quota is below 16** (the operator cannot read Service Quotas; a refusal launches nothing):
   `HOSTS=4 NS=1,2,4 DENSE_REPS=5` (N = 8 as 2 per host), or ask AWS for a quota increase. Root is needed to see the
   quota.
8. **Larger scale:** if more than ≈ 400 MiB/s from 8 small VMs, that needs an IAM change
   (instance types beyond t3/t4g ≤ large) and a vCPU quota above 16. That is a separate yes and a separate estimate.
9. **H3.7 wording** (above): accept, edit or reject before the run is analysed.

## Not verified

- **Nothing has run on EC2:**
  - the `ami-launch-index` slot (one RunInstances call);
  - chrony on AL2023 (installed by default per AWS docs; recorded best effort);
  - ENA counters and `ethtool`;
  - XFS reflink on the AL2023 root volume: if it is absent, the sustained step is skipped;
  - real S3 request profiles. The per-batch request counts were measured against MinIO; S3 may add retries or other
    calls. The default config's key probe uses HEAD here, and S3's answer may differ;
  - the 8-host barrier latency over real S3.
- **RunInstances permission for `--count 8:8`.** The operator policy limits type, region, profile and tags, and
  `RunInstances --dry-run` was not called for this shape (PERF's dry-run covered a single t4g.large with the same
  profile and tags). A refusal launches nothing.
- **The account's vCPU quota** (unknown; see decision 7).
- **Time and request estimates:** from COST on one host and MinIO locally; the first steps give the real numbers.

## Observations for other stages

- **COST's `max_rss_mib` is the driver's peak RSS, not the engine's** (likely; seen here, not re-checked on COST's
  files). On Linux, `wait4` rusage `ru_maxrss` of a child started by fork/vfork + exec carries the parent's RSS
  high-water mark. In the AL2023 test the engine's own `VmHWM` was ≈ 68 MiB, while `ru_maxrss` read 139 MiB once the
  Python driver had grown. COST's ≈ 1,640 MiB "engine RSS" on HDFS is about HDFS's size (1.58 GB), which `cost.py`'s
  oracle patch maps into the driver. **Do not quote COST's RSS figure.** This stage records `VmHWM` instead (labelled
  `peak_rss_mib`; `ru_maxrss_mib` is kept and labelled as contaminated). Candidate entry for `LESSONS.md` (not
  edited here).
- **Several files in one source directory are archived concurrently** (v0.10.2, local probe: batches of 3 files
  committed at the same moments), and files renamed into the watched directory are picked up while the engine runs.
  A multi-file source therefore changes an engine's throughput regime; this stage keeps one file per engine.
- **The final batch at end of file can exceed `max_bytes` slightly** in run mode (local, 48 MiB slices:
  16,777,260 B against 16,777,216, +44 B, every run). On the full HDFS file in COST it never did (0 runs over).
  This is a minor #824 edge case worth a question to the engine maintainer; `analyze.py` reports
  `batches_over_max_bytes` and does not fail on it.
