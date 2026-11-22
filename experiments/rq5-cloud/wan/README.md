# E-RQ5-WAN: VTOP over real long-distance links to Amazon S3

**Status: run (2026-11-21, v0.10.2).** All four regions ran: 272/272 runs passed with no unsafe commit. Combined analysis in `results/combined/ANALYSIS.md`; hypothesis H3.6 and `../LOG.md`. The text below was written before launch.

**Why.** The paper's long-distance numbers are emulated. §VII-D cites the engine's own measurement of the v0.7.0
upload path on one shared host (100 ms emulated RTT, 10 Mbit/s tbf; 10 Mbit/s policer with a 5,000 B bucket, where
the default committed nothing because of a fixed 300 s request deadline, #663) and our MinIO runs with +20/+50 ms
added by Toxiproxy (`rq3-cost-ablation/code/exp_minio_cost.py --proxy --latency-ms`). This stage measures the same
questions on real inter-region paths: the engine VM sits in a far region, the bucket stays in us-east-1, and the
round trip is whatever the network gives (measured and recorded). Low bandwidth, loss, jitter, a partition, a crash
and the policed uplink are then applied on the VM.

## Design

| Factor | Value | Reason |
|---|---|---|
| Regions (engine VM) | eu-west-1, ap-southeast-2, sa-east-1, ap-northeast-1 (D8); one VM each, launched separately (parallel is fine) | four distances to us-east-1 |
| Bucket | `vtop-eval-XXXXXXXXXXXX`, us-east-1; `upload.region: us-east-1` | real cross-region path |
| Instance | t4g.medium, AL2023 arm64, default VPC, public IPv4, outbound only | same type as E-RQ5-COST, so the native-link cells differ from it only by distance |
| Engine | v0.10.2 placeholders (`VTOP_COMMIT`, `TARBALL_SHA256`), same values as `cost/user-data.sh` | D5; checked as in cost/ |
| Dataset | SSH (73,417,506 B + 1 newline); policed cells use its first 32 MiB, cut at a newline (`ssh32`, made by `wan.py`) | SSH compresses to ≈ 5.2 MB (3 objects, smoke stage), so a run moves ≈ 5 MB each way. It drains in seconds on a native far link and in ≈ 25–50 s at 2 Mbit/s, keeping transfer charges at cents. The policed cells must run long enough that a single request exceeds 300 s, so they use 32 MiB uncompressed. |
| Batching | `max_bytes` 16 MiB (D6), age 2 s, 8 concurrent batches, zstd 3, receipts on | as cost/ |
| Multipart | threshold 64 MiB in the three verification modes, so store SHA-256 stays single-PUT (checked per run); engine default (8 MiB, 4 parts in flight) in the policed cells | as cost/; the policed cells keep the #663 workload shape |
| Never shaped or cut | link-local 169.254.0.0/16 (IMDS credentials, Route 53 resolver, NTP); the VPC CIDR is also kept open during the partition (DNS) | the instance role must keep working |

### Conditions per region (the default `PLAN` in `wan.py`)

| # | Condition | Modes × reps | What is applied (VM side; only while the engine runs) |
|---|---|---|---|
| 1 | `native` | size-only, sha256-store, blake3-readback × **10** | nothing: the real far link. Ten reps give bootstrap CIs comparable with E-RQ5-COST (10 reps) and RQ3. |
| 2 | `tc10`, `tc2` | sha256-store, blake3-readback × **3** | `tbf rate 10/2 mbit burst 15140 latency 100ms` on egress; ingress redirected (clsact + `mirred`) to `ifb0` with the same tbf, so read-back downloads are shaped too. Falls back to an ingress policer if the kernel has no ifb (recorded). |
| 3 | `cap10`, `cap2` | sha256-store, blake3-readback × **3** | VTOP's `upload.max_egress_bytes_per_second` = 1,250,000 / 250,000 B/s (s3_native only, ≥ 1024). It limits upload bodies only, so blake3 read-back is not limited: the difference from row 2 is part of the result. |
| 4 | `loss1`, `loss5`, `jitter` | sha256-store × **3** | netem on egress: random loss 1% / 5%; `delay 20ms 10ms distribution normal` (adds a mean of 20 ms one way and reorders packets) |
| 5 | `partition` | sha256-store × 1 | when 256 KiB have left the NIC (uploads under way), every egress packet except link-local and the VPC is dropped (clsact u32 `drop`) for 60 s, then restored. It measures retries and failed batches, the time to the first commit and to drain after the restore, and checks for no unsafe commit and complete coverage. |
| 6 | `sigkill` | sha256-store, blake3-readback × 1 | SIGKILL at the same trigger, then `vtopctl run` restarts on the same ledger. It measures the drain after the restart, counts duplicates by object URI (duplicate bytes, unreferenced objects, committed rows sharing an object), and checks for no unsafe commit and complete coverage. |
| 7 | `policed` | sha256-raw-mp × 1 | egress policer **1 Mbit/s**, bucket 5,000 B, drop (no queue), as #663's lab but at a lower rate. The engine runs with **shipped** request deadlines: 300 s + body / 4,096 B/s per attempt (#663, PR #666). This is the regression check: everything must commit, though single requests take more than 300 s. `old_deadline_exceeded` records whether the longest seal-to-commit was > 300 s; with 0 SDK retries and 0 failed batches, all 4 parts were in flight for that whole window. A region where it is false is reported as "not a #663 check". 1 Mbit/s rather than 2: at 2 Mbit/s and 100 ms the lab drained in 242 s, which the old deadline would also have passed. |
| 8 | `policed-fixed` | sha256-raw-mp × 1 | **control**, same link: `request_min_bytes_per_second: 10^9` (body allowance ≈ 0) and `request_timeout_seconds: 120`, which recreates the pre-#663 fixed deadline, shortened so the 480 s window covers 4 deadlines. Expected: nothing commits and nothing unsafe. This shows the link is slow enough that the regression check bites. |

Per region that is **68 runs**, in one order shuffled with seed 20261122 (recorded), so drift in the path over the
hour is spread over all cells. Kernel conditions turn TSO/GSO/GRO off while applied (64 KB super-packets would be
dropped whole by the 5,000 B policer and lost whole by netem), and the offloads are restored after.

**Every run is checked** by the same independent oracle as smoke and cost (`smoke/oracle.py`, unchanged; the mode
`sha256-raw-mp` is checked as the oracle's `sha256-multipart`). The checks are:

- byte-exact coverage, with 0 unsafe commits;
- the verification method agrees across the `verification_passed` log line, the ledger class, the receipts and
  the oracle's own HEAD with `ChecksumMode=ENABLED`;
- single-PUT where required, multipart seen where expected;
- duplicates counted by object URI.

`no_unsafe_commit` is checked on every run, including the control. Any unsafe commit stops the matrix and keeps the
data. A shaped run also needs every `tc` command to have succeeded. A cap run needs the config accepted and the
measured upload body rate ≤ cap × 1.05. (`validate --print-effective` does not list the cap at v0.10.1.)

**Measured per run** (`runs.jsonl`):

- wall and drain time; input MiB/s end-to-end (input bytes / time to the last commit) and the cost.py "steady" rate;
- NIC tx/rx bytes and Mbit/s over the drain;
- engine CPU s and peak RSS (both processes in the SIGKILL case); CPU busy/steal and ENA allowance counters;
- RTT: 5 TCP handshakes before every run;
- ledger seal-to-commit seconds per batch, and the messages of failed rows;
- from the engine log:
  - `upload_throttled` events;
  - "batch failed" / "did not reach VERIFIED" / REPLAY_REQUIRED messages;
  - SDK attempts and **retry attempts** (`RUST_LOG=info,aws_smithy_runtime::client::retries=debug`, which logs
    "attempt #N classified as …" once per attempt);
- the full `tc -s` statistics and every tc command with its exit code;
- multipart uploads aborted under the run's prefix.

**Per region once** (`run.json`):

- the tc capability preflight on the real NIC;
- RTT at the start and the end of the matrix: 20 TCP handshakes to both S3 names, with the resolved addresses, and
  20 HeadBucket calls on a kept-alive connection;
- link calibration per condition: a timed 2 MiB (1 MiB for loss and policed) PUT and GET through the shaping.

### Files

| File | Role |
|---|---|
| `user-data.sh` | cloud-init (9.4 KB < 16 KB). In order: failsafe `shutdown -h +150`; EXIT trap; refuses an unfilled pin; records `ldd`; installs `iproute-tc`, `ethtool`, `kernel-modules-extra` (AL2023 keeps `sch_netem` there) and records `modprobe` results; downloads the release and checks the pinned SHA-256, `SHA256SUMS` and `vtopctl --version`; optional cosign; Python venv as smoke/cost; SSH input from `smoke/input/` (or Zenodo), MD5 and SHA-256 checked; runs `wan.py`. The EXIT trap **first removes every tc rule and ifb0**, so results are never uploaded through a shaped or cut link. It then aborts open multipart uploads under `wan/data/<run_id>/`, deletes that prefix unless `KEEP_DATA`, writes `exit.json` and uploads `/var/log/vtop-wan/`. `RUN_ID` contains the region. |
| `wan.py` | Driver. It imports `../cost/cost.py` (counters, ledger and log evidence, oracle wrapper, incremental result upload, prefix deletion) and the smoke oracle, and adds the conditions above. Outputs: `run.json`, `runs.jsonl`, `ledger/`, `logs/`, `oracle/`, uploaded after every run. |
| `analyze.py` | Run locally on one or more region directories. It writes `summary.json`, `wan.csv` and `ANALYSIS.md`, with these tables: link (RTT), native per mode with bootstrap CIs (`ci`/`pct_ci` imported from `rq3-cost-ablation/code/analyze_minio_cost.py`, B = 10,000), vs size-only and, with `--baseline-runs`, vs the us-east-1 E-RQ5-COST SSH runs (the distance effect), tc vs engine cap, loss/jitter, partition, SIGKILL, policed. It fails on any unsafe commit, always, and on any failed run unless `--allow-failures`. |

## Launch (only after v0.10.2 is tagged and the author says "go" for E-RQ5-WAN)

From the repo root:

```bash
export AWS_PROFILE=vtop-op
B=vtop-eval-XXXXXXXXXXXX; W=research/experiments/rq5-cloud/wan
```

1. **Pin the release:** copy `VTOP_COMMIT` and `TARBALL_SHA256` from `cost/user-data.sh` (same v0.10.2 pin, filled
   in by `cost/README.md` step 1) into `$W/user-data.sh`.
2. **Check** `bash $W/user-data.sh --check-params && bash -n $W/user-data.sh`.
3. **Read-only pre-flight, per region:** the credit default should be `unlimited`; no project instance should be
   left; the AMI must resolve.
   ```bash
   for R in eu-west-1 ap-southeast-2 sa-east-1 ap-northeast-1; do
     AWS_REGION=$R aws ec2 get-default-credit-specification --instance-family t4g --query InstanceFamilyCreditSpecification.CpuCredits
     AWS_REGION=$R aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
       Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
   done
   ```
4. **Launch, one command per region.** Each stages `wan.py`, `cost.py` and `oracle.py` to `wan/code/` (same files
   for every region), waits for the VM, and downloads `wan/results/<run_id>/` into `$W/results/`. `MAX_MIN` must
   exceed the 150-min failsafe. They can run in parallel from four shells:
   ```bash
   AWS_REGION=eu-west-1      MAX_MIN=170 research/experiments/rq5-cloud/run-ec2.sh wan $W/user-data.sh t4g.medium $W/wan.py research/experiments/rq5-cloud/cost/cost.py research/experiments/rq5-cloud/smoke/oracle.py
   AWS_REGION=ap-southeast-2 MAX_MIN=170 research/experiments/rq5-cloud/run-ec2.sh wan $W/user-data.sh t4g.medium $W/wan.py research/experiments/rq5-cloud/cost/cost.py research/experiments/rq5-cloud/smoke/oracle.py
   AWS_REGION=sa-east-1      MAX_MIN=170 research/experiments/rq5-cloud/run-ec2.sh wan $W/user-data.sh t4g.medium $W/wan.py research/experiments/rq5-cloud/cost/cost.py research/experiments/rq5-cloud/smoke/oracle.py
   AWS_REGION=ap-northeast-1 MAX_MIN=170 research/experiments/rq5-cloud/run-ec2.sh wan $W/user-data.sh t4g.medium $W/wan.py research/experiments/rq5-cloud/cost/cost.py research/experiments/rq5-cloud/smoke/oracle.py
   ```
   Recommended: launch **one region first** (eu-west-1, the cheapest) and read its first runs (console output, or
   `aws s3 cp s3://$B/wan/results/<run_id>/runs.jsonl -`). This shows that the netem/ifb modules loaded
   (`netmods.txt`, `run.json` preflight) before the other three spend. A VM whose kernel lacks a primitive skips
   those cells and records why (`run.json` `skipped`).
5. **Analyze:**
   `research/.venv/bin/python $W/analyze.py $W/results/<eu> $W/results/<syd> $W/results/<sp> $W/results/<tok> --out $W/results/combined [--baseline-runs research/experiments/rq5-cloud/cost/results/<run_id>/runs.jsonl]`.
   Then record the outcome in `../LOG.md`; nothing enters the paper before its hypothesis is answered.
6. **Clean up**. `exit.json` shows `data_objects_left`.
   ```bash
   aws s3 rm --region us-east-1 --recursive s3://$B/wan/data/
   aws s3api list-multipart-uploads --region us-east-1 --bucket $B --prefix wan/ --query 'Uploads[].[Key,UploadId]' --output text \
     | while read -r k u; do [ "$k" = None ] || aws s3api abort-multipart-upload --region us-east-1 --bucket $B --key "$k" --upload-id "$u"; done
   for R in us-east-1 eu-west-1 ap-southeast-2 sa-east-1 ap-northeast-1; do
     AWS_REGION=$R aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
       Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text
   done
   aws ce get-cost-and-usage --time-period Start=<day>,End=<day+1> --granularity DAILY --metrics UnblendedCost \
     --group-by Type=DIMENSION,Key=REGION     # lags up to a day; transfer shows under the source region
   ```

### `run-ec2.sh` changes for other regions (patched 2026-11-21)

Read for non-us-east-1 use. The AMI filter (`al2023-ami-2023.*-kernel-6.1-arm64`, owner amazon), the default VPC
lookup and the instance profile already work per region. Changed:

- (a) Every S3 call (`cp` of code, `ls` and `cp` of results) now passes `--region $BUCKET_REGION` (default
  us-east-1). Otherwise, with `AWS_REGION=eu-west-1`, the CLI would address the bucket in the wrong region and rely
  on redirects.
- (b) A launch now fails early if no AMI is found.
- (c) The subnet is a default subnet in an AZ that **offers the instance type**
  (`describe-instance-type-offerings`), not `Subnets[0]`, because not every AZ offers t4g.
- (d) The region now appears in the launch and leftover-check log lines and in the console-output hint.

`bash -n` passes. `shellcheck` reports only SC2015 (info) on the last line, an `A && B || C` form that the original
line already had. The us-east-1 behaviour is unchanged apart from the AZ filter.

## Time and cost (estimate; prices read 2026-11-21)

**Time per region** (from the lab runs below, scaled to the real RTT; the real numbers come from the first region):

| Part | Estimate |
|---|---|
| bootstrap (dnf incl. kernel-modules-extra, pip, release, input) | 5–7 min |
| RTT probes, preflight, calibration (tc2 ≈ 18 s, policed 1 MiB ≈ 25–60 s) | ≈ 3 min |
| native, 30 runs × (≈ 4–10 s drain + oracle ≈ 5 s + overhead ≈ 4 s) | ≈ 8–10 min |
| tc/cap, 24 runs (tc2 blake3 ≈ 50 s, tc2/cap2 ≈ 25 s, 10 Mbit/s ≈ 6–15 s) + oracle | ≈ 12–15 min |
| loss/jitter, 9 runs | ≈ 4–8 min |
| partition (60 s cut + recovery) and 2 SIGKILL runs | ≈ 4 min |
| policed treatment: 32 MiB at ≈ 0.3–0.6 Mbit/s aggregate goodput behind the 1 Mbit/s policer | ≈ 8–15 min |
| policed control: 480 s window + up to 120 s shutdown | ≈ 10 min |
| **Expected** | **≈ 60–85 min; planned as 1.25 h** |
| **Hard cap** (failsafe `shutdown -h +150`; no new run after 125 min; policed timeout 45 min) | **2.5 h** |

**Transfer per region (planned bytes).** About 0.6 GB uploaded from the VM to S3 us-east-1:

- 68 runs × ≈ 5.3 MB;
- policed ≈ 34 MB plus retransmissions;
- the control's retries, at most 1 Mbit/s for ≤ 600 s, so ≤ 75 MB;
- calibration and results.

About 0.6 GB downloaded from S3 us-east-1 to the VM:

- the oracle reads every object of every run;
- BLAKE3 read-back in 23 runs;
- the policed read-back and the oracle read 34 MB each.

The bound used for the cap is 1.0 GB each way.

**Requests per region:** ≈ 1,700 PUT/LIST-class and ≈ 3,000 GET-class (≈ 5 batches × 3 PUTs per run, the engine's
HEAD/GET probes and verification, the oracle's 5 per batch, result uploads, multipart listings).

| Item | Price used | eu-west-1 | ap-southeast-2 | sa-east-1 | ap-northeast-1 |
|---|---|---:|---:|---:|---:|
| t4g.medium on demand, USD/h [1] | per region | 0.0368 | 0.0424 | 0.0536 | 0.0432 |
| EC2 → us-east-1 (S3 uploads), USD/GB [2] | InterRegion Outbound | 0.02 | 0.098 | 0.138 | 0.09 |
| us-east-1 → region (S3 downloads), USD/GB [2] | InterRegion Outbound from us-east-1 | 0.02 | 0.02 | 0.02 | 0.02 |
| gp3 root 8 GiB, USD/GB-month [3] | per region | 0.088 | 0.096 | 0.152 | 0.096 |
| **Expected (1.25 h, 0.6 GB each way)**: instance | | 0.046 | 0.053 | 0.067 | 0.054 |
| public IPv4 (USD 0.005/h [4]) | | 0.006 | 0.006 | 0.006 | 0.006 |
| EBS | | 0.001 | 0.001 | 0.002 | 0.001 |
| transfer up / down | | 0.012 / 0.012 | 0.059 / 0.012 | 0.083 / 0.012 | 0.054 / 0.012 |
| S3 requests (us-east-1 rates [5]) | | 0.010 | 0.010 | 0.010 | 0.010 |
| **subtotal without CPU credits** | | **0.09** | **0.14** | **0.18** | **0.14** |
| CPU surplus credits, bound (1.6 vCPU-h/h × USD 0.04 [6]) | | ≤ 0.08 | ≤ 0.08 | ≤ 0.08 | ≤ 0.08 |
| **Cap (2.5 h, 1.0 GB each way, credits at the bound)** | | **0.33** | **0.42** | **0.49** | **0.41** |

**Totals for the four regions.**

| | USD |
|---|---|
| Expected, without CPU credits | **≈ 0.55** |
| Expected, with the CPU-credit bound | ≈ 0.87 |
| Failsafe cap | **≈ 1.65** |

The runs are network-bound, so the credits should stay near 0. With E-RQ5-COST (≤ 0.40) the month stays far under
the USD 10 budget. **Suggested failsafe for the author's go: USD 2.00 for the stage.** If Cost Explorer shows more
than USD 1 after the first region, stop and look.

Sources (read 2026-11-21; not guessed):

1. AWS public price list for EC2 on-demand Linux, one file per region
   (`b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/<Region name>/Linux/index.json`,
   hawkFilePublicationDate 2026-11-20T20:22:16Z). t4g.medium: EU (Ireland) 0.0368, Asia Pacific (Sydney) 0.0424,
   South America (Sao Paulo) 0.0536, Asia Pacific (Tokyo) 0.0432, US East (N. Virginia) 0.0336.
2. AWS Price List API, AWSDataTransfer offer, region files of version 20261119105841 (publicationDate
   2026-11-19T10:58:41Z). Each is an "InterRegion Outbound" product:
   - "$0.02 per GB - EU (Ireland) data transfer to US East (Northern Virginia)"
   - "$0.098 per GB - Asia Pacific (Sydney) data transfer to US East (Northern Virginia)"
   - "$0.138 per GB - South America (Sao Paulo) data transfer to US East (Northern Virginia)"
   - "$0.09 per GB - Asia Pacific (Tokyo) data transfer to US East (Northern Virginia)"
   - "$0.02 per GB - US East (Northern Virginia) data transfer to" each of the four
   - InterRegion Inbound $0.00

   The S3-accelerated and "MultipathResilience" products in the same files do not apply: no Transfer Acceleration
   is used.
3. AWS public price list for EBS (`…/meteredUnitMaps/ec2/USD/current/ebs.json`, published 2026-11-20T21:09Z),
   "Storage General Purpose gp3 GB Mo".
4. <https://aws.amazon.com/vpc/pricing/>: "Hourly charge for In-use Public IPv4 Address $0.005". One rate, not
   per region.
5. As cost/README source 5: AmazonS3 us-east-1 offer, $0.005 per 1,000 PUT/COPY/POST/LIST and $0.004 per 10,000
   GET. Requests are billed in the bucket's region.
6. <https://aws.amazon.com/ec2/instance-types/t4/>: "$0.04 per vCPU-hour". **Not checked per region:** the page
   gives one figure, and the per-region CPU-credit rows of the EC2 offer files were not read. The figure enters only
   the bound.

## Local tests (2026-11-21; no AWS calls)

See the LOG entry of 2026-11-21 for the run identifiers.

**A. macOS dry run.** MinIO `RELEASE.2025-09-07T16-13-09Z` in Docker, the v0.10.1 `aarch64-apple-darwin` binary,
SSH full, `--no-tc` (no kernel conditions on macOS).

- `native` × size-only / sha256-store / blake3-readback: all passed, drain 2.4–2.8 s, 3 batches of 33,554,257 /
  33,554,257 / 6,308,993 B. These are v0.10.1 run-mode batches, ≈ 2 × max_bytes; #824 fixes this in v0.10.2.
- `cap2` sha256-store: passed, drain 23.7 s, upload body 217 kB/s against a 250 kB/s cap.
- `cap10`: passed, 784 kB/s against 1.25 MB/s; at loopback speed compression, not the cap, limited it.
- `sigkill` × sha256-store and blake3-readback, run with a lab-only 1 MB/s engine cap so that the kill lands mid-run:
  - killed at 3.0 s with 33,554,257 B committed;
  - the restart drained in 5.3 s / 6.1 s;
  - oracle complete, 0 unsafe, 0 duplicate bytes, 0 unreferenced objects;
  - one REPLAY_REQUIRED recovered in the sha256 case.
- A first version of the cap check read the cap from `validate --print-effective`, which does not list it, so those
  two runs failed. The check was replaced by "config accepted and measured rate ≤ cap".

**B. Linux, Docker Desktop (kernel 7.0.14-linuxkit), AL2023 userland** (`amazonlinux:2023` + `iproute-tc`
6.10, Python 3.11, the same pip pins). The engine container had NET_ADMIN, and MinIO ran on a Docker network with
`netem delay 100ms` on its side as the "far link". The v0.10.1 aarch64 Linux release was verified against SHA256SUMS.
This kernel has **no ifb**, so the run also exercised the recorded fallback, an ingress policer. Results in the
table below.

**C2. Policed at 1 Mbit/s** (Lima VM, `lima-2`, same 100 ms layout, after lowering the policer):

- `policed` sha256-raw-mp: drained in **364.5 s**, wire tx 0.77 Mbit/s.
- Longest seal-to-commit **361.7 s > 300 s**, so `old_deadline_exceeded` is true, with 0 SDK retries and 0 failed
  batches. The 4 parts of the 32 MiB object were each in flight for > 300 s, and the shipped deadline
  (300 s + 8 MiB / 4,096 B/s = 2,348 s) let them finish.
- Oracle complete, 0 unsafe.
- Offloads: TSO and GSO were on and were turned off for the run, then restored.

**C. Linux, Lima VM `vtop-bench-483a` (Ubuntu 24.04, kernel 6.8, ifb available)**, the same layout in Docker
(python:3.11-slim + iproute2), 100 ms added RTT:

- preflight: every primitive available, including **ifb + mirred**.
- Calibration through tc10: 7.67 / 7.55 Mbit/s PUT / GET. Through tc2: 1.87 / 1.89 Mbit/s. Native: 12.4 / 16.4
  Mbit/s (2 MiB in slow start at 100 ms).
- `tc2` sha256-store: drain 26.0 s, tx 1.67 Mbit/s.
- `tc2` blake3-readback: drain 48.2 s, tx 0.92 / **rx 0.92 Mbit/s**, so the read-back was shaped on ingress through
  ifb0.
- `tc10` blake3-readback: drain 15.2 s.
- `partition` sha256-store:
  - the cut came mid-run, with nothing committed yet;
  - 0 commits during the 60 s cut;
  - tx resumed 1.1 s after the restore, the first commit came at 2.8 s and the run drained at 7.8 s;
  - 4 SDK retry attempts and 1 "batch failed" (replayed);
  - oracle complete, 0 unsafe.
- All passed.

Lab B results (`lab-1`; 1 rep per cell; v0.10.1 run-mode batches of ≈ 32 MiB, so SSH = 3 objects and the
policed slice = 1 multipart object of 4 parts). Preflight: clsact, u32 drop, police, tbf, netem yes; ifb/mirred
no. Calibration (2 MiB, 1 MiB under loss and the policer), PUT / GET Mbit/s:

| Calibration | PUT Mbit/s | GET Mbit/s |
|---|---:|---:|
| native | 18.1 | 20.7 |
| tc2 (ingress policer fallback) | 1.87 | 1.84 |
| loss5 | 1.81 | 7.42 |
| jitter | 7.02 | 10.7 |
| policer 2 Mbit/s | 0.35 | 9.2 |

| Cell | Pass | Drain s | Wire tx / rx Mbit/s | SDK retries | Notes |
|---|---|---:|---:|---:|---|
| native sha256-store | yes | 6.1 | 6.84 / 0.04 | 0 | |
| tc2 sha256-store | yes | 26.2 | 1.66 / 0.04 | 0 | |
| tc2 blake3-readback | yes | 51.3 | 0.85 / 0.86 | 0 | read-back limited by the ingress policer |
| cap2 sha256-store (VTOP cap 250,000 B/s) | yes | 25.7 | 1.67 / 0.04 | 0 | upload body 200,265 B/s |
| loss5 sha256-store | yes | 60.4 | 0.72 / 0.02 | 0 | ≈ 10 × native; TCP recovered every loss, so no request was retried |
| jitter sha256-store | yes | 6.6 | 6.56 / 0.17 | 0 | |
| partition sha256-store | yes | 67.0 | 0.70 / 0.01 | 4 | cut at 0 B committed; 0 commits during; tx resumed 0.7 s, first commit 1.9 s, drained 6.1 s after the restore; 1 batch failed and was replayed |
| sigkill sha256-store | yes | 6.9 | 7.14 / 0.05 | 0 | killed at 0 B committed; restart drained in 6.0 s; 1 REPLAY_REQUIRED recovered; 0 duplicate bytes, 0 unreferenced objects |
| policed (2 Mbit/s) sha256-raw-mp | yes | 242.4 | 1.16 / 1.13 | 0 | 1 multipart object; seal-to-commit 239.5 s, **< 300 s**, so not a #663 check at 2 Mbit/s: the policer was lowered to 1 Mbit/s (lab C2 below) |
| policed-fixed (2 Mbit/s; control: fixed 120 s deadline) | yes (as expected) | – (window 480 s) | 1.56 / – | 4 | **0 B committed** in 480 s. The 4 retry attempts are SDK TransientError = request timeouts. The SIGTERM flush did not finish within 120 s, so the driver SIGKILLed the engine, leaving 1 `checksummed` row; 0 unsafe. The wire rate shown is the corrected one: the first driver version reported 2.01 Mbit/s by dividing bytes counted through the shutdown by the shorter window (fixed). |

**B2. Final regression run** of the edited driver (`lab-2`, same container): `native` size-only 5.7 s; `loss1` 17.0 s
(≈ 3 × native); `sigkill` blake3-readback, killed at 0 B, restart drained in 8.4 s, 1 replay, 0 duplicates. All
passed, 0 unsafe. `policed` was **skipped**, as designed, by `--hard-stop-epoch`, because its 45 min timeout would
have passed the failsafe.

Every run: oracle complete and byte-exact, 0 unsafe commits, receipts = HEAD-inferred method = log line = ledger
class, 0 open multipart uploads left, data prefix deleted at the end.


## Not verified here (needs the first real run, or the author)

- **v0.10.2 itself**: tag, digest, #824 batch size, log texts. The same caveats as cost/README apply.
- **AL2023's kernel modules.** No local test ran on the AL2023 *kernel* (6.1): Docker uses the host kernel. The
  claim that `sch_netem` (and possibly `ifb`) come from `kernel-modules-extra` is from memory of the AL2023/Fedora
  packaging, not checked. `user-data.sh` installs the package and records `modprobe` results, and `wan.py` skips any
  condition whose primitive fails on the real NIC, recording why. Launch one region first (step 4).
- **ENA offload toggles.** Whether `ethtool -K ens5 tso/gso/gro off` is accepted on ENA was not tested. Failures are
  recorded and do not stop a run, but a policed run with TSO still on would see whole super-packets dropped (visible
  in `tc_stats`).
- **Which S3 addresses the engine reaches** from each region. They are resolved and recorded per run, not known in
  advance. The partition does not depend on them: it drops all egress but link-local and the VPC.
- **That the policed treatment really holds a request for > 300 s on each real path.** In the lab (100 ms), 1 Mbit/s
  gave 361.7 s (2 Mbit/s gave 239.5 s, too short). Paths faster than the lab could fall under 300 s; eu-west-1, at
  ≈ 70 ms, is the candidate. `old_deadline_exceeded` and the analysis flag it per region; such a region's
  treatment is reported as "not a #663 check", not as a pass of one.
- **CPU-credit price per region** (source 6) and **real transfer volumes**: Cost Explorer after the run is the
  record.
- **Instance role** `vtop-eval-ec2` from a non-us-east-1 VM. IAM is global, and the role is bucket-scoped, not
  region-scoped, as read from its creation command (cost/README). Not exercised from another region yet.
