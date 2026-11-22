# E-RQ5-COST: verification cost on EC2 + Amazon S3 (H3.5)

**Status: run (2026-11-21, v0.10.2).** 90/90 runs passed on t4g.medium (`results/20261121T002644Z-i-07a97d2f7a612888e`), and the HDFS re-check on t4g.large passed 30/30 (`results/20261121T012945Z-i-072d88074e58c7845`, `user-data-hdfs-recheck.sh`). The text below was written before launch; results are in each run's `ANALYSIS.md`, hypothesis H3.5 and `../LOG.md`.

**Question (H3.5, `research/hypotheses.md`).** On Amazon S3, does the RQ3 ordering hold? The store-computed
SHA-256 should add no measurable throughput cost against size-only, and BLAKE3 read-back should. Prediction:
on every dataset, the CI on the difference of medians includes 0 for store SHA-256 and lies below 0 for BLAKE3
read-back. Every run drains and passes the independent oracle.

## Design

| Factor | Value | Source / reason |
|---|---|---|
| Datasets | SSH (73,417,506 B + 1 newline), BGL (743,185,031 B), HDFS (1,577,982,906 B), Loghub record 8196385 | archive MD5 as `research/datasets/loghub/fetch.sh`; log SHA-256 and size as `inventory.csv`; all checked on the VM |
| Modes | `size-only` (checksum `none`, `require_strong_verification: false`), `sha256-store` (`sha256`, S3 `ChecksumSHA256` via HEAD), `blake3-readback` (`blake3`, GET + hash); all zstd level 3 | `exp_cost.py` VARIANTS, `smoke.py` MODES |
| Reps | 10 per dataset × mode = **90 runs** in one shuffled order (`random.Random(SEED=20261121)`; order recorded in `run.json`) | as `exp_minio_cost.py` |
| Batching | `max_bytes` 16 MiB, `max_records` 1,000,000, age 2 s, 8 concurrent batches | `vtoplab.Variant` defaults |
| Multipart | `upload.multipart_threshold_bytes` **64 MiB** in every mode, part size 8 MiB | Even the v0.10.1 run-mode defect produces batches of at most ≈ 2 × 16 MiB uncompressed. Objects are compressed, so every object is a single PUT. Each run checks `multipart_objects == 0` and that the largest object is < 64 MiB. |
| Receipts | `manifest.verification_receipts: true` | proof of the method that ran |
| S3 prefix | `cost/data/<run_id>/<dataset>-<mode>-r<rep>/`, fresh per run | v0.10.x reuses stably named objects, so a shared prefix would measure reuse |
| Host | **one t4g.medium**, Amazon Linux 2023 arm64, us-east-1, same region as the bucket | see below |

### Per-run procedure (`cost.py`)

1. Write a fresh config, ledger directory (0700) and work dir.
2. Run `vtopctl validate --print-effective` and record the planned method and the effective `max_bytes`.
3. Read the input once (`warm_read`, with the disk bytes it caused) so the engine reads from the page cache.
4. Start `vtopctl run` with umask 077 and poll the ledger every 20 ms. The run counts as drained only when the
   ledger shows the last byte committed **before** SIGTERM. A commit made by the shutdown flush after a timeout
   is recorded as `drained_after_stop` and fails the run (the dry run found this case).
5. Send SIGTERM and reap the engine with `wait4` to get its CPU time and peak RSS.

After the run, `cost.py` records:

- **Ledger:** states, the **uncompressed size of every committed batch** and the object sizes. Batch size is
  measured, never taken from the setting.
- **Engine log:** the `verification_passed` object-check messages and verification classes, `object_key_*`
  events, warnings and errors, and the shutdown line. It also sums every `*_ms` field of `batch_metrics`.
  v0.10.1 logs only `total_ms` and `state_write_ms`. The per-stage fields of #843 (planned for v0.10.2) are
  summed if present; absent fields are not zeros.
- **Linux counters:** CPU busy, iowait and **steal** (`/proc/stat`), root-disk bytes read and written
  (`/proc/diskstats`), NIC bytes, and the **ENA allowance-exceeded counters** (`ethtool -S`). Together they show
  whether CPU, disk or network throttling touched a run.
- **Oracle:** `smoke/oracle.py`, imported unchanged. It downloads every object and compares it byte for byte
  with the source range, checks digests, HEADs with `ChecksumMode=ENABLED`, and checks that receipts match the
  method inferred from HEAD, coverage is complete and no object is multipart. `cost.py` replaces only the
  oracle's whole-file read of the source with an mmap: HDFS is 1.58 GB, and the result is the same (checked in
  the dry run).
- **Rep 0 of each dataset × mode only:** `vtopctl scrub` and `audit-coverage --check-storage` exit codes.

**A run passes** only if all of these hold:

- it drained before SIGTERM, and the oracle passes;
- the log's `object_check` set is exactly the mode's message;
- the ledger class is `size_only` or `strong` as expected;
- receipt methods equal the HEAD-inferred methods;
- it was single-PUT.

The driver stops the matrix at the first run that does not drain or at the third failed run. It also starts no
new run after `DEADLINE_MIN` (160 min).

**Throughput** is computed as in `exp_cost.py`/`exp_minio_cost.py`: steady MiB/s is the committed bytes at the
second-to-last commit divided by its time, which excludes the final batch's 2 s age wait. End-to-end MiB/s is
also recorded. **Analysis** (`analyze.py`): per dataset, the median per mode and the difference of medians
against size-only (MiB/s and %). Each comes with a percentile-bootstrap 95% CI computed by `ci()`/`pct_ci()`
**imported** from `rq3-cost-ablation/code/analyze_minio_cost.py` (B = 10,000), so the method is identical.

### Why t4g.medium (not t4g.small)

Prices, network and EBS limits are from AWS (cited below). Both sizes have 2 Graviton2 vCPUs.

| | t4g.small | t4g.medium |
|---|---|---|
| Memory | 2 GiB | 4 GiB |
| EBS baseline / burst (128 KiB I/O) | 21.75 / 260.62 MB/s | 43.38 / 260.62 MB/s |
| Network baseline / burst | 0.128 / 5.0 Gbps | 0.256 / 5.0 Gbps |
| On-demand, us-east-1 | USD 0.0168/h | USD 0.0336/h |

- **RAM decides.** HDFS (1.58 GB) cannot stay in a 2 GiB page cache next to the engine (~0.1–0.25 GiB RSS seen
  so far), Python with boto3, and the OS. So on t4g.small every HDFS run would read 1.58 GB from EBS.
- **EBS would bias the measurement.** EBS bursts for only about 30 min a day. After that, reads drop to the
  21.75 MB/s baseline, and disk time would dominate and drift during the matrix, masking the effect being
  measured.
- **Network.** At the RQ3 compression ratio (≈ 10×), even 200 MiB/s of input is ≈ 20 MB/s of upload plus the
  same again for BLAKE3 read-back. That is above t4g.small's 16 MB/s network baseline but below t4g.medium's
  32 MB/s.
- **Inputs fit in the medium's cache.** The three inputs total 2.39 GB, which fits beside the engine in
  t4g.medium's ≈ 3.7 GiB. Each run re-reads its input first and records any disk read, so a cache miss is
  visible, not silent.
- **Fallback:** if the first results show engine-time disk reads on HDFS, use t4g.large (8 GiB, USD 0.0672/h,
  also allowed by the operator policy).
- **Disk.** The default 8 GiB gp3 root, the same volume smoke used, has room. Archives (0.25 GB) are deleted
  after extraction, and the inputs take 2.39 GB. `user-data.sh` refuses to start with less than 3.5 GB free
  and records `df` at start and end.
- **CPU credits.** T4g launches in **unlimited** mode by default, so the CPU is never throttled to baseline.
  Surplus credits are billed (see cost). The operator can confirm the account default before launch (step 3).

### Differences from the plan and from RQ3 (for the author)

- **One host.** The stage table in `../README.md` says "engine host + source host", and H3.5 says "engine on a
  separate EC2 host". Here the file sources are local files on the engine host, and the separate system is
  S3, as the brief for this stage asked. A separate source host only matters for Kafka (E-RQ5-PERF).
- **Batch size.** With #824, v0.10.2 should cut 16 MiB batches. The v0.7.0 MinIO campaign actually ran
  ≈ 32 MiB batches. Per-batch overhead therefore weighs twice as much here, so do not compare MiB/s directly
  with RQ3. To match RQ3's effective batch size instead, change `MAX_BYTES` in `cost.py` to 32 MiB before
  launch, and decide it before launch, not after.
- **Oracle on every run.** RQ3 ran the oracle on rep 0 only. It runs between engine runs, so it does not
  affect timing.

## Files

| File | Role |
|---|---|
| `user-data.sh` | cloud-init for AL2023 arm64. It does the following, in order: failsafe `shutdown -h +180`; EXIT trap; refuses an unfilled pin; records `ldd`; downloads the release and checks the pinned SHA-256 and `SHA256SUMS`; runs `vtopctl --version`, which must contain the version; optional cosign check; Python venv (boto3 1.44.0, zstandard 0.25.0, blake3 1.0.11, as smoke); fetches and verifies the inputs; runs `cost.py`. The EXIT trap deletes `cost/data/<run_id>/` unless `KEEP_DATA` exists, writes `exit.json` (with `data_objects_left`), uploads `/var/log/vtop-cost/` and powers off. 9.0 KB, well under the 16 KB user-data limit. `bash user-data.sh --check-params` checks the pin locally. |
| `cost.py` | Driver (above). It writes `run.json` (instance type, AMI, AZ, kernel, glibc, memory, engine version/commit/archive digest/`vtopctl` SHA-256, cosign status, `AWS_REQUEST_CHECKSUM_CALCULATION` and the other SDK variables, input SHA-256s, config, seed and order). It also writes `runs.jsonl` (one record per run), `ledger/<cell>.db`, `logs/<cell>.jsonl.gz` and `oracle/<cell>.json.gz`, and uploads them to `cost/results/<run_id>/` after every run. |
| `../smoke/oracle.py` | The independent oracle, staged unchanged beside `cost.py` |
| `analyze.py` | Validation and statistics, run locally: `summary.json`, `cost.csv`, `ANALYSIS.md`. Refuses an incomplete or failed matrix unless `--allow-failures` is given. |

## Launch (only after v0.10.2 is tagged and the author says "go")

From the repo root, with the operator profile:

```bash
export AWS_PROFILE=vtop-op AWS_REGION=us-east-1
B=vtop-eval-XXXXXXXXXXXX; C=research/experiments/rq5-cloud/cost
```

1. **Pin the release.** Fill in `VTOP_COMMIT` and `TARBALL_SHA256` in `$C/user-data.sh`, then check the
   signature locally. Check the v0.10.2 notes for #824 (batch size), #843 (stage timings), bounded object names
   and the glibc floor. Each run measures or records these anyway.
   ```bash
   gh release list -R allamiro/vtop-engine | sed -n 1,5p
   gh api repos/allamiro/vtop-engine/commits/v0.10.2 --jq .sha                       # -> VTOP_COMMIT
   d=$(mktemp -d) && gh release download v0.10.2 -R allamiro/vtop-engine -D "$d" \
     -p 'vtop-engine-0.10.2-aarch64-unknown-linux-gnu.tar.gz*' -p SHA256SUMS
   (cd "$d" && shasum -a 256 -c --ignore-missing SHA256SUMS && grep aarch64-unknown-linux-gnu.tar.gz SHA256SUMS)  # -> TARBALL_SHA256
   cosign verify-blob "$d/vtop-engine-0.10.2-aarch64-unknown-linux-gnu.tar.gz" \
     --bundle "$d/vtop-engine-0.10.2-aarch64-unknown-linux-gnu.tar.gz.sigstore.json" \
     --certificate-identity https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/v0.10.2 \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```
2. **Check the pin and the script:** `bash $C/user-data.sh --check-params && bash -n $C/user-data.sh`.
   Optionally, dry-run again with the v0.10.2 macOS binary (see [Local dry run](#local-dry-run-2026-11-20)).
3. **Read-only pre-flight.** The first command should report `unlimited`. The second should list no instance
   still running.
   ```bash
   aws ec2 get-default-credit-specification --instance-family t4g
   aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
     Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
   ```
4. **Stage the inputs** (recommended; otherwise the VM downloads them from Zenodo, which is slower and less
   reliable). SSH can also come from `smoke/input/`.
   ```bash
   for f in SSH.tar.gz BGL.zip HDFS_v1.zip; do aws s3 cp research/datasets/loghub/raw/$f s3://$B/cost/input/$f; done
   ```
5. **Launch.** The command stages `cost.py` and smoke's `oracle.py` to `cost/code/`, waits, and downloads the
   results to `$C/results/<run_id>/`. `MAX_MIN` must exceed the 180-min failsafe. **This is the exact
   command:**
   ```bash
   MAX_MIN=200 research/experiments/rq5-cloud/run-ec2.sh cost $C/user-data.sh t4g.medium \
     $C/cost.py research/experiments/rq5-cloud/smoke/oracle.py
   ```
   While it runs, you can check the credit mode and the console:
   `aws ec2 describe-instance-credit-specifications --instance-ids <id>`,
   `aws ec2 get-console-output --instance-id <id> --latest --output text | tail -40`.
6. **Analyze:** `research/.venv/bin/python $C/analyze.py $C/results/<run_id>`. Then record the result in
   `../LOG.md` and, if it holds up, in `hypotheses.md` (H3.5), with the author.
7. **Clean up**. `exit.json` shows `data_objects_left`. The data prefix is kept on purpose if a
   run failed (`KEEP_DATA`). Keep `cost/results/`; `cost/input/` and `cost/code/` are optional to remove.
   ```bash
   aws s3 rm --recursive s3://$B/cost/data/
   aws s3api list-multipart-uploads --bucket $B --prefix cost/ --query 'Uploads[].[Key,UploadId]' --output text \
     | while read -r k u; do [ "$k" = None ] || aws s3api abort-multipart-upload --bucket $B --key "$k" --upload-id "$u"; done
   aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
     Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
   aws ce get-cost-and-usage --time-period Start=<day>,End=<day+1> --granularity DAILY --metrics UnblendedCost   # lags ≤ 1 day
   ```

## Time and cost (estimate; prices checked 2026-11-20)

**Time.** The engine throughput on Graviton2 is an estimate. The smoke stage reached ≈ 60 MiB/s steady on SSH
on a t4g.small with v0.10.1 (`../smoke/results/20261120T181226Z-…`); the matrix has not run.

- **Engine:** at 50–100 MiB/s, the 2.39 GB per dataset × mode set takes 25–50 s, plus 3 × 2 s batch age:
  ≈ 30–55 s.
- **Oracle:** ≈ 145 batches × 5 requests, plus decompress and compare of 2.39 GB: ≈ 20 s.
- **Per-run overhead** (validate, warm read, ledger copy): ≈ 5 s.
- **Per rep-set:** ≈ 1 min per mode, so 30 sets take ≈ 30–40 min.
- **Other:** bootstrap ≈ 5 min, rep-0 scrub/audit ≈ 2 min, cleanup and upload ≈ 1 min.

| | Instance-hours (t4g.medium) |
|---|---|
| Expected | **≈ 0.75–1.0 h** (planned as 1 h) |
| Slow case (30 MiB/s) | ≈ 1.3 h |
| Hard cap (failsafe `shutdown -h +180`; no new run after 160 min) | **3.0 h** |

**Requests.** v0.10.2 should cut batches at ≤ 16 MiB: SSH ≥ 5, BGL ≥ 45 and HDFS ≥ 95, so 145 per
dataset × mode set and 4,350 for the matrix. Per batch the engine makes 3 PUTs (object, manifest, receipt) and
about 2–4 HEAD/GETs (key probe, verification HEAD, BLAKE3 GET). The oracle makes 5 GET-class requests per
batch. That gives ≈ 13,300 PUT/LIST and ≈ 42,000 GET-class requests in total, including scrub, audit and
listings. If #824 does not take effect, there are about half as many.

| Item | Expected (1 h) | Cap (3 h) | Price used (us-east-1) |
|---|---:|---:|---|
| t4g.medium on demand | 0.034 | 0.101 | USD 0.0336/h [1] |
| Public IPv4 while running | 0.005 | 0.015 | USD 0.005/h [2] |
| EBS gp3 8 GiB root | 0.001 | 0.003 | USD 0.08/GB-month [3] |
| CPU surplus credits (unlimited; bound: both vCPUs at 100% minus the 2 × 20% baseline = 1.6 vCPU-h/h) | ≤ 0.064 | ≤ 0.192 | USD 0.04/vCPU-h [4] |
| S3 PUT/COPY/POST/LIST, ≈ 13.3k | 0.067 | 0.067 | USD 0.005 per 1,000 [5] |
| S3 GET and other, ≈ 42k | 0.017 | 0.017 | USD 0.0004 per 1,000 [5] |
| S3 storage (≤ 6.5 GB for ≤ 1 h; inputs 0.25 GB for a few days) | < 0.002 | < 0.002 | USD 0.023/GB-month [5] |
| S3 → EC2 same region; data in from the internet; DELETE requests | 0 | 0 | free [6] |
| **Total** | **≈ 0.19** | **≈ 0.40** | well under USD 1 |

Sources (read 2026-11-20):

1. AWS public price list for EC2 on-demand Linux us-east-1 (`b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/US East (N. Virginia)/Linux/index.json`, published 2026-11-20T03:48Z): t4g.small 0.0168, t4g.medium 0.0336, t4g.large 0.0672 USD/h.
2. <https://aws.amazon.com/vpc/pricing/>: "Hourly charge for In-use Public IPv4 Address $0.005".
3. AWS public price list for EBS (`…/meteredUnitMaps/ec2/USD/current/ebs.json`, published 2026-11-20T21:09Z): gp3 storage, us-east-1, 0.08 USD/GB-month.
4. <https://aws.amazon.com/ec2/instance-types/t4/>: "a small additional charge of $0.04 per vCPU-hour". Unlimited is the T4g default and gives no launch credits (<https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/burstable-performance-instances-unlimited-mode-concepts.html>). The rate was **not** read from the on-demand pricing table itself, because the page renders by script.
5. AWS Price List API, AmazonS3 us-east-1 offer (`pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json`, publication 2026-11-20T02:23Z): Requests-Tier1 "$0.005 per 1,000 PUT, COPY, POST, or LIST requests"; Requests-Tier2 "$0.004 per 10,000 GET and all other requests"; Standard storage "$0.023 per GB - first 50 TB / month".
6. <https://aws.amazon.com/s3/pricing/>: "DELETE and CANCEL requests are free". Data transferred in from the internet, and data transferred to AWS services in the same Region, are listed as exceptions to transfer charges.
7. Instance memory, network and EBS limits: <https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html> (t4g.small 0.128/5.0 Gbps, 21.75/260.62 MB/s; t4g.medium 0.256/5.0 Gbps, 43.38/260.62 MB/s).

## Local dry run (2026-11-20)

This exercised the driver, oracle import, evidence checks, clean-up and analysis. It measured nothing about
the cloud.

- **Setup:**
  - MinIO `RELEASE.2025-09-07T16-13-09Z` in a container (`rq5cost-dry-minio`, 127.0.0.1:19400, removed after
    the run), with `COST_LAB_ENDPOINT` set. That variable adds `endpoint_url`, `force_path_style` and
    `verify_tls: false`, and is never set on EC2.
  - The v0.10.1 `aarch64-apple-darwin` `vtopctl`.
  - A 75,497,450-byte slice of BGL (the first 72 MiB, cut at the last newline), in an `in/bgl/` directory in
    a scratch directory.
  - 1 rep × 3 modes.
- **Command (shape):**
  `COST_LAB_ENDPOINT=http://127.0.0.1:19400 AWS_ACCESS_KEY_ID=… python cost.py --run-id dryrun-final-20261120 --vtopctl <v0.10.1 macOS> --input-dir <scratch>/in --bucket vtop-eval-dryrun --out <scratch>/out/final --work <scratch>/w --datasets bgl --reps 1 --seed 20261121 --vtop-version 0.10.1 --vtop-commit 44f6619… --tarball-sha256 … --delete-data`,
  then `analyze.py <out> --expect-reps 1`.

**Outcome: all three modes passed** (rc 0).

| Mode | Steady MiB/s | Wall s | Batches (uncompressed B) | Method (receipt = HEAD-inferred) | Log object_check | Multipart |
|---|---:|---:|---|---|---|---:|
| size-only | 147.6 | 2.53 | 33,554,277 / 33,554,342 / 8,388,831 | size_only | "object present and size matches (checksums disabled)" | 0 |
| sha256-store | 186.4 | 2.43 | same | store_checksum | "S3 service-computed SHA-256 verified" | 0 |
| blake3-readback | 171.6 | 2.47 | same | read_back | "stored content BLAKE3 verified" | 0 |

- **Every run:**
  - drained before SIGTERM, with "shutdown flush complete";
  - oracle byte-exact and complete, with 0 unsafe commits, 0 duplicate bytes and 0 unreferenced objects;
  - ledger class as expected, receipts matching HEAD;
  - `scrub` and `audit-coverage --check-storage` returned 0;
  - CPU 0.30–0.35 s and peak RSS ≈ 100 MiB.
- **Clean-up:** at the end, the driver deleted the 27 objects under `cost/data/<run_id>/`.
- **Single runs on a laptop are noise.** The same cells measured 180/143/159 MiB/s in an earlier dry run.
- **Batch-size defect, measured as designed.** v0.10.1 cut 2 of 3 batches at ≈ 32 MiB against
  `max_bytes` 16 MiB (`batches_over_max_bytes: 2`). With v0.10.2 this field should be 0 (#824).
- **Engine metrics.** `batch_metrics` carried only `total_ms` and `state_write_ms` (v0.10.1); the analysis
  reports these and nothing else.
- **Negative check.** One object of a size-only run was overwritten with the same size and one byte flipped.
  The oracle then failed (`pass: false`, `complete: false`, `unsafe_commits: 1`, "decompressed bytes differ
  from source range"). Before the corruption, the mmap-backed oracle result equalled the builtin-`open`
  result (all fields except per-batch detail).
- **Failure path.** With `--timeout 0.15`, the first run did not drain. The driver stopped the matrix (rc 1)
  and wrote `KEEP_DATA`, so the EXIT trap would keep the data for diagnosis. An earlier `--timeout 0.3` run
  exposed the shutdown-flush case: SIGTERM let the engine commit the remainder. `drained` now means drained
  before the stop.
- **Linux probes.** `/proc/stat`, `/proc/net/dev` and `ru_maxrss` (KiB on Linux) were checked in a
  `python:3.12-bookworm` container. `/proc/diskstats` and `/proc/mounts` parsing were unit-checked for
  `nvme0n1p1`, `xvda1` and `sda2`. Not exercised: `ethtool -S` on an ENA NIC, and the root-disk counter on
  real EBS.
- **user-data.sh:** `bash -n` passes, `shellcheck` reports nothing, and `--check-params` refuses the
  unfilled pin (rc 1) and malformed values (rc 2). A filled copy passed.

## Not verified here

- **v0.10.2 itself:** its tag, its digest, whether it contains #824, #843 and the object-name bound, and
  whether the `verification_passed` texts are unchanged. A changed text makes `log_as_expected` false, and
  the record shows the new text.
- **Instance role permissions: checked (operator, 2026-11-20).** The inline policy `vtop-eval-s3` on `vtop-eval-ec2`, as created (LOG.md 2026-11-20), allows `s3:ListBucket`, `GetBucketLocation`, `ListBucketMultipartUploads` on the bucket and `GetObject`, `PutObject`, `DeleteObject`, `GetObjectAttributes`, `AbortMultipartUpload`, `ListMultipartUploadParts` on its objects, so the in-VM clean-up can delete `cost/data/<run_id>/`. (Read from the creation command; the operator user cannot read IAM.)
- **The VM image:** whether `ethtool` and `unzip` install on the current AL2023 image. Both are best effort:
  the ENA counters are simply absent without `ethtool`; without `unzip`, the BGL and HDFS extraction fails
  early and the trap powers off.
- **The time estimate:** Graviton2 throughput at 16 MiB batches. The first rep-set's records give the real
  rate; the deadline and failsafe bound the cost either way.
