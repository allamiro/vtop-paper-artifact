# RQ5 cloud — run log

Chronological, UTC+1 unless marked. One entry per action that creates, changes, runs, costs, or deletes
something. Format: time — actor — action — result — evidence.

## 2026-11-20

- 05:27 — scope of the cloud campaign set: all earlier runs were on a single laptop, and there was no
  quantitative comparison with prior similar work.
- — author — chose AWS after comparing DigitalOcean Spaces and Google Cloud Storage (see LESSONS.md,
  "choosing a provider").
- — operator — installed AWS CLI 2.41.2 (Homebrew) on the author's Mac.
- — author + operator — `aws login --profile vtop-eval --region us-east-1`: browser sign-in by the author;
  CLI session is the account root (`arn:aws:iam::XXXXXXXXXXXX:root`). Pre-existing state checked: no EC2
  instances, no budgets, two unrelated buckets (left untouched).
- — operator (root session) — created bucket `vtop-eval-XXXXXXXXXXXX` (us-east-1, public access blocked,
  tag project=vtop-eval).
- — operator — created IAM role and instance profile `vtop-eval-ec2` (inline policy: S3 on the test bucket
  only).
- — operator — attempt to create assumable role `vtop-eval-operator` failed twice: zsh `:r` modifier garbled
  the principal ARN (`MalformedPolicyDocument`); root cannot assume roles (`AccessDenied`). Role not created.
- — operator — creating an IAM user access key needed the author's explicit approval.
- — operator — created IAM user `vtop-eval-operator` with the restricted inline policy; access key stored
  directly in local profile `vtop-op` (not printed). Verified: identity is the user; test bucket access OK;
  listing other buckets refused; other regions refused.
- — operator (root session) — created budget `vtop-eval-monthly`: USD 10/month; e-mail alerts to the
  author's address at 50% and 85% actual and 100% forecast.
- — operator — started preparing the `smoke/` scripts (no AWS calls, no runs).
- Cost so far: USD 0 (no compute started; an empty bucket and IAM objects are free).
- — author — decision D4: build on the latest release, vtop-engine v0.8.0 (6669b3b), for the cloud
  campaign and re-run RQ1–RQ4 on it so the paper describes one build. smoke/ preparation redirected to v0.8.0;
  engine maintainer asked for the v0.7.0→v0.8.0 behaviour changes.
- — author — pointed out the engine is at v0.10.1. Verified with `gh release list`: v0.9.0 (11-14),
  v0.10.0 and v0.10.1 (11-15) exist; v0.10.1 tag → 44f6619; main is 20 commits ahead. D4 corrected to
  v0.10.1. smoke/ preparation redirected.
- — operator — wrote `smoke/` (README.md, user-data.sh, smoke.py, oracle.py) for v0.10.1 (archive
  SHA-256 173aa74c…82f0, matches SHA256SUMS). Dry run on local MinIO with the v0.10.1 macOS binary: all four
  modes (size-only, sha256-store, blake3-readback, sha256-multipart) passed; a planted same-size corruption
  failed the oracle as intended. No AWS calls.
- — operator — reviewed safety of user-data.sh (terminate-on-shutdown launch flag, 120-min failsafe shutdown,
  no keys on the VM, checksum-verified release, endpoint variables stripped). Checked the dry-run findings
  against the v0.7.0 records: run-mode batches averaged 23–31 MiB at a 16 MiB setting (paper §VII-A wording
  wrong for run-mode results; change ledger X1); zero per-stage sums are unused by the analysis (X2).
- 2026-11-20 — author — "go" for E-RQ5-SMOKE.
- — operator — staged `smoke.py`, `oracle.py` and `SSH.tar.gz` (4,596,666 B) to `s3://vtop-eval-XXXXXXXXXXXX/smoke/`.
  AMI `ami-0697409d504dfb93d` (al2023-ami-2023.12.20261120.0-kernel-6.1-arm64; found with describe-images,
  the operator policy has no SSM).
- — operator — the account had **no default VPC** in us-east-1 (two non-default VPCs, untouched). Created the
  default VPC `vpc-0a3c0da0f712a8b93` (tag project=vtop-eval; public subnets in 1a/1b/1c; default security
  group: no inbound from outside, all outbound).
- 18:05:50Z — operator — launched `i-0e8cada773da9188c` (t4g.small, 2 vCPU, 2 GiB, us-east-1a,
  subnet-06f60d660fa1815b6, public IPv4, IMDSv2 required, shutdown → terminate, instance profile
  vtop-eval-ec2, no SSH key, no inbound rule). Waiting for self-termination.
- 18:07:17Z — VM — attempt 1 terminated itself after ~90 s: `exit.json` exit_code 141 (SIGPIPE). Cause:
  `ldd --version 2>&1 | head -1 | tee …` under `set -euo pipefail` — head closes the pipe, ldd dies of
  SIGPIPE, pipefail aborts the script. Evidence kept: `smoke/results/20261120T180601Z-i-0e8cada773da9188c/`
  (glibc 2.34 recorded; no engine run, no S3 data written). Safety net worked: logs uploaded, VM terminated.
- — operator — fixed user-data.sh (write ldd output to a file, then head the file); reproduced the old
  pipeline's 141 and the new form's success locally; no other `| head` pipeline in the scripts. Relaunching.

- 18:08:16Z — operator — launched attempt 2 `i-09b286e1bf195227b` (same settings, fixed user-data.sh).
- 18:10:23Z — VM — attempt 2 (`i-09b286e1bf195227b`) terminated after ~2 min, exit 1. **VTOP v0.10.1 on real
  S3: all four modes drained and committed** (3 batches each, 33,554,257 B — the run-mode batch-size defect
  reproduced on AWS; 3.2–4.7 s per mode). sha256-multipart passed every check (2 multipart objects read back,
  the 6 MB tail object verified by the store checksum; receipts agree). size-only, sha256-store and
  blake3-readback failed in **our oracle**: `MemoryError` — `zstandard.decompress(max_output_size=2 GiB)`
  preallocates the limit for frames without a content size; a 2 GiB t4g.small cannot. Evidence:
  `smoke/results/20261120T180829Z-i-09b286e1bf195227b/`.
- — operator — oracle.py: streaming zstd decompression (`stream_reader(..., read_across_frames=True).readall()`);
  tested locally on no-size, sized and two-frame inputs (peak RSS 319 MiB for 33 MiB). Deleted attempt 2's
  `smoke/data/` objects first, because v0.10.1 reuses an existing stably named object and would not upload.
- 18:12:14Z — operator — attempt 3 launched with the launcher (`run-ec2.sh`): `i-0579609654faf2afe`,
  t4g.small, us-east-1a; terminated 18:14:17Z; exit 0.
- **E-RQ5-SMOKE PASSED (attempt 3), VTOP v0.10.1 on Amazon S3, us-east-1.** All four modes drained, 0 unsafe
  commits, complete byte-exact coverage of the 73,417,507-byte SSH log, 0 bad objects, 0 duplicate bytes;
  verification method inferred by the oracle = method in VTOP's receipts in every mode: size-only →
  size_only; sha256-store → store_checksum (S3 HeadObject with ChecksumMode returned a FULL_OBJECT
  ChecksumSHA256 equal to the stored bytes' hash and to VTOP's digest); blake3-readback → read_back;
  sha256-multipart → read_back for the 2 multipart objects, store_checksum for the 6 MB tail. 3 batches of
  33,554,257 B per mode (run-mode batch-size defect, X1). Wall 3.3–4.7 s per mode (single run; not a cost
  result). Sigstore signature not checked on the VM (COSIGN_VERSION empty); archive matched the pinned SHA-256
  and SHA256SUMS. Evidence: `smoke/results/20261120T181226Z-i-0579609654faf2afe/`.
- — operator — clean-up: `smoke/data/` emptied (0 objects), 0 open multipart uploads, 0 project instances
  running. Kept: `smoke/results/`, `smoke/code/`, `smoke/input/`.
- Cost: Cost Explorer for 2026-11-20 shows USD 0.000000012 so far (data lags up to a day; three VMs of
  ~2 min each). Re-check tomorrow.


## 2026-11-20 — E-RQ5-COST prepared (not launched; waits for v0.10.2, decision D5)

- 22:29Z — operator — wrote `cost/` (user-data.sh, cost.py, analyze.py, README.md); oracle = `smoke/oracle.py`
  imported unchanged (source read through an mmap for the 1.58 GB HDFS input). Matrix SSH/BGL/HDFS × size-only /
  sha256-store / blake3-readback × 10 = 90 runs, shuffled (seed 20261121), one t4g.medium (4 GiB keeps HDFS in the
  page cache; t4g.small's 2 GiB would read HDFS from EBS at a 21.75 MB/s baseline after the burst). Fresh prefix
  `cost/data/<run_id>/<cell>/` per run; multipart threshold 64 MiB in every mode (single-PUT checked per run);
  batch sizes measured from the ledger. `user-data.sh` refuses to run until `VTOP_COMMIT`/`TARBALL_SHA256` are
  filled (`--check-params` checks it locally). No AWS calls.
- — operator — local dry run, MinIO container + v0.10.1 macOS binary, 72 MiB BGL slice, 1 rep × 3 modes:
  **all three passed** (drained, oracle byte-exact, receipts = HEAD-inferred method = log message = ledger class,
  0 multipart, scrub/audit rc 0; data prefix deleted, 27 objects). Batches 33,554,277/33,554,342/8,388,831 B at
  `max_bytes` 16 MiB (v0.10.1 run-mode defect, measured as designed). Negative check: a same-size corrupted object
  failed the oracle (`unsafe_commits: 1`); failure path stops the matrix and keeps the data (`KEEP_DATA`).
  Found and fixed in the driver: after a timeout the SIGTERM shutdown flush can commit the rest, so "drained" now
  means drained before SIGTERM.
- Estimate (README "Time and cost"; prices read 2026-11-20 from AWS price lists): ≈ 1 instance-hour expected,
  3 h hard cap; ≈ USD 0.19 expected, ≈ USD 0.40 at the cap. Launch command in `cost/README.md` step 5; needs the
  author's "go".

## 2026-11-20 — E-RQ5-WAN permissions (author approval, decision D8)

- 23:5x — operator (root profile `vtop-eval`) — operator inline policy `vtop-eval-operator`, statement 1
  (`ec2:*`): `aws:RequestedRegion` changed from `us-east-1` to `[us-east-1, eu-west-1, ap-southeast-2, sa-east-1,
  ap-northeast-1]`. Nothing else changed (instance-type deny, purchase denies, PassRole, bucket-only S3, cost read).
  Before/after documents diffed; applied with `put-user-policy`; read back.
- Verified as `vtop-op`: `describe-vpcs` answers in the four regions, each has a default VPC (eu-west-1 vpc-e2726585,
  ap-southeast-2 vpc-1de2027a, sa-east-1 vpc-be937cd9, ap-northeast-1 vpc-baf30add); us-west-2 still refused
  (UnauthorizedOperation). The instance profile `vtop-eval-ec2` is global; the bucket stays in us-east-1.

## 2026-11-21 — E-RQ5-COST launch preparation (author go given, D7; waiting for the v0.10.2 tag)

- — operator — engine maintainer: v0.10.2 bump PR #853 in CI; tag pushed automatically on green; maintainer will send
  tag and hashes after checking signatures. Watch for the release running locally (every 15 min).
- — operator — README step 3 pre-flight (operator): `get-default-credit-specification --instance-family t4g` →
  `unlimited`; project instances pending/running/stopping/stopped → 0.
- — operator — README step 4: staging `SSH.tar.gz`, `BGL.zip`, `HDFS_v1.zip` from `research/datasets/loghub/raw/` to
  `s3://vtop-eval-XXXXXXXXXXXX/cost/input/` (≈ 249 MB; the VM checks each archive's MD5 and each log's SHA-256).
- 00:02Z — staged: SSH.tar.gz 4,596,666 B (ETag = local MD5 a44e40b4…), BGL.zip 57,489,019 B and HDFS_v1.zip
  186,645,559 B (multipart ETags `-7`/`-23`, so not MD5; sizes match; the VM's MD5 check covers them). Storage
  ≈ 0.25 GB-month ≈ USD 0.006/month; delete `cost/input/` after the campaign.

## 2026-11-21 — E-RQ5-WAN prepared (not launched; waits for v0.10.2 and the author's go, D5/D8)

- — operator — wrote `wan/` (user-data.sh, wan.py, analyze.py, README.md). Engine VMs (t4g.medium) in eu-west-1,
  ap-southeast-2, sa-east-1, ap-northeast-1 write to the us-east-1 bucket. Per region 68 runs in a shuffled order
  (seed 20261122), SSH full:
  - native × size-only / sha256-store / blake3-readback × 10;
  - tc 10/2 Mbit/s (tbf egress + ifb ingress) and VTOP `max_egress_bytes_per_second` 10/2 Mbit/s × sha256-store /
    blake3-readback × 3;
  - netem loss 1% / 5% / jitter × 3;
  - partition 60 s × 1;
  - SIGKILL + restart × 2;
  - policed uplink (1 Mbit/s policer, 5,000 B bucket, 32 MiB uncompressed slice) with shipped deadlines, and the
    control with a fixed 120 s deadline.

  Every run is checked by smoke's oracle (unchanged). The driver imports cost/cost.py. RTT to the S3 endpoint is
  measured at start, per run and at the end. user-data refuses an unfilled v0.10.2 pin. No AWS call that creates or
  spends; price pages and Price List files only.
- — operator — `research/experiments/rq5-cloud/run-ec2.sh` patched for far regions:
  - S3 calls pinned to `--region $BUCKET_REGION` (us-east-1);
  - fail if no AMI is found;
  - the subnet comes from an AZ that offers the instance type (`describe-instance-type-offerings`);
  - the region appears in the log lines.

  Not run against AWS.
- — operator — local tests, v0.10.1 (no AWS):
  - (A) macOS + MinIO, darwin binary: native × 3 modes, VTOP caps and SIGKILL+restart passed.
  - (B) Docker Desktop, AL2023 userland with NET_ADMIN, Linux aarch64 release (SHA256SUMS-verified), +100 ms on the
    MinIO side; kernel without ifb, so the ingress-policer fallback was used:
    - all 10 cells passed with 0 unsafe;
    - the partition recovered with its first commit 1.9 s after the restore;
    - SIGKILL restart drained in 6.0 s;
    - the control committed nothing in 480 s.
  - (C) Lima VM vtop-bench-483a (kernel 6.8, ifb):
    - tc2 shaped read-back on ingress at 0.92 Mbit/s;
    - the partition passed.
  - **Finding:** at 2 Mbit/s the policed 32 MiB slice drained in 242 s (< 300 s), so it was no #663 regression check.
    The policer was lowered to 1 Mbit/s: 364.5 s drain, longest seal-to-commit 361.7 s, 0 SDK retries, oracle pass.
  - **Bug found and fixed:** a wire rate that counted bytes past the run window.
- Estimate (wan/README "Time and cost", prices read 2026-11-21): ≈ 1.25 instance-hours per region (cap 2.5 h,
  failsafe 150 min). About USD 0.09 / 0.14 / 0.18 / 0.14 (eu / syd / sp / tok) without CPU credits, ≈ 0.55 in total
  (≈ 0.87 with the CPU-credit bound); cap ≈ 1.65. Suggested stage failsafe: USD 2. Launch needs the author's go.

## 2026-11-21 — v0.10.2 published and verified

- 00:23Z — release v0.10.2 published (tag object 90e15fa3 → commit 0abf11696e47f4db871bd4c2cda882756c114390), 14
  assets incl. glibc and musl Linux archives and cosign v3 bundles.
- — operator — downloaded `vtop-engine-0.10.2-aarch64-unknown-linux-gnu.tar.gz` and the aarch64 macOS archive:
  both match the release's `SHA256SUMS` (file digest 2cd50f9f…41b4); `cosign verify-blob` (cosign v3.1.4, installed with
  Homebrew) **Verified OK** for both, identity `release.yml@refs/tags/v0.10.2`, issuer GitHub Actions OIDC. Linux arm64
  archive SHA-256 **5872e5a34b75a38425fcb99db2f78631b89d121b6a615a570192d5d4f2ccdf39** pinned in `cost/` and `wan/`
  user-data (both `--check-params` OK).
- — operator — local check of #824 with the macOS binary (`vtopctl 0.10.2`), `vtopctl run`, SSH, `max_bytes` 16 MiB:
  5 committed batches of 16,777,073 / 16,777,115 / 16,777,142 / 16,777,184 / 6,308,993 B — all ≤ 16,777,216
  (v0.10.1: 33,554,257). Oracle: complete, 0 unsafe. The batch-size defect is fixed in run mode.
- Waiting for the engine maintainer's own signature/hash confirmation before launching E-RQ5-COST (their request).

## 2026-11-21 — E-RQ5-COST launched (author go, D7; engine maintainer confirmed the v0.10.2 digests)

- 00:26:20Z — operator — staged `cost.py` and `smoke/oracle.py` to `cost/code/`.
- 00:26:26Z — operator — **launched i-07a97d2f7a612888e** (t4g.medium, us-east-1, ami-0697409d504dfb93d,
  subnet-06f60d660fa1815b6), v0.10.2 gnu archive (5872e5a3…df39), 90 runs (3 datasets × 3 modes × 10 reps, seed
  20261121), 16 MiB batches (D6), failsafe 180 min. Launcher waiting (MAX_MIN 200); results to `cost/results/<run_id>/`.
- 01:14:32Z — VM finished: exit 0, elapsed 2,868 s, `data_objects_left` 0; terminated 01:15:07Z; results downloaded to
  `cost/results/20261121T002644Z-i-07a97d2f7a612888e/` (17 MB). No project instances left.
- — operator — `analyze.py`: **90/90 runs pass.** BLAKE3 read-back resolved slower on all three datasets (−14% to −20%);
  store SHA-256 unresolved on SSH and BGL, **resolved −11.9% [−23.2, −5.3] on HDFS**. Every batch ≤ 16 MiB (#824
  confirmed in the cloud). Confound: ENA `bw_out_allowance_exceeded` events on BGL/HDFS (t4g.medium baseline
  0.256 Gbps), more in verified modes; no order drift; disk not involved. H3.5 → PARTLY SUPPORTED (hypotheses.md).
  Proposed follow-up (needs author go): re-run the HDFS cell on t4g.large (twice the baseline network allowance).
  Cost: Cost Explorer lags a day; check 2026-11-22.

## 2026-11-21 — HDFS re-check and E-RQ5-WAN (author go, D9)

- 01:29:33Z — operator — **launched i-072d88074e58c7845** (t4g.large, us-east-1) for the HDFS re-check:
  `cost/user-data-hdfs-recheck.sh` = `cost/user-data.sh` with `DATASETS=hdfs` (same v0.10.2 pin, seed, modes, 10 reps).
- — operator — WAN pre-flight: t4g credits `unlimited` and no project instances in eu-west-1, ap-southeast-2, sa-east-1,
  ap-northeast-1; AL2023 arm64 AMIs ami-08013e14eb49164c6 / ami-002d6ec2e8c2a5ddf / ami-00b9fe5f3b887869b /
  ami-05187038e74cc1766.
- 01:29:57Z — operator — **launched i-066226a42ae3e96de** (t4g.medium, **eu-west-1**, subnet-648f8f2d), E-RQ5-WAN first
  region; the other three wait until its netem/ifb preflight is read.
- 01:31Z — operator — eu-west-1 preflight read from S3 (`wan/results/20261121T013009Z-eu-west-1-i-066226a42ae3e96de/`):
  AL2023 kernel 6.1.190 with `kernel-modules-extra`; `sch_netem`, `sch_tbf`, `sch_ingress`, `ifb`, `act_mirred`,
  `act_police`, `act_gact`, `cls_u32` all load; no condition skipped. RTT to the us-east-1 bucket endpoint: TCP connect
  median **66.47 ms** (min 64.98, p90 67.58, n = 20).
- 01:32Z — operator — launched the other three regions: **i-0bbfd295401175f9f** (ap-southeast-2, subnet-b6993cff),
  **i-0bc93b4bc564eb2c7** (sa-east-1, subnet-64f79503), **i-041c05fa26d6f25c1** (ap-northeast-1, subnet-8c0e93c5),
  t4g.medium, same v0.10.2 pin and script.
- 01:58:20Z — HDFS re-check VM finished: exit 0, 1,715 s, data_objects_left 0; results `research/experiments/rq5-cloud/cost/results/20261121T012945Z-i-072d88074e58c7845`; no instances left in
  us-east-1. **30/30 pass.** Store SHA-256 −5.07% [−7.06, −2.62]; BLAKE3 read-back −21.11% [−22.19, −19.23]; shaping
  events balanced between size-only and SHA-256 (75.5 / 74.5). H3.5 revised: store SHA-256 ≤ ~5%, read-back 14–21%.
- 02:11:34Z — eu-west-1 VM finished: exit 0, 2,485 s, data_objects_left 0; no instances left in eu-west-1; results
  `research/experiments/rq5-cloud/wan/results/20261121T013009Z-eu-west-1-i-066226a42ae3e96de`. First look (single region; combined analysis after all four): RTT 66.5 ms; native SSH store SHA-256 −1.0%
  [−4.8, 1.4] (unresolved), BLAKE3 read-back −23.8% [−27.0, −20.9]; tc2/cap2 uploads at 0.82 of the cap; loss5 ×9.5
  drain, 0 failures; 60 s partition: 0 commits during the cut, first commit 2.0 s after restore, complete, 0 unsafe;
  SIGKILL: 0 duplicate bytes; policed 1 Mbit/s completed (longest seal-to-commit 198 s < 300 s, so not a #663 check
  here), fixed-120 s control committed nothing.
- 02:22Z / 02:29Z / 02:39Z — sa-east-1, ap-northeast-1, ap-southeast-2 VMs finished: exit 0 (2,990 / 3,457 / 4,010 s),
  data_objects_left 0, no instances left in any region. Combined analysis `wan/results/combined/`: **272/272 runs pass,
  0 unsafe** (68 per region). RTT 66.5 / 110.8 / 147.9 / 196.1 ms. H3.6 SUPPORTED (hypotheses.md). Sydney is the only
  region where the policed run exceeded 300 s seal-to-commit (308 s): the valid #663 regression check.
  Cost: Cost Explorer lags; check on 2026-11-22.

## 2026-11-21 — E-RQ5-TAMPER prepared (H2.5–H2.7 on v0.10.2; H2.8 pending v0.10.3); no AWS call, nothing launched

- — operator — wrote `tamper/` (tamper.py, analyze.py, user-data.sh, README, iam-and-bucket-change.md,
  iam/*.json): 55 cases. Each case has a verdict expected per verifier (scrub, verify-manifest with and without a
  ledger, audit-coverage --check-storage, recovery after F4 state injection, S3's answer, start-up refusal, audit
  verify). NOT CAUGHT cases assert the wrong verdict. The tampering and the 0.4 hash/MAC/receipt/audit-chain formats
  are re-implemented independently of the engine suite (PR #859) and checked against the engine's own output before
  each run.
- — operator — local runs against MinIO RELEASE.2025-09-07 (container `tamper-minio-c33`, removed afterwards):
  the macOS v0.10.2 binary, a replication, and the Linux aarch64 release binary in `amazonlinux:2023` (glibc 2.34).
  **Identical verdicts in 55/55 cases in all three.** 54 as expected (28 caught, 6 prevented, 10 caught + a predicted
  NOT CAUGHT, 7 NOT CAUGHT as predicted, 3 controls). **1 DIFFERS:** with versioning suspended under a running engine,
  v0.10.2 commits nothing but keeps writing (the object once, then a manifest per retry; "manifest upload returned
  no object version to pin"). The engine maintainer's evidence says "writes nothing".
- Findings to report: `verify-manifest` does not bind a manifest to its key, so a swapped genuine manifest passes the
  no-ledger and ledger checks with the MAC on or off (scrub and recovery catch it). H2.6 UNSURE answered: scrub
  **reports** a current key that differs from the pinned version; recovery **does not follow** it. Receipts are a
  second binding when a forger leaves them alone.
- Needs author approval before any spend: the new bucket `vtop-eval-lock-XXXXXXXXXXXX` (Object Lock, no default
  retention, lifecycle clean-up), managed policies `vtop-eval-tamper-ec2` (role) and `vtop-eval-tamper-operator`
  (user), optional `s3:BypassGovernanceRetention` and `s3:PutBucketVersioning` on the lock bucket. Then the launch:
  one t4g.small, ≈ 0.5 h, ≈ USD 0.03 expected, ≈ USD 0.13 cap (tamper/README "Time and cost").

## 2026-11-21 — E-RQ5-TAMPER bucket and permissions (author approval D12; root profile after re-login)

- — operator (root `vtop-eval`, `aws login` renewed by the author in the browser) — created bucket
  **`vtop-eval-lock-XXXXXXXXXXXX`** (us-east-1, Object Lock enabled → versioning Enabled, no default retention), public
  access blocked, tagged `project=vtop-eval`, lifecycle `tamper/` (expire after 2 days, noncurrent versions after 1 day,
  expired delete markers, abort incomplete uploads). Created and attached managed policies (exact JSON in
  `tamper/iam/`): `vtop-eval-tamper-ec2` and `vtop-eval-tamper-ec2-optional` (BypassGovernanceRetention on `tamper/*`,
  PutBucketVersioning on the lock bucket only) to role `vtop-eval-ec2`; `vtop-eval-tamper-operator` (read + clean, no
  write, no bypass) to user `vtop-eval-operator`.
- — operator — verified as `vtop-op`: lock-bucket Object Lock config and lifecycle readable; main bucket versioning `{}`
  (unversioned); PutObject and PutBucketVersioning on the lock bucket refused for the operator.
- 14:32:49Z — operator — **launched i-0938589e4d715ff8d** (t4g.small, us-east-1) for E-RQ5-TAMPER (v0.10.2, OPTIONAL_PERMS=bypass,suspend; failsafe 75 min).
- 14:37:01Z — tamper VM finished: exit 0, 240 s, data_objects_left 0, lock-bucket versions left 82 (retention/lifecycle
  clears them); terminated; no instances left. **55 cases; verdicts match the local runs in 53/53 comparable cases**
  (2 suspended-versioning cases skipped: local-only by design). H2.5–H2.7 answered with measured limits (hypotheses.md).

## 2026-11-21 — E-RQ5-PERF prepared (C30; not launched, no spend; waits for the author's go)

- — operator — wrote `perf/`: `perf.py` (agent / orchestrate / produce), `user-data-broker.sh`,
  `user-data-sink.sh`, `launch.sh` (one leg = broker + sink t4g.large in one subnet, 30 GiB gp3, IMDSv2 hop 2),
  `analyze.py`, `reqcount_proxy.py`, `dryrun.sh`, `README.md`, `request-profile-dryrun.json`, `dryrun-20261121/`.
  Same systems as RQ4, by the RQ4 index digests (the tag `fluent/fluent-bit:5.2.0` now resolves to another index,
  e2718c47; RQ4's a48190bf is pinned and still pullable); kafka-connect-s3 12.1.15 zip by SHA-256; VTOP v0.10.2
  pin as cost/ (sink user-data refuses an unfilled pin). All images have linux/arm64.
- Design:
  - Kafka source on its own host; topics lz4.
  - tput: HDFS 11.04 M records on 4 partitions (exact multiple of 120,000 = 16 MiB objects).
  - live: 1.8 M records on 1 partition at 30,000 rec/s; latency = max(object listed, offset committed) − produce
    time, broker clock.
  - kill: one SIGKILL at 40%.
  - Every run is judged by RQ4's `locate`/`judge`, imported unchanged.
  - VTOP size-only / store SHA-256 / BLAKE3 (+ gzip-6 variant); Confluent tasks.max 4; common 10 s age.
  - Legs: us-east-1 (tput 5, live 3, kill 3) and ap-southeast-2 (author scope addition; 3/2/2; RTT measured with
    `wan.py`).
  - Storage-fault phase (D13, us-east-1 only): RQ4 corrupt/lost on real S3 without a proxy. The broker lists every
    0.1 s and damages the N-th data object (N in 2..8, RQ4 seed) when it is listed. Each trial is classified by
    damage time vs the sink's commit and, for VTOP, vs its own verification; VTOP scrub/audit-coverage run after.
- Read-only AWS checks (operator, aws-mcp):
  - default security groups in us-east-1 and ap-southeast-2 allow traffic between members;
  - t4g.large offered in us-east-1a/c/d/e/f and ap-southeast-2a/b/c;
  - T4g credits `unlimited` in Sydney;
  - `RunInstances --dry-run` with the launcher's exact shape: "Request would have succeeded" in both regions. No
    new permission is needed.
  - Prices re-read from the EC2 and AWSDataTransfer price lists: t4g.large 0.0672 / 0.0848 USD/h; Sydney→us-east-1
    0.098 USD/GB, us-east-1→Sydney 0.02 USD/GB.
- Local dry runs (macOS + Docker, MinIO behind a request-counting proxy, v0.10.2 darwin binary; noise; another
  agent's Docker soak ran meanwhile):
  - full matrix 16/17 (the failed run is a correct catch: on the Mac, Confluent drained before the 40% kill);
  - fault phase 8/8 as predicted: Confluent and Vector damage-before-commit → unsafe, 40,000 lost committed
    records; Fluent Bit commits before the object exists; VTOP's damage landed after its own verification, and
    `vtopctl scrub` flagged it (rc 1);
  - regression run 3/3.
  - Own containers were removed each time; other containers were untouched.
- Estimate (perf/README "Time and cost"): us-east-1 ≈ 4.4 h × 2 hosts incl. the fault phase, ≈ USD 1.5 (cap 2.1);
  Sydney ≈ 3.2 h × 2 hosts, ≈ USD 1.8 incl. ≈ 5 GB of inter-region upload (cap 3.2). Total ≈ USD 3.3, cap ≈ 5.3.
  The author must answer the 13 decisions in perf/README before launch.

## 2026-11-21 — E-RQ5-PERF leg 1 (us-east-1) launched (D12/D13; estimate USD 3.3, cap 5.3 for both legs)

- Pre-flight: t4g credits unlimited and no project instances in us-east-1 and ap-southeast-2; HDFS input staged.
- First attempt did nothing (launch.sh not executable; fixed, f4264cb+1). 14:43:18Z — operator — **launched sink
  i-0165dde62e00c6479** and 14:43:20Z **broker i-0d79946597ff9b865** (t4g.large, us-east-1a), session
  `20261121T144252Z-use1`, v0.10.2 pin. Design defaults taken (perf/README "Decisions"): HDFS only; 2 × t4g.large arm64;
  Confluent tasks.max 4; reps 5/3/3 (us-east-1) and 3/2/2 (Sydney); gzip-6 VTOP variant kept; S3 request counts
  estimated from the dry-run profile; common 10 s age; VTOP receipts off; fault phase 5 trials per system and fault,
  VTOP in BLAKE3 as RQ4; fault phase reported as the cloud reproduction of RQ4's baselines plus VTOP's detection.
  Sydney leg after the first rounds of leg 1 are read.

## 2026-11-21 — E-RQ5-SCALE prepared (D13a; not launched)

- — operator — wrote `scale/` (scale.py, user-data.sh, launch.sh, analyze.py, dryrun.sh, README.md).
  - Design: 8 × t4g.large (16 vCPU = cap) in one subnet, launched by one RunInstances call (MinCount = MaxCount = 8,
    so all or nothing); each host's slot is its ami-launch-index.
  - Engines: one VTOP v0.10.2 engine per host, each with its own HDFS file, ledger and S3 prefix.
  - Steps: N = 1/2/4/8 store SHA-256 × 10 rounds plus a size-only control at N = 8 × 5 and one warm-up step,
    shuffled within rounds (seed 20261124): 46 steps, 198 engine-runs. Optional: N = 16 (2 engines per host) and a
    sustained step fed with reflink clones.
  - Coordination: a leaderless S3 barrier (`scale/coord/<session>/`). Every host starts at T = ceil(max marker
    time + 3 s), so all hosts reach the same decision; abort and deadline markers stop every host.
  - Measurements: S3 requests, retries and throttling verdicts counted from the engines' own AWS SDK DEBUG lines
    (RUST_LOG; no new permission); ENA counters per host and step; the smoke oracle on every engine-run.
  - Proposed H3.7 in `scale/README.md` (`hypotheses.md` not edited).
  - No AWS account call. Prices were re-read from the public price lists: t4g.large 0.0672 USD/h (2026-11-21T00:48Z);
    S3 Tier1 0.005 per 1k, Tier2 0.004 per 10k (2026-11-21T00:14Z).
- — operator — local tests (no AWS; container `rq5scale-minio` on :19510, removed afterwards):
  - 4 workers on macOS + MinIO, 15 steps: 53/53 engine-runs pass, 0 unsafe, data deleted; analyze.py OK. The
    throughput numbers are laptop noise.
  - The abort, dead-host (boot timeout) and shared-deadline paths stopped every host consistently.
  - AL2023 container (Linux aarch64 release): all runs pass. Without reflink on overlayfs the sustained step is
    skipped on all hosts.
  - launch.sh against a fake `aws`: the vCPU guard refuses, VcpuLimitExceeded launches nothing, the launcher moves to
    the next AZ on InsufficientInstanceCapacity, and a short fleet is terminated.
- **Finding:** Linux `ru_maxrss` of the engine child carries the Python driver's RSS high-water mark (container:
  139 MiB vs the engine's own VmHWM ≈ 68 MiB). COST's ≈ 1,640 MiB "engine RSS" on HDFS is therefore likely the
  oracle mmap in the driver, so do not quote it. SCALE records VmHWM instead.
- Estimate (scale/README): default plan ≈ 0.85 h × 8 hosts, ≈ USD 1.0–1.35 (cap ≈ 2.9). Optional parts: N = 16
  + ≈ 0.2; 20 min sustained + ≈ 0.85.
- Launch only after the author's go. It must also wait until the E-RQ5-PERF us-east-1 leg ends: 4 vCPUs are in use,
  and the launcher refuses above 16.
- 15:0xZ — operator — leg 1 first rounds read from S3: the four warm-up runs (confluent, vector, vtop, fluentbit) all
  pass on EC2 (IMDS credentials, plugin loading, oracle OK).
- 15:01:44Z / 15:01:47Z — operator — **launched Sydney leg**: sink **i-053a7c01ab7768c56**, broker
  **i-0f10944cf30cfb95c** (t4g.large, ap-southeast-2, subnet-b6993cff), session `20261121T150129Z-apse2`, v0.10.2 pin.
- 15:3xZ — operator (root, read-only) — EC2 quota "Running On-Demand Standard instances" in us-east-1 = **1,920 vCPUs**;
  the 16-vCPU guard was ours, not AWS's. Raised the launcher guard to `VCPU_CAP=24` (fleet 16 + PERF leg 1's 4).
- 15:38:33Z — operator — **launched E-RQ5-SCALE**: 8 × t4g.large, session `20261121T153823Z-scale`, i-02ac077819d4b7a25
  i-0a09d8730bb9a6699 i-0bef6ea15d5e5e135 i-09a91ebf9dda1b3a4 i-0a1ced042198ef172 i-08f92384eb354d066
  i-01348131f91072184 i-0a226a534bf572930; 46 steps, 198 engine-runs (N = 1, 2, 4, 8 × 10 + size-only control ×5 +
  warm-up); failsafe 130 min. Defaults: no N = 16, no sustained step, SDK request logging on. Runs alongside PERF legs.
- 16:22–16:23Z — SCALE fleet finished: 8/8 hosts exit 0, ~2,620–2,670 s, data_objects_left 0; terminated. Results
  `research/experiments/rq5-cloud/scale/results/20261121T153823Z-scale` (8 of 8 hosts). **198/198 engine-runs pass, 0 unsafe; N = 8: 373.09 MiB/s, efficiency 1.001 [0.985, 1.021];
  0 throttling.** H3.7 SUPPORTED (hypotheses.md).
- 17:43Z / 18:19Z — PERF Sydney and us-east-1 legs finished: exit 0, data_objects_left 0, terminated; no project
  instances left in either region. Sydney 38/38 runs pass, us-east-1 105/105 (incl. 40 storage-fault trials). H4.2
  answered (hypotheses.md). Combined analysis `perf/results/combined/`.

## 2026-11-21 — AWS close-out (decision D15)
- Before deleting: `aws s3 sync --dryrun --size-only` from `s3://vtop-eval-XXXXXXXXXXXX/<exp>/results/` to the local
  `research/experiments/rq5-cloud/<exp>/results/` reported 0 missing objects for cost, perf, scale, tamper, wan and smoke.
  The perf and scale coordination files (4 + 368) were copied to `<exp>/coord-s3/` (git-ignored, local only). Code is in
  git; the staged inputs (cost/input 237 MiB, perf/input 71 MiB, smoke/input) are rebuilt from the Loghub datasets.
- Deleted: every object in `vtop-eval-XXXXXXXXXXXX` (unversioned), then the bucket; the 82 object versions in
  `vtop-eval-lock-XXXXXXXXXXXX` (GOVERNANCE retention until 2026-11-22 14:36 UTC, bypassed with the root profile), then
  the bucket; IAM policies `vtop-eval-tamper-ec2`, `vtop-eval-tamper-ec2-optional` (detached from role `vtop-eval-ec2`)
  and `vtop-eval-tamper-operator` (detached from user `vtop-eval-operator`).
- Already gone: no EC2 instances, volumes, Elastic IPs, NAT gateways, snapshots or key pairs in us-east-1, eu-west-1,
  ap-southeast-2, sa-east-1 or ap-northeast-1 (every launch had its failsafe; the scale and perf hosts had terminated).
- Kept (no charge): IAM user `vtop-eval-operator`, role/instance profile `vtop-eval-ec2`, the budget. A rerun needs the
  bucket recreated (`setup` in the vtop-aws-experiments skill) and the inputs restaged.
- Not ours, untouched: SageMaker and DeepRacer security groups and buckets from earlier account use.
- All-region sweep (root): no instances, volumes or Elastic IPs in any region (me-south-1 endpoint timed out; never used).
- Cost Explorer 2026-11-19..21 (partial, lags ~1 day): USD 0.66. To do: re-check on 2026-11-22 for the final lab cost.

## 2026-11-22 — Re-pin to v0.10.4 (decision D16): AWS set up again and batch 1 launched
- v0.10.4 (be5b419) verified locally: SHA256SUMS 90e807f5…; gnu tarball 0266c37b…7c84 and darwin tarball fc132e7c…9b71
  match; cosign verify-blob OK (release.yml@refs/tags/v0.10.4). The seven `user-data*.sh` pins now name v0.10.4.
- Recreated with the root profile (deleted on 2026-11-21 under D15): study bucket `vtop-eval-XXXXXXXXXXXX` (private,
  public access blocked, tag project=vtop-eval, unversioned); lock bucket `vtop-eval-lock-XXXXXXXXXXXX` (Object Lock,
  versioning, no default retention, lifecycle `tamper/`); policies `vtop-eval-tamper-ec2` and
  `vtop-eval-tamper-ec2-optional` on role `vtop-eval-ec2`, `vtop-eval-tamper-operator` on user `vtop-eval-operator`
  (same JSON as 2026-11-21).
- Inputs staged from `research/datasets/loghub/raw/` (local MD5 = Loghub MD5; the VMs check again): `cost/input/`
  SSH.tar.gz, BGL.zip, HDFS_v1.zip; `smoke/input/SSH.tar.gz`.
- Pre-flight: t4g credits unlimited and no project instances in us-east-1, eu-west-1, ap-southeast-2, sa-east-1,
  ap-northeast-1.
- 02:18Z — batch 1 launched with the unchanged launcher and arguments: COST (t4g.medium), TAMPER (t4g.small), WAN in
  four regions (t4g.medium). Launcher logs `.tmp/v0104-*.txt`. Batch 2 after it: HDFS re-check (t4g.large), PERF (two
  legs), SCALE (8 × t4g.large).
- Batch 1 done, every VM exit 0, terminated, 0 data objects left; each run records `vtopctl 0.10.4` and commit
  be5b419 (the remaining "0.10.2" strings in the result folders are code comments copied with the scripts):
  TAMPER `20261122T021852Z-i-0e178e36bc660a51f` (242 s; 82 lock versions left under retention, as before), COST
  `20261122T021852Z-i-0d197e09de6af68de` (2,528 s), WAN eu-west-1 `20261122T021853Z-…-i-036821b8632514571` (2,424 s),
  sa-east-1 `20261122T021935Z-…-i-0093971b23ec6758c` (2,979 s), ap-northeast-1 `20261122T021956Z-…-i-047cf9c9a10e500d0`
  (3,462 s), ap-southeast-2 `20261122T021917Z-…-i-0819b6cd4872adbb3` (4,023 s).
- Batch 2: SCALE (8 × t4g.large, fills the 16-vCPU cap in us-east-1) and PERF leg apse2 launched together; PERF leg
  use1 and the HDFS re-check follow SCALE. Logs `.tmp/v0104-scale.txt`, `.tmp/v0104-perf-apse2.txt`.
- 03:27:41Z SCALE session `20261122T032729Z-scale` (8 × t4g.large, failsafe 130 min): all 8 hosts exit 0 at
  04:11–04:13Z (2,632–2,691 s), terminated, 0 data objects left; results `scale/results/20261122T032729Z-scale`.
- 03:27:53Z PERF leg apse2 session `20261122T032729Z-apse2`: sink i-0714a5c8e630ca404, broker i-0867590bfc4cd5b2a.
- After SCALE: HDFS re-check (`run-ec2.sh cost cost/user-data-hdfs-recheck.sh t4g.large`) and PERF leg use1 launched;
  logs `.tmp/v0104-hdfs-recheck.txt`, `.tmp/v0104-perf-use1.txt`.
- 04:14Z HDFS re-check i-03c2592cd984f1276: exit 0 (1,682 s), 0 data objects left;
  `cost/results/20261122T041424Z-i-03c2592cd984f1276`.
- PERF apse2 (`20261122T032729Z-apse2`): broker and sink exit 0 at 06:03Z (9,317 s).
- PERF use1 (`20261122T041404Z-use1`): sink exit 0; broker exit 1 at 07:45Z with 108 of 109 runs passing. The failed
  run, `106-tput-vector-r4` (Vector baseline, no fault), failed on the oracle's tie-break for an HDFS record that is
  identical at p1 and p2 offset 1,610,302, not on Vector. Re-judged offline from the 92 kept objects with the fixed
  oracle: complete, no loss, no duplicates (`perf/LESSONS.md`, `perf/rejudge.py`,
  `results/20261122T041404Z-use1-broker-…/rejudge-106-tput-vector-r4.json`). The kept prefix is copied to
  `.tmp/perf-diag-106/` and deleted from S3 at close-out.
- Analysis (2026-11-22, D16): per-campaign analyzers re-run on the v0.10.4 sessions (COST both runs, WAN
  `results/combined-v0104`, TAMPER AWS + three local v0.10.4 runs, SCALE, PERF legs + `results/combined-v0104`);
  `analyze.py --set v0104` and `verify_v0104_data.py` (345 checks) pass. Close-out (D15) still to do: buckets,
  `perf/data/20261122T041404Z-use1/` (92 objects, copied to `.tmp/perf-diag-106/`), tamper policies.

## 2026-11-22 — AWS close-out after the v0.10.4 re-pin (decision D15)
- Before deleting: `aws s3 sync --dryrun --size-only` from `s3://vtop-eval-XXXXXXXXXXXX/<exp>/results/` to the local
  `<exp>/results/` reported 0 missing objects for cost, perf, scale, tamper, wan and smoke. Coordination files (perf 4,
  scale 368 + the earlier session's) copied to `<exp>/coord-s3/` (git-ignored). The 92 kept PERF objects of
  `perf/data/20261122T041404Z-use1/` are in `.tmp/perf-diag-106/` (0 missing). Code is in git; inputs rebuild from Loghub.
- Deleted (root profile after the author's `aws login`): every object in `vtop-eval-XXXXXXXXXXXX`, then the bucket; the 82
  object versions in `vtop-eval-lock-XXXXXXXXXXXX` (GOVERNANCE retention from the 02:18Z TAMPER run, bypassed), then the
  bucket; policies `vtop-eval-tamper-ec2`, `vtop-eval-tamper-ec2-optional` (detached from role `vtop-eval-ec2`) and
  `vtop-eval-tamper-operator` (detached from user `vtop-eval-operator`).
- Sweep: no instances, volumes or Elastic IPs in us-east-1, eu-west-1, ap-southeast-2, sa-east-1, ap-northeast-1.
- Kept (no charge): user `vtop-eval-operator`, role/instance profile `vtop-eval-ec2`, the budget. Not ours, untouched:
  the rSCAP lab's resources (`rscap-eval-*`, recreated today for its 3.18.3 retest), SageMaker/DeepRacer leftovers.
- Cost Explorer (estimated, read by the rSCAP lab agent before its run): November vtop $2.17, rscap $4.29, untagged
  $2.88; re-check after Cost Explorer catches up.
