# RQ5 cloud smoke test: VTOP v0.10.1 on EC2 against Amazon S3

**Status: run and passed (2026-11-20, VTOP v0.10.1, attempt 3, run `20261120T181226Z-i-0579609654faf2afe`).** The text below was written before the AWS runs and describes the preparation; results and the run log are in `results/` and `../LOG.md`.

One question per verification mode: **does the engine drain the input and commit it, and does
the stored evidence pass an independent check?** For the store-SHA-256 mode the test also
records **whether verification really used the S3-computed checksum**, rather than a read-back.

| Mode | `checksum.algorithm` | `compression.type` | `require_strong_verification` | Expected verification path |
|---|---|---|---|---|
| `size-only` | `none` | `zstd` | `false` (explicit opt-out) | size and existence only |
| `sha256-store` | `sha256` | `zstd` | `true` | S3 `ChecksumSHA256` read by HEAD (`store_checksum`) |
| `blake3-readback` | `blake3` | `zstd` | `true` | GET and BLAKE3 over the stored bytes (`read_back`) |
| `sha256-multipart` (extra) | `sha256` | `none` | `true` | multipart objects are read back; the single-PUT tail uses `store_checksum` |

The fourth mode is the paper's `sha256-nocomp` variant. It costs about 73 MB of transfer within
the region, and it runs the multipart path on real S3: SDK part checksums, and the read-back that
replaces the store checksum for a multipart object. Drop it with `MODES=` in `user-data.sh`.

## Target build

- vtop-engine **v0.10.1**: tag object `a0145129`, commit `44f66194fdc96090e6f5222693d0fe5fe39200a1`, released
  2026-11-15. The archive is `vtop-engine-0.10.1-aarch64-unknown-linux-gnu.tar.gz`, with SHA-256
  `173aa74cd79af1d20dc0a6d6b831eab800e83e87926942b1e9be3ff9dd0f82f0`. This matches the release's `SHA256SUMS`,
  checked 2026-11-20.
- Why not v0.7.0, the build evaluated in the paper: its aarch64 Linux `vtopctl` needs `GLIBC_2.38` (`__isoc23_strtol` and
  others; checked with `objdump -T`). The v0.8.0 build needs the same version. Amazon Linux 2023 ships glibc 2.34, so
  neither starts there. v0.10.1 needs at most `GLIBC_2.28` and has no `libssl` or `libsasl2` dependency.
  Its docs say the same (`docs/install/binaries.md:12-26`, #808).
- Sigstore bundle: its certificate SAN is
  `https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/v0.10.1`, and its
  `messageDigest` equals the archive's SHA-256 (both inspected locally). **The signature itself
  has not been verified yet**: cosign is not installed here, and `gh attestation verify` finds
  no attestation, because this is a `cosign sign-blob` bundle. Step 2 below verifies it.

## Files

| File | Role |
|---|---|
| `user-data.sh` | cloud-init script for Amazon Linux 2023 arm64. It records `ldd --version`, downloads and verifies the archive (against the pinned digest and `SHA256SUMS`), and runs `vtopctl --version` before anything else. Then it installs Python, fetches and verifies the Loghub SSH log, runs `smoke.py`, uploads `/var/log/vtop-smoke/`, and powers off. |
| `smoke.py` | Writes one config per mode, runs `vtopctl validate --print-effective`, and starts `vtopctl run`. It polls the ledger until the input is committed to its last byte, then sends SIGTERM. Afterwards it calls `oracle.py`, `vtopctl scrub` and `vtopctl audit-coverage --check-storage`. It writes `<mode>.json`, `engine-<mode>.jsonl` and `run.json`. |
| `oracle.py` | Independent check with boto3; it uses no VTOP code. It lists the prefix and downloads every manifest and object named by the ledger. It re-hashes each object (SHA-256 and BLAKE3), decompresses it and compares it byte for byte with the source range. It HEADs each object with `ChecksumMode=ENABLED`, then checks coverage from byte 0 to the end of the input. Adapted from `s3_oracle()` in `../../rq3-cost-ablation/code/exp_minio_cost.py` and `oracle()` in `../../lib/vtoplab.py`. It can also run on its own (`--db --source --bucket --prefix --mode`). |

## Config per mode (v0.10.1 keys and their sources)

`smoke.py` generates the configs (`config_text()`), following the template in
`lib/vtoplab.py` `Run._write_config` and the upload block of `exp_minio_cost.py` `MinioRun`.
The batch values come from `exp_cost.py` `VARIANTS`. The `sha256-store` config:

```yaml
engine:
  name: smoke
  tenant: smoke
  state_store: "sqlite:///opt/vtop-smoke/runs/sha256-store/state/vtop.db"
  work_dir: /opt/vtop-smoke/runs/sha256-store/work
batching:
  max_records: 1000000
  max_bytes: 16777216
  max_batch_age_seconds: 2
  idle_poll_interval_ms: 200
  max_concurrent_batches: 8
compression:
  type: zstd          # size-only, blake3-readback: zstd; sha256-multipart: none
  level: 3
checksum:
  algorithm: sha256   # size-only: none; blake3-readback: blake3
manifest:
  verification_receipts: true
sources:
  kafka: {enabled: false, bootstrap_servers: ["localhost:9092"], consumer_group: lab}
  file: {enabled: true, paths: ["/opt/vtop-smoke/runs/sha256-store/input/*.log"]}
  syslog_spool: {enabled: false, paths: ["/opt/vtop-smoke/runs/sha256-store/input/*.log"]}
upload:
  backend: s3_native
  region: us-east-1
  bucket: vtop-eval-XXXXXXXXXXXX
  prefix: "smoke/data/<run_id>/sha256-store/"
  create_bucket: false
  require_strong_verification: true    # size-only: false
  multipart_threshold_bytes: 67108864  # sha256-multipart: 8388608
  multipart_part_size_bytes: 8388608
```

Paths below are in the v0.10.1 source. `ref.md` is `docs/configure/reference.md`; `config.rs` is
`crates/vtop-core/src/config.rs`. (The engine repo has no `docs/reference/config.md`; the config
reference lives at `docs/configure/reference.md`.)

| Key | Value(s) | Source |
|---|---|---|
| `engine.name`, `engine.tenant` | `smoke` | ref.md:40-41; config.rs:225, 227 |
| `engine.state_store` | `"sqlite://<abs path>"` | ref.md:42; config.rs:231 |
| `engine.work_dir` | per mode | ref.md:43; config.rs:232 |
| `batching.max_records` | 1,000,000 (default 10,000) | ref.md:56; config.rs:451, 528-530 |
| `batching.max_bytes` | 16 MiB (default 100 MiB) | ref.md:57; config.rs:453, 531-533 |
| `batching.max_batch_age_seconds` | 2 (default 60) | ref.md:58; config.rs:455 |
| `batching.idle_poll_interval_ms` | 200 (default 2,000) | ref.md:60; config.rs:481, 540-542 |
| `batching.max_concurrent_batches` | 8 (= default) | ref.md:61; config.rs:492, 543-545 |
| `compression.type`, `.level` | `zstd`/`none`, 3 | ref.md:69-70; config.rs:548-552 (`#[serde(rename = "type")]`); values `types.rs:108` |
| `checksum.algorithm` | `none`/`sha256`/`blake3` | ref.md:29, 80; config.rs:208; values `types.rs:150` |
| `manifest.verification_receipts` | `true` (default `false`) | ref.md:104; config.rs:73; `docs/operate/verification-receipts.md` |
| `sources.kafka` (disabled) | as in the harness | ref.md:179-182; config.rs:565, 573 |
| `sources.file.enabled`, `.paths` | `true`, input glob | ref.md:196-197; config.rs:567, 615 |
| `sources.syslog_spool` (disabled) | as in the harness | ref.md:207-208; config.rs:569 |
| `upload.backend` | `s3_native` | ref.md:214; config.rs:703 |
| `upload.bucket`, `.prefix` | test bucket, per-run prefix | ref.md:215-216; config.rs:704, 706 |
| `upload.region` | `us-east-1` | ref.md:218; config.rs:710 |
| `upload.endpoint_url`, `force_path_style`, `verify_tls` | **not set**: SDK endpoint, virtual-hosted style, TLS on | ref.md:217, 219-220; config.rs:708-714 |
| `upload.create_bucket` | `false` | ref.md:227; config.rs:781 |
| `upload.require_strong_verification` | `false` only for size-only | ref.md:228; config.rs:791 |
| `upload.multipart_threshold_bytes` | 64 MiB, or 8 MiB for sha256-multipart | ref.md:238; config.rs:825, 1401 |
| `upload.multipart_part_size_bytes` | 8 MiB (= default) | ref.md:237; config.rs:820, 1398 |

Not set: `manifest.write_version` (default `"0.3"`; if set, it must be quoted, ref.md:102),
`streams.yaml` (optional, `crates/vtop-cli/src/commands.rs:297-308`), and the object-lock and
egress-ceiling keys. The engine runs with `VTOP_LOG_FORMAT=json` (`vtop-observe/src/logging.rs:17`),
and neither smoke.py nor user-data.sh sets `AWS_REQUEST_CHECKSUM_CALCULATION`. `run.json` records the value
the process saw. `vtopctl --json validate --config … --print-effective` is saved per mode. Its
`verification_method` is the planned method (`effective_config.rs:35-53`).

### Batch size

With `max_bytes: 16 MiB`, `vtopctl run` cut the 73,417,507-byte SSH input into **three batches of
about 32 MiB** (33,554,257 bytes), not five of 16 MiB. The v0.7.0 MinIO campaign shows the same thing:
`results/20261111-v070-minio-cost*/runs.jsonl` records 3 batches for SSH. The likely cause is that
the size test counts record bytes without their newlines (`crates/vtop-core/src/batch.rs:163-201`),
so a 16 MiB read falls just short of the limit and a second read is added before the seal. This is
**inferred from the code, not confirmed** with the maintainer. Hence the 64 MiB threshold: in the three
main modes, every object (at most about 32 MiB, about 2.3 MB after zstd) stays a single PUT.

## What "pass" means

Every mode must satisfy all of these:

1. **Drained:** the ledger's highest `source_committed` `end_byte` for the input equals its size
   (73,417,507 bytes, the SSH log plus one terminating newline). The time limit is 900 s per mode.
2. **Engine log:** every `verification_passed` line carries the `object_check` message expected for
   the mode (`engine.rs:1545-1560`). The messages are in `s3_native.rs:1372-1374, 1414-1416, 1435` and
   `vtop-upload/src/base.rs:391`.
3. **Oracle** (`oracle.py`, `pass: true`):
   - at least one committed row;
   - for each committed row, the object is downloaded, decompressed and **byte-identical** to the
     source range in the ledger;
   - the digest matches the manifest for its algorithm, and the sizes match the manifest and the HEAD;
   - when S3 returns a whole-object `ChecksumSHA256`, it equals the SHA-256 of the downloaded bytes;
   - the receipt's `verification.method` equals the method inferred from S3's own HEAD answer;
   - coverage reaches the end of the input from byte 0, and no committed row names a bad object.

Each mode must also show the following:

| Mode | Additional requirement |
|---|---|
| `size-only` | manifest algorithm `none`. Ledger `verification_class = size_only`. Receipt `{class: size_only, method: size_only}`. Log: `object present and size matches (checksums disabled)`. |
| `sha256-store` | **No multipart object** (no `-N` ETag). For every object, the HEAD `ChecksumSHA256` equals the manifest checksum, so `judge_service_sha256` passes and no read-back runs (`s3_native.rs:1389-1394`, `base.rs:385-399`). Receipt method `store_checksum`. Log: `S3 service-computed SHA-256 verified`. |
| `blake3-readback` | BLAKE3 matches. Receipt method `read_back`. Log: `stored content BLAKE3 verified`. |
| `sha256-multipart` | At least one multipart object. Each multipart object has no whole-object SHA-256 at S3, a receipt method of `read_back`, and the log line `stored content SHA-256 verified by read-back …`. The single-PUT tail uses `store_checksum`. |

The service-checksum question is therefore answered three ways:

- the engine's own `object_check` log message;
- the signed receipt's `verification.method`;
- an independent HEAD with `ChecksumMode=ENABLED` that shows `ChecksumSHA256` equal to the manifest digest.

Not part of the pass rule, but recorded:

- the `vtopctl scrub` and `vtopctl audit-coverage --check-storage` exit codes;
- `clean_ledger` (only `source_committed` rows);
- the `shutdown flush complete` / `incomplete` line and the exit status;
- the S3 HEAD fields of every object and manifest: `ChecksumSHA256`, `ChecksumCRC32`, `ChecksumCRC64NVME`, `ChecksumType`, ETag;
- unreferenced data objects.

## Steps

All commands run from the repo root on the operator machine with the `vtop-op` profile.

```bash
export AWS_PROFILE=vtop-op AWS_REGION=us-east-1
B=vtop-eval-XXXXXXXXXXXX
S=research/experiments/rq5-cloud/smoke
```

1. **Permissions check (read the policy; do not change it here).** The instance role `vtop-eval-ec2` needs
   `s3:ListBucket` on the bucket, plus Get, Put, Delete and AbortMultipartUpload on objects. The oracle
   lists the prefix. Without ListBucket, S3 answers a HEAD on a missing key with 403, so v0.10.1 cannot
   probe its stable object key (#626) and falls back to per-attempt keys (`fallback_unchecked`,
   `engine.rs:1031-1037`).
   ```bash
   aws iam list-role-policies --role-name vtop-eval-ec2
   aws iam get-role-policy --role-name vtop-eval-ec2 --policy-name <name>
   ```
2. **Verify the Sigstore bundle** (not done yet; see [Target build](#target-build)):
   ```bash
   brew install cosign
   d=$(mktemp -d) && cd "$d" && gh release download v0.10.1 -R allamiro/vtop-engine \
     -p 'vtop-engine-0.10.1-aarch64-unknown-linux-gnu.tar.gz*' -p SHA256SUMS
   shasum -a 256 -c --ignore-missing SHA256SUMS
   cosign verify-blob vtop-engine-0.10.1-aarch64-unknown-linux-gnu.tar.gz \
     --bundle vtop-engine-0.10.1-aarch64-unknown-linux-gnu.tar.gz.sigstore.json \
     --certificate-identity https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/v0.10.1 \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   cd -
   ```
   Alternatively, set `COSIGN_VERSION` in `user-data.sh` and the VM will try the same check. The
   VM's result is recorded in `run.json` either way.
3. **Stage the code** (and, optionally, the input, so the VM does not depend on Zenodo):
   ```bash
   aws s3 cp $S/smoke.py  s3://$B/smoke/code/smoke.py
   aws s3 cp $S/oracle.py s3://$B/smoke/code/oracle.py
   aws s3 cp research/datasets/loghub/raw/SSH.tar.gz s3://$B/smoke/input/SSH.tar.gz   # optional, 4.6 MB
   ```
4. **Find the AMI** (Amazon Linux 2023, arm64):
   ```bash
   AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
           --query Parameter.Value --output text 2>/dev/null) || \
   AMI=$(aws ec2 describe-images --owners amazon \
           --filters 'Name=name,Values=al2023-ami-2023.*-kernel-6.1-arm64' 'Name=state,Values=available' \
           --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
   echo "$AMI"
   ```
5. **Launch.** The instance needs outbound internet access for GitHub, PyPI and Zenodo; the
   default-VPC subnet's public IP provides it. It needs no inbound rule and no SSH key.
   ```bash
   ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t4g.small \
         --iam-instance-profile Name=vtop-eval-ec2 \
         --user-data file://$S/user-data.sh \
         --metadata-options HttpTokens=required,HttpEndpoint=enabled \
         --instance-initiated-shutdown-behavior terminate \
         --tag-specifications 'ResourceType=instance,Tags=[{Key=project,Value=vtop-eval},{Key=Name,Value=vtop-smoke}]' \
         --query 'Instances[0].InstanceId' --output text); echo "$ID"
   ```
   The script powers the VM off when it finishes, and on failure too. With the shutdown behaviour
   `terminate`, power-off terminates the instance. A `shutdown -h +120` failsafe caps a hung run at
   two hours.
6. **Wait and watch:**
   ```bash
   aws ec2 wait instance-terminated --instance-ids "$ID" || aws ec2 wait instance-terminated --instance-ids "$ID"
   aws ec2 get-console-output --instance-id "$ID" --latest --output text | tail -50   # while it runs
   aws s3 ls s3://$B/smoke/results/
   ```
7. **Collect the results:**
   ```bash
   RUN=$(aws s3 ls s3://$B/smoke/results/ | awk '{print $2}' | tr -d / | grep "$ID")
   aws s3 cp --recursive s3://$B/smoke/results/$RUN/ $S/results/$RUN/
   python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['summary'], indent=1))" $S/results/$RUN/run.json
   cat $S/results/$RUN/exit.json
   ```
   The results contain:
   - `run.json`: instance type, AMI, region, AZ, `ldd`, `vtopctl --version`, tag and commit, archive SHA-256,
     Sigstore status, SDK crate versions and environment, pip versions, code SHA-256 and summary;
   - `<mode>.json`: config, `validate` output, the commit time series, ledger states, the log summary,
     the oracle report, scrub and audit output;
   - `engine-<mode>.jsonl`, `bootstrap.log`, `cloud-init-output.log`, `exit.json`, and copies of the scripts.
8. **Clean up.** Delete the data prefix, abort any multipart upload left open, and confirm the instance
   is gone. Keep `smoke/results/`.
   ```bash
   aws s3 rm --recursive s3://$B/smoke/data/
   aws s3api list-multipart-uploads --bucket $B --prefix smoke/ \
     --query 'Uploads[].[Key,UploadId]' --output text | while read -r k u; do
       [ "$k" = None ] || aws s3api abort-multipart-upload --bucket $B --key "$k" --upload-id "$u"; done
   aws s3 rm --recursive s3://$B/smoke/code/          # optional
   aws s3 rm --recursive s3://$B/smoke/input/         # optional
   aws ec2 terminate-instances --instance-ids "$ID"   # only if it is not already terminated
   aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
     Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId'
   ```

## Expected cost (check every price before launch)

| Item | Amount | Price |
|---|---|---|
| t4g.small, on demand, us-east-1 | ≈ 0.3 h: bootstrap of a few minutes plus four modes; 2 h failsafe cap | check |
| Public IPv4 address while the instance runs | ≈ 0.3 h | check |
| EBS gp3 root, default 8 GiB | ≈ 0.3 h | check |
| S3 requests | under 1,000 per run (PUT/HEAD/GET/LIST) | check |
| S3 storage | about 90 MB under `smoke/data/` until step 8, plus results of a few MB | check |
| EC2↔S3 transfer, same region | about 180 MB | check (same-region S3 transfer is normally not charged) |
| Internet ingress (GitHub 17 MB, PyPI, Zenodo 4.6 MB) | — | check |

## Local dry run (2026-11-20)

The scripts ran against MinIO `RELEASE.2025-09-07T16-13-09Z` on localhost with the v0.10.1
`aarch64-apple-darwin` binary (SHA-256 checked against `SHA256SUMS`). The run set
`SMOKE_LAB_ENDPOINT`, which adds `endpoint_url`, `force_path_style: true` and `verify_tls: false`. It is never set on EC2.

- All four modes passed. Each had 3 committed batches, 3 manifests and 3 receipts, and `shutdown flush complete` with exit 0.
- Methods: size-only `size_only`; sha256-store `store_checksum` for every object; blake3 `read_back`.
  sha256-multipart had 2 multipart objects (`read_back`) and one single-PUT tail (`store_checksum`).
  The engine log, the receipts and the oracle's HEAD inference agreed in every mode. scrub and audit-coverage returned 0.
- On MinIO, size-only and BLAKE3 objects also carried a whole-object `ChecksumSHA256`. This is the backend's own header (#778).
- Negative check: one size-only object was overwritten with the same size and one flipped byte. The oracle then failed
  (`unsafe_commits: 1`, `complete: false`).
- MinIO is not S3. It says nothing about S3's handling of the SDK's per-part checksums or of the
  backend's SHA-256 header, or about IMDS credentials, real endpoint resolution or latency.

## Differences from the v0.7.0 harness (`exp_minio_cost.py`, `lib/vtoplab.py`)

- Engine v0.10.1 instead of v0.7.0 (6c8f386). Config key spellings did not change for the keys the harness
  uses; v0.10.1 only adds keys.
- Real S3: no `endpoint_url`, `force_path_style` or `verify_tls: false`; credentials come from the instance role, not static keys.
- New `manifest.verification_receipts: true`. It adds one `.receipt.json` per batch and is not part of the
  commit. It records the method that actually ran.
- `multipart_threshold_bytes` is now set explicitly per mode. The harness left the 8 MiB default, so its
  `sha256-nocomp` objects (about 32 MiB) were multipart and read back.
- v0.10.1 behaviour the harness did not have:
  - The BLAKE3 and no-checksum paths send a backend-computed `x-amz-checksum-sha256` on PutObject (#778,
    `s3_native.rs:875-882`), which costs one extra local read per object.
  - Object keys are stable and probed with a HEAD before the PUT (#626). Several ledger rows may name
    one object, and manifests are per attempt.
  - The ledger has `verification_class` and `sealed_at` columns.
  - `verification_passed` log lines carry `object_check` and `manifest_check`.
  - `batch_metrics` log lines carry only `total_ms` and `state_write_ms`. This was already true at
    v0.7.0 (`engine.rs:995-1006`), so the `sum_*_ms` stage fields in the v0.7.0 MinIO `run`-mode
    results are all `0.0`. smoke.py records the log fields as they are and does not sum stages.
- `Run.__init__` uses `cp -c` (APFS clone), which does not exist on Linux, so smoke.py copies the input
  with `shutil` and does not import `vtoplab`. `host_info()` uses macOS `sysctl`; `run.json` reads IMDS instead.
