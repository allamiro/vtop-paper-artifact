# Cloud evaluation (AWS) — lessons learned

Running log for the real-cloud campaign that closes two gaps of the earlier, local-only evaluation: all runs were on a
single laptop, and there was no quantitative comparison with prior similar work. Each entry:
what happened, why it matters, what we do now. Newest last. Facts only; mark anything unverified.

## 2026-11-20 — choosing a provider

- **Store-computed SHA-256 is the deciding feature.** VTOP's strongest cost result (store checksum costs
  nothing measurable, read-back costs 19–27% under latency) depends on the store computing SHA-256 over the
  stored body. Amazon S3 documents this (additional checksums). DigitalOcean Spaces' S3-compatibility page
  (last verified 2026-06-22) does not mention checksum headers at all and says unsupported operations return
  `NotImplemented`. Google Cloud Storage's native hashes are CRC32C and MD5; its S3 interoperability docs do
  not list `x-amz-checksum-*`. **Lesson:** "S3-compatible" does not imply the verification class VTOP relies
  on; the available verification class is a property of the backend (the paper's "backend-limited" class).
  Untested on Spaces/GCS — a smoke test there would turn this into evidence.
- **Decision:** Amazon S3 (us-east-1) for storage; compute on EC2 in the same region (free transfer).

## 2026-11-20 — account access

- **`aws login` (AWS CLI ≥ 2.32; installed 2.41.2 via Homebrew) gives the CLI temporary console credentials
  through a browser sign-in**, so no long-lived key is needed to bootstrap. When the person signs in as the
  account root, the CLI session *is root* (`arn:aws:iam::<acct>:root`).
- **Root cannot assume IAM roles** (`AccessDenied: Roles may not be assumed by root accounts`). The plan to
  bootstrap as root and then work through a restricted role fails; a restricted **IAM user** is needed
  instead (or an IAM Identity Center user).
- **zsh expands `$VAR:r` as a modifier.** In a heredoc, `"$ACC:root"` became `"XXXXXXXXXXXXoot"` (`:r` strips
  an extension), producing a `MalformedPolicyDocument`. Use `${ACC}` inside heredocs.
- **Least-privilege setup that worked** (verified by negative tests): user `vtop-eval-operator` with an inline
  policy allowing EC2 only in us-east-1, denying `RunInstances` for anything but t3/t4g micro–large, denying
  reservations, hosts and fleets, `iam:PassRole` only for `vtop-eval-ec2`, and S3 only on the test bucket.
  Listing other buckets and calling other regions were refused. The VMs use the instance role
  `vtop-eval-ec2` (bucket-only S3), so no keys are placed on them. The access key was written straight into
  the local profile `vtop-op` without being printed.
- **Prebuilt binaries avoid build cost.** vtop-engine v0.7.0 has release tarballs for x86_64 and aarch64
  Linux, so EC2 instances need no Rust toolchain and ARM (Graviton t4g) instances can be used.
- **Local source builds on macOS need care** (from the author's attempt): librdkafka is built with SSL and
  SASL; with Homebrew's `openssl@4` linked by default, the link fails with undefined `_ENGINE_*` unless
  `OPENSSL_DIR`/`OPENSSL_ROOT_DIR` point at `openssl@3`, and cached `openssl-sys`/`rdkafka-sys` builds must
  be cleaned. Recorded in the artifact README (v1.0.4).

## 2026-11-20 — choosing the engine version

- **Check the release list, not memory.** v0.8.0 was first pinned as "latest" from memory; the author pointed out v0.9.0, v0.10.0 and v0.10.1 had shipped since
  (`gh release list -R allamiro/vtop-engine`). Always list releases before pinning.
- **Prebuilt binaries must match the VM's C library.** v0.8.0's Linux archives needed glibc 2.39 (#808), so
  they would not have started on Amazon Linux 2023 (glibc 2.34) or Ubuntu 22.04; v0.10.1 builds them on
  AlmaLinux 8 (glibc 2.28). Record `ldd --version` on every host.

## 2026-11-20 — re-pinning a harness to a newer engine

- **Summaries drift; keep the source's words.** A summary given to the author said "all six defects are
  fixed" and "ledger writes are now fenced"; the engine maintainer corrected both (only the fix PRs were shown
  to be contained in v0.8.0; the ledger fence predates v0.7.0). Record the primary statement, mark inferences
  as inferences, and verify each fix against its own issue.
- **A fault emulation can silently become unrealistic when the system improves.** Once VERIFIED and
  SOURCE_COMMITTED were fsynced (#626), deleting the whole WAL stopped emulating power loss and started
  destroying committed, synced data. Re-derive every emulation from the current durability contract.
- **Metrics can change meaning.** With stable object names, several manifests (attempts) may point at one
  object; "duplicate objects" must be counted by object URI, not by manifest, or v0.7.0 and v0.10.1 numbers
  are not comparable.
- **Schema guards break injectors.** v0.10.1 refuses ledger INSERTs in any state but `batching`; a tamper
  injector must insert then UPDATE.

## 2026-11-20 — smoke-test preparation (dry run on local MinIO with the v0.10.1 macOS binary)

- **glibc, measured:** v0.7.0 and v0.8.0 aarch64 Linux `vtopctl` need GLIBC_2.38 (`__isoc23_strtol`, by
  objdump), so they cannot start on Amazon Linux 2023 (glibc 2.34); v0.10.1 needs at most GLIBC_2.28 and has
  no libssl/libsasl2 dependency. (The engine docs say 2.39 for v0.8.0–v0.10.0 and do not mention v0.7.0.)
- **Every upload now carries a SHA-256.** In v0.10.1, BLAKE3 and no-checksum uploads also send an
  engine-computed `x-amz-checksum-sha256` (#778, s3_native.rs:875-882), so S3 stores a SHA-256 in every mode;
  only `checksum.algorithm: sha256` on a single-request upload is verified from it. Multipart objects are
  always read back. `AWS_REQUEST_CHECKSUM_CALCULATION` is recorded per run.
- **Configured batch size ≠ observed batch size.** With `batching.max_bytes` 16 MiB, `vtopctl run` cut the
  73,417,507-byte SSH log into three batches of 33,554,257 bytes (v0.7.0 on MinIO and v0.10.1 in the dry
  run); `process-once` produced exact 16 MiB batches. Record the observed batch sizes, not the config.
- **Per-stage times are not logged in run mode.** The per-batch log line has only `total_ms` and
  `state_write_ms`, so per-stage sums in run-mode results are 0.0 and must not be interpreted.
- **v0.10.1 can say which check ran**: the `verification_passed` log line, a signed per-batch receipt
  (`manifest.verification_receipts: true`) and the ledger's `verification_class` column; in the dry run all
  three matched the oracle's own HEAD with `ChecksumMode=ENABLED`.
- **Release signatures:** the `.sigstore.json` files are `cosign sign-blob` bundles; `gh attestation verify`
  does not apply; the VM installs cosign to check them.
- **The stable-key lookup needs `s3:ListBucket`**; without it v0.10.1 gets 403 and falls back to a new key per
  attempt (engine.rs:1031-1037). The instance role grants it.

## 2026-11-20 — launching

- **Accounts may have no default VPC.** This one had two non-default VPCs and no default VPC in us-east-1, so
  a plain `run-instances` has nowhere to go. `aws ec2 create-default-vpc` recreates the standard one (free).
- **The engine's run-mode batch size is a defect** (maintainer, 2026-11-20): the file reader counts each line
  with its newline toward `max_bytes` (file_source.rs:2892, :2851), the batcher counts records without it
  (batch.rs:188, :200), and in run mode the next read asks for a full `max_bytes` again (engine.rs:3060-3066),
  so batches close at ≈ 2 × max_bytes (33,554,257 = 2 × 16,777,216 − 175). process-once reads once and is
  exact. Present in v0.7.0 and v0.10.1; fix planned on main for the next release (PR number after 22 Nov; no issue is filed by project rule). Kafka is likely affected through the same full-budget re-read (kafka_source.rs:367-375), magnitude unmeasured. **Lesson: validate that a
  configured parameter took effect by measuring it.**

- **`cmd | head -1` under `set -o pipefail` kills a script** (exit 141, SIGPIPE) whenever `cmd` writes more
  than head reads. The first smoke launch died on `ldd --version | head -1`; the macOS dry run could not
  show it (no `ldd` there). Lesson: a dry run on another OS does not exercise platform probes — test the
  bootstrap on the target image, and never pipe a chatty command into `head` under pipefail.

- **Test tools need the same memory budget as the target.** The oracle's
  `zstandard.decompress(max_output_size=1<<31)` preallocates 2 GiB for frames without a content size: fine on
  a large laptop, `MemoryError` on a 2 GiB VM. Stream decompression instead.
- **Stable object keys make reruns non-independent.** v0.10.1 reuses an object with the same derived name
  if it verifies, so a rerun over leftover `data/` objects measures reuse, not upload. Empty the data prefix
  (or use a fresh prefix per run) before every run.

## Harness port to v0.10.x (2026-11-20, local, before the v0.10.2 pin)

- **Object names grow with the input path.** v0.10.1 names objects
  `vtop-<engine>-<full sanitized source path>-b<start>-<end>-i<inode>-<32 hex>`, unbounded. With inputs under a long
  directory, the second range's name passed 255 bytes; localfs failed with ENAMETOOLONG on every pass and the run never
  drained (nothing unsafe committed). The archived v0.7.0 result layouts would hit it (predicted 262–271 bytes). The
  harness now clones inputs to `.tmp/in/<hash>/` when the predicted name is over 240 bytes; the engine bounds names
  in v0.10.2. S3 keys allow 1,024 bytes, so the cloud runs are not affected. *Lesson: a smoke test on a short path
  hides limits a real layout hits; test the harness at the real path depth.*
- **Power loss must be emulated from the WAL, not by deleting it.** v0.10.x fsyncs VERIFIED and SOURCE_COMMITTED
  writes (`writer_for`, sqlite_store.rs:350); deleting the WAL after SIGKILL discards durable commits, which a power loss
  cannot do. `vtoplab.power_loss_truncate()` parses the WAL (frame salts and checksums), replays each commit prefix on
  a copy, and truncates after the last commit that wrote verified/source_committed. Four local trials on v0.10.1:
  2–5 unsynced commits dropped each, every restart drained, 0 unsafe commits, 0 duplicate objects. During `vtopctl run`
  no other write uses the FULL connection (audit appends on FULL come only from the `replay`/`prune-ledger`/`migrate`
  commands).
- **Count objects, not manifests.** A replay usually reuses its stably named object (2 of the 4 power-loss trials
  re-used one); manifests are per attempt. The oracle now checks each object once and reports
  `manifests_reusing_object`.
- **Pick the build by setting, never by editing paths.** `VTOP_BUILD=v0.10.1` (or a new `BUILDS` entry) selects the
  engine; the default stays v0.7.0 so archived results reproduce. One build per result set.
- **The model found an engine bug before any experiment ran.** The bounded model's Kafka resume variant reached a
  state where the newest-created committed row has a lower end than an older one, so a later restart re-archives a
  range (duplicate, not loss). The maintainer tried resuming from the max committed end (PR #844) and found it opens a *loss* when a Kafka topic is
  deleted and recreated, so v0.10.2 keeps the newest-row rule. *Lesson: the obvious fix for a duplicate can trade it for
  a loss; check a proposed fix against every way the source can restart its numbering.*

## Widening to more regions (2026-11-20)

- One condition change (`aws:RequestedRegion` list) is the whole widening; keep the other guardrails as they are and
  prove both sides after applying: the new regions answer, a region outside the list is still refused.
- Unlike us-east-1 at the start, the four new regions already had default VPCs (accounts get them per region; the
  missing us-east-1 one had been deleted at some point).

## Measuring engine memory (2026-11-21, found while preparing E-RQ5-SCALE)

- **Do not quote E-RQ5-COST's "max RSS" (≈ 1,640 MiB on HDFS) as engine memory.** On Linux, a child's `ru_maxrss`
  (from `wait4`) inherits the parent's peak when the child was forked from a large parent, so it carried the Python
  driver's peak, which had the 1.58 GB HDFS file memory-mapped for the oracle. In an AL2023 container the engine's own
  peak (`/proc/<pid>/status` VmHWM, sampled) was ≈ 68 MiB while `ru_maxrss` read 139 MiB. E-RQ5-SCALE records VmHWM per
  engine; COST's RSS column is withdrawn (throughput, CPU seconds and correctness are unaffected).

## Resources created (for clean-up)

| Resource | Name | Created |
|---|---|---|
| S3 bucket (private, public access blocked, tag project=vtop-eval) | `vtop-eval-XXXXXXXXXXXX` | 2026-11-20 |
| IAM role + instance profile (bucket-only S3) | `vtop-eval-ec2` | 2026-11-20 |
| IAM user + inline policy + one access key (local profile `vtop-op`) | `vtop-eval-operator` | 2026-11-20 |
| Budget, USD 10/month, alerts 50%/85% actual and 100% forecast | `vtop-eval-monthly` | 2026-11-20 |
| Default VPC us-east-1 (tag project=vtop-eval; free; keep or delete with `aws ec2 delete-vpc` after removing its subnets/IGW) | `vpc-0a3c0da0f712a8b93` | 2026-11-20 |
| Smoke VM (self-terminating) | `i-0e8cada773da9188c` | 2026-11-20 18:05Z |
- 2026-11-21: bucket `vtop-eval-lock-XXXXXXXXXXXX` (Object Lock, versioned, lifecycle on `tamper/`) and managed policies `vtop-eval-tamper-ec2`, `vtop-eval-tamper-ec2-optional` (role) and `vtop-eval-tamper-operator` (user). Remove after the campaign: detach and delete the policies, empty (after retention) and delete the bucket.
