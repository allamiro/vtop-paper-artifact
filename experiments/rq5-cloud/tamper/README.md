# E-RQ5-TAMPER: signed manifests, Object Lock and versioning, audit trail on S3 (vtop-engine v0.10.2)

Hypotheses: H2.5 (signed manifests make forgery evident without the ledger), H2.6 (Object Lock and versioning resist or
expose overwrite and rollback), H2.7 (hash-chained audit trail is tamper-evident). H2.8 (server-side encryption) is
designed below and **pending v0.10.3** (engine PR #856). See [../../../hypotheses.md](../../../hypotheses.md).

**Status (2026-11-21): prepared and tested locally; nothing launched, created or spent on AWS.** The launch needs three
author approvals: the bucket and IAM change ([iam-and-bucket-change.md](iam-and-bucket-change.md)), the optional
permissions, and a "go" on the cost below.

| File | What |
|---|---|
| `tamper.py` | Driver (VM or local MinIO): 55 cases. Archive, tamper, run each verifier, compare with the expected verdict |
| `analyze.py` | Case table and summary from one or more result directories (local and AWS side by side) |
| `user-data.sh` | cloud-init for AL2023 arm64, t4g.small. Pinned v0.10.2 archive, verified inputs, EXIT trap, failsafe 75 min |
| `iam-and-bucket-change.md`, `iam/*.json` | The second bucket and the IAM policies, **for approval, not applied** |
| `results/20261121-local-minio-v0102/` | Local run (macOS release binary, MinIO), with `ANALYSIS.md` comparing all local runs |
| `results/20261121-local-minio-v0102-rep2/` | Its replication (same verdicts in 55/55 cases) |
| `results/20261121-local-al2023-linux-v0102/` | The Linux aarch64 release binary (the VM's) in an `amazonlinux:2023` container, glibc 2.34, Python 3.11 + the pinned pip packages, against the same MinIO: same verdicts in 55/55 |
| `results/20261122T021852Z-i-0e178e36bc660a51f/` | **v0.10.4** (D16) AWS run, with `ANALYSIS.md` against the first v0.10.4 local run |
| `results/20261122-local-minio-v0104/`, `-rep2/`, `results/20261122-local-al2023-linux-v0104/` | **v0.10.4** local runs (macOS ×2, Linux), `ANALYSIS.md` in the first. Verdicts identical in all four v0.10.4 runs (AWS = each local run in 53/53 comparable cases) |

**v0.10.4 notes (2026-11-22).**
- **Verdict changes against v0.10.2 (code-verified, maintainer-confirmed; `state-machine/v0104-repin-notes.md`).**
  - MAC-SWAP-OFF/ON: verify-manifest (`vm_noledger`, `vm_ledger`) now **failed**, i.e. the swap is caught, by #882
    (v0.10.3; a manifest must name the key it was read from). Scrub and recovery are unchanged.
  - VER-SUSPEND-RUNNING: 1 write after suspension instead of 10, by #883 (v0.10.4). These are the objects already
    in flight, at most the upload width; no manifest and no commit.
- **Driver versions.** The AWS run and the two macOS runs used driver `f2f52011…` (expectations written for v0.10.2:
  MAC-SWAP verify-manifest `passed`, a stated limit), so they mark MAC-SWAP as DIFFERS. The Linux run used `b387f4a5…`,
  the current `tamper.py`. The diff between the two is only the MAC-SWAP-OFF/ON `expect`, `limit` and `note` fields
  (checked with `git diff`). The paper pipeline compares observed verdicts, not the `as_expected` flags
  (`analyze.py --set v0104`, `tamper_engine_changes`).
- **Linux recipe.** The only difference from v0.10.2's is a Python 3.11 venv in `amazonlinux:2023` (same pinned
  packages, `/opt/venv/bin/python -I tamper.py`): `blake3` was not importable under `-I` from `/usr/local`.

## Design

### Attacker and what "caught" means

Each case is a pair: someone changes the archive, then a VTOP defence gives a verdict. The attacker can:

| Code | Can write | Holds the MAC key |
|---|---|---|
| bucket | the bucket (a bare S3 client, never through the engine) | no |
| bucket+ledger | the bucket and the SQLite ledger (rewrites the row to agree with the forgery) | no |
| +own key | as above, and signs with a key of its own under a made-up key id (`mallory`) | its own |
| +leaked k1 | as above, with the real signing key | yes (leaked) |
| ledger file | the ledger file only (audit cases; edits it with SQL) | no / leaked |
| break-glass | a principal with `s3:BypassGovernanceRetention` | n/a |

Each case states, for each verifier, the verdict it **expects**, taken from the hypothesis and the engine maintainer's
evidence (H2.5–H2.7; PR #859 `docs/reference/tamper-evidence.md`). Where a defence is off or cannot see the change,
the expected verdict is the **wrong** one, and the check is marked a *limit*. Observing it measures the limit
(NOT CAUGHT); the test asserts the wrong verdict, as the engine suite does. A case passes when every observed verdict
equals its expectation. Anything else is **DIFFERS**, a result about the hypothesis, the engine or the store.

### Verifiers

| Name | What runs | Verdict |
|---|---|---|
| `scrub` | `vtopctl scrub --json` on the case's ledger (`--check-retention` under the lock profile). Receipts are on in every profile | the target batch's result (`ok`, `mismatched`, `missing`, `unauthenticated`); `scrub.receipt`, `scrub.retention`, `scrub.passed` from the same run |
| `scrub_require` | the same with `--require-receipts` | as above |
| `scrub_rotated` / `scrub_retired` | the same ledger under a keyring with k1 rotated out (k2 active, k1 verify-only) or k1 retired | as above |
| `vm_noledger` | `vtopctl verify-manifest --manifest <current key>` with a **fresh empty ledger**: the auditor or restore that has config + key + bucket but no ledger (H2.5's no-ledger arm) | `passed` / `failed` |
| `vm_ledger` | the same with the case's ledger | `passed` / `failed` |
| `check_storage` | `vtopctl audit-coverage --check-storage` (one HEAD per object) | `failed` if the target is reported |
| `recovery` | exp_tamper.py's fault F4: the target row is set back to VERIFIED (crash between verification and source commit), then `vtopctl run` restarts | `replayed` (refused and re-archived) / `committed`; plus an independent content oracle |
| `store` | S3's own answer to the tamper request (Object Lock) | `refused` / `accepted` |
| `engine_start` | `vtopctl run` against a bucket that does not meet the profile | `refused` / `started` |
| `audit_verify`, `anchor`, `coverage`, `sql` | `vtopctl audit verify --json` on a copy of a 6-row chain (3 × `vtopctl replay`). `anchor` compares the row count and last link kept from the untouched chain. `vtopctl audit-coverage`. The ledger's own answer to UPDATE/DELETE | `passed`/`failed`; `same`/`differs`; `hole`/`no_hole`; `refused`/`accepted` |

### Independence from the engine's own tests

- The tampering is done by `tamper.py`, not by the engine suite (PR #859 is not in v0.10.2 anyway). It uses the mutation
  classes of `rq2-corruption-recovery/code/exp_tamper.py` (M1 byte flip, M2 truncate, M3 delete, M4 another batch's
  evidence, M6 field edit + recomputed self-hash, M7 consistent forgery with the same `Failed password` →
  `Accepted passwd` edit). These are extended to 0.4 manifests (canonical JSON hash), keyed MACs, receipts, the audit
  hash chain, S3 versions and retention.
- The 0.4 self-hash, the keyed BLAKE3 MAC (manifests and receipts) and the audit chain link (BLAKE3 with a key derived
  under `vtop-engine 2026 audit_events chain v1`) are re-implemented from the published format. **Before any case, the
  driver checks the re-implementation against what the engine wrote** (`run.json` → `reimpl_check`). If it fails, no
  case runs. It passed in all three local runs.
- The recovery oracle uses no VTOP code. It re-reads every committed object, decompresses it and compares it with the
  source bytes, which gives `forged_content_committed`.
- Lab keys k1 = `6b`×32, k2 = `6c`×32, mallory = `a7`×32 are published on purpose. They protect nothing.

### Workload and profiles

The input is the first 3,145,728 B of Loghub SSH.log, cut after the last newline: 3,145,699 B, SHA-256
`7ac17dce…2572`. With `batching.max_bytes` 1 MiB this gives 3 committed batches. The objects are zstd, about 70 KB,
single PUT, SHA-256 store checksum. The manifest is version "0.4" and receipts are on. Each case has its own ledger and
prefix (`tamper/data/<run_id>/<case>/`). The **target** is the batch with the highest end byte (as exp_tamper.py); the
**other** batch is the lowest.

| Profile | Bucket | `manifest.mac` | `require_object_versioning` | `object_lock` |
|---|---|---|---|---|
| plain-off / plain-mac | study bucket (unversioned) | off / keyring {k1} | false | – |
| ver-off / ver-mac | lock bucket used without the lock profile (versioned, unlocked writes) | off / {k1} | true | – |
| lock-mac | lock bucket | {k1} | true | governance, `retain_days: 1` |

## Cases, expected verdicts and local outcomes

Local outcome: the same in all three local runs (MinIO RELEASE.2025-09-07, the macOS and Linux v0.10.2 release
binaries). *L* = a limit check (the expected verdict is the wrong one). Full table with every verdict:
[results/20261121-local-minio-v0102/ANALYSIS.md](results/20261121-local-minio-v0102/ANALYSIS.md).

### H2.5: signed manifests (study bucket, unversioned)

| Case | Attacker | Tamper | Expected | Local |
|---|---|---|---|---|
| MAC-FORGE-OFF-B | bucket | M7 forgery, MAC off (receipt rewritten too) | scrub unauthenticated (ledger digest); vm_noledger passed *L*; vm_ledger failed; recovery replayed | as expected |
| MAC-FORGE-OFF-BL | bucket+ledger | same, ledger rewritten | scrub ok + passed *L*; vm_noledger, vm_ledger passed *L*; recovery **committed** *L* (oracle: forged content committed) | NOT CAUGHT, measured |
| MAC-FORGE-OFF-BL-RCPT | bucket+ledger | same, receipt left as the engine wrote it | scrub batch ok *L* but **receipt mismatched**, scrub fails; vm_noledger passed *L*; recovery committed *L* | as expected: the receipt is a second signal if the attacker forgets it |
| MAC-FORGE-ON-B | bucket | M7, MAC on, MAC left stale | scrub unauthenticated; vm_noledger failed; vm_ledger failed; recovery replayed | caught |
| MAC-FORGE-ON-BL | bucket+ledger | same, ledger rewritten | same | caught |
| MAC-STRIP-ON-BL | bucket+ledger | MAC and key id stripped | scrub unauthenticated; vm_noledger failed; recovery replayed | caught |
| MAC-FOREIGN-BL | +own key | signed by `mallory` | same ("not in this keyring") | caught |
| MAC-SWAP-OFF | bucket | M4: another batch's genuine manifest over this one's | scrub unauthenticated ("is batch X's"); recovery replayed; **vm_noledger, vm_ledger passed** *L* | caught by scrub/recovery; NOT CAUGHT by verify-manifest |
| MAC-SWAP-ON | bucket | same, MAC on | same: the MAC does not catch a swap | same |
| MAC-LEAKED-BL | +leaked k1 | M7 signed with k1, ledger + receipt rewritten | scrub ok + passed *L*; vm_noledger passed *L*; recovery committed *L* | NOT CAUGHT, measured |
| MAC-RETIRED-BL | +leaked k1 | same, then k1 rotated / retired | scrub_rotated ok *L*; scrub_retired unauthenticated (all 3 batches, honest ones too, by design); vm_noledger_retired failed; recovery_retired replayed | as expected |
| RCPT-EDIT-ON / -NOHASH-ON | bucket | receipt `verification.method` edited, hash recomputed / not | receipt invalid; batch ok | caught |
| RCPT-SWAP-ON | bucket | another batch's receipt | receipt mismatched | caught |
| RCPT-DEL-ON | bucket | receipt deleted | receipt missing; a finding only with `--require-receipts` | as expected |
| RCPT-CLASS-OFF | bucket | MAC off: `verification.class` → size_only | receipt mismatched (the ledger holds the class) | caught |
| RCPT-BY-OFF / RCPT-METHOD-OFF | bucket | MAC off: `verified_by` / `verification.method` edited | receipt ok *L* | NOT CAUGHT, measured |
| OBJ-FLIP-ON (M1) | bucket | same-size byte flip, honest manifest | scrub mismatched; vm_noledger failed; recovery replayed; **check_storage passed** *L* | caught; HEAD-only audit limit measured |
| OBJ-TRUNC-ON (M2) / OBJ-DEL-ON (M3) | bucket | truncate / delete | scrub mismatched / missing; check_storage failed; recovery replayed | caught |

### H2.6: versioning (lock bucket without the lock profile) and Object Lock

| Case | Tamper | Expected | Local |
|---|---|---|---|
| VER-OVR-MAN | manifest overwritten (M6, MAC stale) | scrub **mismatched** ("current key no longer holds version V the ledger pinned"); original version kept; vm_noledger failed; recovery **committed** (reads the intact pin: not followed, correct) | as expected |
| VER-OVR-OBJ | object overwritten (M1) | scrub mismatched; original kept; recovery replayed | caught |
| VER-OVR-BOTH-OFF | MAC off, M7 forgery of both | scrub mismatched (the pinned manifest's digest); originals kept; vm_noledger passed *L*; recovery replayed | caught + limit |
| VER-ROLLBACK | the current key serves **another validly signed** manifest of the batch | scrub **mismatched** (flagged); recovery committed (not followed); vm_noledger and vm_ledger passed *L* (no pin without the ledger row's version) | answers the H2.6 UNSURE: **scrub flags it; recovery does not follow it** |
| NOVER-ROLLBACK | the same on the unversioned bucket | scrub unauthenticated (ledger manifest digest only); vm_noledger passed *L*; recovery replayed | as expected |
| VER-RESTORE | overwrite, then copy the pinned version back | scrub ok (compares bytes, not version ids) | control |
| VER-DELMARK | manifest DELETE without version | scrub missing; vm_noledger failed; recovery committed (pin readable) | as expected |
| VER-DELPIN | the pinned manifest version deleted | scrub missing; recovery replayed (fails closed, #135) | caught |
| VER-REFUSE-UNVERSIONED | `require_object_versioning` on the unversioned bucket | engine refuses to start | caught |
| VER-SUSPEND-NEW (local only) | versioning suspended, new engine | engine refuses to start | caught |
| VER-SUSPEND-RUNNING (local only) | versioning suspended under a running engine, more input appended | commits nothing **and writes nothing** (engine maintainer) | **DIFFERS**: commits nothing, but **writes 10 objects in 25 s**: the object once, then one manifest per retry. Each batch fails with "manifest upload returned no object version to pin", and the batch retries with back-off. Identical in all 3 runs |
| LOCK-CONTROL | none | scrub ok; retention locked; receipt ok | as expected |
| LOCK-DELVER-OBJ / -MAN | DeleteObjectVersion on a locked version | store refused; scrub ok; locked versions intact | prevented |
| LOCK-SHORTEN | retention shortened 12 h, no bypass | store refused | prevented |
| LOCK-OVR-OBJ / -MAN | PUT over a locked key | store **accepted** (new version); locked version intact; scrub mismatched; recovery replayed | caught (not prevented) |
| LOCK-DELMARK-OBJ | DELETE without version | store accepted (delete marker); scrub missing | caught |
| LOCK-BYPASS-CLEAR | retention cleared with the governance bypass | store accepted; `scrub --check-retention` **missing** | caught (optional permission on AWS) |
| VER-SUSPEND-LOCKED | suspend versioning on the Object Lock bucket | store refused (`InvalidBucketState` on MinIO) | prevented (optional permission on AWS) |
| LOCK-REFUSE-PLAIN | `object_lock` on the study bucket | engine refuses to start | caught |
| LOCK-COMPLIANCE | – | **not run by design**: compliance retention cannot be removed by anyone before it ends | stated limit |

### H2.7: audit trail and coverage (ledger file attacker)

| Case | Tamper | Expected | Local |
|---|---|---|---|
| AUD-CONTROL | none | audit verify passed | as expected |
| AUD-UPDATE-TRIGGER / AUD-DELETE-TRIGGER | UPDATE / DELETE through SQL | refused by the triggers ("audit_events is append-only") | prevented |
| AUD-EDIT / AUD-DELETE-MID / AUD-SWAP | triggers dropped; row 3 edited / deleted / rows 3–4 swapped | BROKEN, row named | caught |
| AUD-TRUNCATE | last row deleted | chain passed *L*; anchor (kept count + last link) differs | NOT CAUGHT by the chain; caught by the anchor |
| AUD-RECOMPUTE-UNKEYED | unkeyed chain (MAC off), edit + every later link recomputed | passed *L* | NOT CAUGHT, measured |
| AUD-RECOMPUTE-NOKEY / AUD-REWRITE-ALL-NOKEY | keyed chain rewritten without the key (from row 3 / every row) | BROKEN ("unkeyed link after a keyed one" / "no keyed row although a key is configured") | caught |
| AUD-RECOMPUTE-LEAKED | keyed chain recomputed with the leaked key | passed *L* | NOT CAUGHT, measured |
| COV-DELETE-MID / COV-DELETE-END | committed ledger row deleted, middle / last | coverage hole / no hole *L* | caught / NOT CAUGHT, measured |

**Totals (local, each run):** 55 cases: 28 caught, 6 prevented, 10 caught + NOT CAUGHT on another verifier, 7 NOT
CAUGHT (all predicted), 3 controls, **1 DIFFERS** (VER-SUSPEND-RUNNING writes). There were 130 checks: 71 caught, 7
prevented, 30 NOT CAUGHT as predicted, 21 as expected, and 1 differs.

### Findings from the local runs that the paper and the engine maintainer should see

1. **`verify-manifest` does not bind a manifest to the key it was read from** (commands.rs `verify_manifest_deep`): a
   swapped-in genuine manifest of another batch passes the no-ledger check, **with the MAC on or off**, and with the
   ledger present too (the row it looks up is the other batch's). So H2.5's prediction "MAC on: every forgery **and
   manifest swap** flagged" holds for `scrub` and recovery, not for `verify-manifest`. Predicted from the code, then
   confirmed (MAC-SWAP-ON/-OFF). The engine suite's swap case uses the scrub only.
2. **Versioning suspended under a running engine:** v0.10.2 commits nothing (as reported), but it keeps writing (the
   object once, then a manifest per retry). This contradicts "writes nothing" (H2.6 engine evidence). MinIO only, local.
3. **Rollback (H2.6 UNSURE) is answered:** with a pinned version, `scrub` **reports** a current key that differs from
   the pin (`mismatched`), and recovery **does not follow** it (it commits on the intact pinned version). Without
   versioning, only the ledger's manifest digest catches it. Without the ledger (verify-manifest), a validly signed
   rollback passes.
4. **Receipts are a second binding.** A MAC-off, bucket+ledger forgery that leaves the engine's receipt in place is
   still reported by the scrub (receipt `mismatched`). A complete attacker rewrites the receipt too (MAC-FORGE-OFF-BL),
   and then nothing reports it.
5. Everything else matches the engine maintainer's PR #859 table, now from independent tampering, including every NOT
   CAUGHT row (MAC off + ledger, leaked key, HEAD-only same-size, truncated audit tail, unkeyed/leaked-key chain
   recompute, last committed row deleted, receipt fields the ledger does not hold).

## AWS run (prepared, not launched)

- **Prerequisites (author):** the bucket `vtop-eval-lock-XXXXXXXXXXXX` and the IAM policies in
  [iam-and-bucket-change.md](iam-and-bucket-change.md); decide `OPTIONAL_PERMS` in `user-data.sh`.
- **One instance:** t4g.small (2 vCPU, 2 GiB) in us-east-1. The work is light (3 MiB input, about 70 KB objects); the
  local matrix took 140 s. Failsafe 75 min, no new case after 60 min.
- **Launch** (from the repo root, after the go; the skill's launcher, unchanged):

  ```bash
  export AWS_PROFILE=vtop-op AWS_REGION=us-east-1
  MAX_MIN=90 research/experiments/rq5-cloud/run-ec2.sh tamper \
    research/experiments/rq5-cloud/tamper/user-data.sh t4g.small research/experiments/rq5-cloud/tamper/tamper.py
  python3 -I research/experiments/rq5-cloud/tamper/analyze.py research/experiments/rq5-cloud/tamper/results/<run_id> \
    research/experiments/rq5-cloud/tamper/results/20261121-local-minio-v0102
  ```

  The second command puts the AWS run next to the local one and marks each case where they disagree.
- **On the VM:** the pinned v0.10.2 gnu archive (5872e5a3…df39, SHA256SUMS) runs `vtopctl --version` first. Then
  Python 3.11/3.12 with boto3 1.44.0, zstandard 0.25.0 and blake3 1.0.11, the SSH archive (MD5 and SHA-256 checked) and
  the slice (SHA-256 checked). `tamper.py` runs its preflight: it stops if the study bucket is versioned, or if the lock
  bucket lacks versioning or Object Lock. Next comes the re-implementation check, then the cases. Results go to
  `tamper/results/<run_id>/` after every case. The EXIT trap deletes `tamper/data/<run_id>/` from the study bucket,
  deletes every **unlocked** version under it in the lock bucket, writes `exit.json` (`data_objects_left`,
  `lock_versions_left`) and powers off.
- **AWS only runs** VER-SUSPEND-NEW and VER-SUSPEND-RUNNING are skipped (local only): AWS cannot suspend versioning on
  an Object Lock bucket, and the study bucket must stay unversioned. A third bucket would be needed (author's
  decision). VER-REFUSE-UNVERSIONED exercises the same start-up check on AWS.
- **Clean-up:** locked versions (engine writes under the lock profile, about 10 cases × 9 objects, about 1 MB) stay
  until their 1-day retention ends. The lock bucket's lifecycle rule removes them within about 3–4 days. No bypass is
  used for clean-up. After that: detach the policies, delete the bucket (iam-and-bucket-change.md §5). Record the bucket
  in the LESSONS.md resource table when it is created.

### Time and cost (estimate; prices as read 2026-11-20/21 for E-RQ5-COST, [../cost/README.md](../cost/README.md) refs 1–6)

Expected wall time: about 5 min bootstrap plus about 10–20 min of cases (each S3 request is a network round trip, not
local). Call it 0.5 h; hard cap 1.25 h (failsafe).

| Item | Expected (0.5 h) | Cap (1.25 h) | Price used (us-east-1) |
|---|---|---|---|
| t4g.small on demand | 0.008 | 0.021 | USD 0.0168/h [1] |
| Public IPv4 while running | 0.003 | 0.006 | USD 0.005/h [2] |
| EBS gp3 8 GiB root | < 0.001 | 0.001 | USD 0.08/GB-month [3] |
| CPU surplus credits (unlimited; bound 1.6 vCPU-h/h) | ≈ 0 (light load) | ≤ 0.080 | USD 0.04/vCPU-h [4] |
| S3 PUT/COPY/LIST, ≈ 3.2k (≈ 40 per case + result sync) | 0.016 | 0.016 | USD 0.005 per 1,000 [5] |
| S3 GET/HEAD and other, ≈ 4.4k | 0.002 | 0.002 | USD 0.0004 per 1,000 [5] |
| S3 storage (a few MB for ≤ 4 days, versions included) | < 0.001 | < 0.001 | USD 0.023/GB-month [5] |
| DELETE requests, S3 → EC2 same region | 0 | 0 | free [6] |
| **Total** | **≈ USD 0.03** | **≈ USD 0.13** | Object Lock and versioning have no separate charge beyond storage and requests (UNVERIFIED: not re-read on the S3 pricing page today) |

## H2.8: server-side encryption (pending v0.10.3; designed, not runnable on v0.10.2)

v0.10.2 has no `upload.server_side_encryption` key (it would be refused as unknown). Cell for when v0.10.3 is pinned:

| Case | Config | Tamper | Expected |
|---|---|---|---|
| SSE-S3-STORE / SSE-S3-READBACK | `server_side_encryption: {mode: sse-s3}`, sha256 store checksum / blake3 read-back, single PUT and one multipart object | none | HeadObject `ServerSideEncryption: AES256` on every object, manifest and receipt (oracle); scrub ok; throughput within the H3.5 noise (a small E-RQ5-COST cell) |
| SSE-KMS-AWSMANAGED | `{mode: sse-kms}` (no key id → `aws/s3`, no monthly key charge) | none | `aws:kms` with the aws/s3 key on every object; scrub ok |
| SSE-KMS-DOWNGRADE | sse-kms | object overwritten by a plain PUT (on AWS this is SSE-S3 by default, since 2023) | engine/scrub report the algorithm mismatch (`mismatched`); recovery replays |
| SSE-MIX | sse-s3 then sse-kms on one archive | none | pre-switch objects reported as scrub mismatches (engine docs) |

It needs no new S3 permission for SSE-S3. SSE-KMS with the AWS managed key should need none either, because the aws/s3
key policy admits principals of the account via S3 (UNVERIFIED). A customer managed key (another key id) has a monthly
charge and is left out unless the author wants the "another key" case.

## Local test (how the results were made)

```bash
docker run -d --name tamper-minio-c33 -p 127.0.0.1:19300:9000 -e MINIO_ROOT_USER=tamperlab \
  -e MINIO_ROOT_PASSWORD=tamperlab-secret vtop-minio:RELEASE.2025-09-07T16-13-09Z server /data
# buckets: plain (no versioning), locked (CreateBucket ObjectLockEnabledForBucket=True), susp (plain, versioning toggled by the driver)
AWS_ACCESS_KEY_ID=tamperlab AWS_SECRET_ACCESS_KEY=tamperlab-secret AWS_REGION=us-east-1 python3 -I tamper.py \
  --vtopctl .tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin/vtopctl --input <dir>/ssh.log \
  --plain-bucket plain --lock-bucket locked --susp-bucket susp --prefix local1 --out results/<name> --work <scratch> \
  --endpoint http://127.0.0.1:19300
docker rm -f tamper-minio-c33
```

The input is the 3,145,699-byte slice above, alone in its directory. The Linux run used `amazonlinux:2023`, the
aarch64 gnu archive (5872e5a3…) and the MinIO container's bridge address. The macOS runs used the darwin archive
(a4a46d53…, SHA256SUMS). Driver digests: the two macOS runs `4d29eb64…`. The Linux run `f2f52011…` is the final
`tamper.py`; it differs only in the result upload (`aws s3 sync`) and the optional-permission gating, which are inactive
locally. Each run took about 2.3 min.

## Unverified items and limits of this preparation

- **AWS behaviour seen only on MinIO:**
  - the refusal code for a locked-version delete and a retention shortening: MinIO answers `InvalidRequest: Object is
    WORM protected`, AWS documents AccessDenied. The control that IAM is not the cause is VER-DELPIN: the same
    permission on the same bucket deletes an unlocked version;
  - clearing a governance retention with `Retention={}` plus the bypass;
  - `InvalidBucketState` when suspending versioning on a locked bucket;
  - the version id S3 returns on an unversioned bucket (none).

  `analyze.py` with both directories shows every disagreement.
- **The study bucket's versioning status** is not checked here: no AWS call was made. The VM's preflight and step 4 of
  the IAM page check it, and the driver stops if it is versioned.
- **IAM:**
  - The existing inline policies cannot be read by the operator. The new ones are therefore separate managed policies,
    which avoids the 2,048-character limit on a user's inline policies.
  - `s3:GetObjectVersionAttributes` may be unnecessary. It is included because the role already has
    `GetObjectAttributes`.
  - `GetObjectLegalHold` is left out: `legal_hold` is false, and the engine docs require it only with a legal hold.
- **The lifecycle JSON** (two rules on one prefix) has not been applied, so S3 has not validated it yet.
- **Not covered:**
  - compliance mode and legal hold;
  - multipart objects (all objects are single PUT);
  - Kafka sources and the PostgreSQL ledger;
  - the no-recheck build (H2.2 already has its causal ablation, `rq2-corruption-recovery/results/20261121-v0102-norecheck`);
  - a third bucket for suspended versioning on AWS;
  - the "bucket admin" attacker, who could change bucket policy or lifecycle;
  - a compromised engine host, which holds the key, the ledger and the credentials together. This is out of scope for
    the engine and for us.
- **Rollback construction:** VTOP writes each manifest key once (per-attempt batch ids), so an "older valid version"
  at the same key only exists if a key holder writes it. VER-ROLLBACK synthesizes one with k1: created_at 1 h earlier,
  record_count − 1. It tests what the pin does, not how an attacker gets such a manifest.
- **One input size and one batch layout.** Verdicts are deterministic per case (3 identical local runs). The AWS run is
  one repetition.
