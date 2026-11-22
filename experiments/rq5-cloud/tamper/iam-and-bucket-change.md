# E-RQ5-TAMPER: bucket and IAM change, for the author's approval

**Nothing on this page has been applied.** It needs the author's explicit yes (rq5-cloud rule: any IAM change needs a
separate yes). Apply with the root profile `vtop-eval`, because the operator cannot write IAM or create buckets. Then
read each change back and run the negative checks in step 4. Write `${ACC}`, not `$ACC:…`, in zsh (LESSONS.md).

## Why a second bucket

S3 Object Lock can only be enabled when a bucket is created, and a bucket with Object Lock is versioned for good. Its
versioning cannot be suspended. The study bucket `vtop-eval-XXXXXXXXXXXX` stays as it is: unversioned, no lock. It is
the "plain" arm of every case. The new bucket is used two ways:

- **with** the engine's `upload.object_lock` profile (governance, `retain_days: 1`, the engine's minimum): the Object
  Lock cases;
- **without** it (`require_object_versioning: true` only): the versioning cases. This is why the bucket gets **no default
  retention**: with a default, every write would be locked, and the versioning-only cases could not delete a pinned
  version (VER-DELPIN).

| Item | Value |
|---|---|
| Name | `vtop-eval-lock-XXXXXXXXXXXX` (us-east-1) |
| Object Lock | enabled at creation; **no default retention** (the engine sets a 1-day governance retention per object) |
| Versioning | enabled (implied by Object Lock) |
| Public access | all four blocks on |
| Tag | `project=vtop-eval` |
| Lifecycle | `iam/lock-bucket-lifecycle.json`, under `tamper/` only. Current versions expire after 2 days (this adds a delete marker). Noncurrent versions are removed 1 day after they become noncurrent, but S3 does not remove a version while its retention is running. Expired delete markers are removed, and incomplete multipart uploads are aborted after 1 day. Locked test data is therefore gone about 3–4 days after the run with no bypass anywhere. |
| Cost while it exists | empty bucket: free. Test data: a few MB for a few days, well under USD 0.01 |
| Removal | once empty: `aws s3api delete-bucket --bucket vtop-eval-lock-XXXXXXXXXXXX` (root profile). Add the bucket to the LESSONS.md resource table when it is created |

## 1. Create the bucket (root profile; commands from the AWS CLI reference, not run)

```bash
export AWS_PROFILE=vtop-eval AWS_REGION=us-east-1
B=vtop-eval-lock-XXXXXXXXXXXX
aws s3api create-bucket --bucket "$B" --object-lock-enabled-for-bucket        # us-east-1: no LocationConstraint
aws s3api put-public-access-block --bucket "$B" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-tagging --bucket "$B" --tagging 'TagSet=[{Key=project,Value=vtop-eval}]'
aws s3api put-bucket-lifecycle-configuration --bucket "$B" \
  --lifecycle-configuration file://research/experiments/rq5-cloud/tamper/iam/lock-bucket-lifecycle.json
# read back
aws s3api get-object-lock-configuration --bucket "$B"   # expect ObjectLockEnabled=Enabled and no Rule
aws s3api get-bucket-versioning --bucket "$B"           # expect Status=Enabled
aws s3api get-bucket-lifecycle-configuration --bucket "$B"
```

Do **not** run `put-object-lock-configuration` with a default retention: see "Why a second bucket".

## 2. Instance role `vtop-eval-ec2` (the VM, which acts as both engine and attacker)

Use a **customer managed policy** attached to the role, not an addition to the existing inline policy. It is easy to
detach and delete after the stage, and it does not touch the inline policy that E-RQ5-COST/-WAN/-PERF depend on.

Required: [`iam/role-policy-vtop-eval-tamper-ec2.json`](iam/role-policy-vtop-eval-tamper-ec2.json)

| Statement | Actions | Resource | Why |
|---|---|---|---|
| TamperLockBucketLevel | ListBucket, ListBucketVersions, GetBucketLocation, GetBucketVersioning, GetBucketObjectLockConfiguration, ListBucketMultipartUploads | the lock bucket | Bucket checks at engine start-up (Object Lock and versioning: OBJECT_LOCK_DEPLOYMENT.md "Identities and permissions"); the stable-key probe (ListBucket, LESSONS.md); listing versions for the tamper tool, the oracle and clean-up |
| TamperLockBucketObjects | GetObject, GetObjectVersion, GetObjectAttributes, GetObjectVersionAttributes, PutObject, DeleteObject, DeleteObjectVersion, PutObjectRetention, GetObjectRetention, AbortMultipartUpload, ListMultipartUploadParts | `lock bucket/tamper/*` only | Engine writes under a lock (PutObjectRetention). It reads back the exact version and its retention (GetObjectVersion, GetObjectRetention: "without it every batch fails as `missing`"). The scrub reads the pinned version. The tamper tool overwrites, deletes, writes delete markers, deletes versions and tries to shorten retention; S3 must answer these itself, so IAM has to allow them. Clean-up deletes versions |
| TamperMainBucketConfigRead | GetBucketVersioning, GetBucketObjectLockConfiguration | the study bucket | Cases VER-REFUSE-UNVERSIONED and LOCK-REFUSE-PLAIN: the engine must *read* that the study bucket is unversioned and unlocked. If these are denied, the engine only WARNs, starts, and fails per batch, so the cases would not test the start-up refusal. Also used by the driver's preflight. Read-only |

Optional, one per case: [`iam/role-policy-vtop-eval-tamper-ec2-optional.json`](iam/role-policy-vtop-eval-tamper-ec2-optional.json)

| Statement | Action | Enables case | Trade-off |
|---|---|---|---|
| OptionalBypassGovernanceForCaseLockBypassClear | `s3:BypassGovernanceRetention` on `lock bucket/tamper/*` | LOCK-BYPASS-CLEAR: a break-glass principal clears a governance retention, and `scrub --check-retention` must report `missing` | The engine docs say never to give the *runtime* identity this permission. Here the VM is also the attacker. The engine never sends the bypass header, so its behaviour does not change. The permission is scoped to the experiment prefix of an experiment-only bucket. Without it, the case is **skipped** (MinIO result only) |
| OptionalSuspendVersioningForCaseVerSuspendLocked | `s3:PutBucketVersioning` on the lock bucket | VER-SUSPEND-LOCKED: S3 itself must refuse to suspend versioning on an Object Lock bucket (`InvalidBucketState` on MinIO) | With the permission, the role could also *enable* versioning, which is already on. Suspension is refused by S3 for a locked bucket. The driver re-enables versioning if a store ever accepted the suspension. Without the permission, the case is **skipped** |

Set `OPTIONAL_PERMS` in `user-data.sh` to whatever is granted (`""`, `bypass`, `suspend`, `bypass,suspend`). A case whose
permission is missing is skipped, not run, because on AWS an IAM denial and an Object Lock refusal both return
`AccessDenied`.

Commands (not run):

```bash
ACC=XXXXXXXXXXXX
aws iam create-policy --policy-name vtop-eval-tamper-ec2 \
  --policy-document file://research/experiments/rq5-cloud/tamper/iam/role-policy-vtop-eval-tamper-ec2.json \
  --tags Key=project,Value=vtop-eval
aws iam attach-role-policy --role-name vtop-eval-ec2 --policy-arn "arn:aws:iam::${ACC}:policy/vtop-eval-tamper-ec2"
# optional (only what the author approves; edit the file down to the approved statements first)
aws iam create-policy --policy-name vtop-eval-tamper-ec2-optional \
  --policy-document file://research/experiments/rq5-cloud/tamper/iam/role-policy-vtop-eval-tamper-ec2-optional.json \
  --tags Key=project,Value=vtop-eval
aws iam attach-role-policy --role-name vtop-eval-ec2 --policy-arn "arn:aws:iam::${ACC}:policy/vtop-eval-tamper-ec2-optional"
aws iam list-attached-role-policies --role-name vtop-eval-ec2        # read back
```

## 3. Operator user `vtop-eval-operator` (laptop: pre-flight, checks after the run, clean-up)

Use a **customer managed policy**, not inline. IAM caps the **sum of all inline policies of a user at 2,048
characters** (whitespace not counted; IAM quotas page, read 2026-11-21). The operator's inline policy already carries
the EC2, region, instance-type, PassRole and S3 statements, so another inline statement could exceed the cap. A
managed policy may hold up to 6,144 characters.

[`iam/operator-policy-vtop-eval-tamper-operator.json`](iam/operator-policy-vtop-eval-tamper-operator.json): read the lock
bucket's configuration, lifecycle, versions and retention; delete objects and **unlocked** versions (no bypass), so a
failed run's leftovers can be removed after the 1-day retention ends instead of waiting for the lifecycle rule; and read
the study bucket's versioning and Object Lock configuration. No write, no bypass, no bucket-configuration change.

```bash
aws iam create-policy --policy-name vtop-eval-tamper-operator \
  --policy-document file://research/experiments/rq5-cloud/tamper/iam/operator-policy-vtop-eval-tamper-operator.json \
  --tags Key=project,Value=vtop-eval
aws iam attach-user-policy --user-name vtop-eval-operator --policy-arn "arn:aws:iam::${ACC}:policy/vtop-eval-tamper-operator"
```

Nothing in `run-ec2.sh` changes. It stages `tamper.py` to and downloads results from the study bucket, as before.

## 4. Checks after applying (operator profile `vtop-op`)

```bash
export AWS_PROFILE=vtop-op AWS_REGION=us-east-1
aws s3api get-object-lock-configuration --bucket vtop-eval-lock-XXXXXXXXXXXX   # answers (new permission)
aws s3api get-bucket-versioning --bucket vtop-eval-XXXXXXXXXXXX               # expect no Status (unversioned)
aws s3api get-bucket-lifecycle-configuration --bucket vtop-eval-lock-XXXXXXXXXXXX
aws s3api put-object --bucket vtop-eval-lock-XXXXXXXXXXXX --key tamper/x --body /dev/null   # expect AccessDenied (operator has no write)
aws s3 ls s3://some-other-bucket                                               # still refused
```

The study bucket **must** answer with no versioning status. If it is versioned, the driver stops before any case,
because the unversioned arms would not be unversioned.

## 5. After the stage

Detach and delete `vtop-eval-tamper-ec2` (and `-optional`) from the role. Keep `vtop-eval-tamper-operator` until the
lock bucket is empty, then delete the bucket and the policy. Log each step in `LOG.md`.
