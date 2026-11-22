#!/bin/bash
# E-RQ5-TAMPER (H2.5-H2.7): cloud-init user-data for Amazon Linux 2023 arm64 (t4g.small; see README.md).
# Runs tamper.py (55 cases: signed manifests, receipts, versioning, Object Lock, audit trail) against
# s3://vtop-eval-XXXXXXXXXXXX (unversioned, "plain") and s3://vtop-eval-lock-XXXXXXXXXXXX (Object Lock + versioning,
# no default retention, lifecycle clean-up). Uploads /var/log/vtop-tamper/ to tamper/results/<run_id>/ (also after
# every case), deletes what it may delete under tamper/data/<run_id>/ in both buckets, and powers off (launched with
# terminate-on-shutdown). Credentials: instance role vtop-eval-ec2 via IMDSv2; no keys. Template: ../cost/user-data.sh.
# Needs the bucket and the IAM change in iam-and-bucket-change.md applied first (author approval).
# Local check before launch:  bash user-data.sh --check-params
set -euo pipefail

# ---- engine release (same pin as E-RQ5-COST / -WAN; decision D5)
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
TARGET=aarch64-unknown-linux-gnu
COSIGN_VERSION=""     # empty = checked by the operator (as cost/wan)
# ---- run
FAILSAFE_MIN=75       # hard power-off (expected ~25 min incl. bootstrap; README "Time and cost")
DEADLINE_MIN=60       # tamper.py starts no new case after this
SHUTDOWN_WHEN_DONE=1
BUCKET=vtop-eval-XXXXXXXXXXXX
LOCK_BUCKET=vtop-eval-lock-XXXXXXXXXXXX
STAGE=tamper
OPTIONAL_PERMS="bypass,suspend"     # both granted 2026-11-21 (D12, scoped to the lock bucket); author's choice (iam-and-bucket-change.md): "", "bypass", "suspend" or "bypass,suspend"
# Loghub SSH (Zenodo 8196385) as ../cost/user-data.sh; the case input is its first 3,145,728 bytes cut after the last
# newline (3,145,699 B), the same slice as the local run (README "Input").
SSH_MD5=a44e40b4348697dabf2bba56885e0a38 SSH_SHA256=89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3
SLICE_BYTES=3145699 SLICE_SHA256=7ac17dced4f4d122ece41893c6116b2340d7789a986323611c0e0d66b3f72572

check_params() {
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
  [[ $SLICE_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "SLICE_SHA256 is not 64 hex digits" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: v$VTOP_VERSION $VTOP_COMMIT $TARBALL_SHA256 lock bucket $LOCK_BUCKET"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1
OUT=/var/log/vtop-tamper
WORK=/opt/t            # short paths: v0.10.x object names embed the input path
mkdir -p "$OUT" "$WORK/in"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h "+$FAILSAFE_MIN" "vtop tamper failsafe" || true
T0=$(date +%s)

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$INSTANCE_ID
DEST=s3://$BUCKET/$STAGE/results/$RUN_ID
PREFIX=$STAGE/data/$RUN_ID
echo "run_id=$RUN_ID dest=$DEST data=s3://$BUCKET/$PREFIX/ and s3://$LOCK_BUCKET/$PREFIX/"

finish() {
  local rc=$?
  set +e
  aws s3 rm --recursive --only-show-errors "s3://$BUCKET/$PREFIX/"
  left=$(aws s3 ls --recursive "s3://$BUCKET/$PREFIX/" | wc -l)
  # Lock bucket: delete every version the store lets us delete (no bypass); locked versions stay until their 1-day
  # retention ends and the bucket's lifecycle rule removes them (README "Clean-up").
  lock_left=unknown
  if [ -x "$WORK/venv/bin/python" ]; then
    lock_left=$("$WORK/venv/bin/python" - "$LOCK_BUCKET" "$PREFIX/" <<'PY'
import sys, boto3
from botocore.exceptions import ClientError
s3, b, p, left = boto3.client("s3", region_name="us-east-1"), sys.argv[1], sys.argv[2], 0
try:
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=b, Prefix=p):
        for v in page.get("Versions", []) + page.get("DeleteMarkers", []):
            try:
                s3.delete_object(Bucket=b, Key=v["Key"], VersionId=v["VersionId"])
            except ClientError:
                left += 1
except ClientError as e:
    left = f"error {e.response['Error']['Code']}"
print(left)
PY
)
  fi
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"data_objects_left\": $left, \"lock_versions_left\": \"$lock_left\", \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params

# ---- 1. platform, then the engine: download, verify, `vtopctl --version` -----------------------------
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true   # never `| head` under pipefail (SIGPIPE, smoke attempt 1)
head -n 1 "$OUT/ldd-full.txt" | tee "$OUT/ldd-version.txt"
command -v aws >/dev/null || dnf install -y -q awscli-2
TARBALL=vtop-engine-${VTOP_VERSION}-${TARGET}.tar.gz
URL=https://github.com/allamiro/vtop-engine/releases/download/v${VTOP_VERSION}
mkdir -p "$WORK/e" && cd "$WORK/e"
curl -fsSL --retry 3 -O "$URL/$TARBALL" -O "$URL/$TARBALL.sigstore.json" -O "$URL/SHA256SUMS"
echo "$TARBALL_SHA256  $TARBALL" | sha256sum --check
sha256sum --check --ignore-missing SHA256SUMS
tar xzf "$TARBALL"
VTOPCTL=$(find "$WORK/e" -type f -name vtopctl -perm -u+x | sort | sed -n 1p)
[ -n "$VTOPCTL" ] || { echo "no vtopctl in $TARBALL" >&2; exit 1; }
"$VTOPCTL" --version | tee "$OUT/vtopctl-version.txt"
grep -q -- "$VTOP_VERSION" "$OUT/vtopctl-version.txt" || { echo "version mismatch" >&2; exit 1; }
COSIGN_STATUS="not checked on the VM (operator checks it, README)"

# ---- 2. Python for the driver (as smoke/cost) ---------------------------------------------------------
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 3. input: the SSH slice ----------------------------------------------------------------------------
mkdir -p "$WORK/a" && cd "$WORK/a"
aws s3 cp --only-show-errors "s3://$BUCKET/cost/input/SSH.tar.gz" SSH.tar.gz \
  || aws s3 cp --only-show-errors "s3://$BUCKET/smoke/input/SSH.tar.gz" SSH.tar.gz \
  || curl -fsSL --retry 3 -o SSH.tar.gz "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1"
echo "$SSH_MD5  SSH.tar.gz" | md5sum --check
tar xzf SSH.tar.gz -O SSH.log > SSH.log
echo "$SSH_SHA256  SSH.log" | sha256sum --check
"$WORK/venv/bin/python" -c "
import sys; d = open('SSH.log', 'rb').read(3145728); d = d[: d.rfind(b'\n') + 1]
open('$WORK/in/ssh.log', 'wb').write(d)"
echo "$SLICE_SHA256  $WORK/in/ssh.log" | sha256sum --check
[ "$(stat -c %s "$WORK/in/ssh.log")" = "$SLICE_BYTES" ]
rm -f SSH.tar.gz SSH.log

# ---- 4. code staged by the operator (run-ec2.sh): tamper.py --------------------------------------------
mkdir -p "$WORK/code"
aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/tamper.py" "$WORK/code/tamper.py"
CODE_SHA256=$(sha256sum "$WORK/code/tamper.py" | cut -d' ' -f1)
cp "$WORK/code/tamper.py" "$OUT/"

# ---- 5. the cases ---------------------------------------------------------------------------------------
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 VTOP_S3_ENDPOINT_URL
AZ=$(imds meta-data/placement/availability-zone); ITYPE=$(imds meta-data/instance-type); AMI=$(imds meta-data/ami-id)
META=$(printf '{"vtop_version":"%s","vtop_commit":"%s","tarball":"%s","tarball_sha256":"%s","cosign":"%s","code_sha256":"%s","instance_id":"%s","instance_type":"%s","ami":"%s","az":"%s","glibc":"%s","aws_request_checksum_calculation":"%s","run_id":"%s"}' \
  "$VTOP_VERSION" "$VTOP_COMMIT" "$TARBALL" "$TARBALL_SHA256" "$COSIGN_STATUS" "$CODE_SHA256" "$INSTANCE_ID" "$ITYPE" \
  "$AMI" "$AZ" "$(cat "$OUT/ldd-version.txt")" "${AWS_REQUEST_CHECKSUM_CALCULATION:-unset}" "$RUN_ID")
cd "$WORK/code"
"$WORK/venv/bin/python" tamper.py --vtopctl "$VTOPCTL" --input "$WORK/in/ssh.log" \
  --plain-bucket "$BUCKET" --lock-bucket "$LOCK_BUCKET" --prefix "$PREFIX" \
  --out "$OUT" --work "$WORK/w" --results-uri "$DEST/" \
  --deadline-epoch $(( T0 + DEADLINE_MIN * 60 )) --optional-permissions "$OPTIONAL_PERMS" --meta "$META"
