#!/bin/bash
# E-RQ5-COST (H3.5): cloud-init user-data for Amazon Linux 2023 arm64 (t4g.medium; see README.md).
# Runs the cost matrix (ssh,bgl,hdfs x size-only,sha256-store,blake3-readback x 10) with cost.py against
# s3://vtop-eval-XXXXXXXXXXXX, uploads /var/log/vtop-cost/ to cost/results/<run_id>/ (also after every run),
# deletes cost/data/<run_id>/ unless a run failed, and powers off (launched with terminate-on-shutdown).
# Credentials: instance role vtop-eval-ec2 via IMDSv2; no keys. Template: ../smoke/user-data.sh.
# Local check before launch:  bash user-data.sh --check-params
set -euo pipefail

# ---- engine release: pinned to v0.10.4 (D16) (README "Pin the release"); the VM refuses to run until then
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
TARGET=aarch64-unknown-linux-gnu
COSIGN_VERSION=""     # e.g. v3.x.y to verify the Sigstore bundle on the VM; empty = checked by the operator
# ---- matrix and limits
DATASETS=ssh,bgl,hdfs
MODES=size-only,sha256-store,blake3-readback
REPS=10
SEED=20261121
FAILSAFE_MIN=180      # hard power-off (expected run ~1 h; README "Time and cost")
DEADLINE_MIN=160      # cost.py starts no new run after this; leaves time to upload
SHUTDOWN_WHEN_DONE=1
BUCKET=vtop-eval-XXXXXXXXXXXX
STAGE=cost
# Loghub (Zenodo record 8196385): archive MD5 as research/datasets/loghub/fetch.sh; log SHA-256 and bytes as
# research/datasets/loghub/inventory.csv
SSH_MD5=a44e40b4348697dabf2bba56885e0a38 SSH_SHA256=89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3 SSH_BYTES=73417506
BGL_MD5=4452953c470f2d95fcb32d5f6e733f7a BGL_SHA256=666130b15ef44eb32fd02bd053e6c6e007c37696b5e7e8b9d8e45b729876a5d2 BGL_BYTES=743185031
HDFS_MD5=76a24b4d9a6164d543fb275f89773260 HDFS_SHA256=0783096174d7832c618337f9609e06e04abd86ddd7089b3c12b407e63bfebc52 HDFS_BYTES=1577982906

check_params() {
  : "${VTOP_COMMIT:?fill in at tag: VTOP_COMMIT (40-hex commit of v$VTOP_VERSION)}"
  : "${TARBALL_SHA256:?fill in at tag: TARBALL_SHA256 (SHA256SUMS entry)}"
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: v$VTOP_VERSION $VTOP_COMMIT $TARBALL_SHA256"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1
OUT=/var/log/vtop-cost
WORK=/opt/c            # short paths: v0.10.x object names embed the input path (LESSONS.md)
mkdir -p "$OUT" "$WORK"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h "+$FAILSAFE_MIN" "vtop cost failsafe" || true
T0=$(date +%s)

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$INSTANCE_ID
DEST=s3://$BUCKET/$STAGE/results/$RUN_ID
DATA=s3://$BUCKET/$STAGE/data/$RUN_ID/
echo "run_id=$RUN_ID dest=$DEST data=$DATA"

finish() {
  local rc=$?
  set +e
  if [ -e "$OUT/KEEP_DATA" ]; then
    echo "a run failed: keeping $DATA for diagnosis (delete it after; README step 7)"
  else
    aws s3 rm --recursive --only-show-errors "$DATA"
  fi
  left=$(aws s3 ls --recursive "$DATA" | wc -l)
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"data_objects_left\": $left, \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  df -B1 / /opt > "$OUT/df-end.txt" 2>&1
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params           # after the trap: an unfilled pin powers the VM off with exit.json uploaded

# ---- 1. platform, then the engine: download, verify, `vtopctl --version` -----------------------------
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true   # never `| head` under pipefail (SIGPIPE, smoke attempt 1)
head -n 1 "$OUT/ldd-full.txt" | tee "$OUT/ldd-version.txt"
for t in tar unzip ethtool; do command -v $t >/dev/null || dnf install -y -q $t || true; done
command -v aws >/dev/null || dnf install -y -q awscli-2
TARBALL=vtop-engine-${VTOP_VERSION}-${TARGET}.tar.gz
URL=https://github.com/allamiro/vtop-engine/releases/download/v${VTOP_VERSION}
mkdir -p "$WORK/e" && cd "$WORK/e"
curl -fsSL --retry 3 -O "$URL/$TARBALL" -O "$URL/$TARBALL.sigstore.json" -O "$URL/SHA256SUMS"
echo "$TARBALL_SHA256  $TARBALL" | sha256sum --check          # pinned digest
sha256sum --check --ignore-missing SHA256SUMS                 # the release's own list
tar xzf "$TARBALL"
VTOPCTL=$(find "$WORK/e" -type f -name vtopctl -perm -u+x | sort | sed -n 1p)
[ -n "$VTOPCTL" ] || { echo "no vtopctl in $TARBALL" >&2; exit 1; }
"$VTOPCTL" --version | tee "$OUT/vtopctl-version.txt"
grep -q -- "$VTOP_VERSION" "$OUT/vtopctl-version.txt" || { echo "version mismatch" >&2; exit 1; }
COSIGN_STATUS="not checked on the VM (operator checks it, README)"
if [ -n "$COSIGN_VERSION" ]; then
  if curl -fsSL --retry 3 -o cosign "https://github.com/sigstore/cosign/releases/download/$COSIGN_VERSION/cosign-linux-arm64" \
     && chmod +x cosign && ./cosign verify-blob "$TARBALL" --bundle "$TARBALL.sigstore.json" \
       --certificate-identity "https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/v$VTOP_VERSION" \
       --certificate-oidc-issuer https://token.actions.githubusercontent.com; then
    COSIGN_STATUS="verified with cosign $COSIGN_VERSION"
  else
    COSIGN_STATUS="cosign $COSIGN_VERSION verification failed or unavailable"
  fi
fi

# ---- 2. Python for the driver and the oracle (as smoke) -----------------------------------------------
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 3. inputs: staged copy in S3 if present, else Zenodo; verify; one newline-terminated file each --
avail=$(df --output=avail -B1 "$WORK" | tail -n 1)
[ "$avail" -gt 3500000000 ] || { echo "only $avail bytes free on $WORK; need 3.5 GB" >&2; exit 1; }
fetch() {  # archive md5
  aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/input/$1" "$1" \
    || aws s3 cp --only-show-errors "s3://$BUCKET/smoke/input/$1" "$1" \
    || curl -fsSL --retry 3 -o "$1" "https://zenodo.org/records/8196385/files/$1?download=1"
  echo "$2  $1" | md5sum --check
}
mkdir -p "$WORK/a" && cd "$WORK/a"
for ds in ${DATASETS//,/ }; do
  mkdir -p "$WORK/in/$ds"
  f=$WORK/in/$ds/$ds.log
  case $ds in
    ssh)  fetch SSH.tar.gz "$SSH_MD5"; tar xzf SSH.tar.gz -O SSH.log > "$f"; rm -f SSH.tar.gz; sha=$SSH_SHA256; n=$SSH_BYTES ;;
    bgl)  fetch BGL.zip "$BGL_MD5"; unzip -p BGL.zip BGL.log > "$f"; rm -f BGL.zip; sha=$BGL_SHA256; n=$BGL_BYTES ;;
    hdfs) fetch HDFS_v1.zip "$HDFS_MD5"; unzip -p HDFS_v1.zip HDFS.log > "$f"; rm -f HDFS_v1.zip; sha=$HDFS_SHA256; n=$HDFS_BYTES ;;
    *) echo "unknown dataset $ds" >&2; exit 1 ;;
  esac
  echo "$sha  $f" | sha256sum --check
  [ "$(stat -c %s "$f")" = "$n" ]
  # VTOP never commits a partial trailing line; SSH.log ends without one (as lib/vtoplab.py Run.__init__)
  if [ "$(tail -c 1 "$f" | od -An -tx1 | tr -d ' \n')" != 0a ]; then printf '\n' >> "$f"; echo "$ds: newline appended"; fi
done
df -B1 / /opt > "$OUT/df-start.txt"; free -b > "$OUT/free-start.txt"

# ---- 4. code staged by the operator (README launch): cost.py and smoke's oracle.py ------------------
mkdir -p "$WORK/code"
for f in cost.py oracle.py; do aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/$f" "$WORK/code/$f"; done
CODE_SHA256=$(cd "$WORK/code" && sha256sum cost.py oracle.py | tr '\n' ';')
cp "$WORK/code/"*.py "$OUT/"

# ---- 5. the matrix --------------------------------------------------------------------------------------
# No endpoint or checksum overrides: AWS_REQUEST_CHECKSUM_CALCULATION stays unset; run.json records it.
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 VTOP_S3_ENDPOINT_URL COST_LAB_ENDPOINT
cd "$WORK/code"
"$WORK/venv/bin/python" cost.py --run-id "$RUN_ID" --stage "$STAGE" --vtopctl "$VTOPCTL" \
  --input-dir "$WORK/in" --bucket "$BUCKET" --out "$OUT" --work "$WORK/w" \
  --datasets "$DATASETS" --modes "$MODES" --reps "$REPS" --seed "$SEED" \
  --deadline-epoch $(( T0 + DEADLINE_MIN * 60 )) --results-uri "$DEST/" \
  --vtop-version "$VTOP_VERSION" --vtop-commit "$VTOP_COMMIT" --tarball-sha256 "$TARBALL_SHA256" \
  --cosign-status "$COSIGN_STATUS" --code-sha256 "$CODE_SHA256" --delete-data
