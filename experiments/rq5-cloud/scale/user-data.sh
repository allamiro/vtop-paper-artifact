#!/bin/bash
# E-RQ5-SCALE (proposed H3.7): cloud-init user-data for Amazon Linux 2023 arm64, one copy per host of the fleet
# (launch.sh starts all hosts with ONE RunInstances call; a host's slot is its ami-launch-index).
# Each host: verifies the pinned VTOP release, builds the Python env, fetches and checks HDFS, then runs
# `scale.py worker`, which meets the other hosts at an S3 barrier before every step and runs this host's engines.
# Results go to s3://<bucket>/scale/results/<session>/h<slot>-<instance-id>/ (also after every step). The EXIT
# trap deletes this host's leftover data unless a run failed, writes an abort marker if this host failed (so
# the other hosts stop at the next barrier instead of waiting), uploads and powers off (terminate-on-shutdown).
# Credentials: instance role vtop-eval-ec2 via IMDSv2; no keys. Template: ../cost/user-data.sh.
# Local check:  bash user-data.sh --check-params   (refuses until launch.sh has filled SESSION)
set -euo pipefail

# ---- filled by launch.sh
SESSION=""
FAILSAFE_MIN=130      # hard power-off; launch.sh sets it from the schedule (expected run ~1 h)
# ---- engine release (same pin as cost/ and wan/; checked 2026-11-21, LOG.md)
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
TARGET=aarch64-unknown-linux-gnu
BUCKET=vtop-eval-XXXXXXXXXXXX
STAGE=scale
SHUTDOWN_WHEN_DONE=1
# HDFS (Loghub, Zenodo record 8196385), as cost/user-data.sh
HDFS_MD5=76a24b4d9a6164d543fb275f89773260
HDFS_SHA256=0783096174d7832c618337f9609e06e04abd86ddd7089b3c12b407e63bfebc52
HDFS_BYTES=1577982906

check_params() {
  [[ $SESSION =~ ^[0-9]{8}T[0-9]{6}Z-scale$ ]] || { echo "SESSION not filled (launch.sh fills it)" >&2; exit 2; }
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
  [[ $FAILSAFE_MIN =~ ^[0-9]+$ ]] && [ "$FAILSAFE_MIN" -le 300 ] || { echo "FAILSAFE_MIN must be <= 300" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: session $SESSION, v$VTOP_VERSION $VTOP_COMMIT, failsafe ${FAILSAFE_MIN} min"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1
OUT=/var/log/vtop-scale
WORK=/opt/s            # short paths: object names embed the input path (LESSONS.md)
mkdir -p "$OUT" "$WORK"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h "+$FAILSAFE_MIN" "vtop scale failsafe" || true
T0=$(date +%s)

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
SLOT=$(imds meta-data/ami-launch-index)
DEST=s3://$BUCKET/$STAGE/results/$SESSION/h$SLOT-$INSTANCE_ID
DATA=s3://$BUCKET/$STAGE/data/$SESSION/
COORD=s3://$BUCKET/$STAGE/coord/$SESSION
echo "session=$SESSION slot=$SLOT instance=$INSTANCE_ID dest=$DEST"

finish() {
  local rc=$?
  set +e
  if [ "$rc" != 0 ] && [ "$rc" != 3 ]; then   # 3 = stopped at the shared deadline (not a failure)
    printf '{"slot": %s, "why": "host exit code %s", "at": "%s"}\n' "$SLOT" "$rc" "$(date -u +%FT%TZ)" \
      | aws s3 cp - "$COORD/abort/h$SLOT-exit.json"
  fi
  if [ -e "$OUT/KEEP_DATA" ]; then
    echo "a run failed: keeping this host's data under $DATA for diagnosis (README clean-up)"
  else
    aws s3 rm --recursive --only-show-errors "$DATA" --exclude '*' --include "*/h${SLOT}e*"
  fi
  left=$(aws s3 ls --recursive "$DATA" | grep -c "/h${SLOT}e")
  echo "{\"exit_code\": $rc, \"session\": \"$SESSION\", \"slot\": $SLOT, \"instance_id\": \"$INSTANCE_ID\", \"data_objects_left\": ${left:-null}, \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  chronyc tracking > "$OUT/chrony-end.txt" 2>&1
  df -B1 / > "$OUT/df-end.txt" 2>&1
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params

# ---- 1. platform, then the engine: download, verify, `vtopctl --version` --------------------------------
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true   # never `| head` under pipefail (smoke attempt 1)
head -n 1 "$OUT/ldd-full.txt" | tee "$OUT/ldd-version.txt"
for t in tar unzip ethtool chrony; do rpm -q "$t" >/dev/null 2>&1 || dnf install -y -q "$t" || true; done
command -v aws >/dev/null || dnf install -y -q awscli-2
chronyc tracking > "$OUT/chrony-start.txt" 2>&1 || true   # Amazon Time Sync: start times across hosts
TARBALL=vtop-engine-${VTOP_VERSION}-${TARGET}.tar.gz
URL=https://github.com/allamiro/vtop-engine/releases/download/v${VTOP_VERSION}
mkdir -p "$WORK/e" && cd "$WORK/e"
curl -fsSL --retry 3 -O "$URL/$TARBALL" -O "$URL/SHA256SUMS"
echo "$TARBALL_SHA256  $TARBALL" | sha256sum --check          # pinned digest
sha256sum --check --ignore-missing SHA256SUMS                 # the release's own list
tar xzf "$TARBALL"
VTOPCTL=$(find "$WORK/e" -type f -name vtopctl -perm -u+x | sort | sed -n 1p)
[ -n "$VTOPCTL" ] || { echo "no vtopctl in $TARBALL" >&2; exit 1; }
"$VTOPCTL" --version | tee "$OUT/vtopctl-version.txt"
grep -q -- "$VTOP_VERSION" "$OUT/vtopctl-version.txt" || { echo "version mismatch" >&2; exit 1; }

# ---- 2. Python for the worker and the oracle (as cost) ---------------------------------------------------
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 3. input: HDFS from the staged copy (cost/input), verified; one newline-terminated file -------------
avail=$(df --output=avail -B1 "$WORK" | tail -n 1)
[ "$avail" -gt 5000000000 ] || { echo "only $avail bytes free on $WORK; need 5 GB" >&2; exit 1; }
mkdir -p "$WORK/a" "$WORK/in/0" && cd "$WORK/a"
aws s3 cp --only-show-errors "s3://$BUCKET/cost/input/HDFS_v1.zip" HDFS_v1.zip \
  || curl -fsSL --retry 3 -o HDFS_v1.zip "https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1"
echo "$HDFS_MD5  HDFS_v1.zip" | md5sum --check
F=$WORK/in/0/hdfs.log
unzip -p HDFS_v1.zip HDFS.log > "$F"; rm -f HDFS_v1.zip
echo "$HDFS_SHA256  $F" | sha256sum --check
[ "$(stat -c %s "$F")" = "$HDFS_BYTES" ]
if [ "$(tail -c 1 "$F" | od -An -tx1 | tr -d ' \n')" != 0a ]; then printf '\n' >> "$F"; echo "hdfs: newline appended"; fi
df -B1 / > "$OUT/df-start.txt"; free -b > "$OUT/free-start.txt"

# ---- 4. code and schedule staged by launch.sh for this session ------------------------------------------
mkdir -p "$WORK/code"
for f in scale.py cost.py oracle.py schedule.json; do
  aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/$SESSION/$f" "$WORK/code/$f"
done
CODE_SHA256=$(cd "$WORK/code" && sha256sum scale.py cost.py oracle.py schedule.json | tr '\n' ';')
cp "$WORK/code/"*.py "$OUT/"

# ---- 5. the schedule -------------------------------------------------------------------------------------
# No endpoint or checksum overrides: AWS_REQUEST_CHECKSUM_CALCULATION stays unset; host.json records it.
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 VTOP_S3_ENDPOINT_URL COST_LAB_ENDPOINT
cd "$WORK/code"
"$WORK/venv/bin/python" scale.py worker --schedule "$WORK/code/schedule.json" --slot "$SLOT" \
  --vtopctl "$VTOPCTL" --in-root "$WORK/in" --bucket "$BUCKET" --out "$OUT" --work "$WORK/w" \
  --stage "$STAGE" --results-uri "$DEST/" --vtop-version "$VTOP_VERSION" --vtop-commit "$VTOP_COMMIT" \
  --tarball-sha256 "$TARBALL_SHA256" --cosign-status "not checked on the VM (operator checked it, LOG.md 2026-11-21)" \
  --code-sha256 "$CODE_SHA256"
