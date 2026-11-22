#!/bin/bash
# E-RQ5-WAN: cloud-init user-data for Amazon Linux 2023 arm64 (t4g.medium) in a FAR region (eu-west-1,
# ap-southeast-2, sa-east-1 or ap-northeast-1; decision D8) writing to s3://vtop-eval-XXXXXXXXXXXX in us-east-1.
# Runs wan.py (native link x 3 modes, tc and VTOP egress caps, loss/jitter, partition, SIGKILL, policed uplink),
# uploads /var/log/vtop-wan/ to wan/results/<run_id>/ (also after every run), removes every tc rule, aborts open
# multipart uploads and deletes wan/data/<run_id>/ unless a run failed, and powers off (terminate-on-shutdown).
# Credentials: instance role vtop-eval-ec2 via IMDSv2 (never shaped or cut). Template: ../cost/user-data.sh.
# Local check before launch:  bash user-data.sh --check-params
set -euo pipefail

# ---- engine release: pinned to v0.10.4 (D16) (same values as cost/user-data.sh); refuses to run until then
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
TARGET=aarch64-unknown-linux-gnu
COSIGN_VERSION=""     # e.g. v3.x.y to verify the Sigstore bundle on the VM; empty = checked by the operator
# ---- matrix and limits (wan.py DEFAULT_PLAN unless PLAN is set; README "Design")
PLAN=""
SEED=20261122
FAILSAFE_MIN=150      # hard power-off (expected ~75 min; README "Time and cost")
DEADLINE_MIN=125      # wan.py starts no new run after this; the longest run (policed) is <= 45 min
SHUTDOWN_WHEN_DONE=1
BUCKET=vtop-eval-XXXXXXXXXXXX
STAGE=wan
SSH_MD5=a44e40b4348697dabf2bba56885e0a38 SSH_SHA256=89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3 SSH_BYTES=73417506

check_params() {
  : "${VTOP_COMMIT:?fill in at tag: VTOP_COMMIT (40-hex commit of v$VTOP_VERSION)}"
  : "${TARBALL_SHA256:?fill in at tag: TARBALL_SHA256 (SHA256SUMS entry)}"
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: v$VTOP_VERSION $VTOP_COMMIT $TARBALL_SHA256"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1   # the bucket's region; the VM's own region comes from IMDS
OUT=/var/log/vtop-wan
WORK=/opt/w            # short paths: v0.10.x object names embed the input path (LESSONS.md)
mkdir -p "$OUT" "$WORK"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h "+$FAILSAFE_MIN" "vtop wan failsafe" || true
T0=$(date +%s)

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
REGION=$(imds meta-data/placement/region)
MAC=$(imds meta-data/mac)
VPC_CIDR=$(imds "meta-data/network/interfaces/macs/$MAC/vpc-ipv4-cidr-block" || true)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$REGION-$INSTANCE_ID
DEST=s3://$BUCKET/$STAGE/results/$RUN_ID
DATA=s3://$BUCKET/$STAGE/data/$RUN_ID/
echo "run_id=$RUN_ID region=$REGION vpc=$VPC_CIDR dest=$DEST data=$DATA"

net_reset() {  # never upload results through a shaped or cut link
  local dev
  read -r _ _ _ _ dev _ < <(ip -o route show default) || true
  if [ -n "${dev:-}" ]; then
    tc qdisc del dev "$dev" root 2>/dev/null
    tc qdisc del dev "$dev" clsact 2>/dev/null
  fi
  ip link del ifb0 2>/dev/null
  return 0
}
finish() {
  local rc=$?
  set +e
  net_reset
  aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$STAGE/data/$RUN_ID/" \
    --query 'Uploads[].[Key,UploadId]' --output text > "$OUT/mpu-left.txt" 2>&1
  while read -r k u; do
    [ -n "$k" ] && [ "$k" != None ] && aws s3api abort-multipart-upload --bucket "$BUCKET" --key "$k" --upload-id "$u"
  done < "$OUT/mpu-left.txt"
  if [ -e "$OUT/KEEP_DATA" ]; then
    echo "a run failed: keeping $DATA for diagnosis (delete it after; README step 7)"
  else
    aws s3 rm --recursive --only-show-errors "$DATA"
  fi
  left=$(aws s3 ls --recursive "$DATA" | wc -l)
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"region\": \"$REGION\", \"data_objects_left\": $left, \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params           # after the trap: an unfilled pin powers the VM off with exit.json uploaded

# ---- 1. platform, network tools, then the engine: download, verify, `vtopctl --version` ----------------
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true   # never `| head` under pipefail (SIGPIPE, smoke attempt 1)
head -n 1 "$OUT/ldd-full.txt" | tee "$OUT/ldd-version.txt"
for p in tar ethtool iproute-tc; do rpm -q "$p" >/dev/null || dnf install -y -q "$p" || true; done
command -v aws >/dev/null || dnf install -y -q awscli-2
# AL2023 ships sch_netem (and possibly ifb) in kernel-modules-extra; wan.py's preflight records what works
dnf install -y -q "kernel-modules-extra-$(uname -r)" || dnf install -y -q kernel-modules-extra || true
{ uname -r; rpm -q kernel-modules-extra iproute-tc ethtool; } > "$OUT/netmods.txt" 2>&1 || true
for m in sch_netem sch_tbf sch_ingress ifb act_mirred act_police act_gact cls_u32; do
  if modprobe "$m" 2>>"$OUT/netmods.txt"; then echo "$m ok"; else echo "$m FAILED"; fi >> "$OUT/netmods.txt"
done
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

# ---- 2. Python for the driver and the oracle (as smoke and cost) ---------------------------------------
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 3. input: SSH (staged copy in us-east-1, else Zenodo), verified, newline-terminated --------------
fetch() {  # archive md5
  aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/input/$1" "$1" \
    || aws s3 cp --only-show-errors "s3://$BUCKET/smoke/input/$1" "$1" \
    || curl -fsSL --retry 3 -o "$1" "https://zenodo.org/records/8196385/files/$1?download=1"
  echo "$2  $1" | md5sum --check
}
mkdir -p "$WORK/a" "$WORK/in/ssh" && cd "$WORK/a"
f=$WORK/in/ssh/ssh.log
fetch SSH.tar.gz "$SSH_MD5"; tar xzf SSH.tar.gz -O SSH.log > "$f"; rm -f SSH.tar.gz
echo "$SSH_SHA256  $f" | sha256sum --check
[ "$(stat -c %s "$f")" = "$SSH_BYTES" ]
printf '\n' >> "$f"   # SSH.log ends without a newline; VTOP never commits a partial trailing line
df -B1 / /opt > "$OUT/df-start.txt"; free -b > "$OUT/free-start.txt"

# ---- 4. code staged by the operator (README launch): wan.py, cost.py, smoke's oracle.py ---------------
mkdir -p "$WORK/code"
for f in wan.py cost.py oracle.py; do aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/$f" "$WORK/code/$f"; done
CODE_SHA256=$(cd "$WORK/code" && sha256sum wan.py cost.py oracle.py | tr '\n' ';')
cp "$WORK/code/"*.py "$OUT/"

# ---- 5. the matrix -------------------------------------------------------------------------------------
# No endpoint or checksum overrides: AWS_REQUEST_CHECKSUM_CALCULATION stays unset; run.json records it.
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 VTOP_S3_ENDPOINT_URL WAN_LAB_ENDPOINT COST_LAB_ENDPOINT
cd "$WORK/code"
"$WORK/venv/bin/python" wan.py --run-id "$RUN_ID" --stage "$STAGE" --vtopctl "$VTOPCTL" \
  --input-dir "$WORK/in" --bucket "$BUCKET" --out "$OUT" --work "$WORK/x" ${PLAN:+--plan "$PLAN"} \
  --seed "$SEED" --deadline-epoch $(( T0 + DEADLINE_MIN * 60 )) --hard-stop-epoch $(( T0 + FAILSAFE_MIN * 60 )) \
  --results-uri "$DEST/" --exempt "$VPC_CIDR" \
  --vtop-version "$VTOP_VERSION" --vtop-commit "$VTOP_COMMIT" --tarball-sha256 "$TARBALL_SHA256" \
  --cosign-status "$COSIGN_STATUS" --code-sha256 "$CODE_SHA256" --delete-data
