#!/bin/bash
# RQ5 cloud smoke test: cloud-init user-data for Amazon Linux 2023 arm64 (t4g.*).
# Runs vtop-engine v0.10.1 against s3://vtop-eval-XXXXXXXXXXXX in four modes, writes results
# to /var/log/vtop-smoke/, uploads them to s3://.../smoke/results/<run_id>/, then powers off
# (launch with --instance-initiated-shutdown-behavior terminate, so power-off terminates).
# Credentials: the instance role vtop-eval-ec2 via IMDSv2. No keys. See README.md.
set -euo pipefail

BUCKET=vtop-eval-XXXXXXXXXXXX
VTOP_VERSION=0.10.1
VTOP_COMMIT=44f66194fdc96090e6f5222693d0fe5fe39200a1      # tag v0.10.1
TARBALL=vtop-engine-${VTOP_VERSION}-aarch64-unknown-linux-gnu.tar.gz
TARBALL_SHA256=173aa74cd79af1d20dc0a6d6b831eab800e83e87926942b1e9be3ff9dd0f82f0  # = SHA256SUMS entry
RELEASE_URL=https://github.com/allamiro/vtop-engine/releases/download/v${VTOP_VERSION}
COSIGN_VERSION=""            # e.g. v3.x.y to try the Sigstore bundle on the VM; empty = not checked here
MODES=size-only,sha256-store,blake3-readback,sha256-multipart
SHUTDOWN_WHEN_DONE=1
# Loghub SSH (Zenodo record 8196385), as in research/datasets/loghub/fetch.sh and inventory.csv
SSH_MD5=a44e40b4348697dabf2bba56885e0a38
SSH_SHA256=89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3
SSH_BYTES=73417506

export AWS_DEFAULT_REGION=us-east-1   # for the aws CLI; the engine takes upload.region from its config
OUT=/var/log/vtop-smoke
WORK=/opt/vtop-smoke
mkdir -p "$OUT" "$WORK"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h +120 "vtop smoke failsafe" || true   # hard stop even if something hangs

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$INSTANCE_ID
DEST=s3://$BUCKET/smoke/results/$RUN_ID
echo "run_id=$RUN_ID dest=$DEST"

finish() {
  local rc=$?
  set +e
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"finished_at\": \"$(date -u +%FT%TZ)\"}" > "$OUT/exit.json"
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT

# --- 1. platform and engine first: glibc, download, verify, `vtopctl --version` -----------
# Write the full output to a file first: `ldd --version | head -1` dies of SIGPIPE (exit 141) under
# pipefail, which stopped the first launch (2026-11-20, i-0e8cada773da9188c).
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true
head -n 1 "$OUT/ldd-full.txt" | tee "$OUT/ldd-version.txt"
command -v tar >/dev/null || dnf install -y -q tar
command -v aws >/dev/null || dnf install -y -q awscli-2
cd "$WORK"
curl -fsSL --retry 3 -O "$RELEASE_URL/$TARBALL" -O "$RELEASE_URL/$TARBALL.sigstore.json" -O "$RELEASE_URL/SHA256SUMS"
echo "$TARBALL_SHA256  $TARBALL" | sha256sum --check          # pinned digest
sha256sum --check --ignore-missing SHA256SUMS                 # the release's own list (./ entries)
tar xzf "$TARBALL"
VTOPCTL=$WORK/vtop-engine-${VTOP_VERSION}-aarch64-unknown-linux-gnu/vtopctl
"$VTOPCTL" --version | tee "$OUT/vtopctl-version.txt"         # fails here if the binary cannot load

COSIGN_STATUS="not checked on the VM (check on the operator machine, README step 2)"
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

# --- 2. Python for the driver and the oracle -----------------------------------------------
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# --- 3. input: Loghub SSH.log (pre-staged copy in S3 if present, else Zenodo) ------------
mkdir -p "$WORK/data" && cd "$WORK/data"
aws s3 cp --only-show-errors "s3://$BUCKET/smoke/input/SSH.tar.gz" SSH.tar.gz \
  || curl -fsSL --retry 3 -o SSH.tar.gz "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1"
echo "$SSH_MD5  SSH.tar.gz" | md5sum --check
tar xzf SSH.tar.gz SSH.log
echo "$SSH_SHA256  SSH.log" | sha256sum --check
[ "$(stat -c %s SSH.log)" = "$SSH_BYTES" ]
# VTOP never commits a partial trailing line; SSH.log ends without one. Terminate the working
# copy as lib/vtoplab.py Run.__init__ does (the dataset file itself is unchanged).
cp SSH.log ssh-input.log
if [ "$(tail -c 1 ssh-input.log | od -An -tx1 | tr -d ' \n')" != 0a ]; then printf '\n' >> ssh-input.log; fi
INPUT_SHA256=$(sha256sum ssh-input.log | cut -d' ' -f1)

# --- 4. smoke code staged by the operator (README step 3) ----------------------------------
mkdir -p "$WORK/code"
aws s3 cp --only-show-errors "s3://$BUCKET/smoke/code/smoke.py" "$WORK/code/smoke.py"
aws s3 cp --only-show-errors "s3://$BUCKET/smoke/code/oracle.py" "$WORK/code/oracle.py"
CODE_SHA256=$(cd "$WORK/code" && sha256sum smoke.py oracle.py | tr '\n' ';')
cp "$WORK/code/"*.py "$OUT/"

# --- 5. run the modes -----------------------------------------------------------------------
# No endpoint or checksum overrides: AWS_REQUEST_CHECKSUM_CALCULATION stays unset (SDK default
# when_supported); smoke.py records the environment it saw in run.json.
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 VTOP_S3_ENDPOINT_URL SMOKE_LAB_ENDPOINT
cd "$WORK/code"
"$WORK/venv/bin/python" smoke.py --run-id "$RUN_ID" --vtopctl "$VTOPCTL" \
  --input "$WORK/data/ssh-input.log" --input-sha256 "$INPUT_SHA256" --bucket "$BUCKET" \
  --out "$OUT" --work "$WORK/runs" --modes "$MODES" \
  --tarball-sha256 "$TARBALL_SHA256 (commit $VTOP_COMMIT)" --cosign-status "$COSIGN_STATUS" \
  --code-sha256 "$CODE_SHA256"
