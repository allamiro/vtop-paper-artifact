#!/bin/bash
# E-RQ5-PERF (H4.2): cloud-init user-data for the SINK host (Amazon Linux 2023 arm64, t4g.large, same subnet as the
# broker host). Installs and verifies every system under test: vtop-engine (pinned release, SHA-256 + SHA256SUMS,
# `vtopctl --version`), Confluent cp-kafka-connect 7.9.0 + kafka-connect-s3 12.1.15 (zip SHA-256), Vector 0.61.0 and
# Fluent Bit 5.2.0 (images by their RQ4 digests); announces its private IP in s3://.../perf/coord/<session>/sink.json
# and runs `perf.py agent`, which the broker host drives (one system at a time, host network). The agent exits on
# the broker's /shutdown or after 30 min without contact; the EXIT trap writes exit.json, uploads /var/log/vtop-perf-sink/
# to perf/results/<session>-sink-<id>/ and powers off (terminate-on-shutdown). Credentials: instance role via IMDSv2.
# launch.sh fills SESSION; the script refuses to run with an unfilled session or engine pin.
# Local check:  bash user-data-sink.sh --check-params
set -euo pipefail

SESSION=""            # filled by launch.sh
# ---- engine release (same values as cost/user-data.sh; decision D5); refuses to run while unfilled
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
TARGET=aarch64-unknown-linux-gnu
BUCKET=vtop-eval-XXXXXXXXXXXX
STAGE=perf
FAILSAFE_MIN=395      # 5 min after the broker's, so the broker can still stop the agent cleanly
SHUTDOWN_WHEN_DONE=1
S3SINK_ZIP=s3sink-12.1.15.zip
S3SINK_ZIP_SHA256=7ba0ea1fbf32251b99a39f47bfcc35a0a49abdeb0a427fbf4866e14df0a32dc7
IMAGES="confluentinc/cp-kafka-connect@sha256:535b1751f64af95bee4bf15ad2ab6b1ca2b369131711801c93e3ceac836dd2a1
timberio/vector@sha256:f6c23b5a7e16fb2838d33619ef7acf4bad2289a7cf22bc90efc2371f520c4ea2
fluent/fluent-bit@sha256:a48190bf67679495a332fd61025421a46e7e4d168380185a8e16da2e9acc3126"

check_params() {
  [[ $SESSION =~ ^[0-9]{8}T[0-9]{6}Z-(use1|apse2)$ ]] || { echo "SESSION is not filled in (launch.sh does it)" >&2; exit 2; }
  : "${VTOP_COMMIT:?fill in at tag: VTOP_COMMIT (40-hex commit of v$VTOP_VERSION)}"
  : "${TARBALL_SHA256:?fill in at tag: TARBALL_SHA256 (SHA256SUMS entry)}"
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: session $SESSION, v$VTOP_VERSION $VTOP_COMMIT $TARBALL_SHA256"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1   # the bucket's region
OUT=/var/log/vtop-perf-sink
WORK=/opt/p            # short paths (LESSONS.md)
mkdir -p "$OUT" "$WORK"
exec > >(tee -a "$OUT/bootstrap.log") 2>&1
shutdown -h "+$FAILSAFE_MIN" "vtop perf failsafe" || true
T0=$(date +%s)

imds() {
  local tok
  tok=$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
  curl -fsS -H "X-aws-ec2-metadata-token: $tok" "http://169.254.169.254/latest/$1"
}
INSTANCE_ID=$(imds meta-data/instance-id)
REGION=$(imds meta-data/placement/region)
AZ=$(imds meta-data/placement/availability-zone)
IP=$(imds meta-data/local-ipv4)
RUN_ID=$SESSION-sink-$INSTANCE_ID
DEST=s3://$BUCKET/$STAGE/results/$RUN_ID
COORD=s3://$BUCKET/$STAGE/coord/$SESSION
echo "run_id=$RUN_ID region=$REGION az=$AZ ip=$IP dest=$DEST"

finish() {
  local rc=$?
  set +e
  for c in rq5perf-confluent rq5perf-vector rq5perf-fluentbit; do docker rm -f "$c" >/dev/null 2>&1; done
  nft list table inet rq5perf > "$OUT/nft-end.txt" 2>&1
  chronyc tracking > "$OUT/chrony-end.txt" 2>&1
  df -B1 / > "$OUT/df-end.txt" 2>&1
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"region\": \"$REGION\", \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params

# ---- 1. platform -----------------------------------------------------------------------------------------
systemctl stop amazon-ssm-agent 2>/dev/null || true    # its traffic would count as S3-bound bytes (nft)
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true
chronyc tracking > "$OUT/chrony-start.txt" 2>&1 || true
dnf install -y -q docker ethtool nftables unzip tar
command -v aws >/dev/null || dnf install -y -q awscli-2
systemctl start docker
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11   # wan.py RTT probe
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 2. vtop-engine: download, verify, `vtopctl --version` (as cost/user-data.sh) -------------------------
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
echo "{\"version\": \"$VTOP_VERSION\", \"commit\": \"$VTOP_COMMIT\", \"tarball_sha256\": \"$TARBALL_SHA256\", \"target\": \"$TARGET\", \"cosign\": \"checked by the operator\"}" > "$OUT/vtop.json"

# ---- 3. baseline images (RQ4 digests) and the Confluent S3 sink plugin ------------------------------------
avail=$(df --output=avail -B1 / | tail -n 1)
[ "$avail" -gt 12000000000 ] || { echo "only $avail bytes free on /; need 12 GB (launch.sh ROOT_GB)" >&2; exit 1; }
while read -r img; do
  docker pull -q "$img"
  docker image inspect "$img" --format "$img {{.Id}} {{.Architecture}}" >> "$OUT/images.txt"
done <<< "$IMAGES"
mkdir -p "$WORK/plugins" && cd "$WORK"
aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/input/$S3SINK_ZIP" "$S3SINK_ZIP"
echo "$S3SINK_ZIP_SHA256  $S3SINK_ZIP" | sha256sum --check
unzip -q "$S3SINK_ZIP" -d "$WORK/plugins"
chmod -R a+rX "$WORK/plugins"
ls "$WORK/plugins" > "$OUT/plugins.txt"
df -B1 / > "$OUT/df-start.txt"; free -b > "$OUT/free-start.txt"

# ---- 4. code -------------------------------------------------------------------------------------------------
mkdir -p "$WORK/code"
for f in perf.py wan.py cost.py oracle.py; do aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/$f" "$WORK/code/$f"; done
(cd "$WORK/code" && sha256sum perf.py wan.py cost.py oracle.py) | tee "$OUT/code-sha256.txt"

# ---- 5. announce, then serve the broker until it says /shutdown ---------------------------------------------
printf '{"ip": "%s", "port": 18080, "instance_id": "%s", "region": "%s", "az": "%s", "at": "%s"}\n' \
  "$IP" "$INSTANCE_ID" "$REGION" "$AZ" "$(date -u +%FT%TZ)" | aws s3 cp - "$COORD/sink.json"
cd "$WORK/code"
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 COST_LAB_ENDPOINT WAN_LAB_ENDPOINT
"$WORK/venv/bin/python" perf.py agent --port 18080 --token "$SESSION" --out "$OUT/agent" --work "$WORK/w" \
  --vtopctl "$VTOPCTL" --plugins-dir "$WORK/plugins" --docker-network host --connect-rest-port 8083 \
  --rtt-host "$BUCKET.s3.us-east-1.amazonaws.com" --idle-min 30 --first-contact-min 120
