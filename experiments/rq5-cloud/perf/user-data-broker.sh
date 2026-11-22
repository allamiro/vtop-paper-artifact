#!/bin/bash
# E-RQ5-PERF (H4.2): cloud-init user-data for the KAFKA BROKER host (Amazon Linux 2023 arm64, t4g.large).
# Leg us-east-1 (both hosts next to the bucket) or ap-southeast-2 (both hosts in Sydney, bucket in us-east-1).
# Starts Kafka 3.9.1 (RQ4 digest) on its private IP, loads and verifies the HDFS topic, waits for the sink host to
# announce itself in s3://.../perf/coord/<session>/sink.json, then runs `perf.py orchestrate` (schedule, observer,
# oracle, cleanup). The EXIT trap deletes perf/data/<session>/ unless a run failed (KEEP_DATA), aborts open multipart
# uploads there, writes exit.json, uploads /var/log/vtop-perf/ to perf/results/<session>-broker-<id>/ and powers
# off (launched with terminate-on-shutdown). Credentials: instance role vtop-eval-ec2 via IMDSv2; no keys.
# launch.sh fills SESSION; until then the script refuses to run.  Local check:  bash user-data-broker.sh --check-params
set -euo pipefail

SESSION=""            # filled by launch.sh: <UTC stamp>-<leg>, e.g. 20261122T010203Z-use1
BUCKET=vtop-eval-XXXXXXXXXXXX
STAGE=perf
SEED=20261123
FAILSAFE_MIN=390      # hard power-off (README "Time and cost")
DEADLINE_MIN=345      # perf.py starts no new run after this
SHUTDOWN_WHEN_DONE=1
KAFKA_IMAGE=apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed   # 3.9.1, RQ4
# engine pin, recorded only (the sink host verifies and runs it): same values as cost/user-data.sh
VTOP_VERSION=0.10.4
VTOP_COMMIT="be5b419ed0658e217284e8324caaff31f39622c5"   # tag v0.10.4, squash of #899 (decision D16); checked 2026-11-22
TARBALL_SHA256="0266c37b1f8740793f71724cddc5283c0aceb8faeda4e6bdea0d714ca8f37c84"   # = SHA256SUMS entry (SHA256SUMS 90e807f5…); cosign verify-blob OK (release.yml@refs/tags/v0.10.4), 2026-11-22
# Loghub HDFS_v1 (Zenodo 8196385): archive MD5 as datasets/loghub/fetch.sh, log SHA-256 and bytes as inventory.csv
HDFS_MD5=76a24b4d9a6164d543fb275f89773260 HDFS_SHA256=0783096174d7832c618337f9609e06e04abd86ddd7089b3c12b407e63bfebc52 HDFS_BYTES=1577982906
SSH_MD5=a44e40b4348697dabf2bba56885e0a38 SSH_SHA256=89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3 SSH_BYTES=73417506

check_params() {
  [[ $SESSION =~ ^[0-9]{8}T[0-9]{6}Z-(use1|apse2)$ ]] || { echo "SESSION is not filled in (launch.sh does it)" >&2; exit 2; }
  [[ $VTOP_COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "VTOP_COMMIT is not 40 hex digits" >&2; exit 2; }
  [[ $TARBALL_SHA256 =~ ^[0-9a-f]{64}$ ]] || { echo "TARBALL_SHA256 is not 64 hex digits" >&2; exit 2; }
}
if [ "${1:-}" = --check-params ]; then
  check_params; echo "ok: session $SESSION, v$VTOP_VERSION $VTOP_COMMIT"; exit 0
fi

export AWS_DEFAULT_REGION=us-east-1   # the bucket's region, whatever region this VM is in
OUT=/var/log/vtop-perf
WORK=/opt/p
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
IP=$(imds meta-data/local-ipv4)
RUN_ID=$SESSION-broker-$INSTANCE_ID
DEST=s3://$BUCKET/$STAGE/results/$RUN_ID
DATA=s3://$BUCKET/$STAGE/data/$SESSION/
COORD=s3://$BUCKET/$STAGE/coord/$SESSION
echo "run_id=$RUN_ID region=$REGION ip=$IP dest=$DEST data=$DATA"

# matrix per leg (README "Design"): Sydney has fewer reps and no codec-matched VTOP gzip variant
TPUT_CONFIGS=vtop-size-only,vtop-sha256-store,vtop-blake3-readback,vtop-sha256-gzip6,confluent,vector,fluentbit
REPS_TPUT=5 REPS_LIVE=3 REPS_KILL=3 REPS_FAULT=5 LEG=us-east-1   # REPS_FAULT: RQ4 storage-fault phase (D13)
case $REGION in
  us-east-1) ;;
  ap-southeast-2) TPUT_CONFIGS=vtop-size-only,vtop-sha256-store,vtop-blake3-readback,confluent,vector,fluentbit
                  REPS_TPUT=3 REPS_LIVE=2 REPS_KILL=2 REPS_FAULT=0 LEG=ap-southeast-2 DEADLINE_MIN=285 ;;
  *) echo "region $REGION is not a leg of this stage" >&2; exit 2 ;;
esac

finish() {
  local rc=$?
  set +e
  docker logs rq5perf-kafka 2>&1 | gzip > "$OUT/kafka.log.gz"
  if [ -e "$OUT/results/KEEP_DATA" ]; then
    echo "a run failed: keeping $DATA for diagnosis (delete it after; README step 8)"
  else
    aws s3 rm --recursive --only-show-errors "$DATA"
  fi
  aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$STAGE/data/$SESSION/" \
    --query 'Uploads[].[Key,UploadId]' --output text > "$OUT/open-mpu.txt" 2>&1
  [ -e "$OUT/results/KEEP_DATA" ] || while read -r k u; do
    [ -n "$k" ] && [ "$k" != None ] && aws s3api abort-multipart-upload --bucket "$BUCKET" --key "$k" --upload-id "$u"
  done < "$OUT/open-mpu.txt"
  left=$(aws s3 ls --recursive "$DATA" | wc -l)
  chronyc tracking > "$OUT/chrony-end.txt" 2>&1
  echo "{\"exit_code\": $rc, \"run_id\": \"$RUN_ID\", \"region\": \"$REGION\", \"data_objects_left\": $left, \"finished_at\": \"$(date -u +%FT%TZ)\", \"elapsed_s\": $(( $(date +%s) - T0 ))}" > "$OUT/exit.json"
  cp /var/log/cloud-init-output.log "$OUT/" 2>/dev/null
  aws s3 cp --recursive --only-show-errors "$OUT" "$DEST/"
  echo "{\"broker_done\": \"$(date -u +%FT%TZ)\", \"exit_code\": $rc}" | aws s3 cp - "$COORD/broker-done.json"
  [ "$SHUTDOWN_WHEN_DONE" = 1 ] && shutdown -h now
}
trap finish EXIT
check_params           # after the trap: an unfilled session powers the VM off with exit.json uploaded

# ---- 1. platform ----------------------------------------------------------------------------------------
systemctl stop amazon-ssm-agent 2>/dev/null || true    # no SSM use; keeps the host quiet
ldd --version > "$OUT/ldd-full.txt" 2>&1 || true       # never `| head` under pipefail (LESSONS.md)
chronyc tracking > "$OUT/chrony-start.txt" 2>&1 || true
dnf install -y -q docker ethtool unzip
command -v aws >/dev/null || dnf install -y -q awscli-2
systemctl start docker
PY=""
for v in 3.12 3.11; do
  if dnf install -y -q "python$v" "python$v-pip"; then PY=python$v; break; fi
done
[ -n "$PY" ] || { echo "no python3.11/3.12 package" >&2; exit 1; }
"$PY" -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install -q --only-binary=:all: boto3==1.44.0 zstandard==0.25.0 blake3==1.0.11 \
  numpy==2.3.5 confluent-kafka==2.16.2
"$WORK/venv/bin/pip" freeze > "$OUT/pip-freeze.txt"

# ---- 2. Kafka (KRaft, one node, host network, advertised on the private IP) --------------------------------
docker pull -q "$KAFKA_IMAGE"
docker image inspect "$KAFKA_IMAGE" --format '{{.Id}} {{.Architecture}} {{json .RepoDigests}}' > "$OUT/kafka-image.txt"
docker run -d --name rq5perf-kafka --network host \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$IP:9092" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  "$KAFKA_IMAGE"

# ---- 3. input: HDFS_v1 from the stage-E-RQ5-COST staging (or Zenodo), verified ------------------------------
mkdir -p "$WORK/in" && cd "$WORK/in"
aws s3 cp --only-show-errors "s3://$BUCKET/cost/input/HDFS_v1.zip" HDFS_v1.zip \
  || curl -fsSL --retry 3 -o HDFS_v1.zip "https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1"
echo "$HDFS_MD5  HDFS_v1.zip" | md5sum --check
unzip -p HDFS_v1.zip HDFS.log > hdfs.log      # member HDFS.log -> hdfs.log
rm -f HDFS_v1.zip
echo "$HDFS_SHA256  hdfs.log" | sha256sum --check
[ "$(stat -c %s hdfs.log)" = "$HDFS_BYTES" ]
aws s3 cp --only-show-errors "s3://$BUCKET/cost/input/SSH.tar.gz" SSH.tar.gz \
  || curl -fsSL --retry 3 -o SSH.tar.gz "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1"
echo "$SSH_MD5  SSH.tar.gz" | md5sum --check
tar xzf SSH.tar.gz -O SSH.log > ssh.log
rm -f SSH.tar.gz
echo "$SSH_SHA256  ssh.log" | sha256sum --check
[ "$(stat -c %s ssh.log)" = "$SSH_BYTES" ]
printf '\n' >> ssh.log   # SSH.log ends without a newline (cost/user-data.sh); RQ4's topic has all 655,147 lines

# ---- 4. code staged by launch.sh, laid out as perf.py expects ------------------------------------------------
mkdir -p "$WORK/code/rq4/code" "$WORK/code/lib"
for f in perf.py wan.py cost.py oracle.py; do aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/$f" "$WORK/code/$f"; done
aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/exp_baseline.py" "$WORK/code/rq4/code/exp_baseline.py"
aws s3 cp --only-show-errors "s3://$BUCKET/$STAGE/code/vtoplab.py" "$WORK/code/lib/vtoplab.py"
(cd "$WORK/code" && sha256sum perf.py wan.py cost.py oracle.py rq4/code/exp_baseline.py lib/vtoplab.py) | tee "$OUT/code-sha256.txt"
cp "$WORK/code/perf.py" "$OUT/"

# ---- 5. wait for Kafka, then for the sink host's announcement -----------------------------------------------
for _ in $(seq 1 120); do
  "$WORK/venv/bin/python" -c "from confluent_kafka.admin import AdminClient as A; A({'bootstrap.servers':'$IP:9092'}).list_topics(timeout=3)" \
    2>/dev/null && break
  sleep 2
done
SINK_IP=""
for _ in $(seq 1 270); do      # up to 45 min: the sink host pulls ~4 GB of images first
  if aws s3 cp --only-show-errors "$COORD/sink.json" "$OUT/sink.json" 2>/dev/null; then
    SINK_IP=$("$WORK/venv/bin/python" -c "import json;print(json.load(open('$OUT/sink.json'))['ip'])"); break
  fi
  sleep 10
done
[ -n "$SINK_IP" ] || { echo "the sink host never announced itself" >&2; exit 1; }
echo "sink at $SINK_IP"

# ---- 6. the schedule ---------------------------------------------------------------------------------------
cd "$WORK/code"
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 COST_LAB_ENDPOINT WAN_LAB_ENDPOINT
"$WORK/venv/bin/python" perf.py orchestrate --bootstrap "$IP:9092" --kafka-native "$IP:9092" --kafka-container "$IP:9092" \
  --agent "http://$SINK_IP:18080" --token "$SESSION" --bucket "$BUCKET" --stage "$STAGE" --session "$SESSION" --leg "$LEG" \
  --lines-file "$WORK/in/hdfs.log" --out "$OUT/results" --results-uri "$DEST/results/" \
  --tput-configs "$TPUT_CONFIGS" --reps-tput "$REPS_TPUT" --reps-live "$REPS_LIVE" --reps-kill "$REPS_KILL" \
  --reps-fault "$REPS_FAULT" --ssh-lines-file "$WORK/in/ssh.log" \
  --seed "$SEED" --deadline-epoch $(( T0 + DEADLINE_MIN * 60 )) --rtt --shutdown-agent --keep-failed-data \
  --vtop-version "$VTOP_VERSION" --vtop-commit "$VTOP_COMMIT" --tarball-sha256 "$TARBALL_SHA256"
