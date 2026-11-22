#!/usr/bin/env bash
# E-RQ5-PERF local dry run (macOS + Docker Desktop; no AWS). Kafka 3.9.1 and MinIO in containers (RQ4 images),
# the three sink images by their RQ4 digests, the v0.10.2 aarch64-apple-darwin vtopctl, a request-counting proxy
# in front of MinIO (reqcount_proxy.py), the agent and the orchestrator as local processes, a small HDFS topic.
#
#   research/experiments/rq5-cloud/perf/dryrun.sh <out-dir> <python with boto3, zstandard, blake3, numpy, confluent-kafka>
#
# Uses only its own names (rq5perf-*) and free ports 19092, 19400, 19401, 18080, 18083; removes its containers at
# the end (KEEP=1 keeps them). Other containers on the host are never touched.
set -euo pipefail
OUT=${1:?out dir}; PY=${2:?python}
REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
P=$REPO/research/experiments/rq5-cloud/perf
VTOPCTL=${VTOPCTL:-$REPO/.tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin/vtopctl}
PLUGINS=${PLUGINS:-$REPO/.tmp/rq4/plugins}
HDFS=${HDFS:-$REPO/research/datasets/loghub/raw/HDFS_v1/HDFS.log}
SSH=${SSH:-$REPO/research/datasets/loghub/raw/SSH/SSH.log}
NET=rq5perf-net; KAFKA=rq5perf-kafka; MINIO=rq5perf-minio; CG=rq5perf-cg
KEY=rq5perf; SECRET=rq5perf-secret; BUCKET=vtop-perf-dryrun; TOKEN=dryrun-token
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

cleanup() {
  set +e
  [ -n "${AGENT_PID:-}" ] && kill "$AGENT_PID" 2>/dev/null
  [ -n "${PROXY_PID:-}" ] && kill "$PROXY_PID" 2>/dev/null
  if [ "${KEEP:-0}" != 1 ]; then
    for c in rq5perf-confluent rq5perf-vector rq5perf-fluentbit "$KAFKA" "$MINIO" "$CG"; do docker rm -f "$c" >/dev/null 2>&1; done
    docker network rm "$NET" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "${SUBNET:-10.231.77.0/24}" "$NET" >/dev/null
docker rm -f "$KAFKA" "$MINIO" "$CG" >/dev/null 2>&1 || true
docker run -d --name "$KAFKA" --network "$NET" -p 19092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,INTERNAL://$KAFKA:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed >/dev/null
docker run -d --name "$MINIO" --network "$NET" -p 19400:9000 -e MINIO_ROOT_USER=$KEY -e MINIO_ROOT_PASSWORD=$SECRET \
  vtop-minio:RELEASE.2025-09-07T16-13-09Z server /data >/dev/null
docker run -d --name "$CG" --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:ro python:3.12-slim sleep infinity >/dev/null

export AWS_ACCESS_KEY_ID=$KEY AWS_SECRET_ACCESS_KEY=$SECRET AWS_REGION=us-east-1
for _ in $(seq 1 60); do
  "$PY" -c "import boto3;boto3.client('s3',endpoint_url='http://127.0.0.1:19400',region_name='us-east-1').list_buckets()" 2>/dev/null && break; sleep 1
done
"$PY" -c "import boto3;boto3.client('s3',endpoint_url='http://127.0.0.1:19400',region_name='us-east-1').create_bucket(Bucket='$BUCKET')"
for _ in $(seq 1 60); do
  "$PY" -c "from confluent_kafka.admin import AdminClient;AdminClient({'bootstrap.servers':'localhost:19092'}).list_topics(timeout=3)" 2>/dev/null && break; sleep 1
done

"$PY" "$P/reqcount_proxy.py" --listen 19401 --upstream 127.0.0.1:19400 --log "$OUT/requests.jsonl" \
  --access-key $KEY --secret-key $SECRET > "$OUT/proxy.log" 2>&1 &
PROXY_PID=$!
"$PY" "$P/perf.py" agent --port 18080 --token $TOKEN --out "$OUT/sink" --work "${TMPDIR:-/tmp}/rq5perf-w" \
  --vtopctl "$VTOPCTL" --plugins-dir "$PLUGINS" --docker-network "$NET" --connect-rest-port 18083 \
  --cg-helper "$CG" --lab > "$OUT/agent.log" 2>&1 &
AGENT_PID=$!
sleep 2

# shellcheck disable=SC2086  # ORCH_EXTRA holds extra flags on purpose
"$PY" "$P/perf.py" orchestrate --bootstrap localhost:19092 --kafka-native localhost:19092 \
  --kafka-container "$KAFKA:29092" --agent http://127.0.0.1:18080 --token $TOKEN --bucket $BUCKET \
  --session dryrun --leg lab --lines-file "$HDFS" --out "$OUT/broker" \
  --s3-endpoint http://127.0.0.1:19400 --s3-native http://127.0.0.1:19401 --s3-container http://host.docker.internal:19401 \
  --records-per-object "${RPO:-15000}" --object-bytes "${OBJ_BYTES:-2097152}" --objects-per-partition "${OPP:-20}" --live-objects "${LIVE_OBJ:-15}" \
  --live-rate "${LIVE_RATE:-7500}" --reps-tput "${REPS:-1}" --reps-live "${REPS:-1}" --reps-kill "${REPS:-1}" \
  --tput-configs "${TPUT:-vtop-size-only,vtop-sha256-store,vtop-blake3-readback,vtop-sha256-gzip6,confluent,vector,fluentbit}" \
  --live-configs "${LIVECFG:-vtop-size-only,vtop-sha256-store,vtop-blake3-readback,confluent,vector,fluentbit}" \
  --kill-configs "${KILLCFG:-vtop-sha256-store,confluent,vector,fluentbit}" --warmup-configs "${WARM:-}" \
  --ssh-lines-file "$SSH" --reps-fault "${FAULT_REPS:-1}" --fault-configs "${FAULTCFG:-vtop-blake3-readback,confluent,vector,fluentbit}" \
  --settle-s 30 --stall-s 120 --run-timeout 600 --vtop-version 0.10.2 --vtop-commit 0abf11696e47f4db871bd4c2cda882756c114390 \
  --tarball-sha256 "$(shasum -a 256 "$REPO/.tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin.tar.gz" | cut -d' ' -f1)" \
  --shutdown-agent ${ORCH_EXTRA:-} 2>&1 | tee "$OUT/orchestrate.log"
