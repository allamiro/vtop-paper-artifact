#!/usr/bin/env bash
# E-RQ5-SCALE local dry run (macOS + Docker; no AWS). Several `scale.py worker` processes stand in for the EC2
# hosts; each has its own input tree, work dir and result dir, and all meet at the S3 barrier in a local MinIO.
# Engine: the v0.10.2 aarch64-apple-darwin vtopctl. Input: the first SLICE_MIB of HDFS.log, cut at a newline.
#
#   research/experiments/rq5-cloud/scale/dryrun.sh <out-dir> [python with boto3, zstandard, blake3, numpy]
#
# Env: HOSTS (4), NS (1,2,4), REPS (2), CONTROL_REPS (1), DENSE_REPS (1), SUSTAIN_S (20), SLICE_MIB (48),
#      FAIL_SLOT (unset; e.g. 2 = that host's engines get a 0.5 s timeout -> abort path), DEAD_SLOT (that host
#      never starts -> barrier timeout), BOOT_S, BARRIER_S, DEADLINE_S (s from now),
#      ENGINE_RUST_LOG (engine log filter instead of scale.py's RUST_LOG), KEEP=1 keeps MinIO.
# Uses only the container rq5scale-minio on 127.0.0.1:19510. Other containers on the host are never touched.
set -euo pipefail
OUT=${1:?out dir}; PY=${2:-research/.venv/bin/python}
REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
P=$REPO/research/experiments/rq5-cloud/scale
VTOPCTL=${VTOPCTL:-$REPO/.tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin/vtopctl}
HDFS=${HDFS:-$REPO/research/datasets/loghub/raw/HDFS_v1/HDFS.log}
HOSTS=${HOSTS:-4}; NS=${NS:-1,2,4}; REPS=${REPS:-2}; CONTROL_REPS=${CONTROL_REPS:-1}; DENSE_REPS=${DENSE_REPS:-1}
SUSTAIN_S=${SUSTAIN_S:-20}; SLICE_MIB=${SLICE_MIB:-48}
MINIO=rq5scale-minio; PORT=19510; BUCKET=vtop-scale-dry
export AWS_ACCESS_KEY_ID=rq5scale AWS_SECRET_ACCESS_KEY=rq5scale-secret AWS_REGION=us-east-1
export COST_LAB_ENDPOINT=http://127.0.0.1:$PORT
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
PIDS=()
# shellcheck disable=SC2329  # invoked by the EXIT trap
cleanup() {
  set +e
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null; done
  [ "${KEEP:-0}" = 1 ] || docker rm -f "$MINIO" >/dev/null 2>&1
}
trap cleanup EXIT

if ! docker inspect "$MINIO" >/dev/null 2>&1; then
  docker run -d --name "$MINIO" -p 127.0.0.1:$PORT:9000 -e MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID \
    -e MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY vtop-minio:RELEASE.2025-09-07T16-13-09Z server /data >/dev/null
fi
"$PY" - <<EOF
import boto3, time
c = boto3.client("s3", endpoint_url="$COST_LAB_ENDPOINT", region_name="us-east-1")
for _ in range(60):
    try:
        c.list_buckets(); break
    except Exception:
        time.sleep(1)
try:
    c.create_bucket(Bucket="$BUCKET")
except c.exceptions.BucketAlreadyOwnedByYou:
    pass
EOF

SESSION=dry-$(date -u +%Y%m%dT%H%M%SZ)
"$PY" "$P/scale.py" plan --session "$SESSION" --hosts "$HOSTS" --ns "$NS" --reps "$REPS" --control-n "$HOSTS" \
  --control-reps "$CONTROL_REPS" --dense-reps "$DENSE_REPS" --sustain-s "$SUSTAIN_S" --seed 20261124 \
  --deadline-epoch $(( $(date +%s) + ${DEADLINE_S:-3600} )) --boot-timeout-s "${BOOT_S:-120}" --barrier-timeout-s "${BARRIER_S:-120}" \
  --engine-timeout-s 120 --out "$OUT/schedule.json"

# inputs: one short tree per host (object names embed the input path)
for h in $(seq 0 $((HOSTS - 1))); do
  mkdir -p "$OUT/h$h/in/0"
  head -c $((SLICE_MIB * 1048576)) "$HDFS" > "$OUT/h$h/in/0/hdfs.log.part"
  "$PY" -c "import sys;b=open(sys.argv[1],'rb').read();open(sys.argv[2],'wb').write(b[:b.rfind(b'\n')+1])" \
    "$OUT/h$h/in/0/hdfs.log.part" "$OUT/h$h/in/0/hdfs.log"
  rm "$OUT/h$h/in/0/hdfs.log.part"
done

TARSHA=$(shasum -a 256 "$REPO/.tmp/vtop-engine-v0.10.2-dl/vtop-engine-0.10.2-aarch64-apple-darwin.tar.gz" | cut -d' ' -f1)
for h in $(seq 0 $((HOSTS - 1))); do
  [ "${DEAD_SLOT:-}" = "$h" ] && continue   # a host that never boots: barrier-timeout path
  if [ "${FAIL_SLOT:-}" = "$h" ]; then
    "$PY" - "$OUT/schedule.json" "$OUT/schedule-h$h.json" <<'EOF'
import json, sys
s = json.load(open(sys.argv[1])); s["engine_timeout_s"] = 0.5; json.dump(s, open(sys.argv[2], "w"), indent=1)
EOF
    sched=$OUT/schedule-h$h.json
  else
    sched=$OUT/schedule.json
  fi
  "$PY" "$P/scale.py" worker --schedule "$sched" --slot "$h" --vtopctl "$VTOPCTL" --in-root "$OUT/h$h/in" \
    --bucket "$BUCKET" --out "$OUT/h$h/out" --work "$OUT/h$h/w" --vtop-version 0.10.2 \
    --vtop-commit 0abf11696e47f4db871bd4c2cda882756c114390 --tarball-sha256 "$TARSHA" \
    --results-uri "s3://$BUCKET/scale/results/$SESSION/h$h-local/" --poll-s 0.5 ${ENGINE_RUST_LOG:+--rust-log "$ENGINE_RUST_LOG"} \
    > "$OUT/worker-h$h.log" 2>&1 &
  PIDS+=($!)
done
rc=0
for p in "${PIDS[@]}"; do wait "$p" || rc=$((rc | $?)); done
PIDS=()
echo "workers finished (rc bits $rc)"

# results as the operator gets them: download scale/results/<session>/ from the store
"$PY" - <<EOF
import boto3, pathlib
c = boto3.client("s3", endpoint_url="$COST_LAB_ENDPOINT", region_name="us-east-1")
dst = pathlib.Path("$OUT/results/$SESSION"); n = 0
for page in c.get_paginator("list_objects_v2").paginate(Bucket="$BUCKET", Prefix="scale/results/$SESSION/"):
    for o in page.get("Contents", []):
        f = dst / o["Key"][len("scale/results/$SESSION/"):]
        f.parent.mkdir(parents=True, exist_ok=True); c.download_file("$BUCKET", o["Key"], str(f)); n += 1
left = sum(len(p.get("Contents", [])) for p in c.get_paginator("list_objects_v2").paginate(Bucket="$BUCKET", Prefix="scale/data/$SESSION/"))
print(f"downloaded {n} result files; data objects left: {left}")
EOF
cp "$OUT/schedule.json" "$OUT/results/$SESSION/schedule.json"
echo "session $SESSION -> $OUT/results/$SESSION"
exit "$rc"
