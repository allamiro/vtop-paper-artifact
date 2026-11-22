#!/usr/bin/env bash
# E-RQ5-SCALE launcher: HOSTS self-terminating hosts in ONE RunInstances call (MinCount = MaxCount = HOSTS, so
# the account either starts the whole fleet or nothing), one subnet, us-east-1. Only after the author's "go"
# (vtop-aws-experiments skill, step 1).
#
#   research/experiments/rq5-cloud/scale/launch.sh            # default plan (README "Design")
#   DRY=1 research/experiments/rq5-cloud/scale/launch.sh      # everything up to RunInstances, which is printed, not sent
#
# Env: TYPE (t4g.large), HOSTS (8), ROOT_GB (16), NS (1,2,4,8), REPS (10), CONTROL_REPS (5), DENSE_REPS (0),
#      SUSTAIN_S (0), SEED (20261124), VCPU_CAP (16), MAX_MIN (failsafe + 15).
# Same guard rails as research/experiments/rq5-cloud/run-ec2.sh (identity, AL2023 AMI, default VPC,
# IMDSv2, terminate-on-shutdown, instance profile, project tags), plus: a vCPU guard over every running instance in
# the region, the session schedule, the fleet launch, and a session-wide download of the results.
set -euo pipefail

export AWS_PROFILE=${AWS_PROFILE:-vtop-op} AWS_REGION=us-east-1
BUCKET=vtop-eval-XXXXXXXXXXXX
TYPE=${TYPE:-t4g.large}; HOSTS=${HOSTS:-8}; ROOT_GB=${ROOT_GB:-16}; VCPU_CAP=${VCPU_CAP:-16}
NS=${NS:-1,2,4,8}; REPS=${REPS:-10}; CONTROL_REPS=${CONTROL_REPS:-5}; DENSE_REPS=${DENSE_REPS:-0}
SUSTAIN_S=${SUSTAIN_S:-0}; SEED=${SEED:-20261124}; DRY=${DRY:-0}
P=research/experiments/rq5-cloud/scale
C=research/experiments/rq5-cloud
log() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { log "REFUSED: $*" >&2; exit 1; }

[ -f "$P/scale.py" ] || die "run from the repo root"
case $TYPE in
  t4g.nano|t4g.micro|t4g.small|t4g.medium|t4g.large) ARCH=arm64; VCPU=2 ;;
  t3.nano|t3.micro|t3.small|t3.medium|t3.large) die "x86_64 (t3) needs the x86_64 archive digest pinned in user-data.sh first (README)" ;;
  *) die "instance type $TYPE is outside the operator policy (t4g/t3 micro-large)" ;;
esac
[ $((HOSTS * VCPU)) -le "$VCPU_CAP" ] || die "$HOSTS x $TYPE = $((HOSTS * VCPU)) vCPUs > cap $VCPU_CAP"
[ "$(aws sts get-caller-identity --query Arn --output text)" = "arn:aws:iam::XXXXXXXXXXXX:user/vtop-eval-operator" ] \
  || die "wrong identity; use profile vtop-op"

# ---- vCPU guard: every pending/running instance in the region (any project), plus this fleet, <= VCPU_CAP
RUNNING=$(aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping \
  --query 'Reservations[].Instances[].[InstanceId,CpuOptions.CoreCount,CpuOptions.ThreadsPerCore]' --output text)
USED=0
while read -r id cores tpc; do
  [ -n "${id:-}" ] || continue
  USED=$((USED + ${cores:-2} * ${tpc:-1})); log "already running: $id ($((${cores:-2} * ${tpc:-1})) vCPU)"
done <<< "$RUNNING"
[ $((USED + HOSTS * VCPU)) -le "$VCPU_CAP" ] \
  || die "$USED vCPUs already running + $((HOSTS * VCPU)) for this fleet > $VCPU_CAP (wait for E-RQ5-PERF/TAMPER to finish)"

# ---- time limits from the plan (README "Time and cost")
FAILSAFE_MIN=$(( 130 + (SUSTAIN_S * 2 / 60) + DENSE_REPS * 2 ))
[ "$FAILSAFE_MIN" -le 300 ] || die "failsafe $FAILSAFE_MIN min > 300; shorten the plan"
DEADLINE_MIN=$(( FAILSAFE_MIN - 25 - (SUSTAIN_S * 2 / 60) ))   # no new step after this; leaves the sustained step and upload time
MAX_MIN=${MAX_MIN:-$(( FAILSAFE_MIN + 15 ))}
SESSION=$(date -u +%Y%m%dT%H%M%SZ)-scale
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

python3 -I "$P/scale.py" plan --session "$SESSION" --hosts "$HOSTS" --ns "$NS" --reps "$REPS" \
  --control-n "$HOSTS" --control-reps "$CONTROL_REPS" --dense-reps "$DENSE_REPS" --sustain-s "$SUSTAIN_S" \
  --seed "$SEED" --deadline-epoch $(( $(date +%s) + DEADLINE_MIN * 60 )) --out "$TMP/schedule.json"
sed -e "s/^SESSION=\"\"/SESSION=\"$SESSION\"/" -e "s/^FAILSAFE_MIN=[0-9]*/FAILSAFE_MIN=$FAILSAFE_MIN/" \
  "$P/user-data.sh" > "$TMP/user-data.sh"
bash -n "$TMP/user-data.sh"
bash "$TMP/user-data.sh" --check-params
[ "$(wc -c < "$TMP/user-data.sh")" -lt 16000 ] || die "user-data is over 16 KB"

aws s3 ls "s3://$BUCKET/cost/input/HDFS_v1.zip" >/dev/null || die "s3://$BUCKET/cost/input/HDFS_v1.zip missing (cost README step 4)"
AMI=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-kernel-6.1-$ARCH" Name=state,Values=available \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
[ -n "$AMI" ] && [ "$AMI" != None ] || die "no AL2023 $ARCH AMI"
ROOTDEV=$(aws ec2 describe-images --image-ids "$AMI" --query 'Images[0].RootDeviceName' --output text)
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" != None ] || die "no default VPC"
AZS=$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
  --filters Name=instance-type,Values="$TYPE" --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ',')
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  Name=availability-zone,Values="$AZS" --query 'sort_by(Subnets,&AvailabilityZone)[].SubnetId' --output text)
[ -n "$SUBNETS" ] && [ "$SUBNETS" != None ] || die "no default subnet offering $TYPE"

run_fleet() {  # subnet -> instance ids on stdout; the AWS error on stderr
  aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --subnet-id "$1" --count "$HOSTS:$HOSTS" \
    --associate-public-ip-address --iam-instance-profile Name=vtop-eval-ec2 \
    --user-data "file://$TMP/user-data.sh" \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings "DeviceName=$ROOTDEV,Ebs={VolumeSize=$ROOT_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=vtop-eval},{Key=Name,Value=vtop-scale},{Key=session,Value=$SESSION}]" \
                         'ResourceType=volume,Tags=[{Key=project,Value=vtop-eval}]' \
    --query 'Instances[].InstanceId' --output text
}

if [ "$DRY" = 1 ]; then
  log "DRY=1: would stage code to s3://$BUCKET/scale/code/$SESSION/ and launch $HOSTS x $TYPE ($AMI) in $SUBNETS"
  log "schedule: $(python3 -I -c "import json,sys;s=json.load(open(sys.argv[1]));print(len(s['steps']),'steps,',sum(len(x['engines']) for x in s['steps']),'engine-runs')" "$TMP/schedule.json"); failsafe $FAILSAFE_MIN min, deadline +$DEADLINE_MIN min"
  exit 0
fi

for f in "$P/scale.py" "$C/cost/cost.py" "$C/smoke/oracle.py" "$TMP/schedule.json"; do
  aws s3 cp --only-show-errors "$f" "s3://$BUCKET/scale/code/$SESSION/$(basename "$f")"; log "staged $f"
done

IDS=""
for SUBNET in $SUBNETS; do
  if IDS=$(run_fleet "$SUBNET" 2> "$TMP/err"); then break; fi
  IDS=""
  if grep -q InsufficientInstanceCapacity "$TMP/err"; then log "no capacity in $SUBNET; trying the next AZ"; continue; fi
  cat "$TMP/err" >&2
  die "RunInstances refused (above); nothing was launched (MinCount = MaxCount = $HOSTS). Staged code stays in scale/code/$SESSION/"
done
[ -n "$IDS" ] || die "no AZ had capacity for $HOSTS x $TYPE; nothing was launched"
read -r -a ID_ARR <<< "$(echo "$IDS" | tr '\t' ' ')"
if [ "${#ID_ARR[@]}" != "$HOSTS" ]; then
  aws ec2 terminate-instances --instance-ids "${ID_ARR[@]}" >/dev/null
  die "asked for $HOSTS instances, got ${#ID_ARR[@]}; terminated them"
fi
log "launched $HOSTS x $TYPE ($AMI, $SUBNET, root ${ROOT_GB} GiB, failsafe $FAILSAFE_MIN min) session $SESSION: ${ID_ARR[*]} — add to rq5-cloud/LOG.md"
trap 'rm -rf "$TMP"; log "interrupted: the hosts power off by themselves (failsafe $FAILSAFE_MIN min); to stop now: aws ec2 terminate-instances --instance-ids ${ID_ARR[*]}"' EXIT

s=""
for _ in $(seq 1 "$MAX_MIN"); do
  s=$(aws ec2 describe-instances --instance-ids "${ID_ARR[@]}" --query 'Reservations[].Instances[].State.Name' --output text)
  [ "$(echo "$s" | tr '\t' '\n' | sort -u)" = terminated ] && break
  sleep 60
done
log "instance states: $s"

OUT=$P/results/$SESSION
mkdir -p "$OUT"
aws s3 cp --recursive --only-show-errors "s3://$BUCKET/scale/results/$SESSION/" "$OUT/"
cp "$TMP/schedule.json" "$OUT/schedule.json"
for e in "$OUT"/h*/exit.json; do [ -f "$e" ] && { printf '%s ' "$(basename "$(dirname "$e")")"; cat "$e"; }; done
log "results in $OUT ($(find "$OUT" -maxdepth 1 -type d -name 'h*' | wc -l | tr -d ' ') of $HOSTS hosts)"
log "data objects left: $(aws s3 ls --recursive "s3://$BUCKET/scale/data/$SESSION/" | wc -l | tr -d ' ')"
left=$(aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
  Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -z "$left" ]; then log "no project instances left running"; else log "STILL RUNNING: $left"; fi
trap 'rm -rf "$TMP"' EXIT
