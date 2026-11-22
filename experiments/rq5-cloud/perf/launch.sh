#!/usr/bin/env bash
# E-RQ5-PERF launcher: one leg = one broker host + one sink host of the same type in ONE subnet, self-terminating.
# Only after the author's "go" for this stage (vtop-aws-experiments skill, step 1).
#
#   research/experiments/rq5-cloud/perf/launch.sh use1     # both hosts in us-east-1 (next to the bucket)
#   research/experiments/rq5-cloud/perf/launch.sh apse2    # both hosts in ap-southeast-2 (bucket stays in us-east-1)
#
# Same flags as research/experiments/rq5-cloud/run-ec2.sh (identity check, AL2023 AMI, default VPC,
# subnet in an AZ that offers the type, IMDSv2, terminate-on-shutdown, instance profile, project tags, S3 pinned to
# the bucket's region), plus what run-ec2.sh cannot do: two instances in the same subnet (free private traffic, equal
# path to S3), a larger gp3 root (the Connect image alone is ~3.7 GB) and a hop limit of 2 for IMDSv2.
# It stages the code and the plugin zip, fills SESSION into copies of both user-data scripts, launches, waits for
# both to terminate, downloads both result folders and lists anything still running.
set -euo pipefail

LEG=${1:?use1|apse2}
case $LEG in use1) REGION=us-east-1 ;; apse2) REGION=ap-southeast-2 ;; *) echo "leg must be use1 or apse2" >&2; exit 2 ;; esac
export AWS_PROFILE=${AWS_PROFILE:-vtop-op} AWS_REGION=$REGION
BUCKET=vtop-eval-XXXXXXXXXXXX
BUCKET_REGION=us-east-1
TYPE=${TYPE:-t4g.large}
ROOT_GB=${ROOT_GB:-30}
MAX_MIN=${MAX_MIN:-410}             # stop waiting after this (the VMs power off at 390/395 min at the latest)
P=research/experiments/rq5-cloud/perf
E=research/experiments
OUTDIR=$P/results
log() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }

[ -f "$P/perf.py" ] || { echo "run from the repo root" >&2; exit 1; }
[ "$(aws sts get-caller-identity --query Arn --output text)" = "arn:aws:iam::XXXXXXXXXXXX:user/vtop-eval-operator" ] \
  || { echo "wrong identity; use profile vtop-op" >&2; exit 1; }

SESSION=$(date -u +%Y%m%dT%H%M%SZ)-$LEG
TMP=$(mktemp -d)
for r in broker sink; do
  sed "s/^SESSION=\"\"/SESSION=\"$SESSION\"/" "$P/user-data-$r.sh" > "$TMP/$r.sh"
  bash -n "$TMP/$r.sh"
  bash "$TMP/$r.sh" --check-params
  [ "$(wc -c < "$TMP/$r.sh")" -lt 16000 ] || { echo "$r user-data is over 16 KB" >&2; exit 1; }
done

# code and inputs (S3 calls go to the bucket's region)
for f in "$P/perf.py" "$E/rq5-cloud/wan/wan.py" "$E/rq5-cloud/cost/cost.py" "$E/rq5-cloud/smoke/oracle.py" \
         "$E/rq4-baselines/code/exp_baseline.py" "$E/lib/vtoplab.py"; do
  aws s3 cp --region "$BUCKET_REGION" --only-show-errors "$f" "s3://$BUCKET/perf/code/$(basename "$f")"; log "staged $f"
done
aws s3 ls --region "$BUCKET_REGION" "s3://$BUCKET/cost/input/HDFS_v1.zip" >/dev/null \
  || { echo "s3://$BUCKET/cost/input/HDFS_v1.zip missing (cost README step 4)" >&2; exit 1; }
if ! aws s3 ls --region "$BUCKET_REGION" "s3://$BUCKET/perf/input/s3sink-12.1.15.zip" >/dev/null; then
  Z=.tmp/rq4/s3sink-12.1.15.zip
  [ "$(shasum -a 256 "$Z" | cut -d' ' -f1)" = 7ba0ea1fbf32251b99a39f47bfcc35a0a49abdeb0a427fbf4866e14df0a32dc7 ] \
    || { echo "$Z missing or wrong digest" >&2; exit 1; }
  aws s3 cp --region "$BUCKET_REGION" --only-show-errors "$Z" "s3://$BUCKET/perf/input/s3sink-12.1.15.zip"; log "staged $Z"
fi

AMI=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-kernel-6.1-arm64" Name=state,Values=available \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
[ -n "$AMI" ] && [ "$AMI" != None ] || { echo "no AL2023 arm64 AMI in $REGION" >&2; exit 1; }
ROOTDEV=$(aws ec2 describe-images --image-ids "$AMI" --query 'Images[0].RootDeviceName' --output text)
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" != None ] || { echo "no default VPC in $REGION" >&2; exit 1; }
AZS=$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
  --filters Name=instance-type,Values="$TYPE" --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ',')
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  Name=availability-zone,Values="$AZS" --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
[ -n "$SUBNET" ] && [ "$SUBNET" != None ] || { echo "no default subnet for $TYPE in $REGION" >&2; exit 1; }

launch() {  # role
  aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --subnet-id "$SUBNET" \
    --associate-public-ip-address --iam-instance-profile Name=vtop-eval-ec2 \
    --user-data "file://$TMP/$1.sh" \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled,HttpPutResponseHopLimit=2 \
    --block-device-mappings "DeviceName=$ROOTDEV,Ebs={VolumeSize=$ROOT_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=vtop-eval},{Key=Name,Value=vtop-perf-$1},{Key=session,Value=$SESSION}]" \
                         'ResourceType=volume,Tags=[{Key=project,Value=vtop-eval}]' \
    --query 'Instances[0].InstanceId' --output text
}
SINK=$(launch sink)
log "launched sink $SINK ($TYPE, $REGION, $AMI, $SUBNET, root ${ROOT_GB} GiB) session $SESSION — add to rq5-cloud/LOG.md"
BROKER=$(launch broker)
log "launched broker $BROKER ($TYPE, $REGION, $AMI, $SUBNET) session $SESSION — add to rq5-cloud/LOG.md"

for _ in $(seq 1 "$MAX_MIN"); do
  s=$(aws ec2 describe-instances --instance-ids "$SINK" "$BROKER" --query 'Reservations[].Instances[].State.Name' --output text)
  [ "$(echo "$s" | tr '\t' '\n' | sort -u)" = terminated ] && break
  sleep 60
done
log "instance states: $s"

for ID in "$BROKER" "$SINK"; do
  RUN=$(aws s3 ls --region "$BUCKET_REGION" "s3://$BUCKET/perf/results/" | awk '{print $2}' | tr -d / | grep -- "$ID" || true)
  if [ -n "$RUN" ]; then
    mkdir -p "$OUTDIR/$RUN"
    aws s3 cp --region "$BUCKET_REGION" --recursive --only-show-errors "s3://$BUCKET/perf/results/$RUN/" "$OUTDIR/$RUN/"
    log "results in $OUTDIR/$RUN"; cat "$OUTDIR/$RUN/exit.json" 2>/dev/null || true
  else
    log "no results for $ID — read: aws ec2 get-console-output --region $REGION --instance-id $ID --latest"
  fi
done
left=$(aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
  Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -z "$left" ]; then log "no project instances left running in $REGION"; else log "STILL RUNNING in $REGION: $left"; fi
rm -rf "$TMP"
