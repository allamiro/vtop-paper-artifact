#!/usr/bin/env bash
# Launch one self-terminating VTOP experiment VM, wait for it to finish, and download its results.
#
# Usage (from the paper repo root, where this artifact's experiments/ is research/experiments/):
#   research/experiments/rq5-cloud/run-ec2.sh <stage> <user-data.sh> [instance-type] [code-file ...]
#   e.g. run-ec2.sh smoke research/experiments/rq5-cloud/smoke/user-data.sh t4g.small \
#          research/experiments/rq5-cloud/smoke/smoke.py research/experiments/rq5-cloud/smoke/oracle.py
#
# Code files are uploaded to s3://$BUCKET/<stage>/code/. Results are downloaded from
# s3://$BUCKET/<stage>/results/<run_id>/ into research/experiments/rq5-cloud/<stage>/results/.
# The user-data script must power the VM off when done (the VM is launched with terminate-on-shutdown)
# and must upload its own results; see the stage README.
set -euo pipefail

STAGE=${1:?stage}; USER_DATA=${2:?user-data script}; TYPE=${3:-t4g.small}; shift $(( $# < 3 ? $# : 3 ))
export AWS_PROFILE=${AWS_PROFILE:-vtop-op} AWS_REGION=${AWS_REGION:-us-east-1}
BUCKET=${BUCKET:-vtop-eval-XXXXXXXXXXXX}
BUCKET_REGION=${BUCKET_REGION:-us-east-1}   # S3 calls go to the bucket's region even when the VM is elsewhere (E-RQ5-WAN)
OUTDIR=research/experiments/rq5-cloud/$STAGE/results
MAX_MIN=${MAX_MIN:-150}          # stop waiting after this many minutes (the VM has its own failsafe)
log() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }

bash -n "$USER_DATA"
[ "$(aws sts get-caller-identity --query Arn --output text)" != "${ARN_EXPECT:-arn:aws:iam::XXXXXXXXXXXX:user/vtop-eval-operator}" ] \
  && { echo "wrong identity; use profile vtop-op" >&2; exit 1; }

for f in "$@"; do aws s3 cp --region "$BUCKET_REGION" --only-show-errors "$f" "s3://$BUCKET/$STAGE/code/$(basename "$f")"; log "staged $f"; done

case "$TYPE" in t4g.*) ARCH=arm64 ;; *) ARCH=x86_64 ;; esac
AMI=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-kernel-6.1-$ARCH" Name=state,Values=available \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
[ -n "$AMI" ] && [ "$AMI" != None ] || { echo "no AL2023 $ARCH AMI found in $AWS_REGION" >&2; exit 1; }
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
[ "$VPC" != None ] || { echo "no default VPC in $AWS_REGION (see LESSONS.md)" >&2; exit 1; }
# a default subnet in an AZ that offers the type (not every AZ of every region offers t4g)
AZS=$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
  --filters Name=instance-type,Values="$TYPE" --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ',')
[ -n "$AZS" ] || { echo "$TYPE is not offered in any AZ of $AWS_REGION" >&2; exit 1; }
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true \
  Name=availability-zone,Values="$AZS" --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text)
[ -n "$SUBNET" ] && [ "$SUBNET" != None ] || { echo "no default subnet in $AZS ($AWS_REGION)" >&2; exit 1; }

ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --subnet-id "$SUBNET" \
  --associate-public-ip-address --iam-instance-profile Name=vtop-eval-ec2 \
  --user-data "file://$USER_DATA" --metadata-options HttpTokens=required,HttpEndpoint=enabled \
  --instance-initiated-shutdown-behavior terminate \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=vtop-eval},{Key=Name,Value=vtop-$STAGE}]" \
                       'ResourceType=volume,Tags=[{Key=project,Value=vtop-eval}]' \
  --query 'Instances[0].InstanceId' --output text)
log "launched $ID ($TYPE, $AWS_REGION, $AMI, $SUBNET) — add this line to rq5-cloud/LOG.md"

for _ in $(seq 1 "$MAX_MIN"); do
  s=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text)
  [ "$s" = terminated ] && break; sleep 60
done
log "instance state: $s"

RUN=$(aws s3 ls --region "$BUCKET_REGION" "s3://$BUCKET/$STAGE/results/" | awk '{print $2}' | tr -d / | grep -- "$ID" || true)
if [ -n "$RUN" ]; then
  mkdir -p "$OUTDIR/$RUN"
  aws s3 cp --region "$BUCKET_REGION" --recursive --only-show-errors "s3://$BUCKET/$STAGE/results/$RUN/" "$OUTDIR/$RUN/"
  log "results in $OUTDIR/$RUN"; cat "$OUTDIR/$RUN/exit.json" 2>/dev/null || true
else
  log "no results uploaded for $ID — read: aws ec2 get-console-output --region $AWS_REGION --instance-id $ID --latest"
fi

left=$(aws ec2 describe-instances --filters Name=tag:project,Values=vtop-eval \
  Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text)
[ -z "$left" ] && log "no project instances left running in $AWS_REGION" || log "STILL RUNNING in $AWS_REGION: $left"
