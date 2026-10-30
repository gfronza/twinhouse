#!/usr/bin/env bash
# Runs cmd/chbench on AWS: twinhouse on ECS + DynamoDB against ClickHouse on
# EC2, from a client instance in the same VPC (deploy/aws-bench/README.md).
#
#   deploy/aws-bench/bench.sh up                # secret, bench stack, image, twinhouse ECS stack
#   deploy/aws-bench/bench.sh check             # both servers answer from the client
#   deploy/aws-bench/bench.sh run [-size M ...] # one chbench run; results in bin/aws/
#   deploy/aws-bench/bench.sh capacity <json>   # DynamoDB capacity per step of a run
#   deploy/aws-bench/bench.sh status
#   deploy/aws-bench/bench.sh down              # deletes everything, chbench_* tables included
#
# AWS_PROFILE (default twinhouse) and AWS_REGION (default us-east-1) pick
# the account and region. Needs the AWS CLI, Docker with buildx, Go and
# python3; run from the repository root.
set -euo pipefail

export AWS_PROFILE=${AWS_PROFILE:-twinhouse}
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_PAGER=""
BENCH_STACK=twinhouse-bench
ECS_STACK=twinhouse-bench-ecs
SECRET_NAME=twinhouse-bench/ch-users
PREFIX=chbench_
DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$DIR/../.." && pwd)
cd "$ROOT"

log() { printf '\n== %s\n' "$*" >&2; }

output() { # stack key
	aws cloudformation describe-stacks --stack-name "$1" \
		--query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text
}

secret_arn() {
	aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --query ARN --output text
}

# remote runs a shell script on the client instance through SSM and streams
# its output (CloudWatch Logs) until it ends. $1 is a label, $2 the script,
# $3 the timeout in seconds.
remote() {
	local label=$1 script=$2 timeout=${3:-600}
	local client group cmd status tailer
	client=$(output "$BENCH_STACK" ClientInstanceId)
	group=$(output "$BENCH_STACK" RunLogGroup)
	cmd=$(aws ssm send-command --instance-ids "$client" --document-name AWS-RunShellScript \
		--comment "$label" --timeout-seconds 600 \
		--cloud-watch-output-config "CloudWatchOutputEnabled=true,CloudWatchLogGroupName=$group" \
		--parameters "$(python3 -c 'import json,sys; print(json.dumps({"commands":[sys.argv[1]],"executionTimeout":[sys.argv[2]]}))' "$script" "$timeout")" \
		--query Command.CommandId --output text)
	log "$label: SSM command $cmd on $client"
	aws logs tail "$group" --log-stream-name-prefix "$cmd/" --follow --format short 2>/dev/null &
	tailer=$!
	status=Pending
	while [[ $status == Pending || $status == InProgress || $status == Delayed ]]; do
		sleep 15
		status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$client" \
			--query Status --output text 2>/dev/null || echo Pending)
	done
	sleep 10 # let the last lines arrive
	kill "$tailer" 2>/dev/null || true
	log "$label: $status"
	[[ $status == Success ]]
}

cmd_up() {
	local arn tag repo vpc subnets clientsg
	log "secret $SECRET_NAME"
	if ! arn=$(secret_arn 2>/dev/null); then
		arn=$(aws secretsmanager create-secret --name "$SECRET_NAME" \
			--description "CH_USERS of the twinhouse benchmark (user:password)" \
			--secret-string "bench:$(openssl rand -hex 20)" --query ARN --output text)
	fi
	echo "$arn"

	log "stack $BENCH_STACK: VPC, NAT, ClickHouse and client instances, ECR, bucket"
	aws cloudformation deploy --stack-name "$BENCH_STACK" --template-file "$DIR/template.yaml" \
		--capabilities CAPABILITY_IAM --no-fail-on-empty-changeset \
		--tags Application=twinhouse-bench --parameter-overrides "ChUsersSecretArn=$arn"

	repo=$(output "$BENCH_STACK" RepositoryUri)
	tag=$(git rev-parse --short HEAD)
	[[ -z $(git status --porcelain --untracked-files=no) ]] || tag="$tag-dirty"
	log "image $repo:$tag"
	aws ecr get-login-password | docker login --username AWS --password-stdin "${repo%%/*}"
	make image-push IMAGE_REPO="$repo" IMAGE_TAG="$tag" PLATFORM=linux/arm64

	vpc=$(output "$BENCH_STACK" VpcId)
	subnets=$(output "$BENCH_STACK" PrivateSubnetIds)
	clientsg=$(output "$BENCH_STACK" ClientSecurityGroupId)
	log "stack $ECS_STACK: twinhouse on ECS (single region, worker in-process, one 4 vCPU / 8 GiB task)"
	aws cloudformation deploy --stack-name "$ECS_STACK" --template-file deploy/aws-ecs/template.yaml \
		--capabilities CAPABILITY_IAM --no-fail-on-empty-changeset --tags Application=twinhouse-bench \
		--parameter-overrides \
		"ImageUri=$repo:$tag" CpuArchitecture=ARM64 "VpcId=$vpc" "SubnetIds=$subnets" \
		AllowedCidr= "ClientSecurityGroupId=$clientsg" "ChUsersSecretArn=$arn" \
		"TablePrefix=$PREFIX" GlobalMode=none WorkerMode=in-process InlineViews=true \
		MaxExecutionTime=100 LoadBalancerIdleTimeout=110 DeregistrationDelay=110 \
		ChserverCpu=4096 ChserverMemory=8192 ChserverDesiredCount=1 ChserverMaxCount=0 \
		LogLevel=info LogRetentionDays=14
	cmd_status
}

# endpoints prints the shell lines the client needs to reach both servers.
endpoints() {
	local arn th ch
	arn=$(secret_arn)
	th=$(output "$ECS_STACK" ClickHouseUrl)
	ch=$(output "$BENCH_STACK" ClickHouseHost)
	cat <<EOF
users=\$(aws secretsmanager get-secret-value --region $AWS_REGION --secret-id '$arn' --query SecretString --output text)
cred=\$(echo "\$users" | cut -d, -f1)
export CHBENCH_TWINHOUSE="http://\$cred@${th#http://}" CHBENCH_CLICKHOUSE="http://\$cred@$ch"
EOF
}

cmd_check() {
	remote check "set -euo pipefail
$(endpoints)
for url in \"\$CHBENCH_TWINHOUSE\" \"\$CHBENCH_CLICKHOUSE\"; do
  host=\${url#*@}
  printf '%s ping: ' \"\$host\"; curl -sSf --max-time 10 \"http://\$host/ping\"
  printf '%s version: ' \"\$host\"; curl -sSf --max-time 10 \"\$url/?max_execution_time=5\" --data-binary 'SELECT version()'
done" 120
}

cmd_run() {
	local bucket name tag script
	bucket=$(output "$BENCH_STACK" BucketName)
	name=chbench-aws-$(date +%Y%m%d-%H%M%S)
	tag=$(git rev-parse --short HEAD)
	log "building chbench for linux/arm64"
	mkdir -p bin/aws
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/aws/chbench ./cmd/chbench
	aws s3 cp bin/aws/chbench "s3://$bucket/chbench" --only-show-errors
	script="set -euo pipefail
cd /tmp
aws s3 cp s3://$bucket/chbench ./chbench --only-show-errors
chmod +x chbench
$(endpoints)
export CHBENCH_COMMIT=$tag
./chbench -settle 2m -align-steps -timeout 100s -out $name.json $* > $name.md
cat $name.md
aws s3 cp $name.json s3://$bucket/results/$name.json --only-show-errors
aws s3 cp $name.md s3://$bucket/results/$name.md --only-show-errors"
	remote "$name" "$script" 14400
	aws s3 cp "s3://$bucket/results/$name.json" "bin/aws/$name.json" --only-show-errors
	aws s3 cp "s3://$bucket/results/$name.md" "bin/aws/$name.md" --only-show-errors
	log "results: bin/aws/$name.md and bin/aws/$name.json"
	echo "DynamoDB capacity per step (CloudWatch lags a few minutes): $0 capacity bin/aws/$name.json"
}

cmd_capacity() {
	local json=${1:?usage: capacity bin/aws/<run>.json}
	local tables
	tables=$(aws dynamodb list-tables --output text --query 'TableNames[]' | tr '\t' '\n' | grep "^$PREFIX" || true)
	# shellcheck disable=SC2086 # one argument per table
	python3 "$DIR/capacity.py" "$json" $tables
}

cmd_status() {
	local s
	for s in "$BENCH_STACK" "$ECS_STACK"; do
		log "$s"
		aws cloudformation describe-stacks --stack-name "$s" \
			--query 'Stacks[0].[StackStatus, Outputs[].[OutputKey, OutputValue]]' --output text 2>/dev/null || echo "not deployed"
	done
}

cmd_down() {
	local t bucket
	log "deleting $ECS_STACK (twinhouse must stop before its tables go)"
	aws cloudformation delete-stack --stack-name "$ECS_STACK"
	aws cloudformation wait stack-delete-complete --stack-name "$ECS_STACK"
	log "deleting DynamoDB tables $PREFIX*"
	for t in $(aws dynamodb list-tables --output text --query 'TableNames[]' | tr '\t' '\n' | grep "^$PREFIX" || true); do
		echo "$t"
		aws dynamodb delete-table --table-name "$t" >/dev/null
	done
	if bucket=$(output "$BENCH_STACK" BucketName 2>/dev/null) && [[ -n $bucket && $bucket != None ]]; then
		log "emptying s3://$bucket"
		aws s3 rm "s3://$bucket" --recursive --only-show-errors
	fi
	log "deleting $BENCH_STACK"
	aws cloudformation delete-stack --stack-name "$BENCH_STACK"
	aws cloudformation wait stack-delete-complete --stack-name "$BENCH_STACK"
	log "deleting secret $SECRET_NAME"
	aws secretsmanager delete-secret --secret-id "$SECRET_NAME" --force-delete-without-recovery >/dev/null || true
	log "done"
}

case ${1:-} in
up | check | run | capacity | status | down)
	c=$1
	shift
	"cmd_$c" "$@"
	;;
*)
	sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
