#!/usr/bin/env bash
# Runs cmd/chbench on AWS: twinhouse on ECS + DynamoDB against ClickHouse on
# EC2, from a client instance in the same VPC (deploy/aws-bench/README.md).
#
#   deploy/aws-bench/bench.sh up                # secret, bench stack, image, twinhouse ECS stack
#   deploy/aws-bench/bench.sh check             # both servers answer from the client
#   deploy/aws-bench/bench.sh run [-size M ...] # one chbench run; results in bin/aws/
#   deploy/aws-bench/bench.sh provisioned <WCU> [-only W1 ...]  # the same on provisioned tables
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
# Keys in the environment would win over the profile.
if [[ -n ${AWS_ACCESS_KEY_ID:-}${AWS_SESSION_TOKEN:-} ]]; then
	echo "AWS_ACCESS_KEY_ID or AWS_SESSION_TOKEN is set and would override profile $AWS_PROFILE; unset it" >&2
	exit 1
fi
echo "AWS profile $AWS_PROFILE, region $AWS_REGION" >&2
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
	local client group cmd status since ids shown
	client=$(output "$BENCH_STACK" ClientInstanceId)
	group=$(output "$BENCH_STACK" RunLogGroup)
	cmd=$(aws ssm send-command --instance-ids "$client" --document-name AWS-RunShellScript \
		--comment "$label" --timeout-seconds 600 \
		--cloud-watch-output-config "CloudWatchOutputEnabled=true,CloudWatchLogGroupName=$group" \
		--parameters "$(python3 -c 'import json,sys; print(json.dumps({"commands":[sys.argv[1]],"executionTimeout":[sys.argv[2]]}))' "$script" "$timeout")" \
		--query Command.CommandId --output text)
	log "$label: SSM command $cmd on $client"
	# Polls the command's log streams instead of `aws logs tail --follow`,
	# which buffers its output when it is not a terminal and loses it when
	# stopped. since is the newest timestamp shown; ids are the events shown
	# at that timestamp, which the next poll returns again.
	since=0 ids="" shown=0 status=Pending
	poll() {
		local out t
		out=$(aws logs filter-log-events --log-group-name "$group" --log-stream-name-prefix "$cmd/" \
			--start-time "$since" --output json 2>/dev/null) || return 0
		jq -r --arg ids "$ids" '($ids | split(" ")) as $skip
			| [.events[] | select(.eventId as $e | $skip | index($e) | not)]
			| sort_by(.timestamp)[] | .message | rtrimstr("\n")' <<<"$out"
		t=$(jq '[.events[].timestamp] | max // empty' <<<"$out")
		if [[ -n $t ]]; then
			shown=1 since=$t
			ids=$(jq -r --argjson t "$t" '[.events[] | select(.timestamp == $t) | .eventId] | join(" ")' <<<"$out")
		fi
	}
	while [[ $status == Pending || $status == InProgress || $status == Delayed ]]; do
		sleep 10
		poll
		status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$client" \
			--query Status --output text 2>/dev/null || echo Pending)
	done
	sleep 5 # the last lines reach CloudWatch after the command ends
	poll
	if [[ $shown == 0 ]]; then
		# Nothing in CloudWatch yet: print what SSM kept (the last 24,000
		# characters of each stream).
		aws ssm get-command-invocation --command-id "$cmd" --instance-id "$client" \
			--query '[StandardOutputContent, StandardErrorContent]' --output text
	fi
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

# chbench_upload builds chbench for the client and copies it to the bucket.
chbench_upload() {
	bucket=$(output "$BENCH_STACK" BucketName)
	log "building chbench for linux/arm64"
	mkdir -p bin/aws
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/aws/chbench ./cmd/chbench
	aws s3 cp bin/aws/chbench "s3://$bucket/chbench" --only-show-errors
}

# chbench_remote runs the uploaded chbench on the client with its arguments.
chbench_remote() { # label timeout args...
	local label=$1 timeout=$2
	shift 2
	remote "$label" "set -euo pipefail
cd /tmp
aws s3 cp s3://$bucket/chbench ./chbench --only-show-errors
chmod +x chbench
$(endpoints)
export CHBENCH_COMMIT=$(git rev-parse --short HEAD)
$*" "$timeout"
}

# run_chbench runs one measured chbench and downloads its results.
run_chbench() {
	local name
	name=chbench-aws-$(date +%Y%m%d-%H%M%S)
	chbench_remote "$name" 14400 "./chbench -settle 2m -align-steps -timeout 100s -out $name.json $* > $name.md
cat $name.md
aws s3 cp $name.json s3://$bucket/results/$name.json --only-show-errors
aws s3 cp $name.md s3://$bucket/results/$name.md --only-show-errors"
	aws s3 cp "s3://$bucket/results/$name.json" "bin/aws/$name.json" --only-show-errors
	aws s3 cp "s3://$bucket/results/$name.md" "bin/aws/$name.md" --only-show-errors
	log "results: bin/aws/$name.md and bin/aws/$name.json"
	echo "DynamoDB capacity per step (CloudWatch lags a few minutes): $0 capacity bin/aws/$name.json"
}

cmd_run() {
	chbench_upload
	run_chbench "$@"
}

# cmd_provisioned runs chbench on tables switched to provisioned capacity:
# it creates the schema, switches the tables named in PROV_TABLES (default
# events) to <write units> and PROV_READ_UNITS, loads with -skip-setup, then
# recreates the schema so twinhouse drops the provisioned tables, and waits
# until they are gone: provisioned capacity bills by the hour while a table
# exists.
cmd_provisioned() {
	local wcu=${1:-} rcu=${PROV_READ_UNITS:-1000} t pattern
	[[ $wcu =~ ^[1-9][0-9]*$ ]] || { echo "usage: $0 provisioned <write units> [chbench args]" >&2; exit 2; }
	shift
	# Globals: the EXIT trap runs after this function has returned.
	prov_args="$*"
	pattern="^${PREFIX}chbench\\.($(tr ' ' '|' <<<"${PROV_TABLES:-events}"))\\."
	log "provisioned: $wcu write and $rcu read units per table, about \$$(python3 -c "print(round($wcu*0.00065+$rcu*0.00013, 2))") an hour each (us-east-1 list price), billed by the hour"
	chbench_upload
	chbench_remote setup 900 "./chbench -timeout 100s -setup-only $prov_args"
	prov_tables=$(aws dynamodb list-tables --output text --query 'TableNames[]' | tr '\t' '\n' | grep -E "$pattern" || true)
	[[ -n $prov_tables ]] || { echo "no table matches $pattern" >&2; exit 1; }
	trap unprovision EXIT
	for t in $prov_tables; do
		log "switching $t to provisioned"
		aws dynamodb update-table --table-name "$t" --billing-mode PROVISIONED \
			--provisioned-throughput "ReadCapacityUnits=$rcu,WriteCapacityUnits=$wcu" >/dev/null
		aws dynamodb wait table-exists --table-name "$t"
		aws dynamodb describe-table --table-name "$t" \
			--query 'Table.[TableStatus, BillingModeSummary.BillingMode, ProvisionedThroughput.WriteCapacityUnits, ProvisionedThroughput.ReadCapacityUnits]' --output text
	done
	run_chbench -skip-setup "$@"
}

# unprovision recreates the schema, so twinhouse drops the provisioned
# tables, and waits until they are gone (EXIT trap of cmd_provisioned).
# shellcheck disable=SC2329 # called by the EXIT trap
unprovision() {
	local left _
	log "recreating the schema so twinhouse drops the provisioned tables"
	chbench_remote setup 900 "./chbench -timeout 100s -setup-only $prov_args" || true
	for _ in $(seq 60); do
		left=$(aws dynamodb list-tables --output text --query 'TableNames[]' | tr '\t' '\n' | grep -Fx "$prov_tables" || true)
		[[ -z $left ]] && { log "provisioned tables deleted"; return; }
		sleep 10
	done
	echo "still present after 10 minutes, delete them by hand: $left" >&2
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
up | check | run | provisioned | capacity | status | down)
	c=$1
	shift
	"cmd_$c" "$@"
	;;
*)
	sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
