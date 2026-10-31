#!/usr/bin/env bash
# Generate local Redis traffic for manual observation of Redis Watcher.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: bash scripts/manual-test.sh [options]

Redis and Redis Watcher must already be running. This script never runs Docker.
Requires Bash, redis-cli (with -e support), and GNU coreutils (timeout, od, sleep).

Options:
  --count N        Number of random workload iterations (default: 60; max: 100000)
  --interval SEC   Pause between iterations (default: 0.2; 0 disables pacing)
  --port N         Local TCP port (default: 6379, host: 127.0.0.1)
  --socket PATH    Use a Unix socket instead of TCP
  --db N           Redis database, 0-15 (default: 0)
  --alerts         Also send harmless EVAL and SCRIPT EXISTS alert probes
  -h, --help       Show this help

Only randomly namespaced test keys are written/deleted. Keys receive a 5-minute
TTL and are deleted on exit, including Ctrl+C. Commands time out after 5 seconds.
Use REDISCLI_AUTH for a password if needed; never pass passwords as arguments.
--alerts may send real notifications through your configured alert channels.
EOF
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

require_value() {
  [[ -n ${2:-} ]] || die "$1 requires a value (see --help)"
}

validate_integer() {
  local name=$1 value=$2 minimum=$3 maximum=$4
  [[ $value =~ ^[0-9]{1,6}$ ]] || die "$name must be an integer from $minimum to $maximum"
  ((10#$value >= minimum && 10#$value <= maximum)) ||
    die "$name must be an integer from $minimum to $maximum"
}

count=60
interval=0.2
port=6379
socket=''
db=0
alerts=false

while (($# > 0)); do
  case $1 in
  --count | --interval | --port | --socket | --db)
    require_value "$1" "${2:-}"
    case $1 in
    --count) count=$2 ;;
    --interval) interval=$2 ;;
    --port) port=$2 ;;
    --socket) socket=$2 ;;
    --db) db=$2 ;;
    esac
    shift 2
    ;;
  --alerts)
    alerts=true
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *) die "unknown option: $1 (see --help)" ;;
  esac
done

validate_integer --count "$count" 1 100000
validate_integer --port "$port" 1 65535
validate_integer --db "$db" 0 15
[[ $interval =~ ^[0-9]{1,4}([.][0-9]{1,3})?$ ]] ||
  die '--interval must be non-negative seconds, e.g. 0, 0.2, or 1'
count=$((10#$count))
port=$((10#$port))
db=$((10#$db))

for dependency in redis-cli timeout od sleep; do
  command -v "$dependency" >/dev/null 2>&1 || die "required command not found: $dependency"
done

# Use arrays so paths, keys, and payloads remain single arguments. In particular,
# do not use eval or interpolate values into redis-cli's interactive command syntax.
cli=(redis-cli -e --raw -n "$db")
if [[ -n $socket ]]; then
  cli+=(-s "$socket")
  target=$socket
else
  cli+=(-h 127.0.0.1 -p "$port")
  target="127.0.0.1:$port"
fi

redis() {
  # -e makes Redis error replies fail too (not just connection errors).
  timeout --signal=TERM --kill-after=1s 5s "${cli[@]}" "$@"
}

send() {
  printf '  >'
  printf ' %q' "$@"
  printf '\n'
  redis "$@" >/dev/null || die "Redis command failed: $1"
}

printf 'Checking Redis at %s (DB %s)...\n' "$target" "$db"
pong=$(redis PING) || die 'cannot reach Redis; start your local stack separately and check access'
[[ $pong == PONG ]] || die 'expected PONG from Redis'

# A fresh, unpredictable namespace prevents collisions with application keys
# and concurrent test runs. RANDOM below is only for non-sensitive test data.
run_id=$(od -An -N12 -tx1 /dev/urandom)
run_id=${run_id//[[:space:]]/}
[[ ${#run_id} == 24 ]] || die 'could not generate a test namespace'
prefix="redis-watcher:manual:$run_id"
keys=("$prefix:string" "$prefix:hash" "$prefix:list" "$prefix:set" "$prefix:zset")
ttl=300

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  printf '\nCleaning up this run\047s five test keys...\n'
  if ! redis DEL "${keys[@]}" >/dev/null; then
    printf 'Warning: cleanup failed; test keys with an assigned TTL will expire. Namespace: %s\n' "$prefix" >&2
    if ((status == 0)); then
      status=1
    fi
  fi
  exit "$status"
}

# Register before the first write. SIGKILL/power loss cannot run these traps;
# TTLs are a fallback (a hard stop between creation and EXPIRE can leave a key).
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'Test namespace: %s\n' "$prefix"
printf 'Sending %s iterations, with %ss between iterations. Ctrl+C stops and cleans up.\n' "$count" "$interval"

# Seed the collection types and set TTLs before the randomized workload.
send SET "${keys[0]}" 'initial value' EX "$ttl"
send HSET "${keys[1]}" message 'initial value'
send EXPIRE "${keys[1]}" "$ttl"
send LPUSH "${keys[2]}" 'initial value'
send EXPIRE "${keys[2]}" "$ttl"
send SADD "${keys[3]}" 'initial value'
send EXPIRE "${keys[3]}" "$ttl"
send ZADD "${keys[4]}" 0 'initial value'
send EXPIRE "${keys[4]}" "$ttl"

if $alerts; then
  printf '\nAlert probes enabled: configured channels may send real notifications.\n'
  send EVAL 'return ARGV[1]' 0 "manual alert probe $run_id"
  send SCRIPT EXISTS 0000000000000000000000000000000000000000
fi

for ((i = 1; i <= count; i++)); do
  # Include spaces, quotes, backslashes, and a newline to exercise MONITOR escaping.
  printf -v message 'message %s: random=%s, "quoted", backslash=\\, newline=\nend' "$i" "$RANDOM"
  printf '\nIteration %s/%s\n' "$i" "$count"
  case $((RANDOM % 6)) in
  0)
    send SET "${keys[0]}" "$message" EX "$ttl"
    send GET "${keys[0]}"
    ;;
  1)
    send HSET "${keys[1]}" message "$message" sequence "$i"
    send EXPIRE "${keys[1]}" "$ttl"
    send HGETALL "${keys[1]}"
    ;;
  2)
    send LPUSH "${keys[2]}" "$message"
    send EXPIRE "${keys[2]}" "$ttl"
    send LTRIM "${keys[2]}" 0 9
    send LRANGE "${keys[2]}" 0 2
    ;;
  3)
    send SADD "${keys[3]}" "$message"
    send EXPIRE "${keys[3]}" "$ttl"
    send SISMEMBER "${keys[3]}" "$message"
    send SREM "${keys[3]}" "$message"
    ;;
  4)
    send ZADD "${keys[4]}" "$RANDOM" "$message"
    send EXPIRE "${keys[4]}" "$ttl"
    send ZRANGE "${keys[4]}" 0 2 WITHSCORES
    send ZREM "${keys[4]}" "$message"
    ;;
  5)
    send PUBLISH "$prefix:channel" "$message"
    ;;
  esac
  if ((i < count)); then
    sleep "$interval"
  fi
done

printf '\nTraffic generation complete. Check Watcher audit outputs and /metrics; this script does not assert Watcher delivery.\n'
