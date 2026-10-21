# SPDX-License-Identifier: Apache-2.0
#
# corTestFunctions.sh - the verbs this repo's tests use; corTest sources it into every section
#
# A test starts a broker that loads THIS build of the plugin (by path - nothing is installed), on a port
# of its own, with ramDB as its store: no database server is needed. A test waits on and asserts on what
# the API says, never on the broker's log - the log is kept for reading when a test fails.
#
#   LWM2M_BROKER   the broker binary (default: coraine on PATH - the coraine package, or
#                 `. <dir>/install/env` after coraine-build)
#   LWM2M_PORT     its port (default 19033 - clear of a broker on 1026, of coraine's own test ports and of the other bridges' tests)
#   LWM2M_PLUGIN   the plugin under test (default: iotalwm2m.so in this repo)
#   LWM2M_TEST_DIR scratch directory: the bridge configuration, the broker's pid and log
#
LWM2M_BROKER="${LWM2M_BROKER:-coraine}"
LWM2M_PORT="${LWM2M_PORT:-19033}"
LWM2M_PLUGIN="${LWM2M_PLUGIN:-${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}/iotalwm2m.so}"
LWM2M_TEST_DIR="${LWM2M_TEST_DIR:-/tmp/iotalwm2m-bridge-test}"


# -----------------------------------------------------------------------------
#
# brokerStart [broker options...] - start the broker with the plugin and $LWM2M_TEST_DIR/bridges.json
#
# Returns once the port answers; 1 (with the log's tail on stderr) if the broker died on the way.
#
brokerStart() {
  brokerStop

  "$LWM2M_BROKER" --port "$LWM2M_PORT" --foreground --pretty-print 2 --httpEndpoint "http://localhost:$LWM2M_PORT" \
                 --database ramDB --bridges "$LWM2M_PLUGIN" --bridgeConfig "$LWM2M_TEST_DIR/bridges.json" "$@" \
                 > "$LWM2M_TEST_DIR/broker.log" 2>&1 &
  echo $! > "$LWM2M_TEST_DIR/broker.pid"

  local i
  for ((i = 0; i < 100; i++)); do
    curl -s -o /dev/null "localhost:$LWM2M_PORT/ngsi-ld/v1/types" && return 0
    if ! kill -0 "$(cat "$LWM2M_TEST_DIR/broker.pid")" 2>/dev/null; then
      echo "brokerStart: the broker died - $LWM2M_TEST_DIR/broker.log:" >&2
      tail -20 "$LWM2M_TEST_DIR/broker.log" >&2
      return 1
    fi
    sleep 0.1
  done

  echo "brokerStart: no answer on port $LWM2M_PORT after 10s" >&2
  return 1
}


# -----------------------------------------------------------------------------
#
# brokerStop - stop the broker brokerStart started (by its pid, never by name)
#
brokerStop() {
  local pidFile="$LWM2M_TEST_DIR/broker.pid"
  [ -f "$pidFile" ] || return 0

  local pid i
  pid=$(cat "$pidFile")
  kill "$pid" 2>/dev/null
  for ((i = 0; i < 50; i++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null
  rm -f "$pidFile"
}


# -----------------------------------------------------------------------------
#
# The LwM2M server port - where the clients register (default 15683 - clear of CoAP's 5683)
#
LWM2M_COAP_PORT="${LWM2M_COAP_PORT:-15683}"


# -----------------------------------------------------------------------------
#
# clientStart <endpoint name> <control port> [lwm2mClient.py options...] - an LwM2M client (lwm2mClient.py);
# returns once it has registered. What the server asks it is in $LWM2M_TEST_DIR/<name>.log
#
clientStart() {
  local ep="$1" ctl="$2" i
  shift 2
  python3 "$(dirname "${BASH_SOURCE[0]}")/lwm2mClient.py" --server "127.0.0.1:$LWM2M_COAP_PORT" --ep "$ep" --control "$ctl" \
          --log "$LWM2M_TEST_DIR/$ep.log" "$@" > "$LWM2M_TEST_DIR/$ep.out" 2>&1 &
  echo "$! $ctl" > "$LWM2M_TEST_DIR/$ep.pid"

  for ((i = 0; i < 50; i++)); do
    grep -q "^REGISTERED" "$LWM2M_TEST_DIR/$ep.log" 2>/dev/null && return 0
    sleep 0.1
  done

  echo "clientStart: $ep did not register - $LWM2M_TEST_DIR/$ep.out:" >&2
  tail -20 "$LWM2M_TEST_DIR/$ep.out" >&2
  return 1
}


# -----------------------------------------------------------------------------
#
# clientStop - every client clientStart started (by pid)
#
clientStop() {
  local f
  for f in "$LWM2M_TEST_DIR"/*.pid; do
    [ -f "$f" ] || continue
    [ "$(basename "$f")" = broker.pid ] && continue
    kill "$(cut -d" " -f1 "$f")" 2>/dev/null
    rm -f "$f"
  done
}


# -----------------------------------------------------------------------------
#
# clientCtl <endpoint name> <request> - the client told something: "set?path=3303/0/5700&value=22", "update",
# "deregister", "register"
#
clientCtl() {
  curl -s -o /dev/null "localhost:$(cut -d" " -f2 "$LWM2M_TEST_DIR/$1.pid")/$2"
}


# -----------------------------------------------------------------------------
#
# clientAwait <endpoint name> <line> [secs] - wait for the client to have been asked <line>; the line echoed
#
clientAwait() {
  local secs="${3:-5}" i

  for ((i = 0; i < secs * 10; i++)); do
    grep -qxF -- "$2" "$LWM2M_TEST_DIR/$1.log" 2>/dev/null && { echo "$1: $2"; return 0; }
    sleep 0.1
  done

  echo "clientAwait: $1 was never asked '$2' - it was asked: $(cat "$LWM2M_TEST_DIR/$1.log" 2>/dev/null)" >&2
  return 1
}


# -----------------------------------------------------------------------------
#
# clientLog <endpoint name> - what the server asked the client
#
clientLog() {
  cat "$LWM2M_TEST_DIR/$1.log"
}


# -----------------------------------------------------------------------------
#
# entityAwait <id> <text> [secs] - wait until the entity (keyValues) has <text>
#
entityAwait() {
  local url="localhost:$LWM2M_PORT/ngsi-ld/v1/entities/$1?options=keyValues" secs="${3:-5}" i

  for ((i = 0; i < secs * 10; i++)); do
    curl -s "$url" | grep -qF -- "$2" && return 0
    sleep 0.1
  done

  echo "entityAwait: $1 is $(curl -s "$url"), without '$2', after ${secs}s" >&2
  return 1
}


# -----------------------------------------------------------------------------
#
# entityGet <id> [options] - the entity, its members sorted
#
entityGet() {
  curl -s "localhost:$LWM2M_PORT/ngsi-ld/v1/entities/$1${2:+?options=$2}" \
    | python3 -c 'import sys, json; print(json.dumps(json.load(sys.stdin), indent=2, sort_keys=True))'
}


# -----------------------------------------------------------------------------
#
# attrPatch <id> <attr> <value as JSON> - PATCH an attribute's value; the HTTP status
#
attrPatch() {
  curl -s -o /dev/null -w "PATCH $2: %{http_code}\n" -X PATCH "localhost:$LWM2M_PORT/ngsi-ld/v1/entities/$1/attrs/$2" \
       -H 'Content-Type: application/json' -d "{\"value\": $3}"
}
