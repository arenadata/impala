#!/bin/bash
#
# Prints the Arenadata component versions pinned in bin/impala-config.sh as
# NAME=VALUE lines, so docker/ci/env.Dockerfile never carries its own (drifting)
# copy. Used by the CI workflow and for local builds:
#
#   docker buildx build -f docker/ci/env.Dockerfile \
#       $(docker/ci/component-versions.sh | sed 's/^/--build-arg /') ...
#
set -euo pipefail

CONFIG="$(dirname "$0")/../../bin/impala-config.sh"

get() {
  local v
  v=$(sed -nE "s/^export $1=\"?([^\"]+)\"?$/\1/p" "$CONFIG" | head -1)
  [[ -n "$v" ]] || { echo "$1 not found in $CONFIG" >&2; exit 1; }
  echo "$v"
}

echo "IMPALA_VERSION=$(get IMPALA_VERSION)"
echo "HADOOP_VERSION=$(get APACHE_HADOOP_VERSION)"
echo "HIVE_VERSION=$(get APACHE_HIVE_VERSION)"
