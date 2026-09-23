#!/bin/bash
#
# Runs a command in the CI environment image (docker/ci/env.Dockerfile) with this
# checkout bind-mounted at IMPALA_HOME, as used by the CI workflow and locally:
#
#   docker/ci/run-in-env.sh "$(docker/ci/env-image.sh tag)" docker/ci/ci-build.sh
#   docker/ci/run-in-env.sh "$(docker/ci/env-image.sh tag)" docker/ci/run-unit-tests.sh ci-results
#
# --init forwards signals, so a cancelled or timed-out step stops the build. State
# kept between calls lives under CI_STATE_DIR (default ../ci-cache): ccache/, m2/
# and tmp/, the last so the container's /tmp is on the host disk rather than the
# overlay filesystem. The container runs as uid 1000, the image's impdev user;
# when the host user differs, as on GitHub-hosted runners, the mounted directories
# are handed over with sudo for the call and handed back afterwards.
#
# Optional:
#   CI_M2_SEED               Maven repository layout mounted at /opt/m2-seed (see ci-build.sh)
#   BUILD_THREADS            passed through to ci-build.sh
#   BE_TESTS / FE_TESTS      passed through to run-unit-tests.sh
set -euo pipefail

image="${1:?usage: run-in-env.sh <image> <command...>}"
shift
root="$(cd "$(dirname "$0")/../.." && pwd)"
state="${CI_STATE_DIR:-$root/../ci-cache}"
mkdir -p "$state/ccache" "$state/m2" "$state/tmp"

args=(--rm --init --hostname impala-ci --user 1000:1000
      -v "$root":/home/impdev/Impala
      -v "$state/ccache":/home/impdev/.ccache
      -v "$state/m2":/home/impdev/.m2/repository
      -v "$state/tmp":/tmp)
if [[ -n "${CI_M2_SEED:-}" ]]; then
  args+=(-v "$CI_M2_SEED":/opt/m2-seed:ro)
fi
for var in BUILD_THREADS BE_TESTS FE_TESTS; do
  if [[ -n "${!var:-}" ]]; then
    args+=(-e "$var=${!var}")
  fi
done
if [[ "$(id -u)" == 1000 ]]; then
  exec docker run "${args[@]}" "$image" "$@"
fi
owner="$(id -u):$(id -g)"
sudo chown -R 1000:1000 "$root" "$state"
trap 'sudo chown -R "$owner" "$root" "$state"' EXIT
docker run "${args[@]}" "$image" "$@"
