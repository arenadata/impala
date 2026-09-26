#!/bin/bash
#
# Builds Impala with its tests inside the CI environment image
# (docker/ci/env.Dockerfile), with the source tree bind-mounted at IMPALA_HOME.
# The backend is built as shared libraries (buildall.sh -so): the test executables
# link against them instead of each carrying a static copy of Impala, which keeps
# the build tree and the link memory within a CI runner.
#
# Optional:
#   BUILD_THREADS   overrides the parallel job count (impala-config.sh defaults
#                   to min(cores, total RAM / 2 GB))
#   /opt/m2-seed    a Maven repository layout copied into ~/.m2/repository first,
#                   never overwriting, for networks that cannot reach a repository
#
# /opt/impala-pydeps (from the env image) stands in for the PyPI download.
set -eo pipefail

cd "${IMPALA_HOME:?}"
if [[ -d /opt/m2-seed ]]; then
  cp -rn /opt/m2-seed/. "$HOME/.m2/repository/"
fi
if [[ -n "${BUILD_THREADS:-}" ]]; then
  export IMPALA_BUILD_THREADS="$BUILD_THREADS"
fi

if [[ -d /opt/impala-pydeps ]]; then
  cp -rn /opt/impala-pydeps/. infra/python/deps/
  export SKIP_PYTHON_DOWNLOAD=true
fi

ccache -z
./buildall.sh -release -skiptests -so
ccache -s
