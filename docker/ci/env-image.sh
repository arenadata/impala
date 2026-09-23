#!/bin/bash
#
# Names, fetches or builds the CI environment image (docker/ci/env.Dockerfile).
#
#   env-image.sh tag              print the image reference for this checkout
#   env-image.sh ensure [--push]  use a local copy, or pull it, or build it (and
#                                 with --push publish it)
#
# The tag is a hash of every file the image is built from, so a change to any of
# them selects (and, if needed, builds) a new image; CI and local runs of the same
# checkout resolve the same image. CI_ENV_REPOSITORY overrides the repository.
set -euo pipefail

cd "$(dirname "$0")/../.."
repository="${CI_ENV_REPOSITORY:-ghcr.io/arenadata/impala/ci-env}"
inputs=(docker/ci/env.Dockerfile docker/ci/settings.xml docker/ci/component-versions.sh
        docker/ci/maven-repo-filters/* bin/impala-config.sh bin/impala-config-branch.sh
        bin/impala-config-java.sh bin/bootstrap_toolchain.py
        infra/python/deps/*requirements.txt infra/python/deps/download_requirements
        infra/python/deps/pip_download.py)

image() {
  local hash
  hash="$(for f in "${inputs[@]}"; do echo "$f"; cat "$f"; done | sha256sum | cut -c1-16)"
  echo "$repository:$hash"
}

case "${1:-}" in
  tag)
    image
    ;;
  ensure)
    img="$(image)"
    if docker image inspect "$img" > /dev/null 2>&1 || docker pull "$img"; then exit 0; fi
    args=()
    while read -r v; do args+=(--build-arg "$v"); done \
      < <(docker/ci/component-versions.sh | grep -v IMPALA_VERSION)
    docker buildx build -f docker/ci/env.Dockerfile --load -t "$img" "${args[@]}" .
    if [[ "${2:-}" == --push ]]; then docker push "$img"; fi
    ;;
  *)
    echo "usage: $0 tag | ensure [--push]" >&2
    exit 2
    ;;
esac
