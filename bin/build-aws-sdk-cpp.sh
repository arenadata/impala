#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Builds aws-sdk-cpp (S3 only, static libraries) for the experimental native S3 reader
# (--s3_native_reader) and installs it into AWS_SDK_CPP_HOME. Called by buildall.sh after
# the toolchain bootstrap; needs bin/impala-config.sh to be sourced.
#
# The SDK is compiled with the toolchain gcc and linked against the toolchain curl and
# zlib, so that its static libraries match impalad. Nothing is done if the installed SDK
# was built by this script with the same versions (stamp file), or if AWS_SDK_CPP_HOME
# holds an SDK installed by other means (no stamp file).

set -euo pipefail

: ${IMPALA_HOME:?"IMPALA_HOME is not set; source bin/impala-config.sh"}
: ${AWS_SDK_CPP_HOME:?"AWS_SDK_CPP_HOME is not set"}
: ${IMPALA_AWS_SDK_CPP_VERSION:?"IMPALA_AWS_SDK_CPP_VERSION is not set"}
: ${IMPALA_TOOLCHAIN_PACKAGES_HOME:?"IMPALA_TOOLCHAIN_PACKAGES_HOME is not set"}

ARCH=$(uname -m)
TOOLCHAIN_BUILD_ID_VAR="IMPALA_TOOLCHAIN_BUILD_ID_${ARCH^^}"
TOOLCHAIN_BUILD_ID=${!TOOLCHAIN_BUILD_ID_VAR:-unknown}
GCC_HOME="${IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-${IMPALA_GCC_VERSION}"
CURL_HOME="${IMPALA_TOOLCHAIN_PACKAGES_HOME}/curl-${IMPALA_CURL_VERSION}"
ZLIB_HOME="${IMPALA_TOOLCHAIN_PACKAGES_HOME}/zlib-${IMPALA_ZLIB_VERSION}"
REPO=${IMPALA_AWS_SDK_CPP_REPO:-https://github.com/aws/aws-sdk-cpp.git}

STAMP_FILE="${AWS_SDK_CPP_HOME}/.impala-build-stamp"
STAMP="aws-sdk-cpp=${IMPALA_AWS_SDK_CPP_VERSION} gcc=${IMPALA_GCC_VERSION}"
STAMP+=" curl=${IMPALA_CURL_VERSION} zlib=${IMPALA_ZLIB_VERSION}"
STAMP+=" toolchain=${TOOLCHAIN_BUILD_ID}"

if [[ -f "${AWS_SDK_CPP_HOME}/lib/libaws-cpp-sdk-s3.a" \
    || -f "${AWS_SDK_CPP_HOME}/lib64/libaws-cpp-sdk-s3.a" ]]; then
  if [[ ! -f "${STAMP_FILE}" ]]; then
    echo "aws-sdk-cpp found in ${AWS_SDK_CPP_HOME} (not built by this script), using it"
    exit 0
  fi
  if [[ "$(cat "${STAMP_FILE}")" == "${STAMP}" ]]; then
    echo "aws-sdk-cpp ${IMPALA_AWS_SDK_CPP_VERSION} is up to date in ${AWS_SDK_CPP_HOME}"
    exit 0
  fi
  echo "aws-sdk-cpp in ${AWS_SDK_CPP_HOME} was built with different versions, rebuilding"
  echo "  installed: $(cat "${STAMP_FILE}")"
  echo "  required:  ${STAMP}"
fi

# Only a directory installed by this script (stamp file) or an empty one is replaced.
if [[ -d "${AWS_SDK_CPP_HOME}" && ! -f "${STAMP_FILE}" \
    && -n "$(ls -A "${AWS_SDK_CPP_HOME}")" ]]; then
  echo "ERROR: ${AWS_SDK_CPP_HOME} is not empty and holds no aws-sdk-cpp installed by" \
      "this script; set AWS_SDK_CPP_HOME to another directory" >&2
  exit 1
fi

for dir in "${GCC_HOME}" "${CURL_HOME}" "${ZLIB_HOME}"; do
  if [[ ! -d "${dir}" ]]; then
    echo "ERROR: ${dir} not found; run bin/bootstrap_toolchain.py first" >&2
    exit 1
  fi
done

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/aws-sdk-cpp.XXXXXX")
# Installed next to the final location and moved into place when complete, so that an
# interrupted build never leaves a half-installed SDK behind.
INSTALL_TMP="${AWS_SDK_CPP_HOME}.tmp.$$"
trap 'rm -rf "${WORK_DIR}" "${INSTALL_TMP}"' EXIT

echo "Building aws-sdk-cpp ${IMPALA_AWS_SDK_CPP_VERSION} into ${AWS_SDK_CPP_HOME}"
git clone --quiet --depth 1 --branch "${IMPALA_AWS_SDK_CPP_VERSION}" \
    --recurse-submodules --shallow-submodules "${REPO}" "${WORK_DIR}/src"

# The rpath lets executables built during the SDK configuration find the toolchain
# libstdc++; LD_LIBRARY_PATH would break the system tools (GLIBCXX version mismatch).
cmake -S "${WORK_DIR}/src" -B "${WORK_DIR}/build" \
    -DCMAKE_C_COMPILER="${GCC_HOME}/bin/gcc" \
    -DCMAKE_CXX_COMPILER="${GCC_HOME}/bin/g++" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_TMP}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_ONLY=s3 \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_TESTING=OFF \
    -DAUTORUN_UNIT_TESTS=OFF \
    -DUSE_OPENSSL=ON \
    -DCURL_INCLUDE_DIR="${CURL_HOME}/include" \
    -DCURL_LIBRARY="${CURL_HOME}/lib/libcurl.a" \
    -DZLIB_INCLUDE_DIR="${ZLIB_HOME}/include" \
    -DZLIB_LIBRARY="${ZLIB_HOME}/lib/libz.a" \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,-rpath,${GCC_HOME}/lib64"
cmake --build "${WORK_DIR}/build" -j "${IMPALA_BUILD_THREADS:-$(nproc)}"
cmake --install "${WORK_DIR}/build"

echo "${STAMP}" > "${INSTALL_TMP}/.impala-build-stamp"
rm -rf "${AWS_SDK_CPP_HOME}"
mv "${INSTALL_TMP}" "${AWS_SDK_CPP_HOME}"
echo "aws-sdk-cpp ${IMPALA_AWS_SDK_CPP_VERSION} installed in ${AWS_SDK_CPP_HOME}"
