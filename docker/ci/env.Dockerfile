# syntax=docker/dockerfile:1.7
#
# CI build environment for Impala on Ubuntu 22.04: everything the build and the
# unit tests need except the source tree, which is bind-mounted at run time.
# Built and published by docker/ci/env-image.sh, used by docker/ci/run-in-env.sh.
# Stages:
#   hadoop-builder / hive-src   Arenadata-versioned components (built/fetched from
#       source @ git tags) pre-seeded into the toolchain so bootstrap_toolchain.py
#       skips the www.apache.org downloads.
#   ci-env   build dependencies, JDK 21, Maven, and the native toolchain at
#       /opt/impala-toolchain, outside IMPALA_HOME so the source bind mount
#       does not hide it.
#

############################################################################
# Component build: Apache Hadoop (Arenadata fork, from source @ git tag).
# Builds only hadoop-hdfs-native-client (-pl ... -am), producing the include/hdfs.h
# and lib/native/libhdfs.{so,a} that FindHDFS.cmake needs. libhdfs++ cannot be
# skipped (libhdfs reuses its x-platform object libraries), hence the protobuf C++
# library and boost below. JDK 11 rather than the 8 that BUILDING.txt asks for:
# test classes are compiled even with -DskipTests and some call Java 9+ APIs.
# Not the headless JDK: hadoop-common's FindJNI needs the AWT libraries.
############################################################################
FROM ubuntu:22.04 AS hadoop-builder

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

ARG HADOOP_VERSION
ARG HADOOP_TAG=v${HADOOP_VERSION}
ARG PROTOBUF_TAG=21.12
ARG PROTOBUF_VERSION=3.21.12

# Native + Java build dependencies (BUILDING.txt: build-essential autoconf
# automake libtool cmake zlib1g-dev pkg-config libssl-dev libsasl2-dev + boost).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates curl wget git unzip ccache \
        openjdk-11-jdk \
        build-essential autoconf automake libtool cmake pkg-config make \
        zlib1g-dev libssl-dev libsasl2-dev libboost-all-dev \
        python3 language-pack-en \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# protobuf from source to match hadoop.protobuf.version: libhdfspp's
# find_package(Protobuf) needs the C++ library and headers, not just protoc, and
# Ubuntu 22.04's libprotobuf-dev is 3.12.
RUN cd /tmp \
 && curl -fsSLO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_TAG}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz" \
 && tar xzf "protobuf-cpp-${PROTOBUF_VERSION}.tar.gz" \
 && cd "protobuf-${PROTOBUF_VERSION}" \
 && ./configure --prefix=/usr/local \
 && make -j"$(nproc)" \
 && make install \
 && ldconfig \
 && cd /tmp && rm -rf "protobuf-cpp-${PROTOBUF_VERSION}.tar.gz" "protobuf-${PROTOBUF_VERSION}" \
 && protoc --version

# Maven 3.9.8 (same version used by the Impala build).
ARG MVN_VERSION=3.9.8
ARG MVN_SHA512=7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2
RUN cd /tmp \
 && wget -nv "https://archive.apache.org/dist/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz" \
 && echo "${MVN_SHA512}  apache-maven-${MVN_VERSION}-bin.tar.gz" | sha512sum -c - \
 && tar -C /usr/local -xzf "apache-maven-${MVN_VERSION}-bin.tar.gz" \
 && ln -sf "/usr/local/apache-maven-${MVN_VERSION}/bin/mvn" /usr/local/bin/mvn \
 && rm "apache-maven-${MVN_VERSION}-bin.tar.gz"

# Same Maven settings and options as the Impala build below: Central mirror, repo
# exclusions, groupId repository filters and finite network timeouts.
COPY docker/ci/settings.xml /root/.m2/settings.xml
COPY docker/ci/maven-repo-filters/ /root/maven-repo-filters/

ENV MAVEN_OPTS=-Xmx2g
RUN --mount=type=cache,target=/root/.m2/repository \
    --mount=type=cache,target=/root/.ccache \
    export CCACHE_DIR=/root/.ccache \
 && export CMAKE_C_COMPILER_LAUNCHER=ccache CMAKE_CXX_COMPILER_LAUNCHER=ccache \
 && test -n "${HADOOP_VERSION}" \
 && git clone --depth 1 --branch "${HADOOP_TAG}" https://github.com/arenadata/hadoop.git /src/hadoop \
 && cd /src/hadoop \
 && mvn -B -e package -Pnative -DskipTests -Dmaven.javadoc.skip=true \
        -Dmaven.resolver.transport=wagon -Dmaven.wagon.http.pool=false \
        -Dmaven.wagon.rto=60000 -Dmaven.wagon.http.connectionTimeout=30000 \
        -Dmaven.wagon.http.retryHandler.count=3 \
        -Daether.remoteRepositoryFilter.groupId=true \
        -Daether.remoteRepositoryFilter.groupId.basedir=/root/maven-repo-filters \
        -pl hadoop-hdfs-project/hadoop-hdfs-native-client -am \
 && D="/out/hadoop-${HADOOP_VERSION}" \
 && mkdir -p "$D/include" "$D/lib/native" \
 && cp "$(find hadoop-hdfs-project/hadoop-hdfs-native-client -path '*/libhdfs/include/hdfs/hdfs.h' | head -1)" "$D/include/" \
 && find hadoop-hdfs-project/hadoop-hdfs-native-client/target \
        \( -name 'libhdfs.so*' -o -name 'libhdfs.a' \) -exec cp -a {} "$D/lib/native/" \; \
 && find hadoop-common-project/hadoop-common/target \
        -name 'libhadoop.so*' -exec cp -a {} "$D/lib/native/" \; \
 && test -f "$D/include/hdfs.h" \
 && test -e "$D/lib/native/libhdfs.so" \
 && test -f "$D/lib/native/libhdfs.a" \
 && tar -C /out -czf "/out/hadoop-${HADOOP_VERSION}.tar.gz" "hadoop-${HADOOP_VERSION}" \
 && rm -rf "$D"

############################################################################
# Component source: Apache Hive (Arenadata fork), source only, no build.
# The compile needs one thing from Hive, the metastore thrift that
# common/thrift/CMakeLists.txt includes, so only that subtree is packaged.
############################################################################
FROM ubuntu:22.04 AS hive-src

ENV DEBIAN_FRONTEND=noninteractive
ARG HIVE_VERSION
ARG HIVE_TAG=v${HIVE_VERSION}

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN test -n "${HIVE_VERSION}" \
 && git clone --depth 1 --branch "${HIVE_TAG}" \
        https://github.com/arenadata/hive.git /tmp/hive \
 && D="/src/apache-hive-${HIVE_VERSION}-src" \
 && mkdir -p "$D" \
 && cp -a /tmp/hive/standalone-metastore "$D/" \
 && test -f "$D/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift" \
 && mkdir -p /out \
 && tar -C /src -czf "/out/apache-hive-${HIVE_VERSION}-src.tar.gz" "apache-hive-${HIVE_VERSION}-src"

############################################################################
# CI build environment (no source tree; see the header).
############################################################################
FROM ubuntu:22.04 AS ci-env

# The RUNs below source bin/impala-config.sh, which needs bash.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Avoid interactive prompts (krb5/tzdata) during apt installs.
ENV DEBIAN_FRONTEND=noninteractive \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

# bin/bootstrap_system.sh's Ubuntu 22.04 package set, minus the minicluster-only
# pieces (postgres, ssh). libtinfo5 is needed by the toolchain's LLVM binaries,
# JDK 21 by the frontend (fe/pom.xml), tzdata by expr-test and the KDC binaries
# by rpc-mgr-kerberized-test.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        sudo ca-certificates curl wget file gawk git unzip \
        g++ gcc make ninja-build pkg-config \
        libffi-dev libkrb5-dev libsasl2-dev libsasl2-modules \
        libsasl2-modules-gssapi-mit libssl-dev libxml2-dev libxslt-dev \
        python3 python3-dev python3-setuptools python3-venv python3-pip \
        openjdk-21-jdk-headless \
        ccache vim-common psmisc lsof net-tools language-pack-en libtinfo5 \
        tzdata krb5-kdc krb5-admin-server \
 && rm -rf /var/lib/apt/lists/*

# Point "python" at python3 (Ubuntu 22.04 has no python2, mirroring
# bootstrap_system.sh's setup_python3).
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 20

# impala-config.sh derives JAVA_HOME from IMPALA_JDK_VERSION.
ENV IMPALA_JDK_VERSION=21 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Impala wants a specific Maven; the apt version (3.6.x) is too old.
ARG MVN_VERSION=3.9.8
ARG MVN_SHA512=7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2
RUN cd /tmp \
 && wget -nv "https://archive.apache.org/dist/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz" \
 && echo "${MVN_SHA512}  apache-maven-${MVN_VERSION}-bin.tar.gz" | sha512sum -c - \
 && tar -C /usr/local -xzf "apache-maven-${MVN_VERSION}-bin.tar.gz" \
 && ln -sf "/usr/local/apache-maven-${MVN_VERSION}/bin/mvn" /usr/local/bin/mvn \
 && rm "apache-maven-${MVN_VERSION}-bin.tar.gz"

# Build as a non-root sudoer, matching the project's devcontainer conventions.
# uid 1000: bind-mounted directories must be writable by it.
ENV IMPALA_HOME=/home/impdev/Impala \
    IMPALA_TOOLCHAIN=/opt/impala-toolchain
RUN adduser --disabled-password --gecos '' --uid 1000 impdev \
 && echo 'impdev ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
 && mkdir -p ${IMPALA_HOME} /home/impdev/.m2/repository /home/impdev/.ccache ${IMPALA_TOOLCHAIN} \
 && chown -R impdev:impdev /home/impdev ${IMPALA_TOOLCHAIN}

# Maven settings: Maven Central mirror and repository exclusions (no credentials).
COPY --chown=impdev:impdev docker/ci/settings.xml /home/impdev/.m2/settings.xml

USER impdev
WORKDIR ${IMPALA_HOME}

# Maven 3.9's default HTTP transport has no read timeout, so a dropped keep-alive
# connection hangs a download forever; the wagon transport with finite timeouts
# aborts and retries instead. The groupId filter (docker/ci/maven-repo-filters/)
# limits the Cloudera repo to the few artifacts that only exist there.
ENV IMPALA_MAVEN_OPTIONS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.pool=false -Dmaven.wagon.rto=60000 -Dmaven.wagon.http.connectionTimeout=30000 -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=30 -Daether.remoteRepositoryFilter.groupId=true -Daether.remoteRepositoryFilter.groupId.basedir=/home/impdev/Impala/docker/ci/maven-repo-filters"

# Index host for the PyPI downloads below (the package files come from
# files.pythonhosted.org). The default, pypi.python.org, 301-redirects here and
# that hop stalls in the container. Point at an internal mirror to use one.
ARG PYPI_MIRROR=https://pypi.org
ENV PYPI_MIRROR=${PYPI_MIRROR}

# pip's 15s default timeout is too short for the container network.
ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

# ccache is only used when CMake is told to launch the compiler through it.
ENV CCACHE_DIR=/home/impdev/.ccache \
    CCACHE_MAXSIZE=4G \
    CCACHE_COMPILERCHECK=content \
    CMAKE_C_COMPILER_LAUNCHER=ccache \
    CMAKE_CXX_COMPILER_LAUNCHER=ccache

# Python packages buildall.sh needs: their tarballs are gitignored, so a fresh
# checkout would download ~90 of them from PyPI on every run. ci-build.sh copies
# these in and sets SKIP_PYTHON_DOWNLOAD. The requirements files are part of this
# image's tag, so adding a package rebuilds the image, and only they and the
# download script come from the context (see .dockerignore).
COPY --chown=impdev:impdev infra/python/deps /opt/impala-pydeps
RUN /opt/impala-pydeps/download_requirements

# Native toolchain, bootstrapped from the four bin/ files it needs (no source
# tree). With both DOWNLOAD_* false only the native-toolchain packages are
# fetched; the Arenadata components are seeded afterwards.
COPY --chown=impdev:impdev bin/impala-config.sh bin/impala-config-branch.sh \
    bin/impala-config-java.sh bin/bootstrap_toolchain.py /tmp/boot/bin/
RUN export IMPALA_HOME=/tmp/boot \
 && . /tmp/boot/bin/impala-config.sh > /dev/null 2>&1 \
 && DOWNLOAD_APACHE_COMPONENTS=false DOWNLOAD_CDH_COMPONENTS=false \
        python3 /tmp/boot/bin/bootstrap_toolchain.py

# Arenadata components: the real hadoop / hive-src tarballs plus empty stub dirs
# for the minicluster-only ones, named exactly as they unpack so that
# bootstrap_toolchain.py treats them as present. A default bootstrap run then
# fetches what is left (the Kudu client), and buildall.sh's own run only verifies.
COPY --from=hadoop-builder /out/ /tmp/components/
COPY --from=hive-src /out/ /tmp/components/
RUN export IMPALA_HOME=/tmp/boot \
 && . /tmp/boot/bin/impala-config.sh > /dev/null 2>&1 \
 && AC="${APACHE_COMPONENTS_HOME}" \
 && mkdir -p "$AC" \
 && for c in "hadoop-${APACHE_HADOOP_VERSION}" "apache-hive-${APACHE_HIVE_VERSION}-src"; do \
        t="/tmp/components/$c.tar.gz"; \
        if [ ! -f "$t" ]; then \
            echo "ERROR: $t missing: pass the versions from docker/ci/component-versions.sh" \
                 "as --build-arg (have: $(ls /tmp/components))" >&2; \
            exit 1; \
        fi; \
        tar xzf "$t" -C "$AC"; \
    done \
 && for d in \
        "apache-hive-${APACHE_HIVE_VERSION}-bin" \
        "hbase-${APACHE_HBASE_VERSION}-hadoop3" \
        "apache-tez-${APACHE_TEZ_VERSION}-bin" \
        "ranger-${APACHE_RANGER_VERSION}-admin" \
        "ozone-${APACHE_OZONE_VERSION}"; do \
        mkdir -p "$AC/$d"; \
    done \
 && python3 /tmp/boot/bin/bootstrap_toolchain.py \
 && sudo rm -rf /tmp/boot /tmp/components
