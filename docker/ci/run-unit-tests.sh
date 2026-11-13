#!/bin/bash
#
# Runs the tests that need no minicluster (no HDFS/HMS/Kudu/impalad), after
# `buildall.sh -skiptests`: the backend gtests (be), the frontend JUnit tests (fe)
# and the Flight SQL end-to-end tests with Python clients (py, tests/flight_sql).
# JUnit XML lands in $1/{be,fe,py}, full output in $1/{be,fe,py}.log, and
# $1/status gets one "<suite>=<exit code>" line per suite. All suites always run;
# the script exits non-zero if any failed.
#
# Test selection lives next to this script:
#   be-tests-exclude.txt   ctest names (regexes) to skip: they need a cluster
#   fe-tests-exclude.txt   FE test patterns to skip: they need a cluster
# BE_TESTS (a ctest regex), FE_TESTS (surefire -Dtest patterns) and PY_TESTS
# (a pytest -k expression, or "all") select tests explicitly instead, ignoring
# the exclude lists and running only the suites they name.
#
# The py suite installs its clients (tests/flight_sql/requirements.txt, prebuilt
# wheels) from $PYPI_MIRROR into a virtualenv under /tmp, reused while the
# requirements stay the same; Impala's own virtualenv installs from source only.
set -o pipefail

# Absolute: the tests write their results from their own working directories.
OUT="$(mkdir -p "${1:?usage: run-unit-tests.sh <results-dir>}" && cd "$1" && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../../bin/impala-config.sh" > /dev/null 2>&1
# Results of an earlier run in the same tree would be reported as this run's.
rm -rf "$OUT/be" "$OUT/fe" "$OUT/py" "$OUT/logs"
mkdir -p "$OUT/be" "$OUT/fe" "$OUT/py"
: > "$OUT/status"

# Joins the non-comment, non-empty lines of a file with $2, dropping trailing
# " # reason" comments (a '#' without whitespace before it, as in surefire's
# Class#method, is kept).
join_list() {
  grep -vE '^\s*(#|$)' "$1" | sed -E 's/\s+#.*//' | paste -sd"$2"
}

# Embedded, empty Derby-backed Hive Metastore for tests that only need a working
# metastore client (see hms-embedded/hive-site.xml). The BE test classpath is
# compile-scope only, so the embedded metastore's test-scope FE dependencies
# (Derby, the javax.jdo API) are taken from the FE test classpath.
HMS_CONF_DIR="$HERE/hms-embedded"
HMS_JARS="$(tr ':' '\n' < "$IMPALA_FE_DIR/target/test-classpath.txt" \
    | grep -E '/(derby|javax\.jdo)-[^/]*\.jar$' | paste -sd:)"

run_be() {
  local exclude rc
  exclude="$(join_list "$HERE/be-tests-exclude.txt" '|')"
  if [[ -n "${BE_TESTS:-}" ]]; then exclude=''; fi
  # The unified binary's generated scripts pass --gtest_output, which wins over
  # GTEST_OUTPUT; the same directory keeps the single-binary reports with them.
  export GTEST_OUTPUT="xml:$IMPALA_BE_TEST_LOGS_DIR/"
  rm -f "$IMPALA_BE_TEST_LOGS_DIR"/*.xml
  export HEAPCHECK=
  export CUSTOM_CLASSPATH="$HMS_CONF_DIR:$HMS_JARS"
  . "$IMPALA_HOME/bin/set-classpath.sh"
  export PATH="$IMPALA_TOOLCHAIN_PACKAGES_HOME/llvm-$IMPALA_LLVM_VERSION/bin:$PATH"
  cd "$IMPALA_HOME" || return
  # --no-tests=error: an exclude list or a BE_TESTS that matches nothing is a
  # mistake, not a pass.
  ctest --output-on-failure --timeout 1200 --no-tests=error \
      ${BE_TESTS:+-R "$BE_TESTS"} ${exclude:+-E "^($exclude)\$"}
  rc=$?
  cp "$IMPALA_BE_TEST_LOGS_DIR"/*.xml "$OUT/be/" 2>/dev/null
  return $rc
}

run_fe() {
  local tests
  # Surefire's default includes, minus the excluded patterns.
  tests="**/Test*.java,**/*Test.java,**/*Tests.java,**/*TestCase.java"
  tests+=",$(join_list "$HERE/fe-tests-exclude.txt" ',' | sed 's/^/!/; s/,/,!/g')"
  tests="${FE_TESTS:-$tests}"
  rm -f "$IMPALA_FE_TEST_LOGS_DIR"/*.xml "$IMPALA_FE_TEST_LOGS_DIR"/*.txt
  cd "$IMPALA_FE_DIR" || return
  # One JVM per test class with a time limit, so a class that crashes or hangs
  # is reported on its own instead of aborting the run.
  "$IMPALA_HOME/bin/mvn-quiet.sh" -fae surefire:test -Dtest="$tests" \
      -DreuseForks=false -Dsurefire.timeout=300 \
      -Dmaven.test.additionalClasspath="$HMS_CONF_DIR" \
      -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
  local rc=$?
  # fe/pom.xml points surefire's reportsDirectory at the FE test logs dir.
  cp "$IMPALA_FE_TEST_LOGS_DIR"/*.xml "$IMPALA_FE_TEST_LOGS_DIR"/*.txt "$OUT/fe/" 2>/dev/null
  return $rc
}

FLIGHT_VENV=/tmp/flight-test-venv
FLIGHT_REQS="$IMPALA_HOME/tests/flight_sql/requirements.txt"

# A JUnit report with one failed case, so that a py suite that could not start is
# shown in the test report instead of missing from it.
py_setup_failed() {
  cat > "$OUT/py/setup.xml" <<XML
<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="flight_sql" tests="1" failures="1" errors="0">
<testcase classname="tests.flight_sql" name="setup">
<failure message="$1"/></testcase>
</testsuite></testsuites>
XML
  echo "py suite: $1"
}

# The clients' virtualenv, (re)created when requirements.txt changed.
py_venv() {
  local sum stamp="$FLIGHT_VENV/.requirements.sha256"
  sum="$(sha256sum "$FLIGHT_REQS" | cut -d' ' -f1)"
  [[ "$(cat "$stamp" 2>/dev/null)" == "$sum" ]] && return 0
  rm -rf "$FLIGHT_VENV"
  python3 -m venv "$FLIGHT_VENV" \
    && "$FLIGHT_VENV/bin/pip" install -q --disable-pip-version-check \
        --index-url "${PYPI_MIRROR:-https://pypi.org}/simple" \
        --cache-dir /tmp/pip-cache -r "$FLIGHT_REQS" \
    && echo "$sum" > "$stamp"
}

run_py() {
  local select=()
  if ! py_venv; then
    py_setup_failed "pip install from ${PYPI_MIRROR:-https://pypi.org} failed"
    return 3
  fi
  if [[ -n "${PY_TESTS:-}" && "$PY_TESTS" != all ]]; then select=(-k "$PY_TESTS"); fi
  cd "$IMPALA_HOME" || return
  # The test servers (FlightSqlTestServer from the FE test classes) and their logs
  # go under $OUT/py/servers, so they are kept with the results.
  FLIGHT_TEST_JAVA="${JAVA:-java}" FLIGHT_TEST_WORK_DIR="$OUT/py/servers" \
    "$FLIGHT_VENV/bin/pytest" tests/flight_sql -p no:cacheprovider \
      --junitxml="$OUT/py/flight-sql.xml" "${select[@]}"
}

# Keeps what is needed to diagnose failures: glog ERROR/WARNING logs of the BE
# tests, JVM crash reports and surefire fork dumps (the INFO logs are too large).
collect_logs() {
  mkdir -p "$OUT/logs"
  cp "$IMPALA_BE_TEST_LOGS_DIR"/*.ERROR "$IMPALA_BE_TEST_LOGS_DIR"/*.WARNING \
      "$IMPALA_LOGS_DIR"/hs_err_pid*.log "$IMPALA_FE_DIR"/hs_err_pid*.log \
      "$IMPALA_LOGS_DIR"/fe_tests/*.dump* \
      "$OUT/logs/" 2>/dev/null
}

# All suites, unless BE_TESTS/FE_TESTS/PY_TESTS name a selection from some of them.
suites="be fe py"
if [[ -n "${BE_TESTS:-}${FE_TESTS:-}${PY_TESTS:-}" ]]; then
  suites="${BE_TESTS:+be} ${FE_TESTS:+fe} ${PY_TESTS:+py}"
fi

# Full output to {be,fe,py}.log, one line per test binary or class to stdout.
if [[ " $suites " == *" be "* ]]; then
  (run_be) 2>&1 | tee "$OUT/be.log" \
      | grep --line-buffered -E '^ *[0-9]+/[0-9]+ Test +#|tests passed|tests failed'
  echo "be=${PIPESTATUS[0]}" >> "$OUT/status"
fi
if [[ " $suites " == *" fe "* ]]; then
  (run_fe) 2>&1 | tee "$OUT/fe.log" \
      | grep --line-buffered -E 'Tests run:|BUILD (SUCCESS|FAILURE)'
  echo "fe=${PIPESTATUS[0]}" >> "$OUT/status"
fi
# After fe: it needs the FE test classes.
if [[ " $suites " == *" py "* ]]; then
  (run_py) 2>&1 | tee "$OUT/py.log" \
      | grep --line-buffered -E '^(FAILED|ERROR) |^=+ .*(passed|failed|error)|^py suite:'
  echo "py=${PIPESTATUS[0]}" >> "$OUT/status"
fi
collect_logs
cat "$OUT/status"
# Fails the step, having let all suites run.
! grep -qv '=0$' "$OUT/status"
