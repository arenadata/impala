# CI: unit tests without a minicluster

`.github/workflows/ci.yml` builds Impala and runs the tests that need no
minicluster: the backend gtests (`be`), the frontend JUnit tests (`fe`) and the
Flight SQL end-to-end tests with Python clients (`py`, `tests/flight_sql`). Every step is a script in this directory, run in
a container of the CI environment image (`env.Dockerfile`: build dependencies,
JDK, Maven, the Arenadata Hadoop and Hive components, the native toolchain) with
the checkout bind-mounted, so the same commands work on a workstation.

## When CI reports a test failure

1. Open the failed run's **Unit tests** check: it lists the failing test cases.
2. Download the run's `unit-test-results` artifact for the details:
   - `status` — one `<suite>=<exit code>` line per suite (`be`, `fe`, `py`)
   - `be.log` / `fe.log` / `py.log` — the full output of each suite
   - `be/*.xml`, `fe/*.xml`, `fe/*.txt`, `py/*.xml` — per-test reports
   - `py/servers/*/server.log` — the Flight SQL test servers' logs
   - `logs/` — the backend tests' glog `ERROR`/`WARNING` files, JVM crash reports
     (`hs_err_pid*.log`) and surefire fork dumps

A failure usually falls into one of three groups:

- **A real defect**: fix the code.
- **A missing piece of the environment** (a package, a config file, a timezone
  database): add it to `env.Dockerfile` and say why in a comment. Editing that
  file, `settings.xml`, `maven-repo-filters/`, `component-versions.sh` or the
  `bin/` files the image is built from changes its tag, so the next run builds
  and publishes a new image.
- **The `py` suite could not install its clients** (`py=3`, a single failed
  `setup` case): it installs `tests/flight_sql/requirements.txt` from
  `$PYPI_MIRROR` (`env.Dockerfile`) on every CI run, so PyPI or the mirror was
  unreachable. Rerun, or point `PYPI_MIRROR` at a reachable mirror.
- **Something that needs the minicluster** — HDFS, a running impalad, Kudu,
  HBase, Ranger, HMS events, or the `functional` test data: add it to
  `be-tests-exclude.txt` or `fe-tests-exclude.txt`, with a reason. Their headers
  describe the patterns each one takes; note that every FE test class that is
  *not* listed runs, so a new one that needs a cluster turns CI red until it is.

## Running it locally

```bash
docker/ci/env-image.sh ensure                       # pull or build the environment image
IMG=$(docker/ci/env-image.sh tag)
docker/ci/run-in-env.sh "$IMG" docker/ci/ci-build.sh
docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
```

`BE_TESTS` (a ctest regex), `FE_TESTS` (surefire `-Dtest` patterns) and
`PY_TESTS` (a pytest `-k` expression, or `all` for the whole suite) run a
selection instead: they ignore the exclude lists and run only the suites they
name, which is also how to check whether an excluded test passes again.

```bash
BE_TESTS='^thread-pool-test$' docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
FE_TESTS='ParserTest,AnalyzeDDLTest#testAlterTable*' docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
PY_TESTS=all docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
PY_TESTS='kerberos and prepared' docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
```

The `py` suite installs its clients (prebuilt wheels, which Impala's own
virtualenv does not take) into `/tmp/flight-test-venv` and reuses it while
`tests/flight_sql/requirements.txt` stays the same.

The container state (ccache, the Maven repository, `/tmp`) lives in `../ci-cache`
next to the checkout; `CI_STATE_DIR` points it elsewhere, `BUILD_THREADS` limits
the parallel build jobs, and `CI_M2_SEED=<dir>` pre-seeds Maven artifacts on a
network that cannot reach a repository.
