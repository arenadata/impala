# CI: unit tests without a minicluster

`.github/workflows/ci.yml` builds Impala and runs the backend and frontend unit
tests that need no minicluster. Every step is a script in this directory, run in
a container of the CI environment image (`env.Dockerfile`: build dependencies,
JDK, Maven, the Arenadata Hadoop and Hive components, the native toolchain) with
the checkout bind-mounted, so the same commands work on a workstation.

## When CI reports a test failure

1. Open the failed run's **Unit tests** check: it lists the failing test cases.
2. Download the run's `unit-test-results` artifact for the details:
   - `status` — one `<suite>=<exit code>` line per suite (`be`, `fe`)
   - `be.log` / `fe.log` — the full output of each suite
   - `be/*.xml`, `fe/*.xml`, `fe/*.txt` — per-test reports
   - `logs/` — the backend tests' glog `ERROR`/`WARNING` files, JVM crash reports
     (`hs_err_pid*.log`) and surefire fork dumps

A failure usually falls into one of three groups:

- **A real defect**: fix the code.
- **A missing piece of the environment** (a package, a config file, a timezone
  database): add it to `env.Dockerfile` and say why in a comment. Editing that
  file, `settings.xml`, `maven-repo-filters/`, `component-versions.sh` or the
  `bin/` files the image is built from changes its tag, so the next run builds
  and publishes a new image.
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

`BE_TESTS` (a ctest regex) and `FE_TESTS` (surefire `-Dtest` patterns) run a
selection instead: they ignore the exclude lists and run only the suite they
name, which is also how to check whether an excluded test passes again.

```bash
BE_TESTS='^thread-pool-test$' docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
FE_TESTS='ParserTest,AnalyzeDDLTest#testAlterTable*' docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
```

The container state (ccache, the Maven repository, `/tmp`) lives in `../ci-cache`
next to the checkout; `CI_STATE_DIR` points it elsewhere, `BUILD_THREADS` limits
the parallel build jobs, and `CI_M2_SEED=<dir>` pre-seeds Maven artifacts on a
network that cannot reach a repository.
