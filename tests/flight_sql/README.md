# Flight SQL end-to-end tests

Python clients (pyarrow Flight, ADBC Flight SQL) against impalad's Arrow Flight SQL
endpoint, without a cluster. The tests start `FlightSqlTestServer`
(`fe/src/test/java/org/apache/impala/service/flight/`): the real `ImpalaFlightSqlServer`
with a fake HS2 behind it (`FakeHs2` serving `FlightTestData`), TLS with a self-signed
certificate, and an in-process LDAP server and Kerberos KDC.

Two servers are started per session: one without authentication and one with LDAP and
Kerberos. Tests that use the `client` / `connect` / `adbc` fixtures run once per login
mode: `none` (user header), `ldap` (Basic) and `kerberos` (SPNEGO from a keytab in a
private ticket cache).

| Module | Covers |
|---|---|
| `test_auth.py` | login per mode, bearer tokens, rejected credentials, tickets per user and single use |
| `test_query.py` | Arrow types and values, column metadata, empty / multi-batch results, error codes |
| `test_prepared.py` | schema at prepare, execution, close, idle expiry, per-user handles |
| `test_update.py` | DoPut row counts (unknown sent as 0), prepared updates |
| `test_metadata.py` | catalogs, schemas, tables (+ schema), table types, keys, SqlInfo, XdbcTypeInfo |

What they cannot cover: real Impala types and metadata, impersonation in Impala, SQL
semantics. The frontend's JUnit tests (`ImpalaFlightJdbcDriverTest` for the JDBC driver)
use the same scripted data.

## In CI

`docker/ci/run-unit-tests.sh` runs this suite as `py`, after the frontend tests,
in the CI image (`.github/workflows/ci.yml`, step "Run unit tests"). It installs
`requirements.txt` from `$PYPI_MIRROR` into `/tmp/flight-test-venv` (reused while
the file is unchanged), writes `py/flight-sql.xml` for the test report and keeps
the servers' logs in `py/servers/`. Locally, in the same container:

```bash
IMG=$(docker/ci/env-image.sh tag)
PY_TESTS=all docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
PY_TESTS=kerberos docker/ci/run-in-env.sh "$IMG" docker/ci/run-unit-tests.sh ci-results
```

## Running directly

Needs a built frontend (`fe/target/classes`, `test-classes`, `test-classpath.txt`),
`java` 17+ and, for Kerberos, `kinit` (`krb5-user`) and `libkrb5-dev` (for `gssapi`).
The clients go into their own virtualenv: they ship as wheels, which Impala's virtualenv
does not install.

```bash
python3 -m venv /tmp/flight-test-venv
/tmp/flight-test-venv/bin/pip install -r tests/flight_sql/requirements.txt
/tmp/flight-test-venv/bin/pytest tests/flight_sql
```

Options: `--impala-home` (default `$IMPALA_HOME` or this checkout), `--java`
(default `$FLIGHT_TEST_JAVA` or `java`). One mode: `-k kerberos`. Server logs are in
`flight-sql-*` directories under `$FLIGHT_TEST_WORK_DIR` (default: the temporary
directory).
