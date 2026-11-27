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

"""Queries: Arrow types and values, empty and multi-batch results, errors."""

from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.flight as flight
import pytest

ARROW_TYPES = [pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.float32(),
               pa.float64(), pa.decimal128(10, 2), pa.string(), pa.string(),
               pa.string(), pa.timestamp("us"), pa.date32(), pa.binary(), pa.string()]

ROWS = [
    [True, 1, 2, 3, 4, 1.5, 2.25, Decimal("12345.67"), "hello", "abc", "ab ",
     datetime(2026, 11, 12, 10, 20, 30, 123456), date(2026, 11, 12), b"\x01\x02\x03",
     "[1,2]"],
    [False, -128, -32768, -2 ** 31, -2 ** 63, -0.25, -1e300, Decimal("-0.01"),
     "привет ✓", "", "xyz", datetime(1970, 1, 1),
     date(1970, 1, 1), b"", "[]"],
    [None] * 15,
]


def check_types_table(table, data):
  names = [c[0] for c in data["type_columns"]]
  assert table.schema.names == names
  assert [f.type for f in table.schema] == ARROW_TYPES
  assert all(f.nullable for f in table.schema)
  assert [list(r.values()) for r in table.to_pylist()] == ROWS


def test_types_and_values(client, data):
  check_types_table(client.query(data["types_query"]), data)


def test_column_metadata(client, data):
  """Type name, precision and scale for ODBC / JDBC clients (ColumnMetadata)."""
  schema = client.execute(data["types_query"]).schema
  meta = {f.name: {k.decode(): v.decode() for k, v in (f.metadata or {}).items()}
          for f in schema}
  assert meta["dec"]["ARROW:FLIGHT:SQL:TYPE_NAME"] == "DECIMAL"
  assert meta["dec"]["ARROW:FLIGHT:SQL:PRECISION"] == "10"
  assert meta["dec"]["ARROW:FLIGHT:SQL:SCALE"] == "2"
  assert meta["v"]["ARROW:FLIGHT:SQL:TYPE_NAME"] == "VARCHAR"
  assert meta["v"]["ARROW:FLIGHT:SQL:PRECISION"] == "5"
  assert meta["c"]["ARROW:FLIGHT:SQL:TYPE_NAME"] == "CHAR"
  assert meta["s"]["ARROW:FLIGHT:SQL:PRECISION"] == str(2 ** 31 - 1)
  assert meta["i"]["ARROW:FLIGHT:SQL:TYPE_NAME"] == "INT"


def test_schema_in_flight_info_matches_the_stream(client, data):
  info = client.execute(data["types_query"])
  schema, batches = client.read_batches(info)
  assert schema.names == info.schema.names
  assert [f.type for f in schema] == [f.type for f in info.schema]
  assert sum(b.num_rows for b in batches) == 3


def test_empty_result(client, data):
  info = client.execute(data["empty_query"])
  assert info.schema.names == ["id", "name"]
  schema, batches = client.read_batches(info)
  assert schema.names == ["id", "name"]
  assert sum(b.num_rows for b in batches) == 0


def test_large_result_in_several_batches(client, data):
  schema, batches = client.read_batches(client.execute(data["big_query"]))
  assert len(batches) > 1
  table = pa.Table.from_batches(batches, schema)
  assert table.num_rows == data["big_rows"]
  assert table.column("id").to_pylist() == list(range(data["big_rows"]))
  assert table.column("name")[data["big_rows"] - 1].as_py() == \
      "row-%d" % (data["big_rows"] - 1)


def test_syntax_error(client, data):
  with pytest.raises(pa.ArrowInvalid, match="Syntax error"):
    client.execute(data["syntax_error"])


def test_missing_table(client, data):
  with pytest.raises(pa.ArrowInvalid, match="Could not resolve table reference"):
    client.execute(data["missing_table"])


def test_missing_privilege(client, data):
  with pytest.raises(flight.FlightUnauthorizedError, match="does not have privileges"):
    client.execute(data["no_privilege"])


def test_error_while_fetching(client, data):
  info = client.execute(data["fetch_error"])
  with pytest.raises(flight.FlightInternalError, match="Failed to open HDFS file"):
    client.read(info)


def test_errors_leave_the_session_usable(client, data):
  for sql in (data["syntax_error"], data["missing_table"]):
    with pytest.raises(pa.ArrowInvalid):
      client.execute(sql)
  assert client.query(data["types_query"]).num_rows == 3


def test_adbc_types_and_values(adbc, data):
  with adbc().cursor() as cur:
    cur.execute(data["types_query"])
    check_types_table(cur.fetch_arrow_table(), data)


def test_adbc_empty_result(adbc, data):
  """ADBC lists the columns of an empty result once (the JDBC driver lists them twice,
  see ImpalaFlightJdbcDriverTest)."""
  with adbc().cursor() as cur:
    cur.execute(data["empty_query"])
    assert [d[0] for d in cur.description] == ["id", "name"]
    assert cur.fetchall() == []


def test_adbc_large_result(adbc, data):
  with adbc().cursor() as cur:
    cur.execute(data["big_query"])
    assert cur.fetch_arrow_table().num_rows == data["big_rows"]


def test_adbc_error(adbc, data):
  import adbc_driver_manager
  with adbc().cursor() as cur:
    with pytest.raises(adbc_driver_manager.Error, match="Syntax error"):
      cur.execute(data["syntax_error"])
