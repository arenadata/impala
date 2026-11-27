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

"""Prepared statements (no parameters): schema at prepare, execution, close, expiry."""

import time

import pyarrow as pa
import pytest

from test_query import ARROW_TYPES, check_types_table

UNKNOWN = "Unknown or expired Flight SQL prepared statement"


def test_schema_is_known_at_prepare(client, data):
  handle, schema = client.prepare(data["types_query"])
  try:
    assert schema.names == [c[0] for c in data["type_columns"]]
    assert [f.type for f in schema] == ARROW_TYPES
  finally:
    client.close_prepared(handle)


def test_execute_prepared_query(client, data):
  handle, _ = client.prepare(data["types_query"])
  try:
    check_types_table(client.read(client.execute_prepared(handle)), data)
    # can run again
    check_types_table(client.read(client.execute_prepared(handle)), data)
  finally:
    client.close_prepared(handle)


def test_prepared_empty_result(client, data):
  handle, schema = client.prepare(data["empty_query"])
  try:
    assert schema.names == ["id", "name"]
    table = client.read(client.execute_prepared(handle))
    assert table.schema.names == ["id", "name"]
    assert table.num_rows == 0
  finally:
    client.close_prepared(handle)


def test_prepared_statement_without_result_set(client, data):
  handle, schema = client.prepare(data["ddl"])
  try:
    assert schema is None or len(schema) == 0
  finally:
    client.close_prepared(handle)


def test_closed_statement_is_gone(client, data):
  handle, _ = client.prepare(data["types_query"])
  client.close_prepared(handle)
  with pytest.raises(pa.ArrowException, match=UNKNOWN):
    client.execute_prepared(handle)
  # closing again is fine
  client.close_prepared(handle)


def test_prepare_error(client, data):
  """Queries run at prepare, so their errors come from it."""
  with pytest.raises(pa.ArrowInvalid, match="Could not resolve table reference"):
    client.prepare(data["missing_table"])


def test_prepared_non_query_fails_at_execution(client, data):
  """Other statements are prepared without running: errors come at execution."""
  handle, _ = client.prepare(data["syntax_error"])
  try:
    with pytest.raises(pa.ArrowInvalid, match="Syntax error"):
      client.update_prepared(handle)
  finally:
    client.close_prepared(handle)


def test_statement_of_another_user_is_not_served(connect, users, data):
  owner, other = connect(users[0]), connect(users[1])
  handle, _ = owner.prepare(data["types_query"])
  try:
    with pytest.raises(pa.ArrowException, match=UNKNOWN):
      other.execute_prepared(handle)
    assert owner.read(owner.execute_prepared(handle)).num_rows == 3
  finally:
    owner.close_prepared(handle)


@pytest.mark.parametrize("mode", ["none"])
def test_idle_statement_expires(servers, client, data):
  info = servers("none").info
  handle, _ = client.prepare(data["types_query"])
  # idle longer than prepared_idle_s, plus one reaper period and a margin
  time.sleep(info["prepared_idle_s"] + info["reaper_period_s"] + 2)
  with pytest.raises(pa.ArrowException, match=UNKNOWN):
    client.execute_prepared(handle)


def test_adbc_prepare(adbc, data):
  with adbc().cursor() as cur:
    # adbc_prepare returns the parameter schema: no parameters
    params = cur.adbc_prepare(data["types_query"])
    assert params is None or len(params) == 0
    result = cur.adbc_execute_schema(data["types_query"])
    assert [f.type for f in result] == ARROW_TYPES
    cur.execute(data["types_query"])
    check_types_table(cur.fetch_arrow_table(), data)
