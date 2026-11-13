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

"""Updates through DoPut: row counts from Impala, unknown counts as 0."""

import pyarrow as pa
import pytest


def test_insert_reports_its_row_count(client, data):
  assert client.update(data["insert"]) == data["insert_rows"]


def test_statement_without_row_count_reports_zero(client, data):
  """Not -1: JDBC drivers read -1 as 'a result set follows' (and fail or retry)."""
  assert client.update(data["ddl"]) == 0


def test_prepared_update(client, data):
  handle, _ = client.prepare(data["insert"])
  try:
    assert client.update_prepared(handle) == data["insert_rows"]
  finally:
    client.close_prepared(handle)


def test_prepared_update_without_row_count(client, data):
  handle, _ = client.prepare(data["ddl"])
  try:
    assert client.update_prepared(handle) == 0
  finally:
    client.close_prepared(handle)


def test_update_error(client, data):
  with pytest.raises(pa.ArrowInvalid, match="Syntax error"):
    client.update(data["syntax_error"])


def test_adbc_update(adbc, data):
  with adbc().cursor() as cur:
    cur.execute(data["insert"])
    assert cur.rowcount in (data["insert_rows"], -1)
    cur.execute(data["ddl"])
