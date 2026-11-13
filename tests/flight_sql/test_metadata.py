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

"""Metadata commands: catalogs, schemas, tables, keys, SqlInfo, XdbcTypeInfo."""

import pyarrow as pa

from flightsql_client import (SQL_INFO_SERVER_NAME, SQL_INFO_SERVER_READ_ONLY,
                              SQL_INFO_SERVER_VERSION)
from test_query import ARROW_TYPES


def column(table, name):
  return table.column(name).to_pylist()


def test_no_catalogs(client):
  assert client.catalogs().num_rows == 0


def test_schemas(client, data):
  assert sorted(column(client.schemas(), "db_schema_name")) == sorted(data["databases"])
  assert column(client.schemas(pattern="flight%"), "db_schema_name") == [data["database"]]
  # Impala has no catalogs: another catalog has no schemas
  assert client.schemas(catalog="other").num_rows == 0


def test_tables(client, data):
  db = data["database"]
  tables = client.tables(schema=db)
  assert column(tables, "table_name") == data["tables"]
  assert set(column(tables, "db_schema_name")) == {db}
  assert column(client.tables(schema=db, types=["VIEW"]), "table_name") == ["big_view"]
  assert column(client.tables(schema=db, table="big%"), "table_name") == \
      ["big", "big_view"]
  assert client.tables(schema="default").num_rows == 0


def test_table_schema(client, data):
  tables = client.tables(schema=data["database"], table="types", include_schema=True)
  assert tables.num_rows == 1
  schema = pa.ipc.read_schema(pa.py_buffer(column(tables, "table_schema")[0]))
  assert schema.names == [c[0] for c in data["type_columns"]]
  assert [f.type for f in schema] == ARROW_TYPES
  meta = schema.field("dec").metadata
  assert meta[b"ARROW:FLIGHT:SQL:TYPE_NAME"] == b"DECIMAL"


def test_table_types(client):
  assert column(client.table_types(), "table_type") == ["TABLE", "VIEW"]


def test_primary_keys(client, data):
  keys = client.primary_keys(data["database"], "parent").to_pylist()
  assert [(k["table_name"], k["column_name"], k["key_name"], k["key_sequence"])
          for k in keys] == [("parent", "id", "parent_pk", 1)]
  assert client.primary_keys(data["database"], "child").num_rows == 0


def test_foreign_keys(client, data):
  db = data["database"]
  imported = client.imported_keys(db, "child").to_pylist()
  exported = client.exported_keys(db, "parent").to_pylist()
  both = client.cross_reference(db, "parent", db, "child").to_pylist()
  for keys in (imported, exported, both):
    assert [(k["pk_table_name"], k["pk_column_name"], k["fk_table_name"],
             k["fk_column_name"], k["fk_key_name"]) for k in keys] == \
        [("parent", "id", "child", "parent_id", "child_fk")]
  assert client.imported_keys(db, "parent").num_rows == 0


def test_sql_info(client):
  info = client.sql_info([SQL_INFO_SERVER_NAME, SQL_INFO_SERVER_VERSION,
                          SQL_INFO_SERVER_READ_ONLY])
  assert info[SQL_INFO_SERVER_NAME] == "Impala"
  assert info[SQL_INFO_SERVER_VERSION]
  assert info[SQL_INFO_SERVER_READ_ONLY] is False


def test_xdbc_type_info(client):
  all_types = client.xdbc_type_info()
  names = column(all_types, "type_name")
  assert "DECIMAL" in names and "VARCHAR" in names and len(names) == 14
  decimal = client.xdbc_type_info(data_type=3).to_pylist()
  assert [t["type_name"] for t in decimal] == ["DECIMAL"]
  assert decimal[0]["create_params"] == ["precision", "scale"]
  bigint = client.xdbc_type_info(data_type=-5).to_pylist()
  assert [t["type_name"] for t in bigint] == ["BIGINT"]


def test_adbc_get_objects(adbc, data):
  objects = adbc().adbc_get_objects(depth="columns").read_all().to_pylist()
  schemas = {s["db_schema_name"]: s for c in objects for s in c["catalog_db_schemas"]}
  assert set(data["databases"]) <= set(schemas)
  tables = {t["table_name"]: t for t in schemas[data["database"]]["db_schema_tables"]}
  assert list(tables) == data["tables"]
  assert [c["column_name"] for c in tables["types"]["table_columns"]] == \
      [c[0] for c in data["type_columns"]]


def test_adbc_table_schema_and_types(adbc, data):
  conn = adbc()
  schema = conn.adbc_get_table_schema("types", db_schema_filter=data["database"])
  assert [f.type for f in schema] == ARROW_TYPES
  assert conn.adbc_get_table_types() == ["TABLE", "VIEW"]
  assert conn.adbc_get_info()["vendor_name"] == "Impala"
