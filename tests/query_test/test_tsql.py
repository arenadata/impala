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

# End-to-end checks for procedure batches. Runs on the minicluster (several
# impalads, HDFS), which is where a removed single-node workaround would show up.
from __future__ import absolute_import, division, print_function

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS


class TestTsql(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestTsql, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')

  def _batch(self, sql):
    return self.execute_query_expect_success(self.client, "TSQL\n" + sql)

  def test_numbers_quotes_and_call_scope(self, vector):
    result = self._batch(
        "DECLARE @acct STRING = '00123';\n"
        "DECLARE @id BIGINT = 9007199254740993;\n"
        "DECLARE @name STRING = 'O''Brien';\n"
        "SELECT @acct AS acct, @id AS id, @name AS name;")
    row = result.data[0]
    assert '00123' in row
    assert '9007199254740993' in row
    assert "O'Brien" in row

    self._batch(
        "CREATE PROCEDURE add_one @x INT, @y INT OUTPUT AS BEGIN\n"
        "  SET @y = @x + 1;\n"
        "  RETURN 7;\n"
        "END")
    called = self._batch(
        "DECLARE @out INT;\nDECLARE @n INT = 1;\nDECLARE @rc INT;\n"
        "EXEC @rc = add_one @x = 3, @y = @out OUTPUT;\n"
        "SELECT @rc AS rc, @out AS out, @n AS n;")
    text = called.data[0]
    assert '7' in text and '4' in text and '1' in text

  @SkipIfFS.qualified_path
  def test_no_duplicate_rows_without_num_nodes(self, vector):
    """Shapes that doubled on the local-filesystem VM must not double on HDFS."""
    self.client.execute("DROP TABLE IF EXISTS tsql_nums")
    self.client.execute("CREATE TABLE tsql_nums (n INT)")
    self.client.execute("INSERT INTO tsql_nums VALUES (1), (2), (3), (4)")
    total = self._batch(
        "DECLARE @n INT;\nDECLARE @s INT = 0;\n"
        "DECLARE c CURSOR FOR SELECT n FROM tsql_nums WHERE n > 1 ORDER BY n;\n"
        "OPEN c;\nFETCH NEXT FROM c INTO @n;\n"
        "WHILE @@FETCH_STATUS = 0 BEGIN\n"
        "  SET @s = @s + @n;\n"
        "  FETCH NEXT FROM c INTO @n;\n"
        "END\nSELECT @s AS total;")
    assert '9' in total.data[0]

    self.client.execute("DROP TABLE IF EXISTS tsql_amounts")
    self.client.execute("CREATE TABLE tsql_amounts (v INT)")
    self.client.execute("INSERT INTO tsql_amounts VALUES (10), (20)")
    snap = self._batch(
        "DROP TABLE IF EXISTS tsql_amount_mv;\n"
        "CREATE TABLE tsql_amount_mv AS SELECT SUM(v) AS total FROM tsql_amounts;\n"
        "INSERT INTO tsql_amounts VALUES (5);\n"
        "SELECT m.total AS materialized_total,\n"
        "  (SELECT SUM(v) FROM tsql_amounts) AS live_total\n"
        "FROM tsql_amount_mv m;")
    assert '30' in snap.data[0]
    assert '35' in snap.data[0]

  def test_owner_cannot_be_replaced(self, vector):
    self._batch("CREATE PROCEDURE secret_proc AS BEGIN SELECT 'owner' AS who; END")
    failure = self.execute_query_expect_failure(
        self.client, "TSQL\nCREATE OR ALTER PROCEDURE secret_proc AS BEGIN "
        "SELECT 'hijacked' AS who; END", user='impala_other')
    text = str(failure).lower()
    assert 'owned by' in text or 'privileges' in text
