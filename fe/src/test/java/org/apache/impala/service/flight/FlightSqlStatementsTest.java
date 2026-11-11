// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.service.flight;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FlightSqlStatementsTest {
  @Test
  public void testIsQuery() {
    for (String q : new String[] {"select 1", "  WITH a AS (SELECT 1) SELECT * FROM a",
        "values (1)", "show tables", "describe t", "desc t", "explain select 1",
        "(select 1) union all (select 2)", "-- c\n/* d */ select 1"}) {
      assertTrue(q, FlightSqlStatements.isQuery(q));
    }
    for (String q : new String[] {"create table t (a int)", "insert into t values (1)",
        "drop table t", "use db", "set mem_limit=1g", "invalidate metadata t", "", "  ",
        null, "/* unterminated"}) {
      assertFalse(String.valueOf(q), FlightSqlStatements.isQuery(q));
    }
    assertEquals("select", FlightSqlStatements.firstKeyword("--x\nSELECT 1"));
    assertNull(FlightSqlStatements.firstKeyword("-- only a comment"));
  }
}
