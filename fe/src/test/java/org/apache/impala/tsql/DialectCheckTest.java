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

package org.apache.impala.tsql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/** Each dialect rejects the other's keywords before any statement runs. */
public class DialectCheckTest {
  @Test
  public void tsqlRejectsLpsqlKeywords() {
    rejectsTsql("FOR i IN 1..3 LOOP SELECT 1; END LOOP;");
    rejectsTsql("EXECUTE IMMEDIATE 'SELECT 1';");
    rejectsTsql("SELECT COUNT(*) INTO n FROM t;");
  }

  @Test
  public void lpsqlRejectsTsqlKeywords() {
    rejectsLpsql("BEGIN TRY SELECT 1; END TRY BEGIN CATCH END CATCH;");
    rejectsLpsql("DECLARE @n INT = 1;");
    rejectsLpsql("FETCH NEXT FROM c INTO n;");
  }

  @Test
  public void leadingBomStillSelectsTheDialect() {
    assertTrue(DialectCheck.leadingIs("\uFEFFTSQL\nSELECT 1;", "TSQL"));
    assertTrue(DialectCheck.leadingIs("\uFEFF  LPSQL\nBEGIN NULL; END", "LPSQL"));
  }

  private static void rejectsTsql(String body) {
    try {
      TsqlParser.parse("TSQL\n" + body);
      fail("accepted: " + body);
    } catch (TsqlParser.ParseException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().length() > 0);
    }
  }

  private static void rejectsLpsql(String body) {
    try {
      org.apache.impala.lpsql.LpsqlParser.parse("LPSQL\n" + body);
      fail("accepted: " + body);
    } catch (TsqlParser.ParseException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().length() > 0);
    }
  }
}
