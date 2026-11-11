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
import static org.junit.Assert.assertNull;

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.Test;

public class ImpalaArrowTypesTest {
  private static void assertType(ArrowType expected, String typeName) {
    assertEquals(typeName, expected, ImpalaArrowTypes.forTypeName(typeName));
  }

  @Test
  public void testTypeNames() {
    assertType(ArrowType.Bool.INSTANCE, "BOOLEAN");
    assertType(new ArrowType.Int(8, true), "tinyint");
    assertType(new ArrowType.Int(16, true), "SMALLINT");
    assertType(new ArrowType.Int(32, true), "INT");
    assertType(new ArrowType.Int(64, true), "BIGINT");
    assertType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "FLOAT");
    assertType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), "DOUBLE");
    assertType(new ArrowType.Decimal(10, 2, 128), "DECIMAL(10,2)");
    assertType(new ArrowType.Decimal(38, 0, 128), "decimal( 38 )");
    assertType(new ArrowType.Decimal(9, 0, 128), "DECIMAL");
    assertType(ArrowType.Utf8.INSTANCE, "STRING");
    assertType(ArrowType.Utf8.INSTANCE, "VARCHAR(5)");
    assertType(ArrowType.Utf8.INSTANCE, "CHAR(3)");
    assertType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), "TIMESTAMP");
    assertType(new ArrowType.Date(DateUnit.DAY), "DATE");
    assertType(ArrowType.Binary.INSTANCE, "BINARY");
    assertType(ArrowType.Utf8.INSTANCE, "ARRAY<INT>");
    assertType(ArrowType.Utf8.INSTANCE, "MAP<STRING,INT>");
    assertType(ArrowType.Utf8.INSTANCE, "STRUCT<a:INT>");
    assertType(ArrowType.Utf8.INSTANCE, null);
  }

  private static FlightSqlColumnMetadata meta(String name, Integer precision,
      Integer scale, Integer length, boolean result) {
    return new FlightSqlColumnMetadata(ImpalaArrowTypes.columnMetadata(
        name, precision, scale, length, result, result ? null : "s",
        result ? null : "t"));
  }

  @Test
  public void testBaseNames() {
    assertEquals("DECIMAL", ImpalaArrowTypes.baseName("decimal(10,2)"));
    assertEquals("ARRAY", ImpalaArrowTypes.baseName("ARRAY<INT>"));
    assertEquals("VARCHAR", ImpalaArrowTypes.baseName("VARCHAR(5)"));
    assertEquals("STRING", ImpalaArrowTypes.baseName((String) null));
    assertEquals("TIMESTAMP", ImpalaArrowTypes.baseName(TTypeId.TIMESTAMP_TYPE));
    assertEquals("INT", ImpalaArrowTypes.baseName(TTypeId.INT_TYPE));
  }

  @Test
  public void testColumnMetadata() {
    FlightSqlColumnMetadata dec = meta("DECIMAL", 10, 2, null, true);
    assertEquals("DECIMAL", dec.getTypeName());
    assertEquals(Integer.valueOf(10), dec.getPrecision());
    assertEquals(Integer.valueOf(2), dec.getScale());
    assertEquals(false, dec.isCaseSensitive());
    assertEquals(true, dec.isSearchable());
    assertEquals(true, dec.isReadOnly());
    assertEquals(false, dec.isAutoIncrement());
    assertNull(dec.getSchemaName());
    // declared lengths win; STRING and complex types get the configured length
    assertEquals(Integer.valueOf(5), meta("VARCHAR", null, null, 5, true).getPrecision());
    assertEquals(Integer.valueOf(3), meta("CHAR", null, null, 3, true).getPrecision());
    FlightSqlColumnMetadata str = meta("STRING", null, null, null, false);
    assertEquals(Integer.valueOf(Integer.MAX_VALUE), str.getPrecision());
    assertEquals(true, str.isCaseSensitive());
    assertEquals(false, str.isReadOnly());
    assertEquals("s", str.getSchemaName());
    assertEquals("t", str.getTableName());
    FlightSqlColumnMetadata arr = meta("ARRAY", null, null, null, true);
    assertEquals(false, arr.isSearchable());
    assertEquals(Integer.valueOf(Integer.MAX_VALUE), arr.getPrecision());
    assertEquals(Integer.valueOf(6),
        meta("TIMESTAMP", null, null, null, true).getScale());
    // no precision for fixed-size types: the client derives it
    assertNull(meta("INT", null, null, null, true).getPrecision());
  }
}
