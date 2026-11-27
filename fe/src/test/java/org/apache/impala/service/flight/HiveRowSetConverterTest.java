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

import static org.apache.impala.service.flight.FakeHs2.column;
import static org.apache.impala.service.flight.FakeHs2.strings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TBinaryColumn;
import org.apache.hive.service.rpc.thrift.TBoolColumn;
import org.apache.hive.service.rpc.thrift.TByteColumn;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TColumnValue;
import org.apache.hive.service.rpc.thrift.TDoubleColumn;
import org.apache.hive.service.rpc.thrift.TI16Column;
import org.apache.hive.service.rpc.thrift.TI32Column;
import org.apache.hive.service.rpc.thrift.TI32Value;
import org.apache.hive.service.rpc.thrift.TI64Column;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TStringValue;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;
import org.apache.hive.service.rpc.thrift.TTypeQualifiers;
import org.junit.Test;

public class HiveRowSetConverterTest {

  private static ByteBuffer nulls(int... rows) {
    byte[] b = new byte[1];
    for (int r : rows) b[r / 8] |= (byte) (1 << (r % 8));
    return ByteBuffer.wrap(b);
  }

  private static TColumnDesc decimal(String name, int precision, int scale) {
    TColumnDesc c = column(name, TTypeId.DECIMAL_TYPE);
    Map<String, TTypeQualifierValue> q = new HashMap<>();
    q.put("precision", TTypeQualifierValue.i32Value(precision));
    q.put("scale", TTypeQualifierValue.i32Value(scale));
    c.getTypeDesc().getTypes().get(0).getPrimitiveEntry()
        .setTypeQualifiers(new TTypeQualifiers(q));
    return c;
  }

  private static TTableSchema schema(TColumnDesc... columns) {
    return new TTableSchema(Arrays.asList(columns));
  }

  @Test
  public void testArrowTypes() {
    Schema s = HiveRowSetConverter.arrowSchema(schema(
        column("b", TTypeId.BOOLEAN_TYPE), column("ti", TTypeId.TINYINT_TYPE),
        column("si", TTypeId.SMALLINT_TYPE), column("i", TTypeId.INT_TYPE),
        column("bi", TTypeId.BIGINT_TYPE), column("f", TTypeId.FLOAT_TYPE),
        column("d", TTypeId.DOUBLE_TYPE), decimal("dec", 10, 2),
        column("s", TTypeId.STRING_TYPE), column("v", TTypeId.VARCHAR_TYPE),
        column("ts", TTypeId.TIMESTAMP_TYPE), column("dt", TTypeId.DATE_TYPE),
        column("bin", TTypeId.BINARY_TYPE), column("arr", TTypeId.ARRAY_TYPE),
        column("n", TTypeId.NULL_TYPE)));
    assertEquals(Arrays.asList(ArrowType.Bool.INSTANCE, new ArrowType.Int(8, true),
        new ArrowType.Int(16, true), new ArrowType.Int(32, true),
        new ArrowType.Int(64, true),
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
        new ArrowType.Decimal(10, 2, 128), ArrowType.Utf8.INSTANCE,
        ArrowType.Utf8.INSTANCE, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null),
        new ArrowType.Date(DateUnit.DAY), ArrowType.Binary.INSTANCE,
        ArrowType.Utf8.INSTANCE, ArrowType.Null.INSTANCE),
        s.getFields().stream().map(f -> f.getType())
            .collect(java.util.stream.Collectors.toList()));
    assertTrue(s.getFields().get(0).isNullable());
    assertEquals("dec", s.getFields().get(7).getName());
    assertEquals(0, HiveRowSetConverter.arrowSchema(null).getFields().size());
  }

  @Test
  public void testResultColumnMetadata() {
    TColumnDesc varchar = column("v", TTypeId.VARCHAR_TYPE);
    Map<String, TTypeQualifierValue> q = new HashMap<>();
    q.put("characterMaximumLength", TTypeQualifierValue.i32Value(5));
    varchar.getTypeDesc().getTypes().get(0).getPrimitiveEntry()
        .setTypeQualifiers(new TTypeQualifiers(q));
    Schema s = HiveRowSetConverter.arrowSchema(schema(decimal("dec", 10, 2), varchar,
        column("s", TTypeId.STRING_TYPE), column("i", TTypeId.INT_TYPE)));
    org.apache.arrow.flight.sql.FlightSqlColumnMetadata dec =
        new org.apache.arrow.flight.sql.FlightSqlColumnMetadata(
            s.getFields().get(0).getMetadata());
    assertEquals("DECIMAL", dec.getTypeName());
    assertEquals(Integer.valueOf(10), dec.getPrecision());
    assertEquals(Integer.valueOf(2), dec.getScale());
    assertEquals(true, dec.isReadOnly());
    assertEquals("VARCHAR", meta(s, 1, "TYPE_NAME"));
    assertEquals("5", meta(s, 1, "PRECISION"));
    assertEquals(String.valueOf(Integer.MAX_VALUE), meta(s, 2, "PRECISION"));
    assertEquals("INT", meta(s, 3, "TYPE_NAME"));
  }

  private static String meta(Schema s, int field, String key) {
    return s.getFields().get(field).getMetadata().get("ARROW:FLIGHT:SQL:" + key);
  }

  @Test
  public void testColumnarValues() {
    TTableSchema hs2 = schema(column("b", TTypeId.BOOLEAN_TYPE),
        column("ti", TTypeId.TINYINT_TYPE), column("si", TTypeId.SMALLINT_TYPE),
        column("i", TTypeId.INT_TYPE), column("bi", TTypeId.BIGINT_TYPE),
        column("f", TTypeId.FLOAT_TYPE), column("d", TTypeId.DOUBLE_TYPE),
        decimal("dec", 10, 2), column("s", TTypeId.STRING_TYPE),
        column("ts", TTypeId.TIMESTAMP_TYPE), column("dt", TTypeId.DATE_TYPE),
        column("bin", TTypeId.BINARY_TYPE), column("arr", TTypeId.ARRAY_TYPE));
    TRowSet rows = new TRowSet(0, Collections.emptyList());
    rows.setColumns(Arrays.asList(
        TColumn.boolVal(new TBoolColumn(Arrays.asList(true, false), nulls(1))),
        TColumn.byteVal(new TByteColumn(Arrays.asList((byte) -1, (byte) 0), nulls())),
        TColumn.i16Val(new TI16Column(Arrays.asList((short) 7, (short) 0), nulls(1))),
        TColumn.i32Val(new TI32Column(Arrays.asList(1, 2), nulls())),
        TColumn.i64Val(new TI64Column(Arrays.asList(1L << 40, 0L), nulls(1))),
        // Impala sends FLOAT as double
        TColumn.doubleVal(new TDoubleColumn(Arrays.asList(1.5, 0.0), nulls(1))),
        TColumn.doubleVal(new TDoubleColumn(Arrays.asList(2.25, 0.0), nulls(1))),
        strings("-12.50", null),
        strings("x", null),
        strings("2026-11-09 07:00:53.975181999", "1969-12-31 23:59:59"),
        strings("2026-11-09", "1969-12-31"),
        TColumn.binaryVal(new TBinaryColumn(Arrays.asList(
            ByteBuffer.wrap(new byte[] {0, 1, (byte) 0xff}), ByteBuffer.allocate(0)),
            nulls(1))),
        strings("[1,2]", null)));
    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(
             HiveRowSetConverter.arrowSchema(hs2), allocator)) {
      assertEquals(2, HiveRowSetConverter.fill(root, rows));
      assertEquals(2, root.getRowCount());
      assertEquals(true, root.getVector("b").getObject(0));
      assertNull(root.getVector("b").getObject(1));
      assertEquals((byte) -1, root.getVector("ti").getObject(0));
      assertEquals((short) 7, root.getVector("si").getObject(0));
      assertEquals(2, root.getVector("i").getObject(1));
      assertEquals(1L << 40, root.getVector("bi").getObject(0));
      assertEquals(1.5f, root.getVector("f").getObject(0));
      assertEquals(2.25, root.getVector("d").getObject(0));
      assertEquals(new BigDecimal("-12.50"), root.getVector("dec").getObject(0));
      assertNull(root.getVector("dec").getObject(1));
      assertEquals("x", root.getVector("s").getObject(0).toString());
      assertNull(root.getVector("s").getObject(1));
      // nanoseconds truncated to microseconds; wall clock stored as UTC
      assertEquals(java.time.LocalDateTime.parse("2026-11-09T07:00:53.975181"),
          root.getVector("ts").getObject(0));
      assertEquals(java.time.LocalDateTime.parse("1969-12-31T23:59:59"),
          root.getVector("ts").getObject(1));
      assertEquals(-1, ((org.apache.arrow.vector.DateDayVector) root.getVector("dt"))
          .get(1));
      assertArrayEquals(new byte[] {0, 1, (byte) 0xff},
          (byte[]) root.getVector("bin").getObject(0));
      assertNull(root.getVector("bin").getObject(1));
      assertEquals("[1,2]", root.getVector("arr").getObject(0).toString());
    }
  }

  @Test
  public void testRowBasedValues() {
    TTableSchema hs2 = schema(column("i", TTypeId.INT_TYPE),
        column("s", TTypeId.STRING_TYPE));
    TRowSet rows = new TRowSet(0, Arrays.asList(
        new TRow(Arrays.asList(TColumnValue.i32Val(new TI32Value()),
            TColumnValue.stringVal(new TStringValue()))),
        new TRow(Arrays.asList(TColumnValue.i32Val(i32(5)),
            TColumnValue.stringVal(string("y"))))));
    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(
             HiveRowSetConverter.arrowSchema(hs2), allocator)) {
      assertEquals(2, HiveRowSetConverter.fill(root, rows));
      assertNull(root.getVector("i").getObject(0));
      assertNull(root.getVector("s").getObject(0));
      assertEquals(5, root.getVector("i").getObject(1));
      assertEquals("y", root.getVector("s").getObject(1).toString());
    }
  }

  private static TI32Value i32(int v) {
    TI32Value value = new TI32Value();
    value.setValue(v);
    return value;
  }

  private static TStringValue string(String v) {
    TStringValue value = new TStringValue();
    value.setValue(v);
    return value;
  }

  @Test
  public void testEmptyAndMalformed() {
    TTableSchema hs2 = schema(column("ts", TTypeId.TIMESTAMP_TYPE));
    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(
             HiveRowSetConverter.arrowSchema(hs2), allocator)) {
      assertEquals(0,
          HiveRowSetConverter.fill(root, new TRowSet(0, Collections.emptyList())));
      assertEquals(0, HiveRowSetConverter.fill(root, null));
      TRowSet bad = new TRowSet(0, Collections.emptyList());
      bad.setColumns(Collections.singletonList(strings("not a timestamp")));
      try {
        HiveRowSetConverter.fill(root, bad);
        fail("malformed timestamp should be rejected");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("Column ts"));
      }
    }
    assertEquals(1_000_000L, HiveRowSetConverter.timestampMicros("1970-01-01 00:00:01"));
    assertEquals(-1L, HiveRowSetConverter.timestampMicros("1969-12-31 23:59:59.999999"));
  }
}
