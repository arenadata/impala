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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TColumnValue;
import org.apache.hive.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeEntry;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;

/**
 * Converts HiveServer2 results of the coordinator into Arrow: the result schema
 * (GetResultSetMetadata) to an Arrow schema, and row sets (FetchResults, columnar V6 or
 * row-based) into a VectorSchemaRoot.
 *
 * Impala's HS2 encoding (be/src/service/hs2-util.cc): BOOLEAN / TINYINT / SMALLINT /
 * INT / BIGINT in the matching columns, FLOAT and DOUBLE as doubles, TIMESTAMP / DATE /
 * DECIMAL / STRING / CHAR / VARCHAR as text, ARRAY / MAP / STRUCT as JSON text, BINARY
 * as bytes. TIMESTAMP becomes timestamp(us) without time zone (Impala's nanoseconds are
 * truncated); complex types stay JSON text (utf8), as in HS2 and Kyuubi.
 */
final class HiveRowSetConverter {
  private HiveRowSetConverter() {}

  /**
   * Arrow schema for an HS2 result schema (null or no columns: empty schema), fields with
   * Flight SQL column metadata (type name, precision, scale, ...; see
   * ImpalaArrowTypes.columnMetadata).
   */
  static Schema arrowSchema(TTableSchema schema) {
    if (schema == null || !schema.isSetColumns()) {
      return new Schema(Collections.emptyList());
    }
    List<Field> fields = new ArrayList<>();
    for (TColumnDesc column : schema.getColumns()) {
      fields.add(new Field(column.getColumnName(),
          new FieldType(true, arrowType(column), null,
              columnMetadata(column)),
          Collections.emptyList()));
    }
    return new Schema(fields);
  }

  private static Map<String, String> columnMetadata(TColumnDesc column) {
    List<TTypeEntry> types = column.getTypeDesc() == null ? null
        : column.getTypeDesc().getTypes();
    TTypeEntry entry = types == null || types.isEmpty() ? null : types.get(0);
    String name;
    Integer precision = null;
    Integer scale = null;
    Integer length = null;
    if (entry != null && entry.isSetPrimitiveEntry()) {
      TPrimitiveTypeEntry primitive = entry.getPrimitiveEntry();
      name = ImpalaArrowTypes.baseName(primitive.getType());
      precision = qualifierOrNull(primitive, "precision");
      scale = qualifierOrNull(primitive, "scale");
      length = qualifierOrNull(primitive, "characterMaximumLength");
    } else if (entry != null && entry.isSetArrayEntry()) {
      name = "ARRAY";
    } else if (entry != null && entry.isSetMapEntry()) {
      name = "MAP";
    } else if (entry != null && entry.isSetStructEntry()) {
      name = "STRUCT";
    } else {
      name = "STRING";
    }
    return ImpalaArrowTypes.columnMetadata(
        name, precision, scale, length, true, null, null);
  }

  private static Integer qualifierOrNull(TPrimitiveTypeEntry primitive, String name) {
    if (!primitive.isSetTypeQualifiers()) return null;
    Map<String, TTypeQualifierValue> q = primitive.getTypeQualifiers().getQualifiers();
    TTypeQualifierValue value = q == null ? null : q.get(name);
    return value != null && value.isSetI32Value() ? value.getI32Value() : null;
  }

  static ArrowType arrowType(TColumnDesc column) {
    List<TTypeEntry> types = column.getTypeDesc() == null ? null
        : column.getTypeDesc().getTypes();
    if (types == null || types.isEmpty() || !types.get(0).isSetPrimitiveEntry()) {
      // array/map/struct type entries: JSON text
      return ArrowType.Utf8.INSTANCE;
    }
    TPrimitiveTypeEntry primitive = types.get(0).getPrimitiveEntry();
    return ImpalaArrowTypes.forTypeId(primitive.getType(),
        qualifier(primitive, "precision", 38), qualifier(primitive, "scale", 0));
  }

  private static int qualifier(TPrimitiveTypeEntry primitive, String name, int dflt) {
    if (!primitive.isSetTypeQualifiers()) return dflt;
    Map<String, TTypeQualifierValue> q = primitive.getTypeQualifiers().getQualifiers();
    TTypeQualifierValue value = q == null ? null : q.get(name);
    return value != null && value.isSetI32Value() ? value.getI32Value() : dflt;
  }

  /** Number of rows in a row set (columnar or row-based). */
  static int rowCount(TRowSet rowSet) {
    if (rowSet == null) return 0;
    if (rowSet.isSetColumns() && !rowSet.getColumns().isEmpty()) {
      return values(rowSet.getColumns().get(0)).size();
    }
    return rowSet.getRowsSize();
  }

  /**
   * Replaces the contents of 'root' with the rows of 'rowSet'; returns the row count.
   * The root's schema must be arrowSchema() of the same result.
   */
  static int fill(VectorSchemaRoot root, TRowSet rowSet) {
    int rows = rowCount(rowSet);
    root.allocateNew();
    List<FieldVector> vectors = root.getFieldVectors();
    boolean columnar = rowSet != null && rowSet.isSetColumns()
        && !rowSet.getColumns().isEmpty();
    if (rows > 0 && columnar && rowSet.getColumnsSize() != vectors.size()) {
      throw new IllegalStateException("Result has " + rowSet.getColumnsSize()
          + " columns, schema " + vectors.size());
    }
    for (int c = 0; c < vectors.size(); c++) {
      FieldVector vector = vectors.get(c);
      if (columnar) {
        TColumn column = rowSet.getColumns().get(c);
        List<?> values = values(column);
        byte[] nulls = nulls(column);
        for (int r = 0; r < rows; r++) {
          set(vector, r, isNull(nulls, r) ? null : values.get(r));
        }
      } else {
        for (int r = 0; r < rows; r++) {
          TRow row = rowSet.getRows().get(r);
          set(vector, r, value(row.getColVals().get(c)));
        }
      }
      vector.setValueCount(rows);
    }
    root.setRowCount(rows);
    return rows;
  }

  private static List<?> values(TColumn column) {
    switch (column.getSetField()) {
      case BOOL_VAL: return column.getBoolVal().getValues();
      case BYTE_VAL: return column.getByteVal().getValues();
      case I16_VAL: return column.getI16Val().getValues();
      case I32_VAL: return column.getI32Val().getValues();
      case I64_VAL: return column.getI64Val().getValues();
      case DOUBLE_VAL: return column.getDoubleVal().getValues();
      case STRING_VAL: return column.getStringVal().getValues();
      case BINARY_VAL: return column.getBinaryVal().getValues();
      default: return Collections.emptyList();
    }
  }

  private static byte[] nulls(TColumn column) {
    switch (column.getSetField()) {
      case BOOL_VAL: return column.getBoolVal().getNulls();
      case BYTE_VAL: return column.getByteVal().getNulls();
      case I16_VAL: return column.getI16Val().getNulls();
      case I32_VAL: return column.getI32Val().getNulls();
      case I64_VAL: return column.getI64Val().getNulls();
      case DOUBLE_VAL: return column.getDoubleVal().getNulls();
      case STRING_VAL: return column.getStringVal().getNulls();
      case BINARY_VAL: return column.getBinaryVal().getNulls();
      default: return null;
    }
  }

  private static boolean isNull(byte[] nulls, int row) {
    return nulls != null && row / 8 < nulls.length
        && (nulls[row / 8] & (1 << (row % 8))) != 0;
  }

  /** Value of a row-based cell, null for NULL. */
  private static Object value(TColumnValue v) {
    switch (v.getSetField()) {
      case BOOL_VAL: return v.getBoolVal().isSetValue() ? v.getBoolVal().isValue() : null;
      case BYTE_VAL:
        return v.getByteVal().isSetValue() ? v.getByteVal().getValue() : null;
      case I16_VAL: return v.getI16Val().isSetValue() ? v.getI16Val().getValue() : null;
      case I32_VAL: return v.getI32Val().isSetValue() ? v.getI32Val().getValue() : null;
      case I64_VAL: return v.getI64Val().isSetValue() ? v.getI64Val().getValue() : null;
      case DOUBLE_VAL:
        return v.getDoubleVal().isSetValue() ? v.getDoubleVal().getValue() : null;
      case STRING_VAL:
        return v.getStringVal().isSetValue() ? v.getStringVal().getValue() : null;
      default: return null;
    }
  }

  private static void set(FieldVector vector, int row, Object value) {
    if (value == null) {
      vector.setNull(row);
      return;
    }
    try {
      if (vector instanceof VarCharVector) {
        ((VarCharVector) vector).setSafe(row, text(value));
      } else if (vector instanceof BigIntVector) {
        ((BigIntVector) vector).setSafe(row, number(value).longValue());
      } else if (vector instanceof IntVector) {
        ((IntVector) vector).setSafe(row, number(value).intValue());
      } else if (vector instanceof SmallIntVector) {
        ((SmallIntVector) vector).setSafe(row, number(value).shortValue());
      } else if (vector instanceof TinyIntVector) {
        ((TinyIntVector) vector).setSafe(row, number(value).byteValue());
      } else if (vector instanceof Float8Vector) {
        ((Float8Vector) vector).setSafe(row, number(value).doubleValue());
      } else if (vector instanceof Float4Vector) {
        ((Float4Vector) vector).setSafe(row, number(value).floatValue());
      } else if (vector instanceof BitVector) {
        ((BitVector) vector).setSafe(row, bool(value) ? 1 : 0);
      } else if (vector instanceof DecimalVector) {
        DecimalVector decimal = (DecimalVector) vector;
        decimal.setSafe(row, new BigDecimal(value.toString().trim())
            .setScale(decimal.getScale(), RoundingMode.UNNECESSARY));
      } else if (vector instanceof TimeStampMicroVector) {
        ((TimeStampMicroVector) vector).setSafe(row, timestampMicros(value.toString()));
      } else if (vector instanceof DateDayVector) {
        ((DateDayVector) vector).setSafe(row,
            (int) LocalDate.parse(value.toString().trim()).toEpochDay());
      } else if (vector instanceof VarBinaryVector) {
        ((VarBinaryVector) vector).setSafe(row, bytes(value));
      } else {
        // NullVector (NULL type) and anything unexpected
        vector.setNull(row);
      }
    } catch (NumberFormatException | ArithmeticException | DateTimeParseException e) {
      throw new IllegalArgumentException("Column " + vector.getName()
          + ": cannot convert '" + value + "' to " + vector.getField().getType(), e);
    }
  }

  /** Impala's 'yyyy-MM-dd HH:mm:ss[.fffffffff]' (wall clock) -> microseconds, as UTC. */
  static long timestampMicros(String text) {
    String s = text.trim();
    LocalDateTime ts = s.length() == 10 ? LocalDate.parse(s).atStartOfDay()
        : LocalDateTime.parse(s.replace(' ', 'T'));
    return Math.addExact(Math.multiplyExact(ts.toEpochSecond(ZoneOffset.UTC), 1_000_000L),
        ts.getNano() / 1000L);
  }

  private static byte[] text(Object value) {
    if (value instanceof ByteBuffer) return bytes(value);
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] bytes(Object value) {
    if (value instanceof byte[]) return (byte[]) value;
    if (value instanceof ByteBuffer) {
      ByteBuffer b = ((ByteBuffer) value).duplicate();
      byte[] out = new byte[b.remaining()];
      b.get(out);
      return out;
    }
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static Number number(Object value) {
    if (value instanceof Number) return (Number) value;
    return new BigDecimal(value.toString().trim());
  }

  private static boolean bool(Object value) {
    if (value instanceof Boolean) return (Boolean) value;
    if (value instanceof Number) return ((Number) value).intValue() != 0;
    return Boolean.parseBoolean(value.toString().trim());
  }
}
