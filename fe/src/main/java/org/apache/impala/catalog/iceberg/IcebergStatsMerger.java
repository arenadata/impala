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

package org.apache.impala.catalog.iceberg;

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import javax.annotation.Nullable;

import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnValue;

/**
 * Merges the column statistics of an Iceberg stats base with those of the files added
 * after it, and parses, unions and estimates the compact theta sketches of both.
 * Stateless; inputs are never modified.
 */
public final class IcebergStatsMerger {
  private static final int NOMINAL_ENTRIES = 4096;
  private static final long SEED = 9001;

  // Preamble of a serialized compact theta sketch.
  private static final int SINGLE_ITEM_BYTES = 16;
  private static final int PRE_LONGS_MASK = 0x3f;
  private static final int SER_VER = 3;
  private static final int THETA_FAMILY_ID = 3;
  private static final int FLAGS_BYTE = 5;
  private static final int EMPTY_FLAG = 1 << 2;
  private static final int ORDERED_FLAG = 1 << 4;

  private IcebergStatsMerger() {}

  /**
   * Returns the statistics of base and gap values together. 'gap.num_distinct_values'
   * is the number of non-null gap values (not an NDV) and is copied to the result for
   * the caller to replace with the sketch estimate. 'baseRecords' is the row count of
   * the base snapshot, null if unknown. A negative operand means "unknown" and makes
   * the merged field unknown (-1).
   */
  public static TColumnStats mergeMetrics(TColumnStats base, TColumnStats gap,
      @Nullable Long baseRecords, Type type) {
    Preconditions.checkState(base != null, "base column stats are null");
    Preconditions.checkState(gap != null, "gap column stats are null");
    long gapValues = Math.max(0, gap.getNum_distinct_values());
    TColumnStats result = new TColumnStats();
    result.setNum_distinct_values(gap.getNum_distinct_values());
    result.setNum_nulls(sum(base.getNum_nulls(), gap.getNum_nulls()));
    result.setNum_trues(sum(base.getNum_trues(), gap.getNum_trues()));
    result.setNum_falses(sum(base.getNum_falses(), gap.getNum_falses()));
    if (type.isStringType() || type.isBinary()) {
      result.setMax_size(base.getMax_size() < 0 || gap.getMax_size() < 0 ?
          -1 : Math.max(base.getMax_size(), gap.getMax_size()));
      result.setAvg_size(mergeAvgSize(base, gap, baseRecords, gapValues));
    } else {
      // The slot size: the same for any snapshot.
      result.setMax_size(gap.getMax_size());
      result.setAvg_size(gap.getAvg_size());
    }
    TColumnValue low;
    TColumnValue high;
    if (gapValues == 0) {
      low = readable(base.getLow_value(), type);
      high = readable(base.getHigh_value(), type);
    } else {
      low = pick(base.getLow_value(), gap.getLow_value(), type, true);
      high = pick(base.getHigh_value(), gap.getHigh_value(), type, false);
    }
    if (low != null) result.setLow_value(low.deepCopy());
    if (high != null) result.setHigh_value(high.deepCopy());
    return result;
  }

  private static long sum(long a, long b) { return a < 0 || b < 0 ? -1 : a + b; }

  /**
   * Average size weighted by the number of non-null values. Base nulls that are unknown
   * count as none; without a base row count both sides weigh the same.
   */
  private static double mergeAvgSize(TColumnStats base, TColumnStats gap,
      @Nullable Long baseRecords, long gapValues) {
    if (base.getAvg_size() < 0 || gap.getAvg_size() < 0) return -1;
    long baseValues = baseRecords == null ?
        gapValues : Math.max(0, baseRecords - Math.max(0, base.getNum_nulls()));
    long values = baseValues + gapValues;
    if (values == 0) return base.getAvg_size();
    return (base.getAvg_size() * baseValues + gap.getAvg_size() * gapValues) / values;
  }

  /**
   * The smaller ('min') or larger operand as is, the base one if equal; null unless
   * both hold a readable value of 'type'.
   */
  private static TColumnValue pick(@Nullable TColumnValue base,
      @Nullable TColumnValue gap, Type type, boolean min) {
    Integer cmp = compare(base, gap, type);
    if (cmp == null) return null;
    if (cmp == 0) return base;
    return (cmp < 0) == min ? base : gap;
  }

  private static TColumnValue readable(@Nullable TColumnValue v, Type type) {
    return compare(v, v, type) != null ? v : null;
  }

  /**
   * Compares the values of 'type' in a and b. Returns null if either lacks the field
   * of 'type', if 'type' has no range in HMS, or if a decimal is not an ASCII number
   * (Hive stores the unscaled value instead).
   */
  @Nullable
  private static Integer compare(@Nullable TColumnValue a, @Nullable TColumnValue b,
      Type type) {
    if (a == null || b == null) return null;
    switch (type.getPrimitiveType()) {
      case TINYINT:
        return a.isSetByte_val() && b.isSetByte_val() ?
            Byte.compare(a.getByte_val(), b.getByte_val()) : null;
      case SMALLINT:
        return a.isSetShort_val() && b.isSetShort_val() ?
            Short.compare(a.getShort_val(), b.getShort_val()) : null;
      case INT:
        return a.isSetInt_val() && b.isSetInt_val() ?
            Integer.compare(a.getInt_val(), b.getInt_val()) : null;
      case BIGINT:
        return a.isSetLong_val() && b.isSetLong_val() ?
            Long.compare(a.getLong_val(), b.getLong_val()) : null;
      case FLOAT:
      case DOUBLE:
        return a.isSetDouble_val() && b.isSetDouble_val() ?
            Double.compare(a.getDouble_val(), b.getDouble_val()) : null;
      case DATE:
        return a.isSetDate_val() && b.isSetDate_val() ?
            Integer.compare(a.getDate_val(), b.getDate_val()) : null;
      case DECIMAL: {
        BigDecimal x = decimal(a);
        BigDecimal y = decimal(b);
        return x != null && y != null ? x.compareTo(y) : null;
      }
      default:
        return null;
    }
  }

  @Nullable
  private static BigDecimal decimal(TColumnValue v) {
    if (!v.isSetDecimal_val()) return null;
    try {
      return new BigDecimal(new String(v.getDecimal_val(), US_ASCII));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Parses a compact theta sketch serialized by the C++ or Java library. No bytes, the
   * NULL result of the backend, is the empty sketch. 'colName' and 'snapshotId' only
   * name the sketch in the error.
   */
  public static Sketch parse(byte[] bytes, String colName, long snapshotId)
      throws ImpalaRuntimeException {
    if (bytes.length == 0) return emptySketch();
    try {
      return CompactSketch.heapify(Memory.wrap(withOrderedSingleItem(bytes)), SEED);
    } catch (SketchesException | IllegalArgumentException e) {
      // The library also reports corrupt bytes as IllegalArgumentException.
      throw new ImpalaRuntimeException(String.format(
          "Invalid theta sketch for column %s (snapshot %d)", colName, snapshotId), e);
    }
  }

  /**
   * The C++ library may write a one-value sketch without the ORDERED flag, which the
   * Java library reads as empty. One value is always ordered, so such a sketch is read
   * from a copy with the flag set.
   */
  private static byte[] withOrderedSingleItem(byte[] bytes) {
    if (bytes.length != SINGLE_ITEM_BYTES || (bytes[0] & PRE_LONGS_MASK) != 1
        || bytes[1] != SER_VER || bytes[2] != THETA_FAMILY_ID
        || (bytes[FLAGS_BYTE] & (EMPTY_FLAG | ORDERED_FLAG)) != 0) {
      return bytes;
    }
    byte[] copy = bytes.clone();
    copy[FLAGS_BYTE] |= ORDERED_FLAG;
    return copy;
  }

  public static CompactSketch emptySketch() {
    return UpdateSketch.builder().setNominalEntries(NOMINAL_ENTRIES).setSeed(SEED)
        .build().compact();
  }

  /** Union of parsed sketches, compact and ordered. */
  public static CompactSketch union(Sketch a, Sketch b) {
    Union union = SetOperation.builder().setNominalEntries(NOMINAL_ENTRIES)
        .setSeed(SEED).buildUnion();
    union.union(a);
    union.union(b);
    return union.getResult(true, null);
  }

  public static long estimate(Sketch s) { return Math.round(s.getEstimate()); }
}
