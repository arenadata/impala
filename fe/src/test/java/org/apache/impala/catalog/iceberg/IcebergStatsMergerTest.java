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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnValue;
import org.junit.Test;

/**
 * Merge formulas of base and gap column metrics and parsing, union and estimate of
 * compact theta sketches written by the C++ (backend) and Java libraries.
 */
public class IcebergStatsMergerTest {
  private static final HexFormat HEX = HexFormat.of();
  // C++ sketches of the int-basic, int-single and empty cases of vectors.txt.
  private static final String CPP_INT_BASIC = "02030300001acc93070000000000000080b328706"
      + "56950126ad77b16debc161a5d9caa2282e0f92485257a80e40d2d318a4b128f7e632d4251f8ad24b"
      + "2fbf877427fb852a8d54b7b";
  private static final List<Object> INT_BASIC_VALUES = Arrays.asList(
      Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, 42, 42, null, 100000);
  private static final String CPP_INT_SINGLE_7 = "01030300001acc93b78a4cbbdbb4af02";
  private static final String CPP_EMPTY = "01030300001ecc93";
  private static final ScalarType DECIMAL = ScalarType.createDecimalType(10, 2);

  private static TColumnStats stats(double avgSize, long maxSize, long ndv, long nulls,
      long trues, long falses) {
    return new TColumnStats(avgSize, maxSize, ndv, nulls, trues, falses);
  }

  private static TColumnStats withRange(TColumnStats s, TColumnValue low,
      TColumnValue high) {
    if (low != null) s.setLow_value(low);
    if (high != null) s.setHigh_value(high);
    return s;
  }

  private static TColumnValue dec(String ascii) {
    return new TColumnValue().setDecimal_val(ascii.getBytes(US_ASCII));
  }

  private static CompactSketch javaSketch(org.apache.iceberg.types.Type type,
      List<?> values) {
    return PuffinFixtures.sketch(type, values);
  }

  private static List<Object> ints(int... values) {
    List<Object> result = new ArrayList<>();
    for (int v : values) result.add(v);
    return result;
  }

  private static Sketch parse(byte[] bytes) throws ImpalaRuntimeException {
    return IcebergStatsMerger.parse(bytes, "c", 1L);
  }

  private static long unionEstimateOfJava(Sketch a, Sketch b) {
    Union u = SetOperation.builder().setNominalEntries(4096).setSeed(9001).buildUnion();
    u.union(a);
    u.union(b);
    return Math.round(u.getResult().getEstimate());
  }

  @Test
  public void countersAddUpAndVariableSizesAreWeightedByValueCount() {
    // Base: 12 rows, 2 nulls -> 10 values; gap: 5 values.
    TColumnStats base = stats(4.0, 10, 50, 2, 1, 2);
    TColumnStats gap = stats(7.0, 12, 5, 3, 4, 8);
    TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, Type.STRING);
    assertEquals(5, r.getNum_nulls());
    assertEquals(5, r.getNum_trues());
    assertEquals(10, r.getNum_falses());
    assertEquals(12, r.getMax_size());
    assertEquals((4.0 * 10 + 7.0 * 5) / 15, r.getAvg_size(), 1e-12);
    assertEquals(5, r.getNum_distinct_values());
    assertFalse(r.isSetLow_value());
    assertFalse(r.isSetHigh_value());

    // Base max size wins when larger.
    r = IcebergStatsMerger.mergeMetrics(stats(4.0, 20, 50, 2, 0, 0), gap, 12L,
        ScalarType.createVarcharType(30));
    assertEquals(20, r.getMax_size());
  }

  @Test
  public void unknownOperandMakesResultUnknown() {
    TColumnStats known = stats(4.0, 10, 5, 2, 1, 1);
    TColumnStats unknown = stats(-1, -1, 5, -1, -1, -1);
    for (boolean baseUnknown : new boolean[] {true, false}) {
      TColumnStats base = baseUnknown ? unknown : known;
      TColumnStats gap = baseUnknown ? known : unknown;
      TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, Type.STRING);
      assertEquals(-1, r.getNum_nulls());
      assertEquals(-1, r.getNum_trues());
      assertEquals(-1, r.getNum_falses());
      assertEquals(-1, r.getMax_size());
      assertEquals(-1.0, r.getAvg_size(), 0);
    }
  }

  @Test
  public void baseWithoutRecordCountWeighsAsGap() {
    TColumnStats r = IcebergStatsMerger.mergeMetrics(stats(4.0, 10, 50, 2, 0, 0),
        stats(7.0, 12, 5, 3, 0, 0), null, Type.STRING);
    assertEquals(5.5, r.getAvg_size(), 1e-12);
  }

  @Test
  public void emptyGapKeepsBaseAverageAndRange() {
    TColumnStats base = withRange(stats(4.0, 10, 50, 2, 0, 0),
        new TColumnValue().setInt_val(-3), new TColumnValue().setInt_val(9));
    TColumnStats gap = stats(0, 0, 0, 3, 0, 0);
    TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, Type.STRING);
    assertEquals(4.0, r.getAvg_size(), 1e-12);
    assertEquals(10, r.getMax_size());
    assertEquals(5, r.getNum_nulls());

    r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, Type.INT);
    assertEquals(new TColumnValue().setInt_val(-3), r.getLow_value());
    assertEquals(new TColumnValue().setInt_val(9), r.getHigh_value());
    assertEquals(0, r.getNum_distinct_values());

    // Neither side has values: all base rows are null, or nulls exceed the records.
    r = IcebergStatsMerger.mergeMetrics(stats(3.5, 10, 0, 12, 0, 0), gap, 12L,
        Type.STRING);
    assertEquals(3.5, r.getAvg_size(), 0);
    r = IcebergStatsMerger.mergeMetrics(stats(3.5, 10, 0, 20, 0, 0), gap, 12L,
        Type.STRING);
    assertEquals(3.5, r.getAvg_size(), 0);
  }

  @Test
  public void fixedWidthSizesComeFromGap() {
    TColumnStats base = stats(-1, -1, 50, 2, 0, 0);
    TColumnStats gap = stats(4.0, 4, 5, 3, 0, 0);
    for (Type t : new Type[] {Type.INT, Type.BIGINT, Type.DOUBLE, Type.DATE,
        Type.TIMESTAMP, DECIMAL}) {
      TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, t);
      assertEquals(t.toSql(), 4.0, r.getAvg_size(), 0);
      assertEquals(t.toSql(), 4, r.getMax_size());
    }
    TColumnStats r = IcebergStatsMerger.mergeMetrics(stats(1, 1, 2, 0, 7, 3),
        stats(1, 1, 2, 1, 2, 5), 10L, Type.BOOLEAN);
    assertEquals(9, r.getNum_trues());
    assertEquals(8, r.getNum_falses());
    assertEquals(1, r.getNum_nulls());
  }

  @Test
  public void variableWidthTypesMergeSizes() {
    TColumnStats base = stats(2.0, 3, 50, 0, 0, 0);
    TColumnStats gap = stats(6.0, 9, 10, 0, 0, 0);
    for (Type t : new Type[] {Type.STRING, Type.BINARY, ScalarType.createCharType(9),
        ScalarType.createVarcharType(9)}) {
      TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 10L, t);
      assertEquals(t.toSql(), 4.0, r.getAvg_size(), 1e-12);
      assertEquals(t.toSql(), 9, r.getMax_size());
    }
  }

  private static void assertRange(Type type, TColumnValue baseLow, TColumnValue baseHigh,
      TColumnValue gapLow, TColumnValue gapHigh, TColumnValue low, TColumnValue high) {
    TColumnStats base = withRange(stats(8, 8, 5, 0, 0, 0), baseLow, baseHigh);
    TColumnStats gap = withRange(stats(8, 8, 5, 0, 0, 0), gapLow, gapHigh);
    TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 10L, type);
    assertEquals(type.toSql() + " low", low, r.getLow_value());
    assertEquals(type.toSql() + " high", high, r.getHigh_value());
  }

  @Test
  public void rangeIsMinAndMaxOfEachType() {
    assertRange(Type.TINYINT, new TColumnValue().setByte_val((byte) 5),
        new TColumnValue().setByte_val((byte) 10), new TColumnValue().setByte_val((byte) -3),
        new TColumnValue().setByte_val((byte) 8), new TColumnValue().setByte_val((byte) -3),
        new TColumnValue().setByte_val((byte) 10));
    assertRange(Type.SMALLINT, new TColumnValue().setShort_val((short) 5),
        new TColumnValue().setShort_val((short) 300),
        new TColumnValue().setShort_val((short) 7),
        new TColumnValue().setShort_val((short) 301),
        new TColumnValue().setShort_val((short) 5),
        new TColumnValue().setShort_val((short) 301));
    assertRange(Type.INT, new TColumnValue().setInt_val(-5),
        new TColumnValue().setInt_val(100000), new TColumnValue().setInt_val(-6),
        new TColumnValue().setInt_val(99999), new TColumnValue().setInt_val(-6),
        new TColumnValue().setInt_val(100000));
    assertRange(Type.BIGINT, new TColumnValue().setLong_val(1L << 40),
        new TColumnValue().setLong_val(1L << 41), new TColumnValue().setLong_val(-1),
        new TColumnValue().setLong_val(1L << 42), new TColumnValue().setLong_val(-1),
        new TColumnValue().setLong_val(1L << 42));
    for (Type t : new Type[] {Type.FLOAT, Type.DOUBLE}) {
      assertRange(t, new TColumnValue().setDouble_val(-0.5),
          new TColumnValue().setDouble_val(2.25), new TColumnValue().setDouble_val(-0.25),
          new TColumnValue().setDouble_val(3.5), new TColumnValue().setDouble_val(-0.5),
          new TColumnValue().setDouble_val(3.5));
    }
    assertRange(Type.DATE, new TColumnValue().setDate_val(-10),
        new TColumnValue().setDate_val(19000), new TColumnValue().setDate_val(0),
        new TColumnValue().setDate_val(18000), new TColumnValue().setDate_val(-10),
        new TColumnValue().setDate_val(19000));
  }

  @Test
  public void decimalRangeComparesNumbersAndKeepsChosenBytes() {
    // As strings "10.5" < "9.75"; as numbers 9.75 < 10.5.
    assertRange(DECIMAL, dec("10.50"), dec("10.50"), dec("9.75"), dec("9.75"),
        dec("9.75"), dec("10.50"));
    assertRange(DECIMAL, dec("9.75"), dec("9.75"), dec("10.5"), dec("10.5"),
        dec("9.75"), dec("10.5"));
    assertRange(DECIMAL, dec("-1.00"), dec("2"), dec("-1"), dec("2.00"),
        dec("-1.00"), dec("2"));
  }

  @Test
  public void unreadableOperandLeavesRangeUnset() {
    // Hive writes the unscaled value of 10.50 as big-endian bytes.
    TColumnValue hive = new TColumnValue().setDecimal_val(new byte[] {0x04, 0x1a});
    assertRange(DECIMAL, hive, hive, dec("9.75"), dec("9.75"), null, null);
    assertRange(DECIMAL, dec("9.75"), dec("9.75"), hive, hive, null, null);
    // Field of another type.
    assertRange(Type.BIGINT, new TColumnValue().setInt_val(1),
        new TColumnValue().setInt_val(2), new TColumnValue().setLong_val(0),
        new TColumnValue().setLong_val(5), null, null);
    // One side without a range.
    assertRange(Type.INT, null, null, new TColumnValue().setInt_val(0),
        new TColumnValue().setInt_val(5), null, null);
    // Types without a range in HMS.
    assertRange(Type.STRING, new TColumnValue().setString_val("a"),
        new TColumnValue().setString_val("b"), new TColumnValue().setString_val("c"),
        new TColumnValue().setString_val("d"), null, null);

    // Empty gap: the base range must be readable as well.
    TColumnStats base = withRange(stats(8, 8, 5, 0, 0, 0), hive, hive);
    TColumnStats r = IcebergStatsMerger.mergeMetrics(base, stats(0, 0, 0, 0, 0, 0), 10L,
        DECIMAL);
    assertFalse(r.isSetLow_value());
    assertFalse(r.isSetHigh_value());
  }

  @Test
  public void inputsStayUnchanged() {
    TColumnStats base = withRange(stats(4.0, 10, 50, 2, 1, 2), dec("1.5"), dec("3"));
    TColumnStats gap = withRange(stats(7.0, 12, 5, 3, 4, 8), dec("0.5"), dec("2"));
    TColumnStats baseCopy = base.deepCopy();
    TColumnStats gapCopy = gap.deepCopy();
    TColumnStats r = IcebergStatsMerger.mergeMetrics(base, gap, 12L, DECIMAL);
    assertEquals(baseCopy, base);
    assertEquals(gapCopy, gap);
    r.getLow_value().getDecimal_val()[0] = 'x';
    r.getHigh_value().getDecimal_val()[0] = 'x';
    assertEquals(gapCopy, gap);
    assertEquals(baseCopy, base);
  }

  @Test
  public void missingOperandIsDefect() {
    TColumnStats s = stats(1, 1, 1, 0, 0, 0);
    try {
      IcebergStatsMerger.mergeMetrics(null, s, 1L, Type.INT);
      fail("null base accepted");
    } catch (IllegalStateException expected) {
    }
    try {
      IcebergStatsMerger.mergeMetrics(s, null, 1L, Type.INT);
      fail("null gap accepted");
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void emptyBytesAndEmptySketchParseAsEmpty() throws Exception {
    Sketch none = parse(new byte[0]);
    assertTrue(none.isEmpty());
    assertEquals(0, IcebergStatsMerger.estimate(none));
    Sketch cppEmpty = parse(HEX.parseHex(CPP_EMPTY));
    assertTrue(cppEmpty.isEmpty());
    assertEquals(0, IcebergStatsMerger.estimate(cppEmpty));
    Sketch javaEmpty = parse(javaSketch(Types.IntegerType.get(),
        Collections.emptyList()).toByteArray());
    assertTrue(javaEmpty.isEmpty());

    Sketch values = parse(HEX.parseHex(CPP_INT_BASIC));
    assertEquals(7, IcebergStatsMerger.estimate(IcebergStatsMerger.union(none, values)));
    assertEquals(7, IcebergStatsMerger.estimate(
        IcebergStatsMerger.union(values, cppEmpty)));
  }

  @Test
  public void cppSketchMatchesJavaSketchOfSameValues() throws Exception {
    Sketch cpp = parse(HEX.parseHex(CPP_INT_BASIC));
    Sketch java = javaSketch(Types.IntegerType.get(), INT_BASIC_VALUES);
    assertEquals(7, IcebergStatsMerger.estimate(cpp));
    // Union with the same set adds nothing.
    assertEquals(7, IcebergStatsMerger.estimate(IcebergStatsMerger.union(cpp, java)));
    assertEquals(7, IcebergStatsMerger.estimate(parse(java.toByteArray())));
  }

  @Test
  public void singleItemWithoutOrderedFlagKeepsItsValue() throws Exception {
    byte[] cpp = HEX.parseHex(CPP_INT_SINGLE_7);
    cpp[5] &= ~0x10;
    assertArrayEquals(cpp,
        PuffinFixtures.cppUnorderedSingleItem(Types.IntegerType.get(), ints(7)));
    byte[] original = cpp.clone();
    Sketch single = parse(cpp);
    assertArrayEquals(original, cpp);
    assertEquals(1, IcebergStatsMerger.estimate(single));
    assertEquals(1, IcebergStatsMerger.estimate(IcebergStatsMerger.union(single,
        javaSketch(Types.IntegerType.get(), ints(7)))));
    assertEquals(2, IcebergStatsMerger.estimate(IcebergStatsMerger.union(single,
        javaSketch(Types.IntegerType.get(), ints(8)))));
    // The ordered C++ single item and the Java one parse the same.
    assertEquals(1, IcebergStatsMerger.estimate(parse(HEX.parseHex(CPP_INT_SINGLE_7))));
    assertEquals(1, IcebergStatsMerger.estimate(
        parse(javaSketch(Types.IntegerType.get(), ints(7)).toByteArray())));
  }

  private static byte[] cppVector(String name) throws IOException {
    String home = System.getenv("IMPALA_HOME");
    assertNotNull("IMPALA_HOME is not set", home);
    try (Stream<String> lines = Files.lines(
        Paths.get(home, "testdata", "iceberg_theta_vectors", "vectors.txt"))) {
      String line = lines.filter(l -> l.startsWith("S\t" + name + "\t")).findFirst()
          .orElseThrow(() -> new AssertionError("no sketch " + name));
      return HEX.parseHex(line.split("\t")[2]);
    }
  }

  private static void assertCovers(Sketch s, long distinct) {
    assertTrue(s.getLowerBound(3) + " > " + distinct, s.getLowerBound(3) <= distinct);
    assertTrue(s.getUpperBound(3) + " < " + distinct, s.getUpperBound(3) >= distinct);
  }

  @Test
  public void estimationModeUnionMatchesJavaUnion() throws Exception {
    // The C++ and Java libraries keep different entries in estimation mode, so their
    // sketches of one set differ; the oracle is the Java union of the same operands.
    Sketch cpp = parse(cppVector("bigint-estimation"));
    assertTrue(cpp.isEstimationMode());
    assertEquals(4096, cpp.getRetainedEntries());
    assertCovers(cpp, 10000);
    assertEquals(Math.round(cpp.getEstimate()), IcebergStatsMerger.estimate(cpp));

    Sketch java = javaSketch(Types.LongType.get(), LongStream.rangeClosed(5001, 15000)
        .boxed().collect(Collectors.toList()));
    CompactSketch merged = IcebergStatsMerger.union(cpp, parse(java.toByteArray()));
    assertTrue(merged.isCompact());
    assertTrue(merged.isOrdered());
    assertEquals(unionEstimateOfJava(cpp, java), IcebergStatsMerger.estimate(merged));
    assertCovers(merged, 15000);
    // Round trip of the serialized result.
    assertEquals(IcebergStatsMerger.estimate(merged),
        IcebergStatsMerger.estimate(parse(merged.toByteArray())));
  }

  @Test
  public void unionOfSmallSetsIsExactCount() throws Exception {
    Sketch cpp = parse(HEX.parseHex(CPP_INT_BASIC));
    Sketch java = parse(javaSketch(Types.IntegerType.get(), ints(42, 43, 44, 100000))
        .toByteArray());
    CompactSketch merged = IcebergStatsMerger.union(cpp, java);
    assertEquals(9, IcebergStatsMerger.estimate(merged));
    assertTrue(merged.isOrdered());
    List<Object> all = new ArrayList<>(INT_BASIC_VALUES);
    all.addAll(ints(42, 43, 44, 100000));
    assertEquals(Math.round(javaSketch(Types.IntegerType.get(), all).getEstimate()),
        IcebergStatsMerger.estimate(merged));
  }

  private static void assertInvalid(byte[] bytes) {
    try {
      IcebergStatsMerger.parse(bytes, "price", 4242L);
      fail("parsed " + HEX.formatHex(bytes));
    } catch (ImpalaRuntimeException e) {
      assertEquals("Invalid theta sketch for column price (snapshot 4242)",
          e.getMessage());
      assertNotNull(e.getCause());
    }
  }

  @Test
  public void foreignSeedAndCorruptBytesAreRejected() {
    UpdateSketch many = UpdateSketch.builder().setSeed(12345).build();
    many.update(1L);
    many.update(2L);
    assertInvalid(many.compact().toByteArray());
    UpdateSketch one = UpdateSketch.builder().setSeed(12345).build();
    one.update(1L);
    assertInvalid(one.compact().toByteArray());
    assertInvalid(new byte[] {1, 2, 3});
    assertInvalid(new byte[16]);
    // Cut in the middle of the hash array.
    assertInvalid(Arrays.copyOf(HEX.parseHex(CPP_INT_BASIC), 40));
  }

  @Test
  public void unionLeavesOperandsUnchanged() throws Exception {
    CompactSketch a = javaSketch(Types.IntegerType.get(), ints(1, 2, 3));
    CompactSketch b = javaSketch(Types.IntegerType.get(), ints(3, 4));
    byte[] aBytes = a.toByteArray();
    byte[] bBytes = b.toByteArray();
    CompactSketch merged = IcebergStatsMerger.union(a, b);
    assertEquals(4, IcebergStatsMerger.estimate(merged));
    assertArrayEquals(aBytes, a.toByteArray());
    assertArrayEquals(bBytes, b.toByteArray());
  }
}
