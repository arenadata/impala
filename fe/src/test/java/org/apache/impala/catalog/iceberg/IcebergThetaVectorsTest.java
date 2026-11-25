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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.HashIterator;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.Test;

/**
 * Confirms testdata/iceberg_theta_vectors/vectors.txt, the reference the backend test of
 * ds_theta_sketch_iceberg compares with, by DataSketches Java and Iceberg Conversions.
 * With -Dimpala.theta.vectors.regenerate=true the test first rewrites the hex fields of
 * the file from its values; comments, values and line order stay as they are.
 */
public class IcebergThetaVectorsTest {
  private static final String REGENERATE_PROPERTY = "impala.theta.vectors.regenerate";
  private static final int K = 4096;
  private static final long SEED = 9001;
  private static final long THETA_MAX = Long.MAX_VALUE;
  private static final short SEED_HASH = ThetaUtil.computeSeedHash(SEED);
  // Header of compact_theta_sketch_alloc::serialize() in DataSketches C++ 3.0.0.
  private static final byte SERIAL_VERSION = 3;
  private static final byte COMPACT_FAMILY = 3;
  private static final byte FLAGS = 0x1a; // READ_ONLY | COMPACT | ORDERED
  private static final byte FLAGS_EMPTY = 0x1e; // FLAGS | EMPTY
  private static final Pattern CASE_NAME = Pattern.compile("[a-z0-9_-]+");
  private static final Pattern HEX = Pattern.compile("([0-9a-f]{2})+");
  private static final Pattern INTEGER = Pattern.compile("-?[0-9]+");
  private static final Pattern DECIMAL_TYPE =
      Pattern.compile("DECIMAL\\((\\d+),(\\d+)\\)");
  private static final BaseEncoding HEX_CODEC = BaseEncoding.base16().lowerCase();

  /** Case name suffix of each hashed type and the type of its cases. */
  private static final Map<String, String> TYPE_CASES = new LinkedHashMap<>();
  static {
    TYPE_CASES.put("boolean", "BOOLEAN");
    TYPE_CASES.put("int", "INT");
    TYPE_CASES.put("bigint", "BIGINT");
    TYPE_CASES.put("float", "FLOAT");
    TYPE_CASES.put("double", "DOUBLE");
    TYPE_CASES.put("date", "DATE");
    TYPE_CASES.put("timestamp", "TIMESTAMP");
    TYPE_CASES.put("decimal9", "DECIMAL(9,2)");
    TYPE_CASES.put("decimal18", "DECIMAL(18,2)");
    TYPE_CASES.put("decimal38", "DECIMAL(38,2)");
    TYPE_CASES.put("string", "STRING");
  }

  @Test
  public void vectorsMatchDataSketchesJava() throws IOException {
    Path path = vectorsPath();
    if (Boolean.getBoolean(REGENERATE_PROPERTY)) {
      Files.write(path, (String.join("\n", regenerate(Files.readAllLines(path, UTF_8)))
          + "\n").getBytes(UTF_8));
    }
    Vectors vectors = parse(Files.readAllLines(path, UTF_8));
    List<String> mismatches = new ArrayList<>();
    for (ValueLine v : vectors.valueLines) {
      String actual = icebergHex(v.vectorCase.type, v.parsed);
      if (!actual.equals(v.icebergHex)) {
        mismatches.add(String.format("line %d: Iceberg bytes are %s, the file has %s",
            v.number, actual, v.icebergHex));
      }
    }
    for (Case c : vectors.cases.values()) {
      Canonical canonical = Canonical.of(PuffinFixtures.sketch(c.type, c.values));
      String actual = HEX_CODEC.encode(canonical.bytes());
      if (!actual.equals(c.sketchHex)) {
        mismatches.add(String.format("line %d: sketch of case %s differs from "
            + "DataSketches Java at hex offset %d", c.sketchLine, c.name,
            firstDifference(actual, c.sketchHex)));
      }
      CompactSketch read = CompactSketch.heapify(Memory.wrap(canonical.bytes()), SEED);
      assertEquals(c.name, canonical.hashes.length == 0, read.isEmpty());
      assertEquals(c.name, canonical.theta, read.getThetaLong());
      assertArrayEquals(c.name, canonical.hashes, sortedHashes(read));
    }
    assertTrue(String.join("\n", mismatches), mismatches.isEmpty());
  }

  @Test
  public void requiredCasesPresent() throws IOException {
    Vectors vectors = parse(Files.readAllLines(vectorsPath(), UTF_8));
    for (Map.Entry<String, String> e : TYPE_CASES.entrySet()) {
      Case basic = requireCase(vectors, e.getKey() + "-basic", e.getValue());
      assertTrue(basic.name, basic.values.size() >= 3 && basic.values.size() <= 20);
      assertTrue(basic.name + " has no NULL", basic.values.contains(null));
      List<Object> nonNull = new ArrayList<>(basic.values);
      nonNull.removeIf(Objects::isNull);
      assertTrue(basic.name + " has no repeat",
          new HashSet<>(nonNull).size() < nonNull.size());
      Case single = requireCase(vectors, e.getKey() + "-single", e.getValue());
      assertEquals(single.name, 1, single.values.size());
      assertNotNull(single.name, single.values.get(0));
    }
    assertTrue(requireCase(vectors, "empty", "INT").values.isEmpty());
    Case allNull = vectors.cases.get("all-null");
    assertNotNull("all-null", allNull);
    assertFalse(allNull.values.isEmpty());
    assertTrue(allNull.values.stream().allMatch(Objects::isNull));
    assertEquals(Arrays.asList("", "abc"),
        requireCase(vectors, "string-empty", "STRING").values);
    assertEquals(10000,
        requireCase(vectors, "bigint-estimation", "BIGINT").values.size());
    assertEquals(10000, requireCase(vectors, "int-estimation", "INT").values.size());
  }

  @Test
  public void basicCasesCoverDomainBounds() throws IOException {
    Vectors vectors = parse(Files.readAllLines(vectorsPath(), UTF_8));
    assertHasValues(vectors.cases.get("int-basic"),
        Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1);
    assertHasValues(vectors.cases.get("bigint-basic"),
        Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L);
    assertHasValues(vectors.cases.get("float-basic"), 0.0f, -0.0f,
        Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY);
    assertHasValues(vectors.cases.get("double-basic"), 0.0, -0.0,
        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    for (String t : new String[] {"decimal9", "decimal18", "decimal38"}) {
      Case c = vectors.cases.get(t + "-basic");
      int precision = ((Types.DecimalType) c.type).precision();
      BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
      assertHasValues(c, decimal(0), decimal(127), decimal(128), decimal(-128),
          decimal(-129), new BigDecimal(max, 2));
    }
    assertTrue(vectors.cases.get("date-basic").values.stream()
        .anyMatch(v -> v != null && (Integer) v < 0));
    assertTrue(vectors.cases.get("timestamp-basic").values.stream()
        .anyMatch(v -> v != null && (Long) v < 0));
    assertTrue(vectors.cases.get("timestamp-basic").values.stream()
        .anyMatch(v -> v != null && (Long) v % 1_000_000 != 0));
    List<Object> strings = vectors.cases.get("string-basic").values;
    assertTrue(strings.stream().anyMatch(v -> v != null && ((String) v).chars()
        .allMatch(ch -> ch < 0x80)));
    assertTrue(strings.stream().anyMatch(v -> v != null && ((String) v).chars()
        .anyMatch(ch -> ch >= 0x80)));
  }

  @Test
  public void canonicalLayoutFollowsContent() {
    String seedHash = HEX_CODEC.encode(ByteBuffer.allocate(2)
        .order(ByteOrder.LITTLE_ENDIAN).putShort(SEED_HASH).array());
    assertEquals("01030300001e" + seedHash, canonicalHex(Types.LongType.get()));
    String single = canonicalHex(Types.LongType.get(), 1L);
    assertEquals(32, single.length());
    assertTrue(single.startsWith("01030300001a" + seedHash));
    String exact = canonicalHex(Types.LongType.get(), 1L, 2L);
    assertEquals(2 * 8 * (2 + 2), exact.length());
    assertTrue(exact.startsWith("02030300001a" + seedHash + "02000000" + "00000000"));
    Object[] many = LongStream.range(0, 10000).boxed().toArray();
    String estimation = canonicalHex(Types.LongType.get(), many);
    assertEquals(2 * 8 * (3 + K), estimation.length());
    assertTrue(
        estimation.startsWith("03030300001a" + seedHash + "00100000" + "00000000"));
  }

  @Test
  public void regenerateRewritesOnlyHexFields() {
    List<String> input = Arrays.asList(
        "# comment", "",
        "V\ta\tINT\t1\t-",
        "V\ta\tINT\tNULL\tab",
        "S\ta\t00",
        "G\tg\tINT\t5\t3",
        "S\tg\t00");
    List<String> expected = Arrays.asList(
        "# comment", "",
        "V\ta\tINT\t1\t01000000",
        "V\ta\tINT\tNULL\t-",
        "S\ta\t" + canonicalHex(Types.IntegerType.get(), 1, null),
        "G\tg\tINT\t5\t3",
        "S\tg\t" + canonicalHex(Types.IntegerType.get(), 5, 6, 7));
    assertEquals(expected, regenerate(input));
    assertEquals(expected, regenerate(expected));
  }

  @Test
  public void malformedLinesFailWithLineNumber() {
    assertParseError("line 2: V needs 5 fields", "# comment", "V\ta\tINT\t1");
    assertParseError("line 1: unknown record", "X\ta\tINT");
    assertParseError("line 1: bad case name", "V\tA!\tINT\t1\t-");
    assertParseError("line 1: unknown type", "V\ta\tVARCHAR\t1\t-");
    assertParseError("line 1: bad INT value", "V\ta\tINT\t1.5\t-");
    assertParseError("line 1: bad INT value", "V\ta\tINT\t2147483648\t-");
    assertParseError("line 1: bad BOOLEAN value", "V\ta\tBOOLEAN\tTRUE\t-");
    assertParseError("line 1: bad FLOAT value", "V\ta\tFLOAT\t0x7fc00000\t-");
    assertParseError("line 1: bad DOUBLE value", "V\ta\tDOUBLE\t1.0\t-");
    assertParseError("line 1: bad DECIMAL(9,2) value",
        "V\ta\tDECIMAL(9,2)\t1000000000\t-");
    assertParseError("line 1: bad STRING value", "V\ta\tSTRING\tff\t-");
    assertParseError("line 1: bad hex", "V\ta\tINT\t1\t0A000000");
    assertParseError("line 1: bad hex", "S\ta\t-");
    assertParseError("line 1: bad G range", "G\ta\tINT\t2147483647\t2");
    assertParseError("line 1: G needs INT or BIGINT", "G\ta\tDATE\t1\t2");
  }

  @Test
  public void inconsistentCasesFail() {
    assertParseError("line 1: case a has no S line", "V\ta\tINT\t1\t-", "", "S\tb\t00");
    assertParseError("line 2: second S line of case a", "S\ta\t00", "S\ta\t00");
    assertParseError("line 2: case a mixes INT and BIGINT",
        "V\ta\tINT\t1\t-", "V\ta\tBIGINT\t1\t-", "S\ta\t00");
    assertParseError("line 2: case a has a record after its S line",
        "S\ta\t00", "V\ta\tINT\t1\t-");
  }

  private static Path vectorsPath() {
    String home = System.getenv("IMPALA_HOME");
    assertNotNull("IMPALA_HOME is not set", home);
    return Paths.get(home, "testdata", "iceberg_theta_vectors", "vectors.txt");
  }

  /** Returns the lines with every hex field recomputed; nothing else changes. */
  private static List<String> regenerate(List<String> input) {
    List<String> lines = new ArrayList<>(input);
    Vectors vectors = parse(lines);
    for (ValueLine v : vectors.valueLines) {
      lines.set(v.number - 1, String.join("\t", "V", v.vectorCase.name,
          v.vectorCase.typeName, v.value, icebergHex(v.vectorCase.type, v.parsed)));
    }
    for (Case c : vectors.cases.values()) {
      byte[] sketch = Canonical.of(PuffinFixtures.sketch(c.type, c.values)).bytes();
      lines.set(c.sketchLine - 1,
          String.join("\t", "S", c.name, HEX_CODEC.encode(sketch)));
    }
    return lines;
  }

  private static String icebergHex(Type type, Object value) {
    if (value == null) return "-";
    byte[] bytes = ByteBuffers.toByteArray(Conversions.toByteBuffer(type, value));
    return bytes.length == 0 ? "-" : HEX_CODEC.encode(bytes);
  }

  private static String canonicalHex(Type type, Object... values) {
    return HEX_CODEC.encode(
        Canonical.of(PuffinFixtures.sketch(type, Arrays.asList(values))).bytes());
  }

  private static long[] sortedHashes(Sketch sketch) {
    LongStream.Builder hashes = LongStream.builder();
    HashIterator it = sketch.iterator();
    while (it.next()) hashes.add(it.get());
    return hashes.build().sorted().toArray();
  }

  private static int firstDifference(String a, String b) {
    int n = Math.min(a.length(), b.length());
    for (int i = 0; i < n; i++) {
      if (a.charAt(i) != b.charAt(i)) return i;
    }
    return n;
  }

  private static Case requireCase(Vectors vectors, String name, String typeName) {
    Case c = vectors.cases.get(name);
    assertNotNull("no case " + name, c);
    assertEquals(name, typeName, c.typeName);
    return c;
  }

  private static void assertHasValues(Case c, Object... expected) {
    for (Object e : expected) {
      // Boxed equals tells -0.0 from 0.0 for FLOAT and DOUBLE.
      assertTrue(c.name + " has no " + e, c.values.contains(e));
    }
  }

  private static BigDecimal decimal(long unscaled) {
    return BigDecimal.valueOf(unscaled, 2);
  }

  private static void assertParseError(String expected, String... lines) {
    try {
      parse(Arrays.asList(lines));
      fail("no error, expected: " + expected);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expected));
    }
  }

  /** Content of the canonical sketch: the k smallest hashes and the theta they imply. */
  private static final class Canonical {
    final long theta;
    final long[] hashes;

    private Canonical(long theta, long[] hashes) {
      Preconditions.checkState(hashes.length > 0 || theta == THETA_MAX);
      this.theta = theta;
      this.hashes = hashes;
    }

    static Canonical of(CompactSketch sketch) {
      long[] hashes = sortedHashes(sketch);
      if (hashes.length > K) return new Canonical(hashes[K], Arrays.copyOf(hashes, K));
      return new Canonical(sketch.getThetaLong(), hashes);
    }

    /** Layout chosen by content: empty, single item, exact or estimation mode. */
    byte[] bytes() {
      int preLongs;
      if (hashes.length == 0) {
        preLongs = 1;
      } else if (theta < THETA_MAX) {
        preLongs = 3;
      } else {
        preLongs = hashes.length == 1 ? 1 : 2;
      }
      ByteBuffer b = ByteBuffer.allocate(8 * (preLongs + hashes.length))
          .order(ByteOrder.LITTLE_ENDIAN);
      b.put((byte) preLongs).put(SERIAL_VERSION).put(COMPACT_FAMILY).put((byte) 0)
          .put((byte) 0).put(hashes.length == 0 ? FLAGS_EMPTY : FLAGS)
          .putShort(SEED_HASH);
      if (preLongs >= 2) b.putInt(hashes.length).putInt(0);
      if (preLongs == 3) b.putLong(theta);
      for (long h : hashes) b.putLong(h);
      return b.array();
    }
  }

  private static final class Case {
    final String name;
    final int firstLine;
    String typeName;
    Type type;
    final List<Object> values = new ArrayList<>();
    int sketchLine;
    String sketchHex;

    Case(String name, int firstLine) {
      this.name = name;
      this.firstLine = firstLine;
    }
  }

  /** A V line; 'parsed' is the Java value Conversions takes, null for NULL. */
  private static final class ValueLine {
    final int number;
    final Case vectorCase;
    final String value;
    final Object parsed;
    final String icebergHex;

    ValueLine(
        int number, Case vectorCase, String value, Object parsed, String icebergHex) {
      this.number = number;
      this.vectorCase = vectorCase;
      this.value = value;
      this.parsed = parsed;
      this.icebergHex = icebergHex;
    }
  }

  private static final class Vectors {
    final Map<String, Case> cases = new LinkedHashMap<>();
    final List<ValueLine> valueLines = new ArrayList<>();
  }

  /** Parses the file; any malformed line or inconsistent case throws with its line. */
  private static Vectors parse(List<String> lines) {
    Vectors vectors = new Vectors();
    for (int i = 0; i < lines.size(); i++) {
      int n = i + 1;
      String line = lines.get(i);
      if (line.isEmpty() || line.startsWith("#")) continue;
      String[] f = line.split("\t", -1);
      switch (f[0]) {
        case "V":
          parseValueLine(vectors, n, f);
          break;
        case "G":
          parseRange(vectors, n, f);
          break;
        case "S":
          parseSketch(vectors, n, f);
          break;
        default:
          throw error(n, "unknown record '" + f[0] + "'");
      }
    }
    for (Case c : vectors.cases.values()) {
      if (c.sketchHex == null) {
        throw error(c.firstLine, "case " + c.name + " has no S line");
      }
    }
    return vectors;
  }

  private static void parseValueLine(Vectors vectors, int n, String[] f) {
    if (f.length != 5) throw error(n, "V needs 5 fields, got " + f.length);
    Case c = caseWithValues(vectors, n, f[1], f[2]);
    Object parsed = parseValue(n, c, f[3]);
    if (!f[4].equals("-")) checkHex(n, f[4]);
    c.values.add(parsed);
    vectors.valueLines.add(new ValueLine(n, c, f[3], parsed, f[4]));
  }

  private static void parseRange(Vectors vectors, int n, String[] f) {
    if (f.length != 5) throw error(n, "G needs 5 fields, got " + f.length);
    Case c = caseWithValues(vectors, n, f[1], f[2]);
    boolean isInt = c.type.typeId() == Type.TypeID.INTEGER;
    if (!isInt && c.type.typeId() != Type.TypeID.LONG) {
      throw error(n, "G needs INT or BIGINT, got " + f[2]);
    }
    BigInteger start = parseInteger(n, f[3]);
    BigInteger count = parseInteger(n, f[4]);
    BigInteger last = start.add(count).subtract(BigInteger.ONE);
    long min = isInt ? Integer.MIN_VALUE : Long.MIN_VALUE;
    long max = isInt ? Integer.MAX_VALUE : Long.MAX_VALUE;
    if (count.signum() < 0 || count.compareTo(BigInteger.valueOf(1_000_000)) > 0
        || start.compareTo(BigInteger.valueOf(min)) < 0
        || last.compareTo(BigInteger.valueOf(max)) > 0) {
      throw error(n, "bad G range " + f[3] + " " + f[4]);
    }
    for (long v = start.longValueExact(), i = 0; i < count.longValue(); v++, i++) {
      c.values.add(isInt ? (Object) (int) v : (Object) v);
    }
  }

  private static void parseSketch(Vectors vectors, int n, String[] f) {
    if (f.length != 3) throw error(n, "S needs 3 fields, got " + f.length);
    Case c = caseNamed(vectors, n, f[1]);
    if (c.sketchHex != null) throw error(n, "second S line of case " + c.name);
    checkHex(n, f[2]);
    if (c.typeName == null) {
      c.typeName = "INT";
      c.type = Types.IntegerType.get();
    }
    c.sketchLine = n;
    c.sketchHex = f[2];
  }

  private static Case caseNamed(Vectors vectors, int n, String name) {
    if (!CASE_NAME.matcher(name).matches()) {
      throw error(n, "bad case name '" + name + "'");
    }
    return vectors.cases.computeIfAbsent(name, k -> new Case(k, n));
  }

  private static Case caseWithValues(
      Vectors vectors, int n, String name, String typeName) {
    Case c = caseNamed(vectors, n, name);
    if (c.sketchHex != null) {
      throw error(n, "case " + name + " has a record after its S line");
    }
    if (c.typeName == null) {
      c.type = icebergType(n, typeName);
      c.typeName = typeName;
    } else if (!c.typeName.equals(typeName)) {
      throw error(n, "case " + name + " mixes " + c.typeName + " and " + typeName);
    }
    return c;
  }

  private static Type icebergType(int n, String name) {
    switch (name) {
      case "BOOLEAN": return Types.BooleanType.get();
      case "INT": return Types.IntegerType.get();
      case "BIGINT": return Types.LongType.get();
      case "FLOAT": return Types.FloatType.get();
      case "DOUBLE": return Types.DoubleType.get();
      case "DATE": return Types.DateType.get();
      case "TIMESTAMP": return Types.TimestampType.withoutZone();
      case "STRING": return Types.StringType.get();
      default:
        Matcher m = DECIMAL_TYPE.matcher(name);
        if (m.matches()) {
          int precision = Integer.parseInt(m.group(1));
          int scale = Integer.parseInt(m.group(2));
          if (precision >= 1 && precision <= 38 && scale <= precision) {
            return Types.DecimalType.of(precision, scale);
          }
        }
        throw error(n, "unknown type '" + name + "'");
    }
  }

  private static Object parseValue(int n, Case c, String s) {
    if (s.equals("NULL")) return null;
    try {
      switch (c.type.typeId()) {
        case BOOLEAN:
          if (s.equals("true") || s.equals("false")) return Boolean.parseBoolean(s);
          break;
        case INTEGER:
        case DATE:
          return parseInteger(n, s).intValueExact();
        case LONG:
        case TIMESTAMP:
          return parseInteger(n, s).longValueExact();
        case FLOAT:
          if (s.matches("0x[0-9a-f]{8}")) {
            float v = Float.intBitsToFloat(Integer.parseUnsignedInt(s.substring(2), 16));
            if (!Float.isNaN(v)) return v;
          }
          break;
        case DOUBLE:
          if (s.matches("0x[0-9a-f]{16}")) {
            double v =
                Double.longBitsToDouble(Long.parseUnsignedLong(s.substring(2), 16));
            if (!Double.isNaN(v)) return v;
          }
          break;
        case DECIMAL:
          Types.DecimalType d = (Types.DecimalType) c.type;
          BigInteger unscaled = parseInteger(n, s);
          if (unscaled.abs().compareTo(BigInteger.TEN.pow(d.precision())) < 0) {
            return new BigDecimal(unscaled, d.scale());
          }
          break;
        case STRING:
          if (s.equals("-")) return "";
          checkHex(n, s);
          return UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(HEX_CODEC.decode(s))).toString();
        default:
          break;
      }
    } catch (IllegalArgumentException | ArithmeticException
        | CharacterCodingException e) {
      // Reported as a bad value with the line number.
    }
    throw error(n, "bad " + c.typeName + " value '" + s + "'");
  }

  private static BigInteger parseInteger(int n, String s) {
    if (!INTEGER.matcher(s).matches()) throw error(n, "bad integer '" + s + "'");
    return new BigInteger(s);
  }

  private static void checkHex(int n, String s) {
    if (!HEX.matcher(s).matches()) throw error(n, "bad hex '" + s + "'");
  }

  private static IllegalArgumentException error(int line, String message) {
    return new IllegalArgumentException("vectors.txt line " + line + ": " + message);
  }
}
