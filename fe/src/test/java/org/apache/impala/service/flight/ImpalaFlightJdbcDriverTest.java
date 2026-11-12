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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * The Arrow Flight SQL JDBC driver (what DBeaver and other JDBC tools use) against the
 * Flight SQL endpoint with a fake HS2 ({@link FlightSqlTestServer}, LDAP login), for each
 * driver version the endpoint must support. The drivers differ in how they send updates
 * (19.0.0 sends DML through DoPut, 16.0.0 does not) and once ran statements several
 * times or failed on an update count of -1, so every statement is also checked to reach
 * HS2 exactly once.
 */
@RunWith(Parameterized.class)
public class ImpalaFlightJdbcDriverTest {
  private static final String DRIVER_CLASS =
      "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver";
  private static final String USER = "Test1Ldap";

  @Parameters(name = "driver {0}")
  public static Collection<Object[]> versions() {
    return Arrays.asList(new Object[][] {{"16.0.0"}, {"19.0.0"}});
  }

  private static FlightSqlTestServer server_;
  private static final Map<String, Driver> drivers_ = new HashMap<>();

  private final String version_;

  public ImpalaFlightJdbcDriverTest(String version) { version_ = version; }

  @BeforeClass
  public static void startServer() throws Exception {
    server_ = new FlightSqlTestServer();
    server_.start("ldap", false,
        Files.createTempDirectory("flight-jdbc-test").toFile());
  }

  @AfterClass
  public static void stopServer() {
    if (server_ != null) server_.close();
  }

  /** The driver of this version, in its own class loader (it bundles its Arrow). */
  private Driver driver() throws Exception {
    Driver d = drivers_.get(version_);
    if (d != null) return d;
    File dir = new File(System.getProperty("flight.jdbc.dir", "target/flight-jdbc"));
    File jar = new File(dir, "flight-sql-jdbc-driver-" + version_ + ".jar");
    assertTrue(jar + " is missing: it is copied by fe/pom.xml in process-test-resources",
        jar.isFile());
    URLClassLoader loader = new URLClassLoader(new URL[] {jar.toURI().toURL()},
        ClassLoader.getPlatformClassLoader());
    d = (Driver) loader.loadClass(DRIVER_CLASS).getDeclaredConstructor().newInstance();
    drivers_.put(version_, d);
    return d;
  }

  private Connection connect() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", USER);
    props.setProperty("password", FlightSqlTestServer.LDAP_USERS.get(USER));
    String url = "jdbc:arrow-flight-sql://127.0.0.1:" + server_.port()
        + "/?useEncryption=false";
    Connection c = driver().connect(url, props);
    assertNotNull("driver " + version_ + " did not accept " + url, c);
    return c;
  }

  /** How often HS2 received 'sql'. */
  private static long executions(String sql) {
    return server_.hs2().statements.stream().filter(sql::equals).count();
  }

  @Test
  public void testTypes() throws Exception {
    try (Connection c = connect(); Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(FlightTestData.TYPES_QUERY)) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(FlightTestData.TYPE_COLUMNS.length, md.getColumnCount());
      for (int i = 0; i < FlightTestData.TYPE_COLUMNS.length; i++) {
        assertEquals(FlightTestData.TYPE_COLUMNS[i][0], md.getColumnName(i + 1));
      }
      int dec = column(md, "dec");
      assertEquals("DECIMAL", md.getColumnTypeName(dec));
      assertEquals(10, md.getPrecision(dec));
      assertEquals(2, md.getScale(dec));

      assertTrue(rs.next());
      assertTrue(rs.getBoolean("b"));
      assertEquals(1, rs.getByte("ti"));
      assertEquals(2, rs.getShort("si"));
      assertEquals(3, rs.getInt("i"));
      assertEquals(4L, rs.getLong("bi"));
      assertEquals(1.5f, rs.getFloat("f"), 0f);
      assertEquals(2.25, rs.getDouble("d"), 0);
      assertEquals(new BigDecimal("12345.67"), rs.getBigDecimal("dec"));
      assertEquals("hello", rs.getString("s"));
      assertEquals("abc", rs.getString("v"));
      assertEquals("ab ", rs.getString("c"));
      assertNotNull(rs.getTimestamp("ts"));
      assertNotNull(rs.getDate("dt"));
      assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes("bin"));
      assertEquals("[1,2]", rs.getString("arr"));

      assertTrue(rs.next());
      assertEquals(Byte.MIN_VALUE, rs.getByte("ti"));
      assertEquals(Long.MIN_VALUE, rs.getLong("bi"));
      assertEquals(new BigDecimal("-0.01"), rs.getBigDecimal("dec"));
      assertEquals("привет ✓", rs.getString("s"));
      assertEquals("", rs.getString("v")); // empty, not NULL
      assertFalse(rs.wasNull());

      assertTrue(rs.next());
      for (int i = 1; i <= md.getColumnCount(); i++) {
        rs.getObject(i);
        assertTrue(md.getColumnName(i) + " should be NULL", rs.wasNull());
      }
      assertFalse(rs.next());
    }
  }

  private static List<String> columnNames(ResultSetMetaData md) throws SQLException {
    List<String> names = new ArrayList<>();
    for (int i = 1; i <= md.getColumnCount(); i++) names.add(md.getColumnName(i));
    return names;
  }

  private static int column(ResultSetMetaData md, String name) throws SQLException {
    for (int i = 1; i <= md.getColumnCount(); i++) {
      if (md.getColumnName(i).equals(name)) return i;
    }
    throw new AssertionError("no column " + name);
  }

  @Test
  public void testEmptyAndLargeResults() throws Exception {
    try (Connection c = connect(); Statement s = c.createStatement()) {
      try (ResultSet rs = s.executeQuery(FlightTestData.EMPTY_QUERY)) {
        // Driver bug (16.0.0 and 19.0.0): a result without batches lists its columns
        // twice. The server sends them once (testEmptyResultSchemaOnTheWire).
        assertEquals(Arrays.asList("id", "name", "id", "name"),
            columnNames(rs.getMetaData()));
        assertFalse(rs.next());
      }
      try (ResultSet rs = s.executeQuery(FlightTestData.BIG_QUERY)) {
        long rows = 0;
        while (rs.next()) {
          assertEquals(rows, rs.getLong(1));
          rows++;
        }
        assertEquals(FlightTestData.BIG_ROWS, rows);
      }
    }
  }

  /** What the server sends for an empty result, below the JDBC driver. */
  @Test
  public void testEmptyResultSchemaOnTheWire() throws Exception {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert("authorization", "Basic " + Base64.getEncoder().encodeToString(
        (USER + ":" + FlightSqlTestServer.LDAP_USERS.get(USER))
            .getBytes(StandardCharsets.UTF_8)));
    CallOption auth = new HeaderCallOption(headers);
    try (BufferAllocator allocator = new RootAllocator();
         FlightSqlClient sql = new FlightSqlClient(FlightClient.builder(allocator,
             Location.forGrpcInsecure("127.0.0.1", server_.port())).build())) {
      FlightInfo info = sql.execute(FlightTestData.EMPTY_QUERY, auth);
      assertEquals(Arrays.asList("id", "name"), fieldNames(info.getSchema()));
      assertEquals(Arrays.asList("id", "name"), streamFields(sql, info, auth));
      FlightSqlClient.PreparedStatement p = sql.prepare(FlightTestData.EMPTY_QUERY, auth);
      try {
        assertEquals(Arrays.asList("id", "name"), fieldNames(p.getResultSetSchema()));
        FlightInfo prepared = p.execute(auth);
        assertEquals(Arrays.asList("id", "name"), fieldNames(prepared.getSchema()));
        assertEquals(Arrays.asList("id", "name"), streamFields(sql, prepared, auth));
      } finally {
        p.close(auth); // close() would send no credentials
      }
    }
  }

  private static List<String> fieldNames(Schema schema) {
    List<String> names = new ArrayList<>();
    schema.getFields().forEach(f -> names.add(f.getName()));
    return names;
  }

  /** Field names of the stream's schema; the stream must have no rows. */
  private static List<String> streamFields(FlightSqlClient sql, FlightInfo info,
      CallOption auth) throws Exception {
    try (FlightStream stream =
             sql.getStream(info.getEndpoints().get(0).getTicket(), auth)) {
      List<String> names = fieldNames(stream.getSchema());
      int rows = 0;
      while (stream.next()) rows += stream.getRoot().getRowCount();
      assertEquals(0, rows);
      return names;
    }
  }

  @Test
  public void testLoginUserIsTheEffectiveUser() throws Exception {
    try (Connection c = connect(); Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("select user(), effective_user()")) {
      assertTrue(rs.next());
      assertEquals(USER, rs.getString(2));
    }
  }

  @Test
  public void testExecuteUpdateRunsOnce() throws Exception {
    long before = executions(FlightTestData.INSERT);
    try (Connection c = connect(); Statement s = c.createStatement()) {
      assertEquals(FlightTestData.INSERT_ROWS, s.executeUpdate(FlightTestData.INSERT));
    }
    assertEquals(before + 1, executions(FlightTestData.INSERT));
  }

  @Test
  public void testPreparedExecuteUpdateRunsOnce() throws Exception {
    long before = executions(FlightTestData.INSERT);
    try (Connection c = connect();
         PreparedStatement p = c.prepareStatement(FlightTestData.INSERT)) {
      assertEquals(FlightTestData.INSERT_ROWS, p.executeUpdate());
    }
    assertEquals(before + 1, executions(FlightTestData.INSERT));
  }

  /** What DBeaver does for a statement typed in its SQL editor. */
  @Test
  public void testPreparedExecuteRunsOnce() throws Exception {
    long before = executions(FlightTestData.INSERT);
    try (Connection c = connect();
         PreparedStatement p = c.prepareStatement(FlightTestData.INSERT)) {
      boolean resultSet = p.execute();
      if (version_.equals("16.0.0")) {
        // 16.0.0 always reports a result set for a prepared statement
        assertTrue(resultSet);
      } else {
        assertFalse(resultSet);
        assertEquals(FlightTestData.INSERT_ROWS, p.getUpdateCount());
      }
    }
    assertEquals(before + 1, executions(FlightTestData.INSERT));
  }

  /** No row count from HS2: reported as 0, as JDBC does for statements without one. */
  @Test
  public void testStatementWithoutRowCount() throws Exception {
    long before = executions(FlightTestData.DDL);
    try (Connection c = connect(); Statement s = c.createStatement()) {
      assertEquals(0, s.executeUpdate(FlightTestData.DDL));
    }
    try (Connection c = connect();
         PreparedStatement p = c.prepareStatement(FlightTestData.DDL)) {
      assertEquals(0, p.executeUpdate());
    }
    assertEquals(before + 2, executions(FlightTestData.DDL));
  }

  @Test
  public void testPreparedQuery() throws Exception {
    try (Connection c = connect();
         PreparedStatement p = c.prepareStatement(FlightTestData.TYPES_QUERY)) {
      // the schema is known before execution
      ResultSetMetaData md = p.getMetaData();
      assertNotNull(md);
      assertEquals(FlightTestData.TYPE_COLUMNS.length, md.getColumnCount());
      try (ResultSet rs = p.executeQuery()) {
        int rows = 0;
        while (rs.next()) rows++;
        assertEquals(3, rows);
      }
    }
  }

  @Test
  public void testErrors() throws Exception {
    try (Connection c = connect(); Statement s = c.createStatement()) {
      assertFails(s, FlightTestData.SYNTAX_ERROR, "Syntax error");
      assertFails(s, FlightTestData.MISSING_TABLE, "Could not resolve table reference");
      assertFails(s, FlightTestData.NO_PRIVILEGE, "does not have privileges");
      assertFails(s, FlightTestData.FETCH_ERROR, "Failed to open HDFS file");
      // the connection is still usable
      try (ResultSet rs = s.executeQuery(FlightTestData.EMPTY_QUERY)) {
        assertFalse(rs.next());
      }
    }
  }

  private static void assertFails(Statement s, String sql, String messagePart) {
    try (ResultSet rs = s.executeQuery(sql)) {
      while (rs.next()) {}
      fail("expected an error for " + sql);
    } catch (SQLException e) {
      StringBuilder all = new StringBuilder();
      for (Throwable t = e; t != null; t = t.getCause()) all.append(t.getMessage());
      assertTrue(all.toString(), all.toString().contains(messagePart));
    }
  }

  @Test
  public void testWrongPasswordIsRejected() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", USER);
    props.setProperty("password", "wrong");
    String url = "jdbc:arrow-flight-sql://127.0.0.1:" + server_.port()
        + "/?useEncryption=false";
    try (Connection c = driver().connect(url, props);
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(FlightTestData.EMPTY_QUERY)) {
      fail("login with a wrong password should fail");
    } catch (SQLException e) {
      // expected
    }
  }

  @Test
  public void testDatabaseMetaData() throws Exception {
    try (Connection c = connect()) {
      DatabaseMetaData md = c.getMetaData();
      List<String> schemas = new ArrayList<>();
      try (ResultSet rs = md.getSchemas()) {
        while (rs.next()) schemas.add(rs.getString("TABLE_SCHEM"));
      }
      assertTrue(schemas.toString(), schemas.containsAll(FlightTestData.DATABASES));

      assertEquals(FlightTestData.tableNames(),
          names(md.getTables(null, FlightTestData.DB, "%", null), "TABLE_NAME"));
      assertEquals(Arrays.asList("big_view"), names(md.getTables(
          null, FlightTestData.DB, "%", new String[] {"VIEW"}), "TABLE_NAME"));
      assertEquals(Arrays.asList("big", "big_view"),
          names(md.getTables(null, FlightTestData.DB, "big%", null), "TABLE_NAME"));

      List<String> columns = names(
          md.getColumns(null, FlightTestData.DB, "types", "%"), "COLUMN_NAME");
      List<String> expected = new ArrayList<>();
      for (String[] col : FlightTestData.TYPE_COLUMNS) expected.add(col[0]);
      assertEquals(expected, columns);

      assertEquals(Arrays.asList("id"),
          names(md.getPrimaryKeys(null, FlightTestData.DB, "parent"), "COLUMN_NAME"));
      try (ResultSet rs = md.getImportedKeys(null, FlightTestData.DB, "child")) {
        assertTrue(rs.next());
        assertEquals("parent", rs.getString("PKTABLE_NAME"));
        assertEquals("id", rs.getString("PKCOLUMN_NAME"));
        assertEquals("parent_id", rs.getString("FKCOLUMN_NAME"));
        assertFalse(rs.next());
      }
      try (ResultSet rs = md.getTableTypes()) {
        List<String> types = new ArrayList<>();
        while (rs.next()) types.add(rs.getString(1));
        assertEquals(Arrays.asList("TABLE", "VIEW"), types);
      }
      assertNotNull(md.getDatabaseProductName());
    }
  }

  private static List<String> names(ResultSet rs, String column) throws SQLException {
    List<String> out = new ArrayList<>();
    try (ResultSet r = rs) {
      while (r.next()) out.add(r.getString(column));
    }
    return out;
  }

  /**
   * Closing the connection with an unread result set. 19.0.0 closes cleanly; 16.0.0
   * closes its allocators before its result sets and fails ("Memory was leaked"), a
   * driver bug fixed in later versions. The server then keeps that stream until
   * CLIENT_WAIT_MS.
   */
  @Test
  public void testCloseWithOpenResultSet() throws Exception {
    Connection c = connect();
    Statement s = c.createStatement();
    ResultSet rs = s.executeQuery(FlightTestData.BIG_QUERY);
    assertTrue(rs.next());
    if (version_.equals("16.0.0")) {
      try {
        c.close();
        fail("driver 16.0.0 closed cleanly with an open result set");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("clean up client resources"));
      }
    } else {
      c.close();
      assertTrue(c.isClosed());
    }
  }
}
