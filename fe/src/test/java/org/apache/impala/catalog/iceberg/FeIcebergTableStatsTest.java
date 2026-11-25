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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/** "Incremental stats" column of SHOW TABLE STATS for Iceberg tables in the cache. */
public class FeIcebergTableStatsTest extends FrontendTestBase {
  private static final String DB = "iceberg_show_stats";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String WRITE = IcebergTable.ICEBERG_WRITE_PUFFIN_STATS;
  private static final int INCREMENTAL_COLUMN = 6;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private boolean savedWriteFlag_;

  @Before
  public void saveWriteFlag() {
    savedWriteFlag_ =
        BackendConfig.INSTANCE.getBackendCfg().isEnable_writing_puffin_stats();
    setWriteFlag(true);
  }

  @After
  public void restoreWriteFlag() {
    setWriteFlag(savedWriteFlag_);
  }

  private static void setWriteFlag(boolean enabled) {
    BackendConfig.INSTANCE.getBackendCfg().setEnable_writing_puffin_stats(enabled);
  }

  private IcebergTable table(String suffix, Map<String, String> params)
      throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    t.append(t.file(100, null));
    return ifx.addPathB(t, params);
  }

  /**
   * Table whose columns have a base at its only snapshot: HMS statistics, the property of
   * base snapshots and theta blobs. 'params' are added to its HMS parameters.
   */
  private IcebergTable tableWithBase(String suffix, Map<String, String> params)
      throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long bh = t.append(t.file(100, null));
    t.setStats(bh, PuffinFixtures.write(t.table(), bh, Arrays.asList(
        PuffinFixtures.blob(1, bh, new byte[] {1}),
        PuffinFixtures.blob(2, bh, new byte[] {2}))));
    Map<String, String> all = new HashMap<>(params);
    all.put(IDS, "1-2:" + bh);
    IcebergTable tbl = ifx.addPathB(t, all);
    for (Column c : tbl.getColumns()) c.getStats().setNumNulls(0);
    return tbl;
  }

  private static Map<String, String> param(String key, String value) {
    return Collections.singletonMap(key, value);
  }

  private static List<String> cells(TResultSet rs) {
    assertEquals(1, rs.getRowsSize());
    TResultRow row = rs.getRows().get(0);
    List<String> res = new ArrayList<>();
    for (TColumnValue v : row.getColVals()) {
      if (v.isSetLong_val()) {
        res.add(Long.toString(v.getLong_val()));
      } else {
        res.add(v.getString_val());
      }
    }
    return res;
  }

  private static String incremental(IcebergTable tbl) {
    return cells(FeIcebergTable.Utils.getTableStats(tbl)).get(INCREMENTAL_COLUMN);
  }

  @Test
  public void resultKeepsColumnsAndOnlyIncrementalStatsChanges() throws Exception {
    IcebergTable withIds = tableWithBase("_ids", Collections.emptyMap());
    IcebergTable without = table("_none", Collections.emptyMap());

    TResultSet rs = FeIcebergTable.Utils.getTableStats(withIds);
    List<String> names = new ArrayList<>();
    for (TColumn c : rs.getSchema().getColumns()) {
      names.add(c.getColumnName());
    }
    assertEquals(Arrays.asList("#Rows", "#Files", "Size", "Bytes Cached",
        "Cache Replication", "Format", "Incremental stats", "Location", "EC Policy"),
        names);

    List<String> expected = Arrays.asList(
        Long.toString(withIds.getNumRows()), "1", "100B", "NOT CACHED", "NOT CACHED",
        "PARQUET", "true", withIds.getLocation(),
        FileSystemUtil.getErasureCodingPolicy(new Path(withIds.getLocation())));
    assertEquals(expected, cells(rs));

    List<String> noIds = new ArrayList<>(cells(
        FeIcebergTable.Utils.getTableStats(without)));
    assertEquals("false", noIds.get(INCREMENTAL_COLUMN));
    assertEquals(expected.subList(1, 6), noIds.subList(1, 6));
  }

  @Test
  public void baseIsTrueWithBaseOfEveryColumnAndFalseWithoutIt() throws Exception {
    assertEquals("true", incremental(tableWithBase("_base", Collections.emptyMap())));
    assertEquals("false", incremental(table("_ids", param(IDS, "1-2:7,5:8"))));
    assertEquals("false", incremental(table("_missing", Collections.emptyMap())));
    assertEquals("false", incremental(table("_empty", param(IDS, ""))));
  }

  @Test
  public void flagOrPropertyOffGivesFalseWithBase() throws Exception {
    IcebergTable withBase = tableWithBase("_base", Collections.emptyMap());
    assertEquals("true", incremental(withBase));

    setWriteFlag(false);
    assertEquals("false", incremental(withBase));
    setWriteFlag(true);

    assertEquals("false", incremental(tableWithBase("_disabled", param(WRITE, "false"))));
  }

  @Test
  public void brokenIdsGiveFalse() throws Exception {
    String[] broken = {"garbage", "1:x", "1-2-3:4", "0-100001:5"};
    for (int i = 0; i < broken.length; ++i) {
      assertEquals(broken[i], "false",
          incremental(table("_broken" + i, param(IDS, broken[i]))));
    }
  }

  @Test
  public void tableWithoutIdsAndPuffinStatsGivesFalse() throws Exception {
    Map<String, String> oldStats = new HashMap<>();
    oldStats.put("impala.lastComputeStatsTime", "1700000000");
    oldStats.put("numRows", "100");
    assertEquals("false", incremental(table("_old", oldStats)));
  }
}
