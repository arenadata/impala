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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * NDV that a loaded Iceberg table gives its columns after the statistics writes of
 * COMPUTE STATS, with Puffin reading off and on. The writes go through
 * CatalogOpExecutor on tables registered in the embedded HMS. Every load checks the
 * column of the IcebergTable and the one of its HdfsTable, which local catalog reads.
 */
public class IcebergTablePuffinNdvLoadTest extends FrontendTestBase {
  private static final String DB = "iceberg_puffin_ndv_load";
  private static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));
  private static final List<String> COLUMNS = Arrays.asList("i", "s");
  private static final long NUM_ROWS = 100;
  // Theta estimates of the sketches of 'values()'.
  private static final long I_NDV = 10;
  private static final long S_NDV = 20;
  private static final long LEGACY_NDV = 7;
  private static final long SAMPLED_NDV = 3;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);
  private final boolean readPuffinBefore =
      BackendConfig.INSTANCE.getBackendCfg().isEnable_reading_puffin_stats();

  @After
  public void restorePuffinReading() {
    setReadPuffin(readPuffinBefore);
  }

  private static void setReadPuffin(boolean enabled) {
    BackendConfig.INSTANCE.getBackendCfg().setEnable_reading_puffin_stats(enabled);
  }

  /** Registered table with two snapshots, loaded by the catalog. */
  private IcebergTestTables newTable() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase(), SCHEMA, PartitionSpec.unpartitioned(),
        2);
    t.append(t.file(100, null));
    t.append(t.file(100, null));
    ifx.registerPathA(t, Collections.emptyMap());
    ifx.reloadPathA(t);
    return t;
  }

  private static int fieldId(String col) {
    return SCHEMA.findField(col).fieldId();
  }

  private static List<Object> values(String col) {
    List<Object> res = new ArrayList<>();
    if (col.equals("i")) {
      for (int v = 1; v <= I_NDV; ++v) res.add(v);
    } else {
      for (int v = 0; v < S_NDV; ++v) res.add("v" + v);
    }
    return res;
  }

  /**
   * Request of COMPUTE STATS on 'snapshotId'. 'ndv' is the HLL NDV of LEGACY; FULL
   * sends theta sketches instead.
   */
  private static TAlterTableUpdateStatsParams params(IcebergTestTables t,
      TIcebergStatsMode mode, long snapshotId, long ndv) {
    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams();
    params.setTable_name(new TTableName(t.db(), t.name()));
    params.setTable_stats(new TTableStats(NUM_ROWS));
    TIcebergPuffinStatsParams p =
        new TIcebergPuffinStatsParams(mode, snapshotId, new HashMap<>());
    for (String col : COLUMNS) {
      TColumnStats stats = new TColumnStats();
      stats.setNum_distinct_values(mode == TIcebergStatsMode.FULL ? -1 : ndv);
      stats.setNum_nulls(0);
      stats.setMax_size(4);
      stats.setAvg_size(4);
      stats.setNum_trues(-1);
      stats.setNum_falses(-1);
      params.putToColumn_stats(col, stats);
      p.putToColumn_field_ids(col, fieldId(col));
      if (mode == TIcebergStatsMode.FULL) {
        p.addToTheta_field_ids(fieldId(col));
        p.putToTheta_sketches(fieldId(col), ByteBuffer.wrap(PuffinFixtures.sketch(
            SCHEMA.findField(col).type(), values(col)).toByteArray()));
      }
    }
    params.setIceberg_puffin_stats(p);
    return params;
  }

  private static long full(IcebergTestTables t) throws Exception {
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.updateStats(params(t, TIcebergStatsMode.FULL, s1, -1));
    return s1;
  }

  /** Loads the table anew and returns the NDV of each column, equal in both copies. */
  private static Map<String, Long> loadNdvs(IcebergTestTables t, boolean readPuffin)
      throws Exception {
    setReadPuffin(readPuffin);
    IcebergTable tbl = ifx.reloadPathA(t);
    Map<String, Long> res = new HashMap<>();
    for (String col : COLUMNS) {
      long ndv = tbl.getColumn(col).getStats().getNumDistinctValues();
      assertEquals("NDV of " + col + " in getHdfsTable()", ndv,
          tbl.getHdfsTable().getColumn(col).getStats().getNumDistinctValues());
      res.put(col, ndv);
    }
    return res;
  }

  private static Map<String, Long> ndvs(long i, long s) {
    Map<String, Long> res = new HashMap<>();
    res.put("i", i);
    res.put("s", s);
    return res;
  }

  @Test
  public void fullGivesSameNdvWithAndWithoutPuffinReading() throws Exception {
    IcebergTestTables t = newTable();
    full(t);

    assertEquals(ndvs(I_NDV, S_NDV), loadNdvs(t, false));
    assertEquals(ndvs(I_NDV, S_NDV), loadNdvs(t, true));
  }

  @Test
  public void legacyAfterFullOnSameSnapshotReadsBlobNdv() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = full(t);
    ifx.updateStats(params(t, TIcebergStatsMode.LEGACY, s1, LEGACY_NDV));

    assertEquals(ndvs(LEGACY_NDV, LEGACY_NDV), loadNdvs(t, false));
    assertEquals(ndvs(I_NDV, S_NDV), loadNdvs(t, true));
  }

  @Test
  public void sampleAfterFullReadsOlderBlobNdv() throws Exception {
    IcebergTestTables t = newTable();
    full(t);
    long s2 = t.append(t.file(100, null));
    ifx.reloadPathA(t);
    TAlterTableUpdateStatsParams sampled =
        params(t, TIcebergStatsMode.LEGACY, s2, SAMPLED_NDV);
    sampled.getIceberg_puffin_stats().setSampled(true);
    ifx.updateStats(sampled);

    assertEquals(ndvs(SAMPLED_NDV, SAMPLED_NDV), loadNdvs(t, false));
    assertEquals(ndvs(I_NDV, S_NDV), loadNdvs(t, true));
  }
}
