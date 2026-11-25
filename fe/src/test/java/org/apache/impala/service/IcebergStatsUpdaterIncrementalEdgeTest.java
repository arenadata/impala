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

package org.apache.impala.service;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.StatisticsFile;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil;
import org.junit.Test;

/**
 * IcebergStatsUpdater#apply with incremental requests whose bases sit at unusual
 * snapshots: the metrics base moved to S1 without sketches, and an NDV base at S1.
 */
public class IcebergStatsUpdaterIncrementalEdgeTest
    extends IcebergStatsUpdaterIncrementalTestBase {
  @Test
  public void metricsBaseAtS1WithNdvBasesAtOlderSnapshotRepeatsSameResult()
      throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, false);
    // LEGACY at S1 moves the metrics base to S1 and leaves no sketches there.
    long s1 = base(t, tbl, TIcebergStatsMode.LEGACY, COLUMNS);
    TAlterTableUpdateStatsParams params =
        incremental(s1, s1, 100, COLUMNS, bases(t, b, THETA));
    // No snapshot lies between the metrics base and S1: the gap is empty.
    for (String col : COLUMNS) {
      int size = col.equals("s") || col.equals("bin") ? 0 : 4;
      long bools = col.equals("b") ? 0 : -1;
      params.putToColumn_stats(col, new TColumnStats(size, size, 0, 0, bools, bools));
    }
    for (int id : params.getIceberg_puffin_stats().getTheta_field_ids()) {
      params.getIceberg_puffin_stats().putToTheta_sketches(id,
          ByteBuffer.wrap(new byte[0]));
    }

    assertEquals(COLUMNS.size(), apply(tbl, params));

    Map<String, ColumnStatisticsObj> stats = hmsStats();
    Map<Integer, Long> ndvs = impalaNdvs(t, s1);
    assertEquals(idsOf(THETA, s1).keySet(), ndvs.keySet());
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, false));
    syncFromHms(tbl);

    assertEquals(COLUMNS.size(), apply(tbl, params));

    assertEquals(stats, hmsStats());
    assertEquals(ndvs, impalaNdvs(t, s1));
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, false));
  }

  @Test
  public void foreignRegistrationOfS1AfterLoadKeepsItAndDeletesNewFile()
      throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, false);
    long s1 = t.append(t.file(100, null));
    register(t, s1, PuffinFixtures.blob(fieldId("b"), s1,
        sketch("b", Arrays.asList(true, false))));
    // The NDV base of "b" is the file of another engine at S1, which apply replaces.
    Map<String, TIcebergNdvBase> bases = bases(t, b, THETA);
    bases.put("b", ndvBase(t, "b", s1));
    TAlterTableUpdateStatsParams params = incremental(s1, b, 100, COLUMNS, bases);
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("b"),
        ByteBuffer.wrap(new byte[0]));
    Set<String> onDisk = puffinFilesOnDisk(t);
    StatisticsFile[] foreign = new StatisticsFile[1];
    IcebergStatsUpdater updater = new IcebergStatsUpdater(feTbl -> {
      org.apache.iceberg.Table loaded = IcebergUtil.loadTable(feTbl);
      if (foreign[0] == null) {
        // Another engine registers a different file for S1 after this load.
        foreign[0] = PuffinFixtures.write(t.table(), s1, Collections.singletonList(
            PuffinFixtures.blob(fieldId("i"), s1, sketchBytes("i"))));
        t.setStats(s1, foreign[0]);
      }
      return loaded;
    });
    int mark = hms_.calls().size();
    String tblName = tbl.getFullName();

    assertFails(() -> apply(updater, tbl, msTbl(tbl, false), params),
        ImpalaRuntimeException.class, "Failed to register Puffin statistics of table "
        + tblName + ": Statistics of snapshot " + s1 + " of table " + tblName
        + " changed concurrently");

    List<StatisticsFile> files = t.table().statisticsFiles();
    assertEquals(foreign[0].path(), statsFile(t.table(), s1).path());
    Set<String> expected = new TreeSet<>(onDisk);
    expected.add(new File(foreign[0].path()).getName());
    assertEquals(expected, puffinFilesOnDisk(t));
    assertEquals(files, t.table().statisticsFiles());
    assertEquals(idsOf(COLUMNS, b), storedIds(t, tbl, false));
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsAfter(mark));
  }
}
