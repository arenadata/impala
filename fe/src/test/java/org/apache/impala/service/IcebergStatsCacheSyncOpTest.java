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

import static org.apache.impala.service.IcebergStatsOpFixture.COLUMNS;
import static org.apache.impala.service.IcebergStatsOpFixture.fieldId;
import static org.apache.impala.service.IcebergStatsOpFixture.idsOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.PartitionSpec;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * A statistics update that fails after writing HMS, while HMS stays unreachable for the
 * reads that would catch the cached HMS table up. A later DDL that writes the cached
 * table must not pair the new column statistics with the snapshot ids before them.
 */
public class IcebergStatsCacheSyncOpTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_cache_sync_op";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final long LEGACY_NDV = 7;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private IcebergTestTables t_;
  private long s0_;
  private long s1_;

  private TTableName tableName() {
    return new TTableName(DB, testName.getMethodName().toLowerCase());
  }

  private void newTable() throws Exception {
    t_ = IcebergTestTables.create(tmp, DB, tableName().getTable_name(),
        IcebergStatsOpFixture.SCHEMA, PartitionSpec.unpartitioned(), 2);
    s0_ = t_.append(t_.file(100, null));
    s1_ = t_.append(t_.file(100, null));
    ifx.registerPathA(t_, Collections.emptyMap());
    ifx.reloadPathA(t_);
  }

  private TAlterTableUpdateStatsParams legacy(long snapshotId, List<String> cols) {
    return IcebergStatsOpFixture.params(tableName(), TIcebergStatsMode.LEGACY,
        snapshotId, cols, LEGACY_NDV);
  }

  private void updateFails(TAlterTableUpdateStatsParams params,
      HmsFailureAfterColumnStats failure) throws Exception {
    try {
      ifx.updateStats(params);
      fail("statistics update succeeded");
    } catch (ImpalaException expected) {
      // The write or reload and the read of the ids that follows it.
      assertTrue("failures: " + failure.failures(), failure.failures() >= 2);
    }
  }

  private void setTblProperties() throws Exception {
    TAlterTableSetTblPropertiesParams props = new TAlterTableSetTblPropertiesParams();
    props.setTarget(TTablePropertyType.TBL_PROPERTY);
    props.setProperties(Collections.singletonMap("x", "y"));
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
    alter.setTable_name(tableName());
    alter.setSet_tbl_properties_params(props);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    ifx.execDdl(req);
    assertEquals("y", hmsParams().get("x"));
  }

  private Map<String, String> hmsParams() throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(DB, tableName().getTable_name())
          .getParameters();
    }
  }

  /** Empty without the property. */
  private Map<Integer, Long> hmsIds() throws Exception {
    String value = hmsParams().get(IDS);
    if (value == null) return new TreeMap<>();
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  private long hmsNdv(String col) throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      ColumnStatisticsObj obj = MetastoreShim.getTableColumnStatistics(
          client.getHiveClient(), DB, tableName().getTable_name(),
          Collections.singletonList(col)).get(0);
      return obj.getStatsData().getLongStats().getNumDVs();
    }
  }

  @Test
  public void setTblPropertiesAfterSetColumnStatsWithHmsDownKeepsColumnDetached()
      throws Exception {
    newTable();
    ifx.updateStats(legacy(s1_, COLUMNS));
    assertEquals(idsOf(COLUMNS, s1_), hmsIds());

    TAlterTableUpdateStatsParams setColumn = new TAlterTableUpdateStatsParams();
    setColumn.setTable_name(tableName());
    setColumn.putToColumn_stats("i", IcebergStatsOpFixture.columnStats("i", 42));
    try (HmsFailureAfterColumnStats failure =
        HmsFailureAfterColumnStats.failTailAlterTableAndReads(ifx.catalog())) {
      updateFails(setColumn, failure);
    }
    List<String> rest = Arrays.asList("s", "b", "d", "userid");
    assertEquals(idsOf(rest, s1_), hmsIds());
    assertEquals(42L, hmsNdv("i"));

    setTblProperties();

    Map<Integer, Long> ids = hmsIds();
    assertTrue("ids " + ids, ids.isEmpty());
    assertEquals(42L, hmsNdv("i"));
  }

  @Test
  public void setTblPropertiesAfterUpdateWithHmsDownDropsIds() throws Exception {
    newTable();
    ifx.updateStats(legacy(s0_, COLUMNS));
    assertEquals(idsOf(COLUMNS, s0_), hmsIds());

    try (HmsFailureAfterColumnStats failure =
        HmsFailureAfterColumnStats.failReads(ifx.catalog())) {
      updateFails(legacy(s1_, Collections.singletonList("i")), failure);
    }
    assertEquals(s1_, (long) hmsIds().get(fieldId("i")));
    assertEquals(LEGACY_NDV, hmsNdv("i"));

    setTblProperties();

    Map<Integer, Long> ids = hmsIds();
    assertTrue("ids " + ids, ids.isEmpty());
    assertEquals(LEGACY_NDV, hmsNdv("i"));
  }
}
