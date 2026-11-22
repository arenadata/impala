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

package org.apache.impala.analysis;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;

import java.util.Collections;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.util.IcebergUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class IcebergAnalysisFixtureTest extends FrontendTestBase {
  private static final String DB = "iceberg_analysis_fixture";
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private final IcebergAnalysisFixture ifx = IcebergAnalysisFixture.create(feFixture_);

  private String tableName() { return testName.getMethodName().toLowerCase(); }

  private org.apache.hadoop.hive.metastore.api.Table hmsTable() throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(DB, tableName());
    }
  }

  @Test
  public void appendDataFileAddsSnapshotEveryCall() throws Exception {
    org.apache.iceberg.Table iceTbl = ifx.createHiveCatalogTable(DB, tableName(), SCHEMA);
    ifx.appendDataFile(iceTbl);
    ifx.appendDataFile(iceTbl);
    iceTbl.refresh();
    assertEquals(2, Iterables.size(iceTbl.snapshots()));
    assertEquals("2", iceTbl.currentSnapshot().summary().get("total-data-files"));
    IcebergTable tbl = ifx.loadHiveCatalogTable(DB, tableName());
    assertEquals(iceTbl.currentSnapshot().snapshotId(), tbl.snapshotId());
  }

  @Test
  public void hiveCatalogTableWithoutPurgeIsNotSynchronized() throws Exception {
    org.apache.iceberg.Table iceTbl =
        ifx.createHiveCatalogTable(DB, tableName(), SCHEMA, false);
    long s1 = ifx.appendDataFile(iceTbl);
    long s2 = ifx.appendDataFile(iceTbl);
    assertNotEquals(s1, s2);

    org.apache.hadoop.hive.metastore.api.Table msTbl = hmsTable();
    assertTrue(Table.isExternalTable(msTbl));
    assertEquals("false",
        msTbl.getParameters().get(Table.TBL_PROP_EXTERNAL_TABLE_PURGE));
    assertFalse(IcebergTable.isSynchronizedTable(msTbl));
    assertTrue(IcebergUtil.isHiveCatalog(msTbl));
    IcebergTable tbl = ifx.loadHiveCatalogTable(DB, tableName());
    assertEquals(s2, tbl.snapshotId());
  }

  @Test
  public void hiveCatalogTableWithPurgeIsSynchronized() throws Exception {
    ifx.createHiveCatalogTable(DB, tableName(), SCHEMA);
    assertTrue(IcebergTable.isSynchronizedTable(hmsTable()));
  }

  @Test
  public void analyzeComputeStatsReturnsStatement() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(100, null));
    ifx.addPathB(t, Collections.emptyMap());
    ComputeStatsStmt stmt =
        ifx.analyzeComputeStats("compute stats " + DB + "." + tableName() + " (s)");
    assertEquals(DB + "." + tableName(), stmt.getTableName().toString());
    assertEquals(s1, stmt.toThrift().getIceberg_puffin_stats().getCalc_snapshot_id());
    assertEquals(1, stmt.getValidatedColumnWhitelist().size());
  }
}
