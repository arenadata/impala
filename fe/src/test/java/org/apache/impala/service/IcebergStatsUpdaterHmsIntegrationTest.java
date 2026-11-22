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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Test;

/**
 * Where IcebergStatsUpdater#apply commits the snapshot ids: in HMS through
 * alterTableProperties without HMS integration, in the Iceberg properties with it.
 */
public class IcebergStatsUpdaterHmsIntegrationTest extends IcebergStatsUpdaterTestBase {
  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  @Test
  public void withoutIntegrationEveryCommitEndsWithAlterTableProperties()
      throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    t.table().updateProperties().set(IDS, "1:" + s1).commit();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    msTbl.putToParameters(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "1234");
    apply(tbl, msTbl, params(TIcebergStatsMode.FULL, s1, 100));
    assertFalse(t.table().properties().containsKey(IDS));
    assertEquals("1234", hms_.last(Method.ALTER_TABLE_PROPERTIES).msTbl.getParameters()
        .get(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME));

    // Same ids again: the transaction only replaces the registration.
    apply(tbl, hms_.last(Method.ALTER_TABLE_PROPERTIES).msTbl.deepCopy(),
        params(TIcebergStatsMode.FULL, s1, 100));

    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES, Method.UPDATE_COLUMN_STATS,
        Method.ALTER_TABLE_PROPERTIES, Method.UPDATE_COLUMN_STATS), hms_.methods());
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
  }

  @Test
  public void withIntegrationCommitsIdsWithoutCatalogIds() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = integrated(tbl);
    msTbl.putToParameters(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "1234");

    assertEquals(5, apply(tbl, msTbl, params(TIcebergStatsMode.FULL, s1, 100)));

    Table table = t.table();
    Map<String, String> props = table.properties();
    assertEquals(idsOf(COLUMNS, s1),
        ComputeStatsSnapshotPropertyConverter.stringToMap(props.get(IDS)));
    assertEquals("1234", props.get(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME));
    assertFalse(props.containsKey(MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey()));
    assertFalse(props.containsKey(MetastoreEventPropertyKey.CATALOG_VERSION.getKey()));
    blobNdvs(table, s1, Arrays.asList("i", "s", "b", "d"));
    assertNull(hms_.last(Method.ALTER_TABLE_PROPERTIES));
    assertEquals(Collections.singletonList(Method.UPDATE_COLUMN_STATS), hms_.methods());
  }

  @Test
  public void withIntegrationCopiesCatalogIdsFromMsTbl() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = integrated(tbl);
    String serviceId = MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey();
    String version = MetastoreEventPropertyKey.CATALOG_VERSION.getKey();
    msTbl.putToParameters(serviceId, "svc");
    msTbl.putToParameters(version, "42");

    apply(tbl, msTbl, params(TIcebergStatsMode.LEGACY, s1, 100));

    Map<String, String> props = t.table().properties();
    assertEquals("svc", props.get(serviceId));
    assertEquals("42", props.get(version));
    assertEquals(idsOf(COLUMNS, s1),
        ComputeStatsSnapshotPropertyConverter.stringToMap(props.get(IDS)));
    assertTrue(t.table().statisticsFiles().isEmpty());
  }

  @Test
  public void withIntegrationSampledRemovesIdsKnownOnlyToHms() throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(t.file(100, null));
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = integrated(tbl);
    String serviceId = MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey();
    String version = MetastoreEventPropertyKey.CATALOG_VERSION.getKey();
    msTbl.putToParameters(IDS, "1-2:" + s0);
    msTbl.putToParameters(serviceId, "svc");
    msTbl.putToParameters(version, "42");
    msTbl.putToParameters(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "1234");
    TAlterTableUpdateStatsParams params =
        params(TIcebergStatsMode.LEGACY, s1, 100, Arrays.asList("i", "s"));
    params.getIceberg_puffin_stats().setSampled(true);

    assertEquals(2, apply(tbl, msTbl, params));

    Table table = t.table();
    assertFalse(table.properties().containsKey(IDS));
    assertFalse(msTbl.getParameters().containsKey(IDS));
    // With HMS integration only a key present in the Iceberg properties is removed
    // from HMS, so the previous metadata must carry it together with the catalog ids.
    TableMetadata current = ((HasTableOperations) table).operations().current();
    List<TableMetadata.MetadataLogEntry> previous = current.previousFiles();
    Map<String, String> setKey = TableMetadataParser.read(table.io(),
        previous.get(previous.size() - 1).file()).properties();
    assertEquals("1-2:" + s0, setKey.get(IDS));
    assertEquals("svc", setKey.get(serviceId));
    assertEquals("42", setKey.get(version));
    assertEquals("1234", setKey.get(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME));
    assertEquals("svc", current.properties().get(serviceId));
    assertEquals("42", current.properties().get(version));
    assertNull(hms_.last(Method.ALTER_TABLE_PROPERTIES));
    assertEquals(Collections.singletonList(Method.UPDATE_COLUMN_STATS), hms_.methods());
    assertEquals(LEGACY_NDV, hmsNdv(hmsStats().get("s")));
  }
}
