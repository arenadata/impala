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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Test;

/**
 * IcebergStatsUpdater starts from the snapshot ids of the storage, not from the cached
 * HMS table: a DDL that failed after writing the ids leaves the cache behind until
 * syncCachedIds() catches it up.
 */
public class IcebergStatsUpdaterStaleCacheTest extends IcebergStatsUpdaterTestBase {
  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private IcebergTestTables t_;
  private IcebergTable tbl_;
  private long s0_;
  private long s1_;

  private void createTable() throws Exception {
    t_ = create();
    s0_ = t_.append(t_.file(100, null));
    s1_ = t_.append(t_.file(100, null));
    tbl_ = ifx.addPathB(t_, Collections.emptyMap());
  }

  private static String idsValue(Map<Integer, Long> ids) {
    return ComputeStatsSnapshotPropertyConverter.mapToString(new TreeMap<>(ids));
  }

  private static Map<Integer, Long> ids(String value) {
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  /** Cached HMS table with 'cachedIds', null for no property. */
  private Table cached(Table msTbl, Map<Integer, Long> cachedIds) {
    if (cachedIds == null) {
      msTbl.getParameters().remove(IDS);
    } else {
      msTbl.putToParameters(IDS, idsValue(cachedIds));
    }
    return msTbl;
  }

  private Table cached(Map<Integer, Long> cachedIds) {
    return cached(tbl_.getMetaStoreTable().deepCopy(), cachedIds);
  }

  /** Makes HMS hold the parameters of the cache with 'storedIds', null for none. */
  private void storeInHms(Map<Integer, Long> storedIds) {
    Map<String, String> params = new HashMap<>(tbl_.getMetaStoreTable().getParameters());
    if (storedIds == null) {
      params.remove(IDS);
    } else {
      params.put(IDS, idsValue(storedIds));
    }
    hms_.setStoredParameters(params);
  }

  private void setIcebergIds(Map<Integer, Long> icebergIds) {
    t_.table().updateProperties().set(IDS, idsValue(icebergIds)).commit();
  }

  private Map<Integer, Long> icebergIds() {
    return ids(t_.table().properties().get(IDS));
  }

  private Map<Integer, Long> storedIds() throws ImpalaException {
    return ids(hms_.readTableParameters(tbl_.getMetaStoreTable()).get(IDS));
  }

  @Test
  public void applyStartsFromIdsStoredInHms() throws Exception {
    createTable();
    Table msTbl = cached(idsOf(Arrays.asList("i", "d"), s0_));
    storeInHms(idsOf(Collections.singletonList("d"), s0_));

    apply(tbl_, msTbl,
        params(TIcebergStatsMode.LEGACY, s1_, 100, Collections.singletonList("s")));

    Map<Integer, Long> expected = idsOf(Collections.singletonList("d"), s0_);
    expected.put(fieldId("s"), s1_);
    assertEquals(expected, hmsIds());
    assertEquals(expected, storedIds());
  }

  @Test
  public void applyIgnoresCachedIdsMissingFromHms() throws Exception {
    createTable();
    Table msTbl = cached(idsOf(Collections.singletonList("i"), s0_));
    storeInHms(null);

    apply(tbl_, msTbl,
        params(TIcebergStatsMode.LEGACY, s1_, 100, Collections.singletonList("s")));

    assertEquals(idsOf(Collections.singletonList("s"), s1_), hmsIds());
  }

  @Test
  public void detachRemovesColumnFromIdsKnownOnlyToHms() throws Exception {
    createTable();
    Table msTbl = cached(null);
    storeInHms(idsOf(Arrays.asList("i", "d"), s0_));

    detach(tbl_, msTbl, Collections.singletonList("i"));

    Map<Integer, Long> expected = idsOf(Collections.singletonList("d"), s0_);
    assertEquals(expected, storedIds());
    assertEquals(expected, ids(msTbl.getParameters().get(IDS)));
  }

  @Test
  public void detachOfOtherColumnLeavesStoredIdsForCaller() throws Exception {
    createTable();
    Table msTbl = cached(idsOf(Arrays.asList("i", "d"), s0_));
    storeInHms(idsOf(Collections.singletonList("d"), s0_));

    detach(tbl_, msTbl, Collections.singletonList("s"));

    // The caller writes 'msTbl' afterwards: it must not bring back the cached column.
    assertEquals(idsOf(Collections.singletonList("d"), s0_),
        ids(msTbl.getParameters().get(IDS)));
  }

  @Test
  public void dropRemovesIdsKnownOnlyToHms() throws Exception {
    createTable();
    Table msTbl = cached(null);
    storeInHms(idsOf(Collections.singletonList("i"), s0_));

    drop(tbl_, msTbl);

    assertTrue(storedIds().isEmpty());
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertEquals(Collections.singletonList(Method.ALTER_TABLE_PROPERTIES),
        hms_.methods());
  }

  @Test
  public void withIntegrationApplyStartsFromIcebergIds() throws Exception {
    createTable();
    setIcebergIds(idsOf(Collections.singletonList("d"), s0_));
    Table msTbl = cached(integrated(tbl_), idsOf(Arrays.asList("i", "d"), s0_));

    apply(tbl_, msTbl,
        params(TIcebergStatsMode.LEGACY, s1_, 100, Collections.singletonList("s")));

    Map<Integer, Long> expected = idsOf(Collections.singletonList("d"), s0_);
    expected.put(fieldId("s"), s1_);
    assertEquals(expected, icebergIds());
  }

  @Test
  public void withIntegrationApplyIgnoresCachedIdsMissingFromStorage() throws Exception {
    createTable();
    Table msTbl = cached(integrated(tbl_), idsOf(Collections.singletonList("i"), s0_));
    storeInHms(null);

    apply(tbl_, msTbl,
        params(TIcebergStatsMode.LEGACY, s1_, 100, Collections.singletonList("s")));

    assertEquals(idsOf(Collections.singletonList("s"), s1_), icebergIds());
  }

  @Test
  public void withIntegrationApplyStartsFromIdsKnownOnlyToHms() throws Exception {
    createTable();
    Table msTbl = cached(integrated(tbl_), idsOf(Collections.singletonList("i"), s0_));
    storeInHms(idsOf(Collections.singletonList("d"), s0_));

    apply(tbl_, msTbl,
        params(TIcebergStatsMode.LEGACY, s1_, 100, Collections.singletonList("s")));

    Map<Integer, Long> expected = idsOf(Collections.singletonList("d"), s0_);
    expected.put(fieldId("s"), s1_);
    assertEquals(expected, icebergIds());
  }

  @Test
  public void withIntegrationDetachStartsFromIcebergIds() throws Exception {
    createTable();
    setIcebergIds(idsOf(Arrays.asList("i", "d"), s0_));
    Table msTbl = cached(integrated(tbl_), null);

    detach(tbl_, msTbl, Collections.singletonList("i"));

    assertEquals(idsOf(Collections.singletonList("d"), s0_), icebergIds());
  }

  @Test
  public void failedHmsReadWritesNothing() throws Exception {
    createTable();
    Table msTbl = cached(idsOf(Collections.singletonList("i"), s0_));
    hms_.failParameterRead();

    try {
      apply(tbl_, msTbl, params(TIcebergStatsMode.FULL, s1_, 100));
      fail("apply succeeded without reading HMS");
    } catch (ImpalaException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("getTable"));
    }

    assertTrue(hms_.methods().isEmpty());
    assertTrue(t_.table().statisticsFiles().isEmpty());
    assertEquals(idsOf(Collections.singletonList("i"), s0_),
        ids(msTbl.getParameters().get(IDS)));
  }

  @Test
  public void failedHmsReadWritesNothingOnDetach() throws Exception {
    createTable();
    setIcebergIds(idsOf(Collections.singletonList("i"), s0_));
    Map<String, String> properties = new HashMap<>(t_.table().properties());
    Table msTbl = cached(idsOf(Collections.singletonList("i"), s0_));
    hms_.failParameterRead();

    try {
      detach(tbl_, msTbl, Collections.singletonList("i"));
      fail("detach succeeded without reading HMS");
    } catch (ImpalaException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("getTable"));
    }

    assertTrue(hms_.methods().isEmpty());
    assertEquals(properties, t_.table().properties());
    assertTrue(t_.table().statisticsFiles().isEmpty());
    assertEquals(idsOf(Collections.singletonList("i"), s0_),
        ids(msTbl.getParameters().get(IDS)));
  }

  @Test
  public void failedHmsReadWritesNothingOnDrop() throws Exception {
    createTable();
    setIcebergIds(idsOf(Collections.singletonList("i"), s0_));
    Map<String, String> properties = new HashMap<>(t_.table().properties());
    Table msTbl = cached(idsOf(Collections.singletonList("i"), s0_));
    hms_.failParameterRead();

    try {
      drop(tbl_, msTbl);
      fail("drop succeeded without reading HMS");
    } catch (ImpalaException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("getTable"));
    }

    assertTrue(hms_.methods().isEmpty());
    assertEquals(properties, t_.table().properties());
    assertTrue(t_.table().statisticsFiles().isEmpty());
    assertEquals(idsOf(Collections.singletonList("i"), s0_),
        ids(msTbl.getParameters().get(IDS)));
  }

  private void syncCachedIds() {
    tbl_.writeLock().lock();
    try {
      IcebergStatsUpdater.syncCachedIds(tbl_, hms_);
    } finally {
      tbl_.releaseWriteLock();
    }
  }

  @Test
  public void syncCachedIdsTakesHmsIds() throws Exception {
    createTable();
    cached(tbl_.getMetaStoreTable(), idsOf(Arrays.asList("i", "d"), s0_));
    storeInHms(idsOf(Collections.singletonList("d"), s0_));

    syncCachedIds();
    assertEquals(idsOf(Collections.singletonList("d"), s0_),
        ids(tbl_.getMetaStoreTable().getParameters().get(IDS)));

    storeInHms(null);
    syncCachedIds();
    assertFalse(tbl_.getMetaStoreTable().getParameters().containsKey(IDS));
    assertTrue(hms_.methods().isEmpty());
  }

  @Test
  public void syncCachedIdsDropsCachedIdsIfHmsReadFails() throws Exception {
    createTable();
    cached(tbl_.getMetaStoreTable(), idsOf(Collections.singletonList("i"), s0_));
    hms_.failParameterRead();

    syncCachedIds();

    assertFalse(tbl_.getMetaStoreTable().getParameters().containsKey(IDS));
    assertTrue(hms_.methods().isEmpty());
  }
}
