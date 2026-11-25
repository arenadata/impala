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
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.iceberg.BlobMetadata;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Before;

/** Fixtures of the tests of IcebergStatsUpdater#apply with incremental requests. */
public abstract class IcebergStatsUpdaterIncrementalTestBase
    extends IcebergStatsUpdaterTestBase {
  protected static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);
  protected static final List<String> THETA = Arrays.asList("i", "s", "b", "d");

  protected interface Action { void run() throws ImpalaException; }

  @Before
  public void keepStoredParameters() {
    hms_.setStoredParameters(new HashMap<>());
  }

  /** FULL statistics of all columns at a new snapshot. */
  protected long fullBase(IcebergTestTables t, IcebergTable tbl, boolean integration)
      throws Exception {
    long b = t.append(t.file(100, null));
    apply(tbl, msTbl(tbl, integration), params(TIcebergStatsMode.FULL, b, 100));
    syncFromHms(tbl);
    return b;
  }

  protected static org.apache.hadoop.hive.metastore.api.Table msTbl(IcebergTable tbl,
      boolean integration) {
    return integration ? integrated(tbl) : tbl.getMetaStoreTable().deepCopy();
  }

  /**
   * The stored snapshot ids: with HMS integration the Iceberg property, without it the
   * HMS parameter, while the Iceberg property stays unset.
   */
  protected Map<Integer, Long> storedIds(IcebergTestTables t, IcebergTable tbl,
      boolean integration) throws ImpalaException {
    Map<String, String> props = t.table().properties();
    String value;
    if (integration) {
      value = props.get(IDS);
    } else {
      assertFalse(props.containsKey(IDS));
      value = hms_.readTableParameters(tbl.getMetaStoreTable()).get(IDS);
    }
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  protected List<Method> callsAfter(int mark) {
    List<Method> all = hms_.methods();
    return new ArrayList<>(all.subList(mark, all.size()));
  }

  protected static void assertFails(Action action,
      Class<? extends ImpalaException> cls, String message) {
    try {
      action.run();
      fail("no error: " + message);
    } catch (ImpalaException e) {
      assertEquals(message, e.getMessage());
      assertEquals(cls, e.getClass());
    }
  }

  /** "ndv" of the Impala blobs registered for 's1', by field id. */
  protected static Map<Integer, Long> impalaNdvs(IcebergTestTables t, long s1) {
    Map<Integer, Long> res = new TreeMap<>();
    for (BlobMetadata blob : statsFile(t.table(), s1).blobMetadata()) {
      Map<String, String> props = blob.properties();
      if (!props.containsKey(IcebergPuffinStatsWriter.IMPALA_CREATED_BY)) continue;
      assertEquals(s1, blob.sourceSnapshotId());
      res.put(blob.fields().get(0),
          Long.parseLong(props.get(IcebergPuffinStatsWriter.NDV)));
    }
    return res;
  }

  protected static Set<String> puffinFilesOnDisk(IcebergTestTables t) throws Exception {
    File dir = new File(new URI(t.table().location() + "/metadata"));
    return Arrays.stream(dir.listFiles((d, n) -> n.endsWith(".stats")))
        .map(File::getName).collect(Collectors.toCollection(TreeSet::new));
  }
}
