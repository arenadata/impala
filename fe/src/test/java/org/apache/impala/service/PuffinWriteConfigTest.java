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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Puffin write switch (flag, table property) and the Thrift fields that carry the stats. */
public class PuffinWriteConfigTest {
  private BackendConfig saved_;

  @Before
  public void saveBackendConfig() { saved_ = BackendConfig.INSTANCE; }

  @After
  public void restoreBackendConfig() { BackendConfig.INSTANCE = saved_; }

  @Test
  public void propertyKeyName() {
    assertEquals("impala.iceberg_write_puffin_stats",
        IcebergTable.ICEBERG_WRITE_PUFFIN_STATS);
  }

  @Test
  public void flagIsReadFromBackendGflags() {
    TBackendGflags cfg = new TBackendGflags();
    cfg.setEnable_writing_puffin_stats(true);
    BackendConfig.create(cfg, false);
    assertTrue(BackendConfig.INSTANCE.isWritingPuffinStatsEnabled());
    cfg.setEnable_writing_puffin_stats(false);
    BackendConfig.create(cfg, false);
    assertFalse(BackendConfig.INSTANCE.isWritingPuffinStatsEnabled());
  }

  @Test
  public void puffinStatsParamsSurviveSerialization() throws Exception {
    byte[] sketch = {(byte) 0xff, 0, (byte) 0x80, 1};
    TIcebergPuffinStatsParams puffin = new TIcebergPuffinStatsParams(
        TIcebergStatsMode.FULL, 42L, Collections.singletonMap("c", 3));
    puffin.setTheta_field_ids(Collections.singletonList(3));
    puffin.setTheta_sketches(
        Collections.singletonMap(3, ByteBuffer.wrap(sketch)));
    puffin.setMetrics_base_snapshot_id(7L);
    puffin.setNdv_bases(Collections.singletonList(
        new TIcebergNdvBase(3, 7L, "/tmp/stats.puffin")));
    puffin.setSampled(false);

    TComputeStatsParams compute = new TComputeStatsParams();
    compute.setTable_name(new TTableName("db", "t"));
    compute.setIceberg_theta_query("select 1");
    compute.setIceberg_puffin_stats(puffin);
    compute.setNo_child_queries_message("nothing to do");
    TAlterTableUpdateStatsParams update = new TAlterTableUpdateStatsParams();
    update.setTable_name(new TTableName("db", "t"));
    update.setIceberg_puffin_stats(puffin);

    TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
    TDeserializer de = new TDeserializer(new TBinaryProtocol.Factory());
    TComputeStatsParams compute2 = new TComputeStatsParams();
    de.deserialize(compute2, ser.serialize(compute));
    TAlterTableUpdateStatsParams update2 = new TAlterTableUpdateStatsParams();
    de.deserialize(update2, ser.serialize(update));

    assertEquals("select 1", compute2.getIceberg_theta_query());
    assertEquals("nothing to do", compute2.getNo_child_queries_message());
    assertEquals(puffin, compute2.getIceberg_puffin_stats());
    assertEquals(puffin, update2.getIceberg_puffin_stats());
    assertArrayEquals(sketch,
        TBaseHelper.byteBufferToByteArray(
            update2.getIceberg_puffin_stats().getTheta_sketches().get(3)));
  }
}
