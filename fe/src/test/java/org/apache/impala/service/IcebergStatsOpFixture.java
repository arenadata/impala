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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;

/**
 * Schema, columns and builders of the statistics update requests shared by the
 * Iceberg statistics op tests. The requests are built as the coordinator sends them.
 */
final class IcebergStatsOpFixture {
  static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "b", Types.BooleanType.get()),
      optional(4, "d", Types.DecimalType.of(1, 0)),
      optional(5, "UserId", Types.StringType.get()));
  /** Column labels of the stats query: lower-case names. */
  static final List<String> COLUMNS = Arrays.asList("i", "s", "b", "d", "userid");
  /** Fewer rows than distinct values of 's': its NDV is capped. */
  static final long NUM_ROWS = 15;

  private IcebergStatsOpFixture() {}

  static int fieldId(String col) {
    return SCHEMA.caseInsensitiveFindField(col).fieldId();
  }

  static List<?> values(String col) {
    List<Object> res = new ArrayList<>();
    switch (col) {
      case "i": for (int v = 1; v <= 10; ++v) res.add(v); break;
      case "s": for (char c = 'a'; c <= 't'; ++c) res.add(String.valueOf(c)); break;
      case "b": res.addAll(Arrays.asList(true, false)); break;
      // More distinct values than DECIMAL(1,0) allows.
      case "d": for (int v = 10; v <= 30; ++v) res.add(BigDecimal.valueOf(v)); break;
      case "userid": res.addAll(Arrays.asList("u1", "u2", "u3", "u4", "u5")); break;
      default: throw new IllegalArgumentException(col);
    }
    return res;
  }

  private static ByteBuffer sketch(String col) {
    return ByteBuffer.wrap(PuffinFixtures.sketch(
        SCHEMA.caseInsensitiveFindField(col).type(), values(col)).toByteArray());
  }

  static TColumnStats columnStats(String col, long ndv) {
    TColumnStats stats = new TColumnStats();
    stats.setNum_distinct_values(ndv);
    stats.setNum_nulls(0);
    stats.setMax_size(4);
    stats.setAvg_size(4);
    stats.setNum_trues(col.equals("b") ? 8 : -1);
    stats.setNum_falses(col.equals("b") ? 7 : -1);
    return stats;
  }

  static TAlterTableUpdateStatsParams params(TTableName table, TIcebergStatsMode mode,
      long s1, List<String> cols, long legacyNdv) {
    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams();
    params.setTable_name(table);
    params.setTable_stats(new TTableStats(NUM_ROWS));
    TIcebergPuffinStatsParams p =
        new TIcebergPuffinStatsParams(mode, s1, new HashMap<>());
    for (String col : cols) {
      params.putToColumn_stats(col,
          columnStats(col, mode == TIcebergStatsMode.LEGACY ? legacyNdv : -1));
      p.putToColumn_field_ids(col, fieldId(col));
      if (mode == TIcebergStatsMode.FULL) {
        p.addToTheta_field_ids(fieldId(col));
        p.putToTheta_sketches(fieldId(col), sketch(col));
      }
    }
    params.setIceberg_puffin_stats(p);
    return params;
  }

  /**
   * INCREMENTAL request for 'cols' with the metrics base 'bh' and the NDV bases of all
   * of them in the statistics file 'basePath' of 'bh'; the gap sketches hold the values
   * of the columns.
   */
  static TAlterTableUpdateStatsParams incremental(TTableName table, long s1, long bh,
      String basePath, List<String> cols) {
    TAlterTableUpdateStatsParams params =
        params(table, TIcebergStatsMode.INCREMENTAL, s1, cols, -1);
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    p.setMetrics_base_snapshot_id(bh);
    p.setNdv_bases(new ArrayList<>());
    for (String col : cols) {
      p.addToNdv_bases(new TIcebergNdvBase(fieldId(col), bh, basePath));
      p.addToTheta_field_ids(fieldId(col));
      p.putToTheta_sketches(fieldId(col), sketch(col));
    }
    return params;
  }

  static Map<Integer, Long> idsOf(List<String> cols, long snapshotId) {
    Map<Integer, Long> res = new TreeMap<>();
    for (String col : cols) res.put(fieldId(col), snapshotId);
    return res;
  }
}
