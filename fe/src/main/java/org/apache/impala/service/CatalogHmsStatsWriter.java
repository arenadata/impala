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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.IcebergUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

/**
 * HMS side of the statistics write of one catalogd command. Each method makes at most
 * one writing HMS call and does not retry.
 */
public class CatalogHmsStatsWriter implements IcebergStatsUpdater.HmsStatsWriter {
  @FunctionalInterface
  public interface AlterTableFn {
    void apply(Table msTbl) throws ImpalaException;
  }

  private final CatalogServiceCatalog catalog_;
  private final EventSequence timeline_;
  private final AlterTableFn applyAlterTable_;

  public CatalogHmsStatsWriter(CatalogServiceCatalog catalog, EventSequence timeline,
      AlterTableFn applyAlterTable) {
    catalog_ = Preconditions.checkNotNull(catalog);
    timeline_ = Preconditions.checkNotNull(timeline);
    applyAlterTable_ = Preconditions.checkNotNull(applyAlterTable);
  }

  @Override
  public void updateColumnStats(ColumnStatistics stats) throws ImpalaException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient(timeline_)) {
      try {
        client.getHiveClient().updateTableColumnStatistics(stats);
      } catch (Exception e) {
        throw new ImpalaRuntimeException(String.format(
            CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR, "updateTableColumnStatistics"),
            e);
      }
    }
    timeline_.markEvent("Updated column stats");
  }

  /**
   * HiveCatalog commits move the metadata locations in HMS; 'msTbl' takes them from HMS
   * so that alter_table does not point the table back at older metadata.
   */
  @Override
  public void alterTableProperties(Table msTbl) throws ImpalaException {
    if (IcebergUtil.isHiveCatalog(msTbl.getParameters())) {
      Table hmsTbl = hmsTable(msTbl);
      copyParameter(hmsTbl, msTbl, IcebergTable.METADATA_LOCATION);
      copyParameter(hmsTbl, msTbl, IcebergTable.PREVIOUS_METADATA_LOCATION);
    }
    applyAlterTable_.apply(msTbl);
    timeline_.markEvent("Updated table properties");
  }

  @Override
  public Map<String, String> readTableParameters(Table msTbl) throws ImpalaException {
    Map<String, String> params = hmsTable(msTbl).getParameters();
    return params == null ? new HashMap<>() : params;
  }

  private Table hmsTable(Table msTbl) throws ImpalaException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient(timeline_)) {
      try {
        return client.getHiveClient().getTable(msTbl.getDbName(), msTbl.getTableName());
      } catch (TException e) {
        throw new ImpalaRuntimeException(String.format(
            CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR, "getTable"), e);
      }
    }
  }

  private static void copyParameter(Table from, Table to, String key) {
    String value = from.getParameters() == null ? null : from.getParameters().get(key);
    if (value == null) {
      to.getParameters().remove(key);
    } else {
      to.putToParameters(key, value);
    }
  }

  @Override
  public Map<String, ColumnStatisticsObj> readColumnStats(Table msTbl,
      List<String> colNames) throws ImpalaException {
    List<String> lowerCaseNames = new ArrayList<>();
    for (String colName : colNames) lowerCaseNames.add(colName.toLowerCase());
    List<ColumnStatisticsObj> objs;
    try (MetaStoreClient client = catalog_.getMetaStoreClient(timeline_)) {
      try {
        objs = MetastoreShim.getTableColumnStatistics(client.getHiveClient(),
            msTbl.getDbName(), msTbl.getTableName(), lowerCaseNames);
      } catch (TException e) {
        throw new ImpalaRuntimeException(String.format(
            CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR, "getTableColumnStatistics"), e);
      }
    }
    Map<String, ColumnStatisticsObj> res = new HashMap<>();
    for (ColumnStatisticsObj obj : objs) res.put(obj.getColName().toLowerCase(), obj);
    return res;
  }
}
