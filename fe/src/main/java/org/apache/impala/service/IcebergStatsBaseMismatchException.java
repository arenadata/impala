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

import java.util.List;

import org.apache.impala.common.ImpalaRuntimeException;

/**
 * The base statistics of an incremental COMPUTE STATS do not match HMS; the cached table
 * has to be reloaded.
 */
public final class IcebergStatsBaseMismatchException extends ImpalaRuntimeException {
  private static final int MAX_LISTED_COLUMNS = 10;

  IcebergStatsBaseMismatchException(String tableName, List<String> columns) {
    super(String.format("Base statistics of column(s) %s of table %s do not match the "
        + "Hive Metastore; run COMPUTE STATS.", listed(columns), tableName));
  }

  private static String listed(List<String> columns) {
    if (columns.size() <= MAX_LISTED_COLUMNS) return String.join(", ", columns);
    return String.join(", ", columns.subList(0, MAX_LISTED_COLUMNS)) + ", …";
  }
}
