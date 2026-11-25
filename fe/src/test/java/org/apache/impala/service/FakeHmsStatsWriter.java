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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;

import com.google.common.base.Preconditions;

/**
 * HmsStatsWriter that records its calls instead of talking to HMS. Call 'n' (1-based,
 * over all methods) can be made to fail; column stats returned by readColumnStats are
 * set by the test. readTableParameters is not recorded and so is not counted by
 * failAt: it returns the parameters set by the test, which a successful
 * alterTableProperties replaces, or those of its argument if none are set.
 */
public class FakeHmsStatsWriter implements IcebergStatsUpdater.HmsStatsWriter {
  public enum Method { UPDATE_COLUMN_STATS, ALTER_TABLE_PROPERTIES, READ_COLUMN_STATS }

  /** One recorded call with a deep copy of its argument. */
  public static final class Call {
    public final Method method;
    /** Argument of UPDATE_COLUMN_STATS, null otherwise. */
    public final ColumnStatistics stats;
    /** Argument of ALTER_TABLE_PROPERTIES and READ_COLUMN_STATS, null otherwise. */
    public final Table msTbl;
    /** The call threw the injected failure, so HMS would not have the argument. */
    public final boolean failed;

    private Call(Method method, ColumnStatistics stats, Table msTbl, boolean failed) {
      this.method = method;
      this.stats = stats;
      this.msTbl = msTbl;
      this.failed = failed;
    }
  }

  private final List<Call> calls_ = new ArrayList<>();
  private final Map<Integer, ImpalaException> failures_ = new HashMap<>();
  private Map<String, ColumnStatisticsObj> storedStats_ = new HashMap<>();
  private Map<String, String> storedParams_;
  private final Set<Integer> interrupting_ = new HashSet<>();
  private boolean failParameterRead_;

  /** Makes call 'n' throw an HMS-like error after recording it. */
  public FakeHmsStatsWriter failAt(int n) {
    Preconditions.checkArgument(n > 0, "call number %s", n);
    failures_.put(n, new ImpalaRuntimeException("Injected HMS failure of call " + n));
    return this;
  }

  /** Like failAt, and the failing call leaves the interrupt flag of its thread set. */
  public FakeHmsStatsWriter failAtInterrupting(int n) {
    failAt(n);
    interrupting_.add(n);
    return this;
  }

  public FakeHmsStatsWriter setStoredStats(Map<String, ColumnStatisticsObj> stats) {
    storedStats_ = new HashMap<>(stats);
    return this;
  }

  /** Makes HMS hold 'params' rather than the parameters of the table passed in. */
  public FakeHmsStatsWriter setStoredParameters(Map<String, String> params) {
    storedParams_ = new HashMap<>(params);
    return this;
  }

  public FakeHmsStatsWriter failParameterRead() {
    failParameterRead_ = true;
    return this;
  }

  public List<Call> calls() { return Collections.unmodifiableList(calls_); }

  public List<Method> methods() {
    return calls_.stream().map(c -> c.method).collect(Collectors.toList());
  }

  /** Last call of 'method', or null if there was none. */
  public Call last(Method method) {
    for (int i = calls_.size() - 1; i >= 0; --i) {
      if (calls_.get(i).method == method) return calls_.get(i);
    }
    return null;
  }

  /** Last call of 'method' that did not fail, or null if there was none. */
  public Call lastSucceeded(Method method) {
    for (int i = calls_.size() - 1; i >= 0; --i) {
      Call call = calls_.get(i);
      if (call.method == method && !call.failed) return call;
    }
    return null;
  }

  @Override
  public void updateColumnStats(ColumnStatistics stats) throws ImpalaException {
    record(Method.UPDATE_COLUMN_STATS, stats.deepCopy(), null);
  }

  @Override
  public void alterTableProperties(Table msTbl) throws ImpalaException {
    record(Method.ALTER_TABLE_PROPERTIES, null, msTbl.deepCopy());
    if (storedParams_ != null) storedParams_ = new HashMap<>(msTbl.getParameters());
  }

  @Override
  public Map<String, String> readTableParameters(Table msTbl) throws ImpalaException {
    if (failParameterRead_) {
      throw new ImpalaRuntimeException("Injected HMS failure of getTable");
    }
    return new HashMap<>(storedParams_ != null ? storedParams_ : msTbl.getParameters());
  }

  @Override
  public Map<String, ColumnStatisticsObj> readColumnStats(Table msTbl,
      List<String> colNames) throws ImpalaException {
    record(Method.READ_COLUMN_STATS, null, msTbl.deepCopy());
    Map<String, ColumnStatisticsObj> res = new HashMap<>();
    for (String col : colNames) {
      ColumnStatisticsObj obj = storedStats_.get(col);
      if (obj != null) res.put(col, obj.deepCopy());
    }
    return res;
  }

  private void record(Method method, ColumnStatistics stats, Table msTbl)
      throws ImpalaException {
    ImpalaException failure = failures_.get(calls_.size() + 1);
    calls_.add(new Call(method, stats, msTbl, failure != null));
    if (interrupting_.contains(calls_.size())) Thread.currentThread().interrupt();
    if (failure != null) throw failure;
  }
}
