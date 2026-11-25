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
import static org.junit.Assert.fail;

import java.util.Collections;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Checks that COMPUTE INCREMENTAL STATS and DROP INCREMENTAL STATS reject the PARTITION
 * clause for Iceberg tables with the exact upstream texts, whatever the partition
 * specification says, and that the other forms of the statements are left alone.
 */
public class IcebergPartitionClauseTest extends FrontendTestBase {
  private static final String DB = "iceberg_partition_clause";
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "p", Types.IntegerType.get()));
  private static final String COMPUTE_ICEBERG =
      "COMPUTE INCREMENTAL STATS ... PARTITION not supported for Iceberg table ";
  private static final String DROP_ICEBERG =
      "DROP INCREMENTAL STATS ... PARTITION not supported for Iceberg table ";
  private static final String HDFS_DB = "iceberg_partition_clause_hdfs";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private IcebergTestTables addTable(boolean partitioned) throws Exception {
    return addTable(partitioned, "");
  }

  private IcebergTestTables addTable(boolean partitioned, String suffix)
      throws Exception {
    PartitionSpec spec = partitioned ?
        PartitionSpec.builderFor(SCHEMA).identity("p").build() :
        PartitionSpec.unpartitioned();
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, SCHEMA, spec, 2);
    t.append(t.file(100, partitioned ? "p=1" : null));
    ifx.addPathB(t, Collections.emptyMap());
    return t;
  }

  /** Analyzes 'sql' in database DB and returns the message of the AnalysisException. */
  private String analysisErrorMessage(String sql) {
    try {
      parseAndAnalyze(sql, createAnalysisCtx(DB));
    } catch (AnalysisException e) {
      return e.getMessage();
    } catch (Exception e) {
      throw new AssertionError("Unexpected exception for: " + sql, e);
    }
    fail("Analysis passed: " + sql);
    return null;
  }

  private void assertErrorText(String sql, String expected) {
    assertEquals(sql, expected, analysisErrorMessage(sql));
  }

  private void assertRejectedWithBothNames(IcebergTestTables t, String partition)
      throws Exception {
    String qualified = t.db() + "." + t.name();
    for (String name : new String[] {qualified, t.name()}) {
      assertErrorText("COMPUTE INCREMENTAL STATS " + name + " PARTITION (" + partition
          + ")", COMPUTE_ICEBERG + name);
      assertErrorText("DROP INCREMENTAL STATS " + name + " PARTITION (" + partition
          + ")", DROP_ICEBERG + name);
    }
  }

  @Test
  public void partitionedTableRejectsPartitionClause() throws Exception {
    assertRejectedWithBothNames(addTable(true), "p=1");
  }

  @Test
  public void unpartitionedTableRejectsPartitionClause() throws Exception {
    assertRejectedWithBothNames(addTable(false), "p=1");
  }

  @Test
  public void missingPartitionIsNotChecked() throws Exception {
    assertRejectedWithBothNames(addTable(true), "p=12345");
    assertRejectedWithBothNames(addTable(false, "_flat"), "nosuchcolumn='x'");
  }

  @Test
  public void formsWithoutPartitionAnalyze() throws Exception {
    IcebergTestTables t = addTable(true);
    for (String name : new String[] {t.db() + "." + t.name(), t.name()}) {
      parseAndAnalyzeInDb("COMPUTE INCREMENTAL STATS " + name);
      parseAndAnalyzeInDb("COMPUTE STATS " + name);
      parseAndAnalyzeInDb("DROP STATS " + name);
    }
  }

  @Test
  public void formsWithoutPartitionAnalyzeOnUnpartitionedTable() throws Exception {
    IcebergTestTables t = addTable(false);
    for (String name : new String[] {t.db() + "." + t.name(), t.name()}) {
      parseAndAnalyzeInDb("COMPUTE INCREMENTAL STATS " + name);
      parseAndAnalyzeInDb("COMPUTE STATS " + name);
      parseAndAnalyzeInDb("DROP STATS " + name);
    }
  }

  private void parseAndAnalyzeInDb(String sql) {
    try {
      parseAndAnalyze(sql, createAnalysisCtx(DB));
    } catch (Exception e) {
      throw new AssertionError("Analysis failed: " + sql, e);
    }
  }

  @Test
  public void partitionedHdfsTableKeepsItsOwnPartitionErrors() {
    feFixture_.addTestDb(HDFS_DB, null);
    feFixture_.addTestTable("create table " + HDFS_DB
        + ".t (id int) partitioned by (p int)");
    for (String sql : new String[] {
        "COMPUTE INCREMENTAL STATS " + HDFS_DB + ".t PARTITION (p=1)",
        "DROP INCREMENTAL STATS " + HDFS_DB + ".t PARTITION (p=1)"}) {
      assertErrorText(sql, "No matching partition(s) found.");
    }
  }
}
