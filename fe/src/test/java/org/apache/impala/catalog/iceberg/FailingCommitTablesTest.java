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

package org.apache.impala.catalog.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Failure;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Mode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FailingCommitTablesTest {
  private static final Schema SCHEMA =
      new Schema(required(1, "id", Types.IntegerType.get()));

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private IcebergTestTables t;

  @Before
  public void setUp() {
    t = IcebergTestTables.create(tmp, "db", "t", SCHEMA, PartitionSpec.unpartitioned(), 2);
    // Iceberg retries failed commits with backoff; the tests count every attempt.
    t.table().updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();
  }

  private Table failing(FailingCommitTables.Rule rule) {
    TableOperations ops = ((HasTableOperations) t.table()).operations();
    return new BaseTable(new FailingCommitTables(ops, rule), t.name());
  }

  private static void setProperty(Table table, String key) {
    table.updateProperties().set(key, "v").commit();
  }

  private String property(String key) { return t.table().properties().get(key); }

  @Test
  public void commitFailedBeforeApplyLeavesTableUnchanged() {
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule()
        .failFrom(1, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    try {
      setProperty(failing(rule), "k");
      fail("commit did not fail");
    } catch (CommitFailedException expected) {
    }
    assertNull(property("k"));
    assertEquals(1, rule.commits());
  }

  @Test
  public void unknownStateAfterApplyKeepsCommit() {
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule()
        .failAt(1, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    try {
      setProperty(failing(rule), "k");
      fail("commit did not fail");
    } catch (CommitStateUnknownException expected) {
    }
    assertEquals("v", property("k"));
  }

  @Test
  public void validationFailureBeforeApplyLeavesTableUnchanged() {
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule()
        .failAt(1, Failure.VALIDATION, Mode.BEFORE_APPLY);
    try {
      t.append(t.file(10, null));
      IcebergTestTables.append(failing(rule), t.file(10, null));
      fail("commit did not fail");
    } catch (ValidationException expected) {
    }
    assertEquals(1, Iterables.size(t.table().snapshots()));
  }

  @Test
  public void countSpansTablesOfOneRule() {
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule()
        .failAt(2, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    setProperty(failing(rule), "k1");
    try {
      setProperty(failing(rule), "k2");
      fail("second commit did not fail");
    } catch (CommitStateUnknownException expected) {
    }
    setProperty(failing(rule), "k3");
    assertEquals("v", property("k1"));
    assertNull(property("k2"));
    assertEquals("v", property("k3"));
    assertEquals(3, rule.commits());
  }

  @Test
  public void ioReplacesIoOfDelegate() {
    TableOperations ops = ((HasTableOperations) t.table()).operations();
    FailingFileIO io = new FailingFileIO(ops.io());
    Table table = new BaseTable(
        new FailingCommitTables(ops, io, new FailingCommitTables.Rule()), t.name());
    assertSame(io, table.io());
    assertSame(ops.io(),
        new BaseTable(new FailingCommitTables(ops, new FailingCommitTables.Rule()),
            t.name()).io());
  }
}
