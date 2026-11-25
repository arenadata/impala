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

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * TableOperations that delegates to another one and fails the commits its Rule picks.
 * Wrap it in a BaseTable to give code under test a table whose commits fail; io()
 * may be replaced too, e.g. by a FailingFileIO.
 */
public class FailingCommitTables implements TableOperations {
  public enum Failure {
    COMMIT_FAILED,
    STATE_UNKNOWN,
    VALIDATION,
    /** RuntimeException caused by InterruptedException, as a client wraps it. */
    INTERRUPTED
  }

  public enum Mode {
    /** The commit is not passed on: the table stays unchanged. */
    BEFORE_APPLY,
    /** The commit is passed on and succeeds, then the failure is thrown. */
    AFTER_APPLY
  }

  /**
   * Picks the failing commits by number, counting from 1 over every table built with
   * this rule, so a table reloaded by the code under test keeps the count.
   * Iceberg retries a commit that throws CommitFailedException, so failing it once
   * may go unnoticed; failFrom() fails the retries too. CommitFailedException and
   * ValidationException thrown AFTER_APPLY make Iceberg delete files of the commit that
   * was in fact applied: both are CleanableFailure.
   */
  public static final class Rule {
    private final AtomicInteger commits_ = new AtomicInteger();
    private final Map<Integer, Step> at_ = new ConcurrentHashMap<>();
    private volatile int from_ = 0;
    private volatile Step fromStep_;

    public Rule failAt(int n, Failure failure, Mode mode) {
      Preconditions.checkArgument(n > 0, "commit number %s", n);
      at_.put(n, new Step(failure, mode));
      return this;
    }

    /** Fails commit 'n' and every later one. */
    public Rule failFrom(int n, Failure failure, Mode mode) {
      Preconditions.checkArgument(n > 0, "commit number %s", n);
      fromStep_ = new Step(failure, mode);
      from_ = n;
      return this;
    }

    /** Number of commits attempted, failed ones included. */
    public int commits() { return commits_.get(); }

    /** Null if commit 'n' succeeds. */
    private Step stepOf(int n) {
      Step step = at_.get(n);
      if (step == null && from_ > 0 && n >= from_) step = fromStep_;
      return step;
    }
  }

  private static final class Step {
    private final Failure failure_;
    private final Mode mode_;

    Step(Failure failure, Mode mode) {
      failure_ = Preconditions.checkNotNull(failure);
      mode_ = Preconditions.checkNotNull(mode);
    }

    RuntimeException exception(int n) {
      switch (failure_) {
        case COMMIT_FAILED:
          return new CommitFailedException("Injected commit failure of commit %s", n);
        case STATE_UNKNOWN:
          return new CommitStateUnknownException(
              new RuntimeException("Injected unknown state of commit " + n));
        case VALIDATION:
          return new ValidationException("Injected validation failure of commit %s", n);
        case INTERRUPTED:
          return new RuntimeException("Injected interruption of commit " + n,
              new InterruptedException());
        default:
          throw new IllegalStateException("unknown failure " + failure_);
      }
    }
  }

  private final TableOperations delegate_;
  private final FileIO io_;
  private final Rule rule_;

  public FailingCommitTables(TableOperations delegate, Rule rule) {
    this(delegate, delegate.io(), rule);
  }

  public FailingCommitTables(TableOperations delegate, FileIO io, Rule rule) {
    delegate_ = Preconditions.checkNotNull(delegate);
    io_ = Preconditions.checkNotNull(io);
    rule_ = Preconditions.checkNotNull(rule);
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    int n = rule_.commits_.incrementAndGet();
    Step step = rule_.stepOf(n);
    if (step == null) {
      delegate_.commit(base, metadata);
      return;
    }
    if (step.mode_ == Mode.AFTER_APPLY) delegate_.commit(base, metadata);
    throw step.exception(n);
  }

  @Override
  public TableMetadata current() { return delegate_.current(); }

  @Override
  public TableMetadata refresh() { return delegate_.refresh(); }

  @Override
  public FileIO io() { return io_; }

  @Override
  public EncryptionManager encryption() { return delegate_.encryption(); }

  @Override
  public String metadataFileLocation(String fileName) {
    return delegate_.metadataFileLocation(fileName);
  }

  @Override
  public LocationProvider locationProvider() { return delegate_.locationProvider(); }

  @Override
  public long newSnapshotId() { return delegate_.newSnapshotId(); }

  @Override
  public boolean requireStrictCleanup() { return delegate_.requireStrictCleanup(); }
}
