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

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * FileIO that delegates to another one and fails chosen operations:
 * - a failing read throws RuntimeIOException on any access to the file: exists(),
 *   getLength() or newStream();
 * - a failing write creates the file and throws IOException on the first byte written,
 *   so the writer is left with a partial file to clean up;
 * - a failing delete throws RuntimeIOException and keeps the file.
 * Each operation counts its calls from 1, failed calls included: reads are counted by
 * newInputFile(), writes by newOutputFile(), deletes by deleteFile(). Rules may be added
 * while the IO is in use.
 */
public class FailingFileIO implements FileIO {
  public enum Op { READ, WRITE, DELETE }

  private static final class Rule {
    private final Op op_;
    /** Null matches any path. */
    private final Predicate<String> path_;
    /** 0 matches any call. */
    private final int call_;

    Rule(Op op, Predicate<String> path, int call) {
      op_ = op;
      path_ = path;
      call_ = call;
    }

    boolean matches(Op op, String path, int call) {
      return op_ == op && (path_ == null || path_.test(path))
          && (call_ == 0 || call_ == call);
    }
  }

  private final FileIO delegate_;
  private final Map<Op, AtomicInteger> calls_ = new EnumMap<>(Op.class);
  private final List<Rule> rules_ = new CopyOnWriteArrayList<>();

  public FailingFileIO(FileIO delegate) {
    delegate_ = Preconditions.checkNotNull(delegate);
    for (Op op : Op.values()) calls_.put(op, new AtomicInteger());
  }

  /** Fails every 'op' on the paths 'path' accepts. */
  public FailingFileIO failPath(Op op, Predicate<String> path) {
    rules_.add(new Rule(op, Preconditions.checkNotNull(path), 0));
    return this;
  }

  /** Fails the 'n'-th call of 'op', counting from 1. */
  public FailingFileIO failCall(Op op, int n) {
    Preconditions.checkArgument(n > 0, "call number %s", n);
    rules_.add(new Rule(op, null, n));
    return this;
  }

  /** Drops all rules; call counts stay. */
  public void clearRules() { rules_.clear(); }

  public int calls(Op op) { return calls_.get(op).get(); }

  private boolean fails(Op op, String path) {
    int call = calls_.get(op).incrementAndGet();
    for (Rule r : rules_) {
      if (r.matches(op, path, call)) return true;
    }
    return false;
  }

  @Override
  public InputFile newInputFile(String path) {
    InputFile file = delegate_.newInputFile(path);
    return fails(Op.READ, path) ? new FailingInputFile(file) : file;
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    InputFile file = delegate_.newInputFile(path, length);
    return fails(Op.READ, path) ? new FailingInputFile(file) : file;
  }

  @Override
  public OutputFile newOutputFile(String path) {
    OutputFile file = delegate_.newOutputFile(path);
    return fails(Op.WRITE, path) ? new FailingOutputFile(file) : file;
  }

  @Override
  public void deleteFile(String path) {
    if (fails(Op.DELETE, path)) {
      throw new RuntimeIOException("Injected delete failure: %s", path);
    }
    delegate_.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() { return delegate_.properties(); }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate_.initialize(properties);
  }

  @Override
  public void close() { delegate_.close(); }

  private static final class FailingInputFile implements InputFile {
    private final InputFile delegate_;

    FailingInputFile(InputFile delegate) { delegate_ = delegate; }

    private RuntimeIOException failure() {
      return new RuntimeIOException("Injected read failure: %s", delegate_.location());
    }

    @Override
    public long getLength() { throw failure(); }

    @Override
    public SeekableInputStream newStream() { throw failure(); }

    @Override
    public String location() { return delegate_.location(); }

    @Override
    public boolean exists() { throw failure(); }
  }

  private static final class FailingOutputFile implements OutputFile {
    private final OutputFile delegate_;

    FailingOutputFile(OutputFile delegate) { delegate_ = delegate; }

    @Override
    public PositionOutputStream create() {
      return new FailingStream(delegate_.create(), location());
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new FailingStream(delegate_.createOrOverwrite(), location());
    }

    @Override
    public String location() { return delegate_.location(); }

    @Override
    public InputFile toInputFile() { return delegate_.toInputFile(); }
  }

  private static final class FailingStream extends PositionOutputStream {
    private final PositionOutputStream delegate_;
    private final String location_;

    FailingStream(PositionOutputStream delegate, String location) {
      delegate_ = delegate;
      location_ = location;
    }

    private IOException failure() {
      return new IOException("Injected write failure: " + location_);
    }

    @Override
    public long getPos() throws IOException { return delegate_.getPos(); }

    @Override
    public void write(int b) throws IOException { throw failure(); }

    @Override
    public void write(byte[] b, int off, int len) throws IOException { throw failure(); }

    @Override
    public void flush() throws IOException { delegate_.flush(); }

    @Override
    public void close() throws IOException { delegate_.close(); }
  }
}
