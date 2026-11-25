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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.impala.catalog.iceberg.FailingFileIO.Op;
import org.apache.impala.common.FileSystemUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FailingFileIOTest {
  private static final byte[] CONTENT = "content".getBytes(StandardCharsets.UTF_8);

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private FailingFileIO io;

  @Before
  public void setUp() {
    io = new FailingFileIO(new HadoopFileIO(FileSystemUtil.getConfiguration()));
  }

  private String path(String name) {
    return new Path(new File(tmp.getRoot(), name).toURI()).toString();
  }

  private File local(String path) { return new File(new Path(path).toUri().getPath()); }

  private String existing(String name) throws IOException {
    String path = path(name);
    Files.write(local(path).toPath(), CONTENT);
    return path;
  }

  private static byte[] read(InputFile file) throws IOException {
    byte[] buf = new byte[(int) file.getLength()];
    try (SeekableInputStream in = file.newStream()) {
      int off = 0;
      while (off < buf.length) {
        int n = in.read(buf, off, buf.length - off);
        assertTrue("unexpected end of " + file.location(), n > 0);
        off += n;
      }
    }
    return buf;
  }

  private static void write(FailingFileIO io, String path) throws IOException {
    try (OutputStream out = io.newOutputFile(path).create()) {
      out.write(CONTENT);
    }
  }

  @Test
  public void readFailsOnMatchedPathOnly() throws Exception {
    String bad = existing("bad.stats");
    String good = existing("good.stats");
    io.failPath(Op.READ, p -> p.endsWith("bad.stats"));

    InputFile failing = io.newInputFile(bad);
    assertEquals(bad, failing.location());
    try {
      failing.exists();
      fail("exists() of " + bad + " did not fail");
    } catch (RuntimeIOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(bad));
    }
    try {
      read(io.newInputFile(bad, CONTENT.length));
      fail("read of " + bad + " did not fail");
    } catch (RuntimeIOException expected) {
    }
    assertArrayEquals(CONTENT, read(io.newInputFile(good)));
    assertEquals(3, io.calls(Op.READ));
  }

  @Test
  public void writeFailsByCallNumberAfterCreatingFile() throws Exception {
    io.failCall(Op.WRITE, 2);
    write(io, path("w1"));
    try {
      write(io, path("w2"));
      fail("second write did not fail");
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(path("w2")));
    }
    write(io, path("w3"));

    assertEquals(3, io.calls(Op.WRITE));
    assertArrayEquals(CONTENT, Files.readAllBytes(local(path("w1")).toPath()));
    assertTrue("failed write left no file", local(path("w2")).exists());
    assertEquals(0, local(path("w2")).length());
    assertArrayEquals(CONTENT, Files.readAllBytes(local(path("w3")).toPath()));
  }

  @Test
  public void deleteFailsWithoutDeletingFile() throws Exception {
    String kept = existing("kept");
    String deleted = existing("deleted");
    io.failPath(Op.DELETE, kept::equals);
    try {
      io.deleteFile(kept);
      fail("delete of " + kept + " did not fail");
    } catch (RuntimeIOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(kept));
    }
    io.deleteFile(deleted);
    assertTrue(local(kept).exists());
    assertFalse(local(deleted).exists());
  }

  @Test
  public void clearedRulesPassEveryCall() throws Exception {
    String file = existing("file");
    io.failCall(Op.READ, 1).failPath(Op.WRITE, p -> true).failPath(Op.DELETE, p -> true);
    io.clearRules();
    assertArrayEquals(CONTENT, read(io.newInputFile(file)));
    write(io, path("written"));
    io.deleteFile(file);
    assertFalse(local(file).exists());
    assertEquals(1, io.calls(Op.READ));
    assertEquals(1, io.calls(Op.WRITE));
    assertEquals(1, io.calls(Op.DELETE));
  }
}
