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

package org.apache.impala.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests JniUtil.getExplicitHadoopConfig(), which the native S3 reader uses to take the
 * fs.s3a.* endpoint settings: only explicitly set values count, not Hadoop's defaults.
 */
public class JniUtilHadoopConfigTest {
  @Test
  public void testDefaultsAreNotExplicit() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-default.xml");
    // Set by core-default.xml only.
    assertNotNull(conf.get("fs.s3a.path.style.access"));
    assertNull(JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.path.style.access"));
    assertNull(JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.connection.ssl.enabled"));
    // Absent.
    assertNull(JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.endpoint.region"));
  }

  @Test
  public void testExplicitValues() {
    Configuration conf = new Configuration(false);
    conf.addResource("core-default.xml");
    conf.set("fs.s3a.path.style.access", "True");
    conf.set("fs.s3a.endpoint", "  https://s3gateway:9879 ");
    conf.set("fs.s3a.endpoint.region", "");
    assertEquals("True",
        JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.path.style.access"));
    // Trimmed.
    assertEquals("https://s3gateway:9879",
        JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.endpoint"));
    // Empty counts as absent.
    assertNull(JniUtil.getExplicitHadoopConfig(conf, "fs.s3a.endpoint.region"));
  }
}
