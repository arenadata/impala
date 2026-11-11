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

package org.apache.impala.service.flight;

/**
 * A failed call on the HS2 loopback. 'broken' means the connection is unusable (transport
 * error) and the session must not be reused; otherwise HS2 reported an error status and
 * the message is Impala's error message.
 */
public class LoopbackException extends Exception {
  private final boolean broken_;

  public LoopbackException(String message, boolean broken, Throwable cause) {
    super(message, cause);
    broken_ = broken;
  }

  public LoopbackException(String message) { this(message, false, null); }

  public boolean isBroken() { return broken_; }
}
