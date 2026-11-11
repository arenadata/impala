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

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TOperationHandle;

/**
 * A statement started on a loopback session: its result is ready to be fetched.
 * Statements without a result set (DDL, DML) have already finished: no session, no
 * operation, empty schema.
 */
final class StartedStatement {
  final LoopbackSession session;
  final TOperationHandle operation;
  final Schema schema;

  StartedStatement(LoopbackSession session, TOperationHandle operation, Schema schema) {
    this.session = session;
    this.operation = operation;
    this.schema = schema;
  }
}
