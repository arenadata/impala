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

package org.apache.impala.tsql;

/**
 * In-memory or HMS-backed procedure catalog. Names are case-insensitive.
 */
public interface TsqlProcedureStore {
  void upsert(String database, String name, String owner, String source);

  /** The stored batch, starting with TSQL or LPSQL, or null when absent. */
  String getSource(String database, String name);

  /** Owner of a stored procedure, or null when the procedure is absent. */
  String getOwner(String database, String name);

  /** Removes a procedure. Does nothing when it is already absent. */
  void drop(String database, String name);
}
