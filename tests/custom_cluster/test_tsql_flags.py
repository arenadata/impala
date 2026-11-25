# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestTsqlFlags(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--enable_tsql=false")
  def test_disabled(self):
    result = self.execute_query_expect_failure(self.client, "TSQL\nSELECT 1 AS n;")
    assert 'disabled' in str(result).lower() or 'enable_tsql' in str(result).lower()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--tsql_max_steps=5")
  def test_step_budget(self):
    result = self.execute_query_expect_failure(
        self.client, "TSQL\nWHILE 1 = 1 BEGIN END\nSELECT 1 AS n;")
    assert 'exceeded' in str(result).lower()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--tsql_max_nesting=2")
  def test_nesting_budget(self):
    self.execute_query_expect_success(
        self.client, "TSQL\nCREATE PROCEDURE self_p AS BEGIN EXEC self_p; END")
    result = self.execute_query_expect_failure(self.client, "TSQL\nEXEC self_p;")
    assert 'nesting' in str(result).lower()
