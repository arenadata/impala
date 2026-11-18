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

#include <map>
#include <set>
#include <sstream>

#include "common/init.h"
#include "statestore/statestore-subscriber.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/asan.h"
#include "util/metrics.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_bool(enable_statestored_ha);

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_port);

namespace impala {

// Object pool containing all objects that must live for the duration of the process.
// E.g. objects that are singletons and never destroyed in a real daemon (so don't support
// tear-down logic), but which we create multiple times in unit tests. We leak this pool
// instead of destroying it to avoid destroying the contained objects.
static ObjectPool* perm_objects;

TEST(StatestoreTest, SmokeTest) {
  // All allocations done by 'new' to avoid problems shutting down Thrift servers
  // gracefully.
  MetricGroup* metrics = perm_objects->Add(new MetricGroup("statestore"));
  Statestore* statestore = perm_objects->Add(new Statestore(metrics));
  // Thrift will internally pick an ephemeral port if we pass in 0 as the port.
  int statestore_port = 0;
  ASSERT_OK(statestore->Init(statestore_port));

  MetricGroup* metrics_2 = perm_objects->Add(new MetricGroup("statestore_2"));
  // Port already in use
  Statestore* statestore_wont_start = perm_objects->Add(new Statestore(metrics_2));
  ASSERT_FALSE(statestore_wont_start->Init(statestore->port()).ok());
  statestore_wont_start->ShutdownForTesting();

  StatestoreSubscriber* sub_will_start = perm_objects->Add(
      new StatestoreSubscriber("sub1", MakeNetworkAddress("localhost", 0),
          MakeNetworkAddress("localhost", statestore->port()), MakeNetworkAddress("", 0),
          new MetricGroup(""), TStatestoreSubscriberType::COORDINATOR_EXECUTOR));
  ASSERT_OK(sub_will_start->Start());

  // Confirm that a subscriber trying to use an in-use port will fail to start.
  StatestoreSubscriber* sub_will_not_start = perm_objects->Add(new StatestoreSubscriber(
      "sub3", MakeNetworkAddress("localhost", sub_will_start->heartbeat_port()),
      MakeNetworkAddress("localhost", statestore->port()), MakeNetworkAddress("", 0),
      new MetricGroup(""), TStatestoreSubscriberType::COORDINATOR_EXECUTOR));
  ASSERT_FALSE(sub_will_not_start->Start().ok());

  statestore->ShutdownForTesting();
}

// Regression test: with statestored HA enabled, the subscriber creates one
// StatestoreStub per statestore instance. The stubs must register metrics with unique
// keys so that the Prometheus exposition (which flattens all metric groups without
// prefixing the group name) contains each metric name only once.
TEST(StatestoreTest, HaPrometheusUniqueMetricNames) {
  auto ha = ScopedFlagSetter<bool>::Make(&FLAGS_enable_statestored_ha, true);
  MetricGroup* metrics = perm_objects->Add(new MetricGroup("ha_metrics"));
  StatestoreSubscriber* sub = perm_objects->Add(
      new StatestoreSubscriber("ha_sub", MakeNetworkAddress("localhost", 0),
          MakeNetworkAddress("localhost", 0), MakeNetworkAddress("localhost", 0),
          metrics, TStatestoreSubscriberType::COORDINATOR_EXECUTOR));
  // Topic metrics are registered by AddTopic(); registration with a running statestore
  // is not required for this test.
  ASSERT_OK(sub->AddTopic("test-topic", true, false, "",
      [](const StatestoreSubscriber::TopicDeltaMap& state,
          vector<TTopicDelta>* topic_updates) {}));

  stringstream prometheus_output;
  metrics->ToPrometheus(true, &prometheus_output);

  // Collect the metric names from '# HELP <name>' lines and sample lines and verify
  // that no metric is exposed more than once. Prometheus rejects an exposition with a
  // second HELP line for the same metric name.
  set<string> help_names;
  map<string, int> sample_counts;
  string line;
  while (getline(prometheus_output, line)) {
    if (line.empty()) continue;
    if (line.compare(0, 7, "# HELP ") == 0) {
      const string name = line.substr(7, line.find(' ', 7) - 7);
      EXPECT_TRUE(help_names.insert(name).second)
          << "duplicate HELP line for metric: " << name;
    } else if (line[0] != '#') {
      ++sample_counts[line.substr(0, line.find(' '))];
    }
  }
  for (const auto& entry : sample_counts) {
    EXPECT_EQ(entry.second, 1) << "duplicate samples for metric: " << entry.first;
  }
}

// Runs an SSL smoke test with provided parameters.
void SslSmokeTestHelper(const string& server_ca_certificate,
    const string& client_ca_certificate, bool sub_should_start) {
  string impala_home(getenv("IMPALA_HOME"));
  stringstream server_cert;
  server_cert << impala_home << "/be/src/testutil/server-cert.pem";
  // Override flags for the duration of this test. Modifying them while the statestore
  // is running is unsafe.
  FLAGS_ssl_server_certificate = server_ca_certificate;
  FLAGS_ssl_client_ca_certificate = client_ca_certificate;
  stringstream server_key;
  server_key << impala_home << "/be/src/testutil/server-key.pem";
  FLAGS_ssl_private_key = server_key.str();

  // Thrift will internally pick an ephemeral port if we pass in 0 as the port.
  int statestore_port = 0;
  MetricGroup* metrics = perm_objects->Add(new MetricGroup("statestore"));
  Statestore* statestore = perm_objects->Add(new Statestore(metrics));
  ASSERT_OK(statestore->Init(statestore_port));

  StatestoreSubscriber* sub = perm_objects->Add(
      new StatestoreSubscriber("smoke_sub", MakeNetworkAddress("localhost", 0),
          MakeNetworkAddress("localhost", statestore->port()), MakeNetworkAddress("", 0),
          new MetricGroup(""), TStatestoreSubscriberType::COORDINATOR_EXECUTOR));
  Status sub_status = sub->Start();
  ASSERT_EQ(sub_should_start, sub_status.ok());

  statestore->ShutdownForTesting();
}

string GetValidServerCert() {
  string impala_home(getenv("IMPALA_HOME"));
  stringstream server_cert;
  server_cert << impala_home << "/be/src/testutil/server-cert.pem";
  return server_cert.str();
}

TEST(StatestoreSslTest, ValidCertSmokeTest) {
  string valid_cert = GetValidServerCert();
  SslSmokeTestHelper(valid_cert, valid_cert, true);
}

TEST(StatestoreSslTest, InvalidCertSmokeTest) {
  string impala_home(getenv("IMPALA_HOME"));
  stringstream invalid_server_cert;
  invalid_server_cert << impala_home << "/be/src/testutil/invalid-server-cert.pem";
  SslSmokeTestHelper(GetValidServerCert(), invalid_server_cert.str(), false);
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);
  perm_objects = new ObjectPool;
  IGNORE_LEAKING_OBJECT(perm_objects);
  return RUN_ALL_TESTS();
}
