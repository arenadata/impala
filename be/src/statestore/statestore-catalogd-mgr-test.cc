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

#include "statestore/statestore-catalogd-mgr.h"

#include "gen-cpp/Types_types.h"
#include "testutil/gtest-util.h"
#include "util/container-util.h"

#include "common/names.h"

DECLARE_int64(catalogd_ha_preemption_wait_period_ms);
DECLARE_bool(catalogd_ha_failover_on_active_reregistration);

namespace impala {

const SubscriberId CATALOGD_A = "catalog-server@catalog-0:26000";
const SubscriberId CATALOGD_B = "catalog-server@catalog-1:26000";

class StatestoreCatalogdMgrTest : public testing::Test {
 protected:
  void SetUp() override {
    saved_wait_period_ms_ = FLAGS_catalogd_ha_preemption_wait_period_ms;
    saved_failover_on_reregistration_ =
        FLAGS_catalogd_ha_failover_on_active_reregistration;
    // Designate the first registered catalogd as active right away.
    FLAGS_catalogd_ha_preemption_wait_period_ms = 0;
  }

  void TearDown() override {
    FLAGS_catalogd_ha_preemption_wait_period_ms = saved_wait_period_ms_;
    FLAGS_catalogd_ha_failover_on_active_reregistration =
        saved_failover_on_reregistration_;
  }

  static TCatalogRegistration Registration(const string& host, bool force = false) {
    TCatalogRegistration registration;
    TNetworkAddress address;
    address.__set_hostname(host);
    address.__set_port(26000);
    registration.__set_address(address);
    registration.__set_enable_catalogd_ha(true);
    registration.__set_force_catalogd_active(force);
    return registration;
  }

  static RegistrationId NewRegistrationId(int64_t lo) {
    RegistrationId id;
    id.__set_hi(1);
    id.__set_lo(lo);
    return id;
  }

  /// Registers CATALOGD_A (active) and CATALOGD_B (standby).
  void RegisterPair(StatestoreCatalogdMgr* mgr) {
    ASSERT_TRUE(mgr->RegisterCatalogd(
        false, CATALOGD_A, NewRegistrationId(1), Registration("10.0.0.1")));
    ASSERT_FALSE(mgr->RegisterCatalogd(
        false, CATALOGD_B, NewRegistrationId(2), Registration("10.0.0.2")));
    ASSERT_TRUE(mgr->IsActiveCatalogd(CATALOGD_A));
  }

  static int64_t ActiveVersion(StatestoreCatalogdMgr* mgr, string* active_host) {
    bool has_active = false;
    int64_t version = 0;
    const TCatalogRegistration& active =
        mgr->GetActiveCatalogRegistration(&has_active, &version);
    EXPECT_TRUE(has_active);
    *active_host = active.address.hostname;
    return version;
  }

 private:
  int64_t saved_wait_period_ms_ = 0;
  bool saved_failover_on_reregistration_ = true;
};

// A restarted active catalogd registers again with the same subscriber id. The standby
// catalogd must become active and the re-registered instance must become standby.
TEST_F(StatestoreCatalogdMgrTest, ActiveReregistrationFailsOverToStandby) {
  StatestoreCatalogdMgr mgr(true);
  RegisterPair(&mgr);
  string active_host;
  int64_t version_before = ActiveVersion(&mgr, &active_host);
  EXPECT_EQ("10.0.0.1", active_host);

  EXPECT_TRUE(mgr.RegisterCatalogd(
      true, CATALOGD_A, NewRegistrationId(3), Registration("10.0.0.3")));

  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_B));
  EXPECT_EQ(version_before + 1, ActiveVersion(&mgr, &active_host));
  EXPECT_EQ("10.0.0.2", active_host);
  EXPECT_EQ("10.0.0.3", mgr.GetStandbyCatalogRegistration().address.hostname);

  // Losing the new standby leaves the active catalogd untouched.
  EXPECT_FALSE(mgr.UnregisterCatalogd(CATALOGD_A));
  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_B));
}

// With the flag disabled the re-registered active catalogd keeps its role, but its
// latest registration (new address) is reported so that coordinators follow it.
TEST_F(StatestoreCatalogdMgrTest, ActiveReregistrationKeepsRoleWhenDisabled) {
  FLAGS_catalogd_ha_failover_on_active_reregistration = false;
  StatestoreCatalogdMgr mgr(true);
  RegisterPair(&mgr);
  string active_host;
  int64_t version_before = ActiveVersion(&mgr, &active_host);

  EXPECT_TRUE(mgr.RegisterCatalogd(
      true, CATALOGD_A, NewRegistrationId(3), Registration("10.0.0.3")));
  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_A));
  EXPECT_EQ(version_before + 1, ActiveVersion(&mgr, &active_host));
  EXPECT_EQ("10.0.0.3", active_host);

  // Same address again: no new designation.
  EXPECT_FALSE(mgr.RegisterCatalogd(
      true, CATALOGD_A, NewRegistrationId(4), Registration("10.0.0.3")));
  EXPECT_EQ(version_before + 1, ActiveVersion(&mgr, &active_host));
}

// A re-registered standby catalogd causes no role change; its registration is updated.
TEST_F(StatestoreCatalogdMgrTest, StandbyReregistrationKeepsRoles) {
  StatestoreCatalogdMgr mgr(true);
  RegisterPair(&mgr);
  string active_host;
  int64_t version_before = ActiveVersion(&mgr, &active_host);

  EXPECT_FALSE(mgr.RegisterCatalogd(
      true, CATALOGD_B, NewRegistrationId(3), Registration("10.0.0.4")));
  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_A));
  EXPECT_EQ(version_before, ActiveVersion(&mgr, &active_host));
  EXPECT_EQ("10.0.0.4", mgr.GetStandbyCatalogRegistration().address.hostname);
}

// force_catalogd_active on the active catalogd never hands the role to the standby.
TEST_F(StatestoreCatalogdMgrTest, ForcedActiveReregistrationKeepsRole) {
  StatestoreCatalogdMgr mgr(true);
  RegisterPair(&mgr);

  mgr.RegisterCatalogd(
      true, CATALOGD_A, NewRegistrationId(3), Registration("10.0.0.1", true));
  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_A));
}

// Without a standby catalogd the re-registered active catalogd stays active.
TEST_F(StatestoreCatalogdMgrTest, SingleActiveReregistrationKeepsRole) {
  StatestoreCatalogdMgr mgr(true);
  ASSERT_TRUE(mgr.RegisterCatalogd(
      false, CATALOGD_A, NewRegistrationId(1), Registration("10.0.0.1")));
  string active_host;
  int64_t version_before = ActiveVersion(&mgr, &active_host);

  EXPECT_TRUE(mgr.RegisterCatalogd(
      true, CATALOGD_A, NewRegistrationId(2), Registration("10.0.0.5")));
  EXPECT_TRUE(mgr.IsActiveCatalogd(CATALOGD_A));
  EXPECT_EQ(version_before + 1, ActiveVersion(&mgr, &active_host));
  EXPECT_EQ("10.0.0.5", active_host);
}

// The admissiond election (admissiond HA) uses a manager with explicit settings.
const SubscriberId ADMISSIOND_A = "admissiond@admissiond-0:29500";
const SubscriberId ADMISSIOND_B = "admissiond@admissiond-1:29500";

// Explicit settings are used instead of the catalogd flags: the fixture sets
// --catalogd_ha_preemption_wait_period_ms to 0, but the first admissiond waits for the
// second one.
TEST_F(StatestoreCatalogdMgrTest, AdmissiondSettingsIgnoreCatalogdFlags) {
  StatestoreCatalogdMgr mgr(true, /* use_subscriber_id_as_priority */ true,
      /* preemption_wait_period_ms */ 100000,
      /* failover_on_active_reregistration */ true);
  EXPECT_FALSE(mgr.RegisterCatalogd(
      false, ADMISSIOND_B, NewRegistrationId(1), Registration("10.0.0.2")));
  EXPECT_FALSE(mgr.CheckActiveCatalog());
  // The second registration designates the one with the lower subscriber id, although
  // it has the higher registration id.
  EXPECT_TRUE(mgr.RegisterCatalogd(
      false, ADMISSIOND_A, NewRegistrationId(2), Registration("10.0.0.1")));
  EXPECT_TRUE(mgr.IsActiveCatalogd(ADMISSIOND_A));
}

// Two statestoreds see the registrations in different orders and must designate the
// same admissiond.
TEST_F(StatestoreCatalogdMgrTest, AdmissiondPriorityIndependentOfOrder) {
  for (bool a_first : {true, false}) {
    StatestoreCatalogdMgr mgr(true, true, 100000, true);
    const SubscriberId& first = a_first ? ADMISSIOND_A : ADMISSIOND_B;
    const SubscriberId& second = a_first ? ADMISSIOND_B : ADMISSIOND_A;
    EXPECT_FALSE(mgr.RegisterCatalogd(
        false, first, NewRegistrationId(a_first ? 1 : 2), Registration("10.0.0.1")));
    EXPECT_TRUE(mgr.RegisterCatalogd(
        false, second, NewRegistrationId(a_first ? 2 : 1), Registration("10.0.0.2")));
    EXPECT_TRUE(mgr.IsActiveCatalogd(ADMISSIOND_A)) << "a_first=" << a_first;
  }
}

// The standby admissiond takes over when the active one fails, and the failed one comes
// back as standby (no failback).
TEST_F(StatestoreCatalogdMgrTest, AdmissiondFailoverWithoutFailback) {
  StatestoreCatalogdMgr mgr(true, true, 100000, true);
  EXPECT_FALSE(mgr.RegisterCatalogd(
      false, ADMISSIOND_A, NewRegistrationId(1), Registration("10.0.0.1")));
  EXPECT_TRUE(mgr.RegisterCatalogd(
      false, ADMISSIOND_B, NewRegistrationId(2), Registration("10.0.0.2")));
  ASSERT_TRUE(mgr.IsActiveCatalogd(ADMISSIOND_A));
  string active_host;
  int64_t version_before = ActiveVersion(&mgr, &active_host);

  EXPECT_TRUE(mgr.UnregisterCatalogd(ADMISSIOND_A));
  EXPECT_TRUE(mgr.IsActiveCatalogd(ADMISSIOND_B));
  EXPECT_EQ(version_before + 1, ActiveVersion(&mgr, &active_host));
  EXPECT_EQ("10.0.0.2", active_host);

  EXPECT_FALSE(mgr.RegisterCatalogd(
      false, ADMISSIOND_A, NewRegistrationId(3), Registration("10.0.0.3")));
  EXPECT_TRUE(mgr.IsActiveCatalogd(ADMISSIOND_B));
  EXPECT_EQ("10.0.0.3", mgr.GetStandbyCatalogRegistration().address.hostname);
}

} // namespace impala

IMPALA_TEST_MAIN();
