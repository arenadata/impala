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

#pragma once

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/admission_control_service.proxy.h"
#include "gen-cpp/admission_control_service.service.h"
#include "scheduling/admission-controller.h"
#include "util/sharded-query-map-util.h"
#include "util/thread-pool.h"
#include "util/unique-id-hash.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class MemTracker;
class MetricGroup;
class QuerySchedulePB;

/// Singleton class that exports the RPC service used for submitting queries remotely for
/// admission.
class AdmissionControlService : public AdmissionControlServiceIf,
                                public CacheLineAligned {
 public:
  AdmissionControlService(MetricGroup* metric_group);

  /// Initializes the service by registering it with the singleton RPC manager.
  /// This mustn't be called until RPC manager has been initialized.
  Status Init();

  /// Blocks until the service shuts down.
  void Join();

  virtual void AdmitQuery(const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp,
      kudu::rpc::RpcContext* context) override;
  virtual void GetQueryStatus(const GetQueryStatusRequestPB* req,
      GetQueryStatusResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void ReleaseQuery(const ReleaseQueryRequestPB* req,
      ReleaseQueryResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void ReleaseQueryBackends(const ReleaseQueryBackendsRequestPB* req,
      ReleaseQueryBackendsResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void CancelAdmission(const CancelAdmissionRequestPB* req,
      CancelAdmissionResponsePB* resp, kudu::rpc::RpcContext* context) override;
  virtual void AdmissionHeartbeat(const AdmissionHeartbeatRequestPB* req,
      AdmissionHeartbeatResponsePB* resp, kudu::rpc::RpcContext* context) override;

  /// Gets a AdmissionControlService proxy to the configured admission control service.
  /// The newly created proxy is returned in 'proxy'. Returns error status on failure.
  /// If 'generation' is not null, it is set to the connection generation of the proxy,
  /// see UseNewConnection().
  static Status GetProxy(std::unique_ptr<AdmissionControlServiceProxy>* proxy,
      int64_t* generation = nullptr);

  /// Makes proxies returned by later GetProxy() calls use a new connection to the
  /// admission service. Called after an admission rpc timed out: KRPC keeps a connection
  /// whose negotiation hangs (e.g. its SYN went to an admissiond host that is gone) for
  /// up to --rpc_negotiation_timeout_ms, and all rpcs on it time out meanwhile.
  /// 'stale_generation' is the generation of the proxy whose rpc timed out; concurrent
  /// timeouts on the same connection switch to a new one only once.
  static void UseNewConnection(int64_t stale_generation);

  /// Relases the resources for any queries currently running on coordinators that do not
  /// appear in 'current_backends'. Called in response to statestore updates.
  void CancelQueriesOnFailedCoordinators(
      const std::unordered_set<UniqueIdPB>& current_backends);

  /// Returns whether AdmissionControlService is healthy and is able to accept admission
  /// related RPCs.
  bool IsHealthy() { return service_started_.load(); }

  /// Asyncly queues a request to remove the query from admission_state_map_.
  /// This is non-blocking, thread-safe, and avoids deadlocks with the caller.
  void CleanupAdmissionStateMapAsync(const UniqueIdPB& query_id);

 private:
  friend class ImpalaHttpHandler;
  friend class AdmissiondEnv;

  struct AdmissionState {
   public:
    AdmissionState(const UniqueIdPB& query_id, const UniqueIdPB& coord_id)
      : query_id(query_id), coord_id(coord_id) {}

    // The following are copied from the AdmitQueryRequestPB for this query and are valid
    // at any point after this AdmissionState has been added to 'admission_state_map_'.
    UniqueIdPB query_id;
    UniqueIdPB coord_id;
    TQueryExecRequest query_exec_request;
    std::unordered_set<NetworkAddressPB> blacklisted_executor_addresses;

    // Protects all of the following members.
    std::mutex lock;

    // True if SubmitForAdmission has been called for this query.
    bool submitted = false;

    // True if a final admission decision has been made for this query.
    bool admission_done = false;

    // If 'admission_done' is true, then this represents the final admission outcome, i.e.
    // an error indicates the query being rejected for admission.
    Status admit_status;

    // Used to indicate cancellation of admission to AdmissionController.
    Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome;

    // If admission was successful, contains the results of admission.
    std::unique_ptr<QuerySchedulePB> schedule;

    // List of backends that have not been released yet.
    std::unordered_set<NetworkAddressPB> unreleased_backends;

    // True if ReleaseQuery() has been called for this query.
    bool released = false;

    // Runtime profile used to record admission related info. Passed into
    // AdmissionController, which updates it.
    ObjectPool profile_pool;
    RuntimeProfile* summary_profile;

    // The name of the request pool for this query. Valid if 'submitted' is true.
    std::string request_pool = "";

    // True if the query was admitted by another admissiond instance and re-registered
    // here from its coordinator's heartbeat. 'schedule' is not set for such queries.
    bool adopted = false;
  };

  /// Tracks the memory usage of payload in the service queue.
  std::unique_ptr<MemTracker> mem_tracker_;

  /// Used to perform the actual work of scheduling and admitting queries, so that
  /// AdmitQuery() can return immediately.
  std::unique_ptr<ThreadPool<UniqueIdPB>> admission_thread_pool_;

  /// Thread-safe map from query ids to info about the query.
  ShardedQueryPBMap<std::shared_ptr<AdmissionState>> admission_state_map_;

  struct CoordinatorHeartbeat {
    // The latest heartbeat version number that was processed.
    int64_t version = 0;
    // True once a heartbeat with the full list of the coordinator's admitted queries was
    // processed while the coordinator was in this admissiond's cluster membership, i.e.
    // its running queries were adopted. Reset when the coordinator leaves the
    // membership, as its running queries are released then.
    bool reported = false;
  };

  /// Protects 'coord_id_to_heartbeat_'.
  std::mutex heartbeat_lock_;
  /// Maps from coordinator ID to its heartbeat state. NOTE: Can contain stale data from
  /// coordinators that have restarted.
  /// TODO: Leverage IMPALA-9155 to add coord_id the first time a coord sends a heartbeat
  /// and delete it when goes down.
  std::unordered_map<UniqueIdPB, CoordinatorHeartbeat> coord_id_to_heartbeat_;

  /// Protects 'adopted_query_ids_'.
  std::mutex adopted_lock_;
  /// Ids of adopted queries that are still in 'admission_state_map_'.
  std::unordered_set<UniqueIdPB> adopted_query_ids_;

  /// Callback for 'admission_thread_pool_'.
  void AdmitFromThreadPool(const UniqueIdPB& query_id);

  /// Helper for serializing 'status' as part of 'response'. Also releases memory
  /// of the RPC payload previously accounted towards the internal memory tracker.
  template <typename ResponsePBType>
  void RespondAndReleaseRpc(
      const Status& status, ResponsePBType* response, kudu::rpc::RpcContext* rpc_context);

  /// For the coordinator identified by 'coord_id', it updates the last processed hearbeat
  /// version to 'update_version' if 'update_version' is higher. Returns true if update
  /// was successful.
  bool CheckAndUpdateHeartbeat(const UniqueIdPB& coord_id, int64_t update_version);

  /// Handles the 'admitted_queries' of a heartbeat from 'coord_id': re-registers
  /// (adopts) admitted queries this admissiond does not know, e.g. after a restart, and
  /// releases adopted queries that the coordinator reports as released. Does not adopt
  /// until 'coord_id' is in this admissiond's cluster membership, so that
  /// CancelQueriesOnFailedCoordinators() does not release the adopted queries again
  /// while the membership is still partial right after startup. Returns true if
  /// 'coord_id' was in the membership.
  bool AdoptOrReleaseReportedQueries(const AdmissionHeartbeatRequestPB& req);

  /// Returns true if the coordinator 'coord_id' should send its admitted queries in its
  /// next heartbeat: it has not reported them yet (see CoordinatorHeartbeat::reported),
  /// and it is in the cluster membership ('in_membership') or admission has not started
  /// yet.
  bool NeedsAdmittedQueries(const UniqueIdPB& coord_id, bool in_membership);

  /// Returns true if 'coord_id' is in this admissiond's cluster membership.
  bool IsInMembership(const UniqueIdPB& coord_id);

  /// Removes 'query_id' from 'adopted_query_ids_' if present.
  void ForgetAdoptedQuery(const UniqueIdPB& query_id);

  /// Background thread loop that removes entries from admission_state_map_.
  void AdmissionStateMapCleanupLoop();

  /// Indicates whether the admission control service is ready.
  std::atomic_bool service_started_{false};

  /// MonotonicMillis() of the first admission rpc (AdmitQuery or heartbeat) this
  /// admissiond received, 0 before.
  std::atomic<int64_t> first_contact_ms_{0};

  /// True once admission no longer waits for coordinators to report their queries.
  std::atomic_bool adoption_grace_over_{false};

  /// Records the first admission rpc, see 'first_contact_ms_'.
  void RecordFirstContact();

  /// Returns true if every coordinator in the cluster membership has reported its
  /// queries in a heartbeat to this admissiond.
  bool AllCoordinatorsReported();

  /// Blocks admission after startup until every coordinator in the cluster membership
  /// has reported its running queries, or the adoption grace period
  /// (--admission_adoption_grace_period_ms) after the first admission rpc, whichever
  /// comes first.
  void WaitForAdoptionGracePeriod();

  /// Generation of the connection used by GetProxy(), see UseNewConnection().
  static std::atomic<int64_t> proxy_generation_;

  /// Flag to signal to exit.
  std::atomic_bool shutdown_{false};

  /// Thread handle for the background cleanup worker.
  std::unique_ptr<Thread> cleanup_thread_;

  /// Protecting the cleanup queue.
  std::mutex cleanup_queue_lock_;

  /// Condition variable to wake up the cleanup thread.
  std::condition_variable cleanup_queue_cv_;

  /// Queue of query ids waiting to be removed from the map.
  std::deque<UniqueIdPB> admission_state_cleanup_queue_;
};

} // namespace impala
