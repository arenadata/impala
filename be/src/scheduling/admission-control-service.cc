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

#include "scheduling/admission-control-service.h"

#include "common/constant-strings.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "rpc/sidecar-util.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "scheduling/admission-controller.h"
#include "scheduling/admissiond-env.h"
#include "util/cpu-info.h"
#include "util/kudu-status-util.h"
#include "util/memory-metrics.h"
#include "util/parse-util.h"
#include "util/promise.h"
#include "util/time.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

static const string QUEUE_LIMIT_MSG = "(Advanced) Limit on RPC payloads consumption for "
                                      "AdmissionControlService. "
    + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit");
DEFINE_string(admission_control_service_queue_mem_limit, "50MB", QUEUE_LIMIT_MSG.c_str());
DEFINE_int32(admission_control_service_num_svc_threads, 0,
    "Number of threads for processing admission control service's RPCs. if left at "
    "default value 0, it will be set to number of CPU cores. Set it to a positive value "
    "to change from the default.");
DEFINE_int32(admission_thread_pool_size, 5,
    "(Advanced) Size of the thread-pool processing AdmitQuery requests.");
DEFINE_int32(max_admission_queue_size, 50,
    "(Advanced) Max size of the queue for the AdmitQuery thread pool.");

DEFINE_string(admission_service_host, "",
    "If provided, queries submitted to this impalad will be scheduled and admitted by "
    "contacting the admission control service at the specified address and "
    "--admission_service_port.");
DEFINE_int32(admission_status_wait_time_ms, 100,
    "(Advanced) The number of milliseconds the GetQueryStatus() rpc in the admission "
    "control service will wait for admission to complete before returning.");
DEFINE_bool(admission_adopt_running_queries, true,
    "(Advanced) If true, the admission control service re-registers running queries "
    "that its coordinators report in their heartbeats but that it does not know, e.g. "
    "after an admissiond restart, so that their resources are accounted for until they "
    "finish. Keep it on together with --admission_coordinator_heartbeat_timeout_s: "
    "queries released for a silent coordinator are re-registered when it is heard "
    "again.");
DEFINE_int32(admission_coordinator_heartbeat_timeout_s, 30,
    "The admission control service releases the running queries of a coordinator that "
    "has not sent an admission heartbeat for this many seconds, e.g. because it now "
    "talks to another admissiond. Raised to at least 3 x (heartbeat rpc timeout + "
    "heartbeat period). 0 disables it.");

DEFINE_int32(admission_adoption_grace_period_ms, 10000,
    "(Advanced) After an admissiond starts, it admits queries only when every "
    "coordinator in the cluster membership has reported its still running queries in a "
    "heartbeat (--admission_adopt_running_queries), or at the latest this many "
    "milliseconds after it received its first admission rpc. Raised to at least "
    "--admission_heartbeat_rpc_timeout_ms + 2 x --admission_heartbeat_frequency_ms, the "
    "time a coordinator may need to report. 0 disables the wait.");

DEFINE_int64(admissiond_ha_statestore_lease_ms, 5000, "(Advanced) With admissiond HA, "
    "an active admissiond stops admitting queries while it has had no heartbeat from the "
    "active statestore for this many milliseconds, so that it does not admit after the "
    "statestore may have designated the other admissiond. Must be shorter than the "
    "statestore's failure detection (--statestore_max_missed_heartbeats x "
    "--statestore_heartbeat_frequency_ms). 0 disables it.");

DECLARE_int32(admission_heartbeat_frequency_ms);
DECLARE_int32(admission_heartbeat_rpc_timeout_ms);
DECLARE_bool(enable_admissiond_ha);
DECLARE_string(hostname);
DECLARE_int32(admission_service_port);

namespace impala {

#define RESPOND_IF_ERROR(stmt)                          \
  do {                                                  \
    const Status& _status = (stmt);                     \
    if (UNLIKELY(!_status.ok())) {                      \
      RespondAndReleaseRpc(_status, resp, rpc_context); \
      return;                                           \
    }                                                   \
  } while (false)

AdmissionControlService::AdmissionControlService(MetricGroup* metric_group)
  : AdmissionControlServiceIf(AdmissiondEnv::GetInstance()->rpc_mgr()->metric_entity(),
        AdmissiondEnv::GetInstance()->rpc_mgr()->result_tracker()),
    is_active_(!FLAGS_enable_admissiond_ha),
    active_admissiond_version_checker_(new ActiveCatalogdVersionChecker()) {
  active_metric_ =
      metric_group->AddGauge("admission-control-service.active", IsActive() ? 1 : 0);
  fenced_metric_ = metric_group->AddGauge("admission-control-service.fenced", 0);
  MemTracker* process_mem_tracker = AdmissiondEnv::GetInstance()->process_mem_tracker();
  bool is_percent; // not used
  int64_t bytes_limit =
      ParseUtil::ParseMemSpec(FLAGS_admission_control_service_queue_mem_limit,
          &is_percent, process_mem_tracker->limit());
  if (bytes_limit <= 0) {
    CLEAN_EXIT_WITH_ERROR(
        Substitute("Invalid mem limit for admission control service queue: "
                   "'$0'.",
            FLAGS_admission_control_service_queue_mem_limit));
  }
  mem_tracker_.reset(new MemTracker(
      bytes_limit, "Admission Control Service Queue", process_mem_tracker));
  MemTrackerMetric::CreateMetrics(
      metric_group, mem_tracker_.get(), "AdmissionControlService");
}

Status AdmissionControlService::Init() {
  int num_svc_threads = FLAGS_admission_control_service_num_svc_threads > 0 ?
      FLAGS_admission_control_service_num_svc_threads :
      CpuInfo::num_cores();
  // The maximum queue length is set to maximum 32-bit value. Its actual capacity is
  // bound by memory consumption against 'mem_tracker_'.
  RETURN_IF_ERROR(AdmissiondEnv::GetInstance()->rpc_mgr()->RegisterService(
      num_svc_threads, std::numeric_limits<int32_t>::max(), this, mem_tracker_.get(),
      AdmissiondEnv::GetInstance()->rpc_metrics()));

  admission_thread_pool_.reset(
      new ThreadPool<UniqueIdPB>("admission-control-service", "admission-worker",
          FLAGS_admission_thread_pool_size, FLAGS_max_admission_queue_size,
          bind<void>(&AdmissionControlService::AdmitFromThreadPool, this, _2)));
  ABORT_IF_ERROR(admission_thread_pool_->Init());

  RETURN_IF_ERROR(Thread::Create("admission-control-service",
      "admission-state-map-cleanup",
      &AdmissionControlService::AdmissionStateMapCleanupLoop, this, &cleanup_thread_));
  // With admissiond HA the loop also drops the adoption floor and tracks the fence.
  if (FLAGS_admission_coordinator_heartbeat_timeout_s > 0 || FLAGS_enable_admissiond_ha) {
    RETURN_IF_ERROR(Thread::Create("admission-control-service",
        "admission-coordinator-ageing", &AdmissionControlService::CoordinatorAgeingLoop,
        this, &ageing_thread_));
  }

  return Status::OK();
}

void AdmissionControlService::Join() {
  admission_thread_pool_->Join();
  shutdown_.store(true);
  {
    // Signal the cleanup thread to exit.
    std::lock_guard<std::mutex> l(cleanup_queue_lock_);
    cleanup_queue_cv_.notify_all();
  }
  DCHECK(cleanup_thread_ != nullptr);
  // Wait for the cleanup thread to finish clearing the queue.
  cleanup_thread_->Join();
  if (ageing_thread_ != nullptr) ageing_thread_->Join();
}

std::atomic<int64_t> AdmissionControlService::proxy_generation_{0};

Status AdmissionControlService::GetProxy(
    unique_ptr<AdmissionControlServiceProxy>* proxy, int64_t* generation) {
  NetworkAddressPB admission_service_address;
  string admission_service_hostname;
  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetAdmissionServiceAddress(
      admission_service_address, &admission_service_hostname));
  // Create a AdmissionControlService proxy to the destination.
  RETURN_IF_ERROR(ExecEnv::GetInstance()->rpc_mgr()->GetProxy(
      admission_service_address, admission_service_hostname,
      proxy));
  // KRPC shares one connection per remote address and network plane; a new plane name
  // gives a new connection.
  int64_t current = proxy_generation_.load();
  if (current > 0) (*proxy)->set_network_plane(Substitute("admission-$0", current));
  if (generation != nullptr) *generation = current;
  return Status::OK();
}

void AdmissionControlService::UseNewConnection(int64_t stale_generation) {
  int64_t expected = stale_generation;
  if (proxy_generation_.compare_exchange_strong(expected, stale_generation + 1)) {
    LOG(INFO) << "Admission service rpc timed out; using a new connection (generation "
              << stale_generation + 1 << ")";
  }
}

void AdmissionControlService::RecordFirstContact() {
  if (first_contact_ms_.load() != 0) return;
  int64_t expected = 0;
  first_contact_ms_.compare_exchange_strong(expected, MonotonicMillis());
}

bool AdmissionControlService::AllCoordinatorsReported() {
  ClusterMembershipMgr::SnapshotPtr snapshot =
      AdmissiondEnv::GetInstance()->cluster_membership_mgr()->GetSnapshot();
  if (snapshot == nullptr) return false;
  int num_coordinators = 0;
  lock_guard<mutex> l(heartbeat_lock_);
  for (const auto& entry : snapshot->current_backends) {
    if (!entry.second.is_coordinator()) continue;
    ++num_coordinators;
    auto it = coord_id_to_heartbeat_.find(entry.second.backend_id());
    if (it == coord_id_to_heartbeat_.end() || !it->second.reported) return false;
  }
  return num_coordinators > 0;
}

void AdmissionControlService::WaitForAdoptionGracePeriod() {
  if (adoption_grace_over_.load() || !FLAGS_admission_adopt_running_queries
      || FLAGS_admission_adoption_grace_period_ms <= 0) {
    return;
  }
  // A coordinator reports within one heartbeat rpc timeout plus two heartbeat periods:
  // a heartbeat in flight may time out, or carry no admitted queries until a response
  // asks for them, and the full report follows one period later. With admissiond HA,
  // cover a heartbeat that is stuck on the previous admissiond plus the next one, so
  // that no coordinator is skipped because of a lost admissiond.
  int64_t grace_period_ms = max<int64_t>(FLAGS_admission_adoption_grace_period_ms,
      2 * (FLAGS_admission_heartbeat_rpc_timeout_ms
          + FLAGS_admission_heartbeat_frequency_ms));
  int64_t start = MonotonicMillis();
  while (!shutdown_.load()) {
    int64_t first = first_contact_ms_.load();
    bool timed_out = first > 0 && MonotonicMillis() >= first + grace_period_ms;
    bool all_reported = AllCoordinatorsReported();
    if (all_reported) MaybeDropAdoptionFloor(0);
    if (timed_out || all_reported) {
      if (!adoption_grace_over_.exchange(true)) {
        LOG(INFO) << "Admitting queries: "
                  << (timed_out ? "adoption grace period over" :
                                  "all coordinators reported their running queries")
                  << " (waited " << MonotonicMillis() - start << " ms)";
      }
      return;
    }
    SleepForMs(50);
  }
}

void AdmissionControlService::AdmitQuery(
    const AdmitQueryRequestPB* req, AdmitQueryResponsePB* resp, RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(1) << "AdmitQuery: query_id=" << req->query_id()
          << " coordinator=" << req->coord_id();
  RecordFirstContact();

  shared_ptr<AdmissionState> admission_state;
  admission_state = make_shared<AdmissionState>(req->query_id(), req->coord_id());

  admission_state->summary_profile =
      RuntimeProfile::Create(&admission_state->profile_pool, "Summary");

  RESPOND_IF_ERROR(GetSidecar(req->query_exec_request_sidecar_idx(), rpc_context,
      &admission_state->query_exec_request));

  for (const NetworkAddressPB& address : req->blacklisted_executor_addresses()) {
    admission_state->blacklisted_executor_addresses.emplace(address);
  }

  Status add_status = admission_state_map_.Add(req->query_id(), admission_state);
  if (add_status.ok()) {
    admission_thread_pool_->Offer(req->query_id());
  } else {
    LOG(INFO) << "Query " << req->query_id()
              << " was already submitted for admission, ignoring.";
  }
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::GetQueryStatus(const GetQueryStatusRequestPB* req,
    GetQueryStatusResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(2) << "GetQueryStatus " << req->query_id();

  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  Status status = Status::OK();
  {
    lock_guard<mutex> l(admission_state->lock);
    if (admission_state->submitted) {
      if (!admission_state->admission_done) {
        bool timed_out;
        int64_t wait_start_time_ms, wait_end_time_ms;
        admission_state->admit_status =
            AdmissiondEnv::GetInstance()->admission_controller()->WaitOnQueued(
                req->query_id(), &admission_state->schedule,
                FLAGS_admission_status_wait_time_ms, &timed_out,
                &wait_start_time_ms, &wait_end_time_ms);
        resp->set_wait_start_time_ms(wait_start_time_ms);
        resp->set_wait_end_time_ms(wait_end_time_ms);
        if (!timed_out) {
          admission_state->admission_done = true;
          if (admission_state->admit_status.ok()) {
            for (const auto& entry : admission_state->schedule->backend_exec_params()) {
              admission_state->unreleased_backends.emplace(entry.address());
            }
          }
        } else {
          DCHECK(admission_state->admit_status.ok());
        }
      }

      if (admission_state->admission_done) {
        if (admission_state->admit_status.ok() && admission_state->schedule == nullptr) {
          // An adopted query: its coordinator already has the schedule and does not ask
          // for it again.
          DCHECK(admission_state->adopted);
          status = Status(Substitute("Query $0 was admitted by another admissiond; its "
              "schedule is not available here.", PrintId(req->query_id())));
        } else if (admission_state->admit_status.ok()) {
          *resp->mutable_query_schedule() = *admission_state->schedule.get();
        } else {
          status = admission_state->admit_status;
        }
      }

      // Always send the profile even if admission isn't done yet.
      TRuntimeProfileTree tree;
      admission_state->summary_profile->ToThrift(&tree);
      int sidecar_idx;
      Status sidecar_status = SetFaststringSidecar(tree, rpc_context, &sidecar_idx);
      if (!sidecar_status.ok()) {
        // We don't need to fail the query just because we can't return the profile, so
        // just log the error.
        LOG(WARNING) << "Failed to set profile sidecar in GetQueryStatus: "
                     << sidecar_status;
      } else {
        resp->set_summary_profile_sidecar_idx(sidecar_idx);
      }
    }
  }

  RespondAndReleaseRpc(status, resp, rpc_context);
  if (admission_state->admission_done && !admission_state->admit_status.ok()) {
    LOG(INFO) << "Query " << req->query_id()
              << " was rejected. Removing admission state to free resources.";
    // If this RPC fails and the admission state is already removed,
    // a retry may fail with an "Invalid handle" error because the entry is gone.
    // This is okay and doesn't cause any real problem.
    // To make it more robust, we may delay the removal using a time-based approach.
    discard_result(admission_state_map_.Delete(req->query_id()));
    VLOG(3) << "Current admission state map size: " << admission_state_map_.Count();
  }
}

void AdmissionControlService::ReleaseQuery(const ReleaseQueryRequestPB* req,
    ReleaseQueryResponsePB* resp, RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(1) << "ReleaseQuery: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  bool already_released;
  {
    lock_guard<mutex> l(admission_state->lock);
    already_released = admission_state->released;
    if (!admission_state->released) {
      AdmissiondEnv::GetInstance()->admission_controller()->ReleaseQuery(req->query_id(),
          admission_state->coord_id, req->peak_mem_consumption(),
          /* release_remaining_backends */ true);
      admission_state->released = true;
    } else {
      LOG(WARNING) << "Query " << req->query_id() << " was already released.";
    }
  }

  if (already_released) {
    // E.g. released from a heartbeat that reported it as released (adopted queries); the
    // state may already be gone.
    discard_result(admission_state_map_.Delete(req->query_id()));
  } else {
    RESPOND_IF_ERROR(admission_state_map_.Delete(req->query_id()));
  }
  if (admission_state->adopted) ForgetAdoptedQuery(req->query_id());
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::ReleaseQueryBackends(
    const ReleaseQueryBackendsRequestPB* req, ReleaseQueryBackendsResponsePB* resp,
    RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(2) << "ReleaseQueryBackends: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));

  {
    lock_guard<mutex> l(admission_state->lock);
    vector<NetworkAddressPB> host_addrs;
    for (const NetworkAddressPB& host_addr : req->host_addr()) {
      auto it = admission_state->unreleased_backends.find(host_addr);
      if (it == admission_state->unreleased_backends.end()) {
        string err = Substitute("Backend $0 was already released for $1",
            NetworkAddressPBToString(host_addr), PrintId(req->query_id()));
        LOG(WARNING) << err;
        RespondAndReleaseRpc(Status(err), resp, rpc_context);
        return;
      }
      host_addrs.push_back(host_addr);
      admission_state->unreleased_backends.erase(it);
    }

    AdmissiondEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
        req->query_id(), admission_state->coord_id, host_addrs);
  }

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::CancelAdmission(const CancelAdmissionRequestPB* req,
    CancelAdmissionResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(1) << "CancelAdmission: query_id=" << req->query_id();
  shared_ptr<AdmissionState> admission_state;
  RESPOND_IF_ERROR(admission_state_map_.Get(req->query_id(), &admission_state));
  admission_state->admit_outcome.Set(AdmissionOutcome::CANCELLED);
  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

void AdmissionControlService::AdmissionHeartbeat(const AdmissionHeartbeatRequestPB* req,
    AdmissionHeartbeatResponsePB* resp, kudu::rpc::RpcContext* rpc_context) {
  if (RejectIfNotActive(rpc_context)) return;
  VLOG(2) << "AdmissionHeartbeat: host_id=" << req->host_id();
  RecordFirstContact();

  if(!CheckAndUpdateHeartbeat(req->host_id(), req->version(), MonotonicMillis())) {
    VLOG(1) << "Stale heartbeat received for coord_id: "<< req->host_id();
    resp->set_report_admitted_queries(
        NeedsAdmittedQueries(req->host_id(), IsInMembership(req->host_id())));
    RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
    return;
  }
  bool in_membership = false;
  if (FLAGS_admission_adopt_running_queries) {
    in_membership = AdoptOrReleaseReportedQueries(*req);
  }
  std::unordered_set<UniqueIdPB> query_ids;
  for (const UniqueIdPB& query_id : req->query_ids()) {
    query_ids.insert(query_id);
  }
  vector<UniqueIdPB> cleaned_up =
      AdmissiondEnv::GetInstance()->admission_controller()->CleanupQueriesForHost(
          req->host_id(), query_ids);

  for (const UniqueIdPB& query_id : cleaned_up) {
    // ShardedQueryMap::Delete will log an error already if anything goes wrong, so just
    // ignore the return value.
    discard_result(admission_state_map_.Delete(query_id));
    ForgetAdoptedQuery(query_id);
  }
  if (in_membership && req->all_admitted_queries()) {
    lock_guard<mutex> l(heartbeat_lock_);
    coord_id_to_heartbeat_[req->host_id()].reported = true;
  }
  resp->set_report_admitted_queries(NeedsAdmittedQueries(req->host_id(), in_membership));

  RespondAndReleaseRpc(Status::OK(), resp, rpc_context);
}

bool AdmissionControlService::NeedsAdmittedQueries(
    const UniqueIdPB& coord_id, bool in_membership) {
  if (!FLAGS_admission_adopt_running_queries) return false;
  // Queries of a coordinator outside the membership are not adopted. Ask for them
  // anyway until admission starts, so that a coordinator that joins the membership
  // meanwhile has sent its report by then, but not later: a coordinator that never
  // joins would otherwise send its full report with every heartbeat.
  if (!in_membership && adoption_grace_over_.load()) return false;
  lock_guard<mutex> l(heartbeat_lock_);
  auto it = coord_id_to_heartbeat_.find(coord_id);
  return it == coord_id_to_heartbeat_.end() || !it->second.reported;
}

bool AdmissionControlService::IsInMembership(const UniqueIdPB& coord_id) {
  ClusterMembershipMgr::SnapshotPtr snapshot =
      AdmissiondEnv::GetInstance()->cluster_membership_mgr()->GetSnapshot();
  return snapshot != nullptr
      && snapshot->current_backends.find(PrintId(coord_id))
          != snapshot->current_backends.end();
}

bool AdmissionControlService::AdoptOrReleaseReportedQueries(
    const AdmissionHeartbeatRequestPB& req) {
  const UniqueIdPB& coord_id = req.host_id();
  AdmissionController* admission_controller =
      AdmissiondEnv::GetInstance()->admission_controller();
  bool in_membership = IsInMembership(coord_id);
  // Pool configs are looked up through JNI; fetch each at most once per heartbeat.
  RequestPoolService* request_pool_service =
      AdmissiondEnv::GetInstance()->request_pool_service();
  std::unordered_map<string, TPoolConfig> pool_configs;
  auto get_pool_config = [&](const string& pool, const TPoolConfig** cfg) -> Status {
    auto it = pool_configs.find(pool);
    if (it == pool_configs.end()) {
      TPoolConfig loaded;
      RETURN_IF_ERROR(request_pool_service->GetPoolConfig(pool, &loaded));
      it = pool_configs.emplace(pool, std::move(loaded)).first;
    }
    *cfg = &it->second;
    return Status::OK();
  };

  for (const AdmittedQueryPB& admitted_query : req.admitted_queries()) {
    const UniqueIdPB& query_id = admitted_query.query_id();
    bool was_adopted;
    {
      lock_guard<mutex> l(adopted_lock_);
      was_adopted = adopted_query_ids_.find(query_id) != adopted_query_ids_.end();
    }
    if (admitted_query.released()) {
      // An adoption from a heartbeat that was sent before the coordinator released the
      // query; the release itself did not find the query here. Undo it.
      if (!was_adopted) continue;
      shared_ptr<AdmissionState> admission_state;
      if (admission_state_map_.Get(query_id, &admission_state).ok()) {
        bool release = false;
        {
          lock_guard<mutex> l(admission_state->lock);
          if (!admission_state->released) {
            admission_state->released = true;
            release = true;
          }
        }
        if (release) {
          LOG(INFO) << "Releasing adopted query " << PrintId(query_id)
                    << " that its coordinator " << PrintId(coord_id)
                    << " reports as released.";
          admission_controller->ReleaseQuery(query_id, coord_id, -1,
              /* release_remaining_backends */ true);
        }
        discard_result(admission_state_map_.Delete(query_id));
      }
      ForgetAdoptedQuery(query_id);
      continue;
    }
    if (was_adopted || !in_membership) continue;
    shared_ptr<AdmissionState> existing;
    if (admission_state_map_.Get(query_id, &existing).ok()) continue;

    bool adopted = false;
    const TPoolConfig* pool_cfg = nullptr;
    const TPoolConfig* root_cfg = nullptr;
    Status status = admitted_query.request_pool().empty() ?
        Status("no request pool") :
        get_pool_config(admitted_query.request_pool(), &pool_cfg);
    if (status.ok()) status = get_pool_config("root", &root_cfg);
    if (status.ok()) {
      // A ReleaseQueryBackends rpc arriving between this call and the insertion into
      // 'admission_state_map_' below gets INVALID_QUERY_HANDLE; its backends stay
      // accounted for until ReleaseQuery (release_remaining_backends).
      status = admission_controller->AdoptRunningQuery(
          coord_id, admitted_query, *pool_cfg, *root_cfg, &adopted);
    }
    if (!status.ok()) {
      LOG(WARNING) << "Could not adopt query " << PrintId(query_id) << " of coordinator "
                   << PrintId(coord_id) << ": " << status.GetDetail();
      continue;
    }
    if (!adopted) continue;
    shared_ptr<AdmissionState> admission_state =
        make_shared<AdmissionState>(query_id, coord_id);
    admission_state->summary_profile =
        RuntimeProfile::Create(&admission_state->profile_pool, "Summary");
    admission_state->submitted = true;
    admission_state->admission_done = true;
    admission_state->adopted = true;
    admission_state->request_pool = admitted_query.request_pool();
    for (const BackendAllocationPB& backend : admitted_query.unreleased_backends()) {
      admission_state->unreleased_backends.emplace(backend.address());
    }
    {
      // Hold 'adopted_lock_' so that a concurrent heartbeat does not see the query as
      // unknown between the two insertions.
      lock_guard<mutex> l(adopted_lock_);
      if (!admission_state_map_.Add(query_id, admission_state).ok()) {
        // An AdmitQuery for the same id arrived meanwhile. The coordinator only reports
        // queries it holds a schedule for, so this is not expected; undo the adoption.
        LOG(WARNING) << "Query " << PrintId(query_id)
                     << " was submitted while being adopted, releasing the adoption.";
        admission_controller->ReleaseQuery(query_id, coord_id, -1,
            /* release_remaining_backends */ true);
        continue;
      }
      adopted_query_ids_.insert(query_id);
    }
  }
  return in_membership;
}

void AdmissionControlService::ForgetAdoptedQuery(const UniqueIdPB& query_id) {
  lock_guard<mutex> l(adopted_lock_);
  adopted_query_ids_.erase(query_id);
}

void AdmissionControlService::CoordinatorAgeingLoop() {
  int64_t min_timeout_ms = 3
      * (FLAGS_admission_heartbeat_rpc_timeout_ms
          + FLAGS_admission_heartbeat_frequency_ms);
  int64_t timeout_ms = max<int64_t>(
      FLAGS_admission_coordinator_heartbeat_timeout_s * MILLIS_PER_SEC, min_timeout_ms);
  bool ageing = FLAGS_admission_coordinator_heartbeat_timeout_s > 0;
  if (ageing
      && timeout_ms > FLAGS_admission_coordinator_heartbeat_timeout_s * MILLIS_PER_SEC) {
    LOG(WARNING) << "--admission_coordinator_heartbeat_timeout_s raised to "
                 << timeout_ms / MILLIS_PER_SEC
                 << " s (3 x heartbeat rpc timeout + period)";
  }
  AdmissionController* admission_controller =
      AdmissiondEnv::GetInstance()->admission_controller();
  while (!shutdown_.load()) {
    SleepForMs(MILLIS_PER_SEC);
    bool fenced = IsActive() && IsFenced();
    // While fenced, coordinators cannot report, so the floor does not expire then.
    MaybeDropAdoptionFloor(fenced ? 0 : timeout_ms);
    if (fenced != (fenced_metric_->GetValue() == 1)) {
      LOG(WARNING) << (fenced ? "Fenced: no heartbeat from the active statestore for "
                                "more than --admissiond_ha_statestore_lease_ms, not "
                                "admitting queries until heartbeats resume." :
                                (IsActive() ? "Not fenced any more: statestore "
                                              "heartbeats resumed." :
                                              "Not fenced any more: no longer active."));
      fenced_metric_->SetValue(fenced ? 1 : 0);
      active_metric_->SetValue(IsActive() && !fenced ? 1 : 0);
      if (!fenced) {
        // Heartbeats were rejected while fenced; that silence is not the coordinators'.
        // The floor gets a full timeout again for their reports.
        int64_t now = MonotonicMillis();
        if (floor_active_.load()) promoted_ms_.store(now);
        lock_guard<mutex> l(heartbeat_lock_);
        for (auto& entry : coord_id_to_heartbeat_) entry.second.last_seen_ms = now;
      }
    }
    // A fenced admissiond rejects the coordinators' heartbeats, so it does not age them.
    if (!ageing || fenced) continue;
    vector<UniqueIdPB> coord_ids =
        admission_controller->GetCoordinatorsWithRunningQueries();
    int64_t now = MonotonicMillis();
    vector<UniqueIdPB> silent;
    {
      lock_guard<mutex> l(heartbeat_lock_);
      for (const UniqueIdPB& coord_id : coord_ids) {
        CoordinatorHeartbeat& heartbeat = coord_id_to_heartbeat_[coord_id];
        // A coordinator seen for the first time (e.g. a query admitted before its first
        // heartbeat) gets a full timeout from now.
        if (heartbeat.last_seen_ms == 0) heartbeat.last_seen_ms = now;
        if (now - heartbeat.last_seen_ms > timeout_ms) silent.push_back(coord_id);
      }
    }
    for (const UniqueIdPB& coord_id : silent) {
      vector<UniqueIdPB> released =
          admission_controller->ReleaseRunningQueriesForHost(coord_id);
      if (released.empty()) continue;
      LOG(WARNING) << "Coordinator " << PrintId(coord_id) << " sent no admission "
                   << "heartbeat for " << timeout_ms / MILLIS_PER_SEC << " s, released "
                   << released.size() << " running queries.";
      for (const UniqueIdPB& query_id : released) {
        discard_result(admission_state_map_.Delete(query_id));
        ForgetAdoptedQuery(query_id);
      }
      // When it is heard again, it must report its queries again to have them adopted.
      lock_guard<mutex> l(heartbeat_lock_);
      coord_id_to_heartbeat_[coord_id].reported = false;
    }
  }
}

void AdmissionControlService::CancelQueriesOnFailedCoordinators(
    const std::unordered_set<UniqueIdPB>& current_backends) {
  std::unordered_map<UniqueIdPB, vector<UniqueIdPB>> cleaned_up =
      AdmissiondEnv::GetInstance()
          ->admission_controller()
          ->CancelQueriesOnFailedCoordinators(current_backends);

  for (const auto& entry : cleaned_up) {
    for (const UniqueIdPB& query_id : entry.second) {
      // ShardedQueryMap::Delete will log an error already if anything goes wrong, so just
      // ignore the return value.
      discard_result(admission_state_map_.Delete(query_id));
      ForgetAdoptedQuery(query_id);
    }
  }
  // Their running queries were released: a coordinator that comes back (e.g. after a
  // transient membership drop) must report them again to have them adopted.
  lock_guard<mutex> l(heartbeat_lock_);
  for (auto& entry : coord_id_to_heartbeat_) {
    if (current_backends.find(entry.first) == current_backends.end()) {
      entry.second.reported = false;
    }
  }
}

void AdmissionControlService::AdmitFromThreadPool(const UniqueIdPB& query_id) {
  WaitForAdoptionGracePeriod();
  shared_ptr<AdmissionState> admission_state;
  Status s = admission_state_map_.Get(query_id, &admission_state);
  if (!s.ok()) {
    LOG(ERROR) << s;
    return;
  }

  {
    lock_guard<mutex> l(admission_state->lock);
    // Demoted meanwhile: the coordinator gets rejected and resubmits to the active one.
    // Drop the state so that a later resubmission of the query is not ignored.
    if (!IsActive()) {
      discard_result(admission_state_map_.Delete(query_id));
      return;
    }
    bool queued;
    AdmissionController::AdmissionRequest request = {admission_state->query_id,
        admission_state->coord_id, admission_state->query_exec_request,
        admission_state->query_exec_request.query_ctx.client_request.query_options,
        admission_state->summary_profile,
        admission_state->blacklisted_executor_addresses};
    admission_state->admit_status =
        AdmissiondEnv::GetInstance()->admission_controller()->SubmitForAdmission(request,
            &admission_state->admit_outcome, &admission_state->schedule, queued,
            &admission_state->request_pool);
    admission_state->submitted = true;
    if (!queued) {
      admission_state->admission_done = true;
      if (admission_state->admit_status.ok()) {
        for (const auto& entry : admission_state->schedule->backend_exec_params()) {
          admission_state->unreleased_backends.emplace(entry.address());
        }
      }
    } else {
      DCHECK(admission_state->admit_status.ok());
    }
  }
}

bool AdmissionControlService::IsFenced() const {
  if (!FLAGS_enable_admissiond_ha || FLAGS_admissiond_ha_statestore_lease_ms <= 0) {
    return false;
  }
  return AdmissiondEnv::GetInstance()
             ->subscriber()
             ->MilliSecondsSinceActiveStatestoreHeartbeat()
      > FLAGS_admissiond_ha_statestore_lease_ms;
}

bool AdmissionControlService::RejectIfNotActive(RpcContext* rpc_context) {
  bool active = IsActive();
  if (LIKELY(active && !IsFenced())) return false;
  mem_tracker_->Release(rpc_context->GetTransferSize());
  rpc_context->RespondRpcFailure(kudu::rpc::ErrorStatusPB::ERROR_UNAVAILABLE,
      kudu::Status::ServiceUnavailable(Substitute("admissiond $0:$1 $2$3", FLAGS_hostname,
          FLAGS_admission_service_port, NOT_ACTIVE_MSG,
          active ? " (fenced: no heartbeat from the active statestore)" : "")));
  return true;
}

void AdmissionControlService::MaybeDropAdoptionFloor(int64_t timeout_ms) {
  if (!floor_active_.load()) return;
  bool all_reported = AllCoordinatorsReported();
  bool expired =
      timeout_ms > 0 && MonotonicMillis() - promoted_ms_.load() >= timeout_ms;
  if (!all_reported && !expired) return;
  // Serialized with Promote() and Demote(), which also set the floor and the retain flag.
  lock_guard<mutex> l(role_lock_);
  if (!IsActive() || !floor_active_.exchange(false)) return;
  AdmissionController* admission_controller =
      AdmissiondEnv::GetInstance()->admission_controller();
  // Stats deleted from now on belong to no failover of this promotion.
  admission_controller->SetRetainRemoteStats(false);
  bool dropped = admission_controller->DropAdoptionFloor();
  LOG(INFO) << "Adoption floor " << (dropped ? "dropped" : "empty") << ": "
            << (all_reported ? "all coordinators reported their running queries" :
                               "coordinator heartbeat timeout since the promotion");
}

void AdmissionControlService::UpdateActiveAdmissiond(bool reset_version,
    int64_t active_admissiond_version,
    const TAdmissiondRegistration& admissiond_registration) {
  lock_guard<mutex> l(role_lock_);
  if (!active_admissiond_version_checker_->CheckActiveCatalogdVersion(
          reset_version, active_admissiond_version)) {
    return;
  }
  bool is_this = admissiond_registration.address.hostname == FLAGS_hostname
      && admissiond_registration.address.port == FLAGS_admission_service_port;
  LOG(INFO) << "Active admissiond is "
            << TNetworkAddressToString(admissiond_registration.address)
            << " (version " << active_admissiond_version << ")"
            << (is_this ? ", this instance." : ".");
  if (is_this && !IsActive()) {
    Promote();
  } else if (!is_this && IsActive()) {
    Demote();
  }
}

void AdmissionControlService::Promote() {
  // Re-arm the adoption gate before admitting anything.
  {
    // Heartbeat state is from an earlier active period, if any.
    int64_t now = MonotonicMillis();
    lock_guard<mutex> l(heartbeat_lock_);
    for (auto& entry : coord_id_to_heartbeat_) {
      entry.second.reported = false;
      entry.second.last_seen_ms = now;
    }
  }
  // The grace period starts with the promotion: coordinators learn about it at the same
  // time and report within a heartbeat.
  first_contact_ms_.store(MonotonicMillis());
  adoption_grace_over_.store(false);
  // Keep counting the queries of the previous active admissiond (its last published
  // stats) until their coordinators report them.
  promoted_ms_.store(MonotonicMillis());
  AdmissiondEnv::GetInstance()->admission_controller()->StartAdoptionFloor();
  floor_active_.store(true);
  is_active_.store(true);
  active_metric_->SetValue(1);
  LOG(INFO) << "This admissiond is now the active admissiond.";
}

void AdmissionControlService::Demote() {
  is_active_.store(false);
  active_metric_->SetValue(0);
  AdmissionController* admission_controller =
      AdmissiondEnv::GetInstance()->admission_controller();
  vector<shared_ptr<AdmissionState>> states;
  admission_state_map_.DoFuncForAllEntries(
      [&](const shared_ptr<AdmissionState>& state) { states.push_back(state); });
  int num_cancelled = 0;
  for (const shared_ptr<AdmissionState>& state : states) {
    lock_guard<mutex> l(state->lock);
    if (!state->submitted || state->admission_done) continue;
    // Take the query off the queue: WaitOnQueued() removes a cancelled queue node and
    // updates the pool stats.
    state->admit_outcome.Set(AdmissionOutcome::CANCELLED);
    state->admit_status =
        admission_controller->WaitOnQueued(state->query_id, &state->schedule);
    state->admission_done = true;
    ++num_cancelled;
  }
  int num_released = 0;
  for (const UniqueIdPB& coord_id :
      admission_controller->GetCoordinatorsWithRunningQueries()) {
    num_released += admission_controller->ReleaseRunningQueriesForHost(coord_id).size();
  }
  for (const shared_ptr<AdmissionState>& state : states) {
    discard_result(admission_state_map_.Delete(state->query_id));
  }
  {
    lock_guard<mutex> l(adopted_lock_);
    adopted_query_ids_.clear();
  }
  floor_active_.store(false);
  admission_controller->DropAdoptionFloor();
  // As a standby, keep the stats of the active admissiond when it fails.
  admission_controller->SetRetainRemoteStats(true);
  LOG(INFO) << "This admissiond is now a standby admissiond: cancelled " << num_cancelled
            << " queued and released " << num_released << " running queries.";
}

template <typename ResponsePBType>
void AdmissionControlService::RespondAndReleaseRpc(
    const Status& status, ResponsePBType* response, RpcContext* rpc_context) {
  status.ToProto(response->mutable_status());
  // Release the memory against the control service's memory tracker.
  mem_tracker_->Release(rpc_context->GetTransferSize());
  rpc_context->RespondSuccess();
}

bool AdmissionControlService::CheckAndUpdateHeartbeat(
    const UniqueIdPB& coord_id, int64_t update_version, int64_t now_ms) {
  lock_guard<mutex> l(heartbeat_lock_);
  CoordinatorHeartbeat& heartbeat = coord_id_to_heartbeat_[coord_id];
  heartbeat.last_seen_ms = now_ms;
  if (heartbeat.version < update_version) {
    heartbeat.version = update_version;
    return true;
  }
  return false;
}

void AdmissionControlService::CleanupAdmissionStateMapAsync(const UniqueIdPB& query_id) {
  {
    std::lock_guard<std::mutex> lock(cleanup_queue_lock_);
    admission_state_cleanup_queue_.push_back(query_id);
  }
  cleanup_queue_cv_.notify_all();
}

void AdmissionControlService::AdmissionStateMapCleanupLoop() {
  std::unique_lock<std::mutex> lock(cleanup_queue_lock_);
  while (true) {
    cleanup_queue_cv_.wait(lock,
        [&] { return !admission_state_cleanup_queue_.empty() || shutdown_.load(); });
    if (admission_state_cleanup_queue_.empty() && shutdown_.load()) return;
    std::deque<UniqueIdPB> local_queue;
    std::swap(local_queue, admission_state_cleanup_queue_);
    lock.unlock();
    for (const UniqueIdPB& query_id : local_queue) {
      discard_result(admission_state_map_.Delete(query_id));
      VLOG_QUERY << "Cleaned up admission state map for query=" << PrintId(query_id);
    }
    lock.lock();
  }
}

} // namespace impala
