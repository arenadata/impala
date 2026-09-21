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

#include <memory>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/common.pb.h"
#include "scheduling/admission-control-client.h"
#include "scheduling/admission-controller.h"

namespace kudu {
class Status;
}

namespace impala {

class AdmissionControlServiceProxy;

/// Implementation of AdmissionControlClient used to submit queries for admission to an
/// AdmissionController running remotely in an admissiond.
///
/// Handles retrying of rpcs for fault tolerance:
/// - For the AdmitQuery() rpc, retries with jitter and backoff for a configurable amount
///   of time, then fails the query if unsuccessful. The default retry time was chosen as
///   a larger value (60 seconds) to minimize the number of failed queries when the
///   admissiond is restarted or temporarily unavailable.
/// - For the ReleaseQuery(), ReleaseQueryBackends(), and CancelAdmission() rpcs, retries
///   just 3 times before giving up. Failures of these rpcs are not considered to fail the
///   overall query, and there are other mechanisms in place to ensure resources are
///   eventually released regardless of failures of these rpcs, eg. AdmissionHeartbeat.
class RemoteAdmissionControlClient : public AdmissionControlClient {
 public:
  RemoteAdmissionControlClient(const TQueryCtx& query_ctx);

  virtual Status SubmitForAdmission(const AdmissionController::AdmissionRequest& request,
      RuntimeProfile::EventSequence* query_events,
      std::unique_ptr<QuerySchedulePB>* schedule_result,
      int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) override;
  virtual void ReleaseQuery(int64_t peak_mem_consumption) override;
  virtual void ReleaseQueryBackends(
      const std::vector<NetworkAddressPB>& host_addr) override;
  virtual void CancelAdmission() override;

  bool GetAdmittedQuery(bool all, AdmittedQueryPB* admitted_query) override;

 private:
  // Owned by the ClientRequestState.
  const TQueryCtx& query_ctx_;

  // The id of the query being considered for admission.
  UniqueIdPB query_id_;

  /// Protects 'pending_admit_' and 'cancelled_'.
  std::mutex lock_;

  /// If true, the AdmitQuery rpc has been sent but a final admission decision has not yet
  /// been recieved by GetQueryStatus().
  bool pending_admit_ = false;

  /// If true, CancelAdmission() was called. If SubmitForAdmission() is called
  /// subsequently, it will not send the AdmitQuery rpc
  bool cancelled_ = false;

  /// Protects 'admitted_' and 'admitted_query_'.
  std::mutex admitted_lock_;

  /// True once the query was admitted. 'admitted_query_' is then valid.
  bool admitted_ = false;

  /// What the admission control service needs to re-register this query as running
  /// after it lost its state, see AdmittedQueryPB. Backends are removed as they are
  /// released; 'released' is set by ReleaseQuery(). Reported in admission heartbeats.
  AdmittedQueryPB admitted_query_;

  /// Constants related to retrying the idempotent rpcs.
  static const int RPC_NUM_RETRIES = 3;
  static const int64_t RPC_TIMEOUT_MS = 10 * MILLIS_PER_SEC;
  static const int64_t RPC_BACKOFF_TIME_MS = 3 * MILLIS_PER_SEC;

  /// Timeout of the AdmitQuery rpc, whose sidecar carries the query plan.
  static const int64_t ADMIT_QUERY_RPC_TIMEOUT_MS = 30 * MILLIS_PER_SEC;

  /// Connection generation of the proxy used by SubmitForAdmission(), see
  /// AdmissionControlService::UseNewConnection().
  int64_t proxy_generation_ = 0;

  /// Maximum number of times a queued query is resubmitted after its admission state was
  /// lost, see --admission_resubmit_on_admissiond_loss.
  static const int MAX_ADMISSION_RESUBMITS = 5;

  /// Checks if admission has already been cancelled, and if not sends the AdmitQuery rpc.
  /// Sets 'rpc_status' to the return Status from the rpc layer, and returns OK if the
  /// query was successfully submitted for admission.
  /// Sends a release or cancel rpc to the active admissiond with
  /// RpcMgr::DoRpcWithRetry(). If a standby admissiond rejected it or the active
  /// admissiond changed meanwhile (admissiond HA), resends it to the active one.
  template <typename ProxyMethod, typename Request, typename Response>
  Status DoRpcOnActiveAdmissiond(const ProxyMethod& rpc_call, const Request& request,
      Response* response, const char* error_msg, const char* debug_action);

  Status TryAdmitQuery(AdmissionControlServiceProxy* proxy,
      const TQueryExecRequest& request, AdmitQueryRequestPB* req,
      kudu::Status* rpc_status);

  /// Sends the AdmitQuery rpc, retrying it on network errors and timeouts for up to
  /// --admission_max_retry_time_s from the time of the call. '*proxy' is replaced on
  /// each retry, and a timed out rpc also switches to a new connection, see
  /// AdmissionControlService::UseNewConnection().
  Status AdmitQueryWithRetry(std::unique_ptr<AdmissionControlServiceProxy>* proxy,
      const AdmissionController::AdmissionRequest& request, AdmitQueryRequestPB* req);

  /// Records the admitted 'schedule' in 'admitted_query_'.
  void RecordAdmission(const QuerySchedulePB& schedule);
};

} // namespace impala
