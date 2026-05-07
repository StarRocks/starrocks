// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/lake/external_cluster_snapshot_task_rpc.h"

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "agent/finish_task.h"
#include "base/utility/defer_op.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "glog/logging.h"
#include "storage/lake/external_cluster_snapshot_task_helper.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

constexpr int64_t kSnapshotRpcTimeoutMs = 60 * 1000; // 1 minute

void ClusterSnapshotRpcCtx::add_rpc_context(std::unique_ptr<BackendSnapshotRpcCtx> rpc_ctx) {
    std::lock_guard<bthread::Mutex> l(mutex);
    all_rpc_ctxs.emplace_back(std::move(rpc_ctx));
}

void ClusterSnapshotRpcCtx::wait() {
    latch->wait();
}

void ClusterSnapshotRpcCtx::handle_failure(const std::string& error_msg, const std::vector<int64_t>& tids) {
    std::lock_guard<bthread::Mutex> l(mutex);
    auto& error_tablet_ids = finish_task_req->error_tablet_ids;
    for (int64_t tid : tids) {
        error_tablet_ids.push_back(tid);
    }
    if (finish_task_req->task_status.status_code == 0) {
        TStatus task_status;
        task_status.__set_status_code(TStatusCode::RUNTIME_ERROR);
        task_status.__set_error_msgs(std::vector<std::string>{error_msg});
        finish_task_req->__set_task_status(task_status);
    }
    if (final_status.ok()) {
        final_status = Status::InternalError(error_msg);
    }
}

bool ClusterSnapshotRpcCtx::has_failure() {
    std::lock_guard<bthread::Mutex> l(mutex);
    return !final_status.ok();
}

void ClusterSnapshotRpcCtx::count_down() {
    latch->count_down();
}

void cluster_snapshot_rpc_cb(brpc::Controller* cntl, UploadSnapshotFilesResponsePB* resp,
                             BackendSnapshotRpcCtx* rpc_ctx) {
    DeferOp defer([rpc_ctx]() { rpc_ctx->global_rpc_ctx->count_down(); });

    if (cntl->Failed()) {
        std::string error_msg = fmt::format("data_snapshot rpc failed, error={}", cntl->ErrorText());
        std::vector<int64_t> failed_tids;
        for (auto& tablet_snapshot : rpc_ctx->request.tablet_snapshots()) {
            failed_tids.push_back(tablet_snapshot.tablet_id());
        }
        rpc_ctx->global_rpc_ctx->handle_failure(error_msg, failed_tids);

    } else if (resp->status().status_code() != 0) {
        std::string node_error_msg = resp->status().error_msgs().size() > 0 ? resp->status().error_msgs(0) : "";
        std::string error_msg = fmt::format("data_snapshot rpc failed, error={}", node_error_msg);
        std::vector<int64_t> failed_tids;
        for (auto& tablet_id : resp->failed_tablets()) {
            failed_tids.push_back(tablet_id);
        }
        rpc_ctx->global_rpc_ctx->handle_failure(error_msg, failed_tids);
    }
    // snapshot success
    // do nothing
}

void send_snapshot_rpc_to_backend(const TBackend& backend, const std::vector<int64_t>& tablet_ids,
                                  const UploadSnapshotFilesRequestPB& node_req, ClusterSnapshotRpcCtx& rpc_ctx) {
    // Get BRPC stub for backend
    auto stub_result = LakeServiceBrpcStubCache::getInstance()->get_stub(backend.host, backend.be_port);
    if (!stub_result.ok()) {
        LOG(ERROR) << "get stub failed, backend: " << backend.host << ":" << backend.be_port
                   << ", status: " << stub_result.status().to_string();
        rpc_ctx.handle_failure(stub_result.status().to_string(), tablet_ids);
        rpc_ctx.count_down();
        return;
    }
    auto stub = std::move(stub_result.value());

    // Prepare RPC context and controller
    auto node_cntl = std::make_unique<brpc::Controller>();
    auto node_resp = std::make_unique<UploadSnapshotFilesResponsePB>();
    node_cntl->set_timeout_ms(kSnapshotRpcTimeoutMs);

    auto rpc_ctx_ptr = std::make_unique<BackendSnapshotRpcCtx>(UploadSnapshotFilesRequestPB(node_req), // Copy request
                                                               std::move(node_cntl), std::move(node_resp), &rpc_ctx);

    // Cache raw pointers before moving ownership, then add to rpc_ctx BEFORE
    // dispatching the async RPC to prevent use-after-free: if the RPC completes
    // instantly, the callback may count_down() the latch, allowing the main
    // thread's wait() to return and destroy rpc_ctx before add_rpc_context runs.
    auto* cntl_raw = rpc_ctx_ptr->cntl.get();
    auto* resp_raw = rpc_ctx_ptr->response.get();
    auto* ctx_raw = rpc_ctx_ptr.get();
    rpc_ctx.add_rpc_context(std::move(rpc_ctx_ptr));

    // Send async RPC (ownership already transferred to rpc_ctx)
    stub->upload_snapshot_files(cntl_raw, &ctx_raw->request, resp_raw,
                                brpc::NewCallback(cluster_snapshot_rpc_cb, cntl_raw, resp_raw, ctx_raw));
}

Status process_tablet_for_snapshot(TabletManager* tablet_mgr, int64_t tablet_id, int64_t pre_version,
                                   int64_t new_version, bool is_filebundling, bool meta_added,
                                   FileSet& pre_bundle_data_files, FileSet& unused_data_files,
                                   FileSet& unused_meta_files, phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                   phmap::flat_hash_set<int64_t>& new_schema_ids,
                                   phmap::flat_hash_set<std::string>& globally_bound_segments,
                                   UploadSnapshotFilesRequestPB& node_req) {
    // Get pre-version tablet metadata
    TabletMetadataPtr pre_tablet_metadata;
    if (pre_version >= 0) {
        auto meta_or_st = tablet_mgr->get_tablet_metadata(tablet_id, pre_version);
        RETURN_IF_ERROR_WITH_WARN(meta_or_st.status(),
                                  fmt::format("get pre tablet metadata failed, tablet_id: {}", tablet_id));
        pre_tablet_metadata = std::move(meta_or_st.value());
    }

    // Get new-version tablet metadata
    auto meta_or_st = tablet_mgr->get_tablet_metadata(tablet_id, new_version);
    RETURN_IF_ERROR_WITH_WARN(meta_or_st.status(),
                              fmt::format("get new tablet metadata failed, tablet_id: {}", tablet_id));
    auto new_tablet_metadata = std::move(meta_or_st.value());

    // Collect file collections
    auto collections = TabletFileCollections::collect(pre_tablet_metadata, new_tablet_metadata);

    // Collect unused files
    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    // Populate tablet snapshot in node request
    auto* tablet_pb =
            populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_segments, node_req);

    // Populate metadata and schema files
    populate_meta_schema_files(is_filebundling, meta_added, tablet_id, pre_version, new_version, pre_tablet_metadata,
                               new_tablet_metadata, pre_schema_ids, new_schema_ids, unused_meta_files, tablet_pb);

    return Status::OK();
}

} // namespace starrocks::lake
