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

#pragma once

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "util/countdown_latch.h"

namespace starrocks {
struct BackendInfo;
class TFinishTaskRequest;
namespace lake {

class TabletManager;
struct ClusterSnapshotRpcCtx;
using FileSet = phmap::flat_hash_set<std::string>;
using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

struct BackendSnapshotRpcCtx {
    UploadSnapshotFilesRequestPB request;
    std::unique_ptr<brpc::Controller> cntl;
    std::unique_ptr<UploadSnapshotFilesResponsePB> response;
    ClusterSnapshotRpcCtx* global_rpc_ctx;

    BackendSnapshotRpcCtx(UploadSnapshotFilesRequestPB req, std::unique_ptr<brpc::Controller> controller,
                          std::unique_ptr<UploadSnapshotFilesResponsePB> resp, ClusterSnapshotRpcCtx* ctx)
            : request(std::move(req)), cntl(std::move(controller)), response(std::move(resp)), global_rpc_ctx(ctx) {}
};

struct ClusterSnapshotRpcCtx {
    bthread::Mutex mutex;
    std::unique_ptr<BThreadCountDownLatch> latch;
    std::vector<std::unique_ptr<BackendSnapshotRpcCtx>> all_rpc_ctxs;
    TFinishTaskRequest* finish_task_req = nullptr;
    Status final_status = Status::OK();

    ClusterSnapshotRpcCtx() = default;
    ~ClusterSnapshotRpcCtx() = default;

    void add_rpc_context(std::unique_ptr<BackendSnapshotRpcCtx> rpc_ctx);
    void wait();
    void handle_failure(const std::string& error_msg, const std::vector<int64_t>& tids);
    bool has_failure();
    void count_down();
};

// RPC related functions
void cluster_snapshot_rpc_cb(brpc::Controller* cntl, UploadSnapshotFilesResponsePB* resp,
                             BackendSnapshotRpcCtx* rpc_ctx);
void send_snapshot_rpc_to_backend(const TBackend& backend, const std::vector<int64_t>& tablet_ids,
                                  const UploadSnapshotFilesRequestPB& node_req, ClusterSnapshotRpcCtx& rpc_ctx);

// Tablet processing for RPC
Status process_tablet_for_snapshot(TabletManager* tablet_mgr, int64_t tablet_id, int64_t pre_version,
                                   int64_t new_version, bool is_filebundling, bool meta_added,
                                   FileSet& pre_bundle_data_files, FileSet& unused_data_files,
                                   FileSet& unused_meta_files, phmap::flat_hash_set<int64_t>& pre_schema_ids,
                                   phmap::flat_hash_set<int64_t>& new_schema_ids,
                                   phmap::flat_hash_set<std::string>& globally_bound_segments,
                                   UploadSnapshotFilesRequestPB& node_req);

} // namespace lake
} // namespace starrocks
