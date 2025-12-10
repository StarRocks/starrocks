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

#include "storage/lake/partition_snapshot_task.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "agent/finish_task.h"
#include "agent/task_signatures_manager.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "util/brpc_stub_cache.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"

namespace starrocks::lake {

namespace {

constexpr int64_t kSnapshotRpcTimeoutMs = 60 * 1000; // 1 minute

using RowsetIndex = absl::flat_hash_map<uint32_t, const RowsetMetadataPB*>;
using FileSet = absl::flat_hash_set<std::string>;

struct TabletFileDiff {
    std::vector<std::string> new_segments;
    std::vector<std::string> new_sstable_files;
    std::vector<std::string> new_dcg_files;
    std::vector<std::string> new_delvec_files;
};

RowsetIndex build_rowset_index(const TabletMetadataPtr& metadata) {
    RowsetIndex index;
    if (metadata == nullptr) {
        return index;
    }
    index.reserve(metadata->rowsets_size());
    for (const RowsetMetadataPB& rowset : metadata->rowsets()) {
        index.emplace(rowset.id(), &rowset);
    }
    return index;
}

FileSet collect_sstable_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_sstable_meta()) {
        return files;
    }
    for (const auto& sstable : metadata->sstable_meta().sstables()) {
        if (!sstable.filename().empty()) {
            files.emplace(sstable.filename());
        }
    }
    return files;
}

FileSet collect_dcg_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_dcg_meta()) {
        return files;
    }
    for (const auto& entry : metadata->dcg_meta().dcgs()) {
        const auto& dcg = entry.second;
        for (const std::string& column_file : dcg.column_files()) {
            if (!column_file.empty()) {
                files.emplace(column_file);
            }
        }
    }
    return files;
}

FileSet collect_delvec_files(const TabletMetadataPtr& metadata) {
    FileSet files;
    if (metadata == nullptr || !metadata->has_delvec_meta()) {
        return files;
    }
    for (const auto& entry : metadata->delvec_meta().version_to_file()) {
        const auto& meta = entry.second;
        if (!meta.name().empty()) {
            files.emplace(meta.name());
        }
    }
    return files;
}

void bind_removed_files(int64_t tablet_id, const FileSet& pre_files, const FileSet& new_files,
                        absl::flat_hash_map<int64_t, std::vector<std::string>>* tablet_to_files) {
    for (const auto& file : pre_files) {
        if (new_files.contains(file)) {
            continue;
        }
        (*tablet_to_files)[tablet_id].emplace_back(file);
    }
}

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;
struct PartitionSnapshotRpcCtx;

struct BackendSnapshotRpcCtx {
    UploadSnapshotFilesRequestPB request;
    std::unique_ptr<brpc::Controller> cntl;
    std::unique_ptr<UploadSnapshotFilesResponsePB> response;
    PartitionSnapshotRpcCtx* global_rpc_ctx;
};

struct PartitionSnapshotRpcCtx {
    bthread::Mutex mutex;
    std::unique_ptr<BThreadCountDownLatch> latch;
    std::vector<std::unique_ptr<BackendSnapshotRpcCtx>> all_rpc_ctxs;
    TFinishTaskRequest* finish_task_req;
    Status final_status = Status::OK();

    PartitionSnapshotRpcCtx() {}

    void add_rpc_context(std::unique_ptr<BackendSnapshotRpcCtx> rpc_ctx) {
        std::lock_guard<bthread::Mutex> l(mutex);
        all_rpc_ctxs.emplace_back(std::move(rpc_ctx));
    }

    void wait() { latch->wait(); }

    void handle_failure(const std::string& error_msg, const std::vector<int64_t>& tids) {
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

    bool has_failure() { return !final_status.ok(); }

    void count_down() { latch->count_down(); }
};

void partition_snapshot_rpc_cb(brpc::Controller* cntl, UploadSnapshotFilesResponsePB* resp,
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
        std::string error_msg = fmt::format("data_snapshot rpc failed, error={}", resp->status().error_msgs(0));
        std::vector<int64_t> failed_tids;
        for (auto& tablet_id : resp->failed_tablets()) {
            failed_tids.push_back(tablet_id);
        }
        rpc_ctx->global_rpc_ctx->handle_failure(error_msg, failed_tids);
    }
    // snasphot success
    // do nothing
}

} // namespace

void run_partition_snapshot_task(const TPartitionSnapshotRequest& request, int64_t signature, ExecEnv* exec_env) {
    LOG(INFO) << "run_partition_snapshot_task, " << request.db_id << ", " << request.table_id << ", "
              << request.partition_id << ", " << request.physical_partition_id;
    auto* tablet_mgr = exec_env->lake_tablet_manager();
    const int64_t pre_version = request.pre_version;
    const int64_t new_version = request.new_version;
    const int64_t table_id = request.table_id;
    const int64_t physical_partition_id = request.physical_partition_id;

    absl::flat_hash_set<std::string> globally_bound_segments;

    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_task_type(TTaskType::PARTITION_SNAPSHOT);
    finish_task_request.__set_signature(signature);
    TPartitionSnapshotInfo info;
    info.__set_db_id(request.db_id);
    info.__set_table_id(request.table_id);
    info.__set_partition_id(request.partition_id);
    info.__set_physical_partition_id(request.physical_partition_id);
    finish_task_request.__set_partition_snapshot_info(info);

    PartitionSnapshotRpcCtx partition_snapshot_rpc_ctx;
    partition_snapshot_rpc_ctx.latch = std::make_unique<BThreadCountDownLatch>(request.node_to_tablets.size());
    partition_snapshot_rpc_ctx.finish_task_req = &finish_task_request;

    for (const auto& [backend, tablet_ids] : request.node_to_tablets) {
        LOG(INFO) << "run partition snapshot rpc, backend: " << backend << ", tablet_ids: " << tablet_ids.size();
        if (partition_snapshot_rpc_ctx.has_failure()) {
            partition_snapshot_rpc_ctx.handle_failure("", tablet_ids);
            partition_snapshot_rpc_ctx.count_down();
            continue;
        }
        UploadSnapshotFilesRequestPB node_req;
        node_req.set_job_id(request.job_id);
        node_req.set_db_id(request.db_id);
        node_req.set_table_id(table_id);
        node_req.set_partition_id(request.partition_id);
        node_req.set_physical_partition_id(physical_partition_id);
        node_req.set_virtual_tablet_id(request.virtual_tablet);

        for (int64_t tablet_id : tablet_ids) {
            TabletMetadataPtr pre_tablet_metadata;
            if (pre_version >= 0) {
                auto meta_or_st = tablet_mgr->get_tablet_metadata(tablet_id, pre_version);
                if (!meta_or_st.ok()) {
                    LOG(ERROR) << "get pre tablet metadata failed, tablet_id: " << tablet_id
                               << ", status: " << meta_or_st.status().to_string();
                    partition_snapshot_rpc_ctx.handle_failure(meta_or_st.status().to_string(), tablet_ids);
                    break;
                }
                pre_tablet_metadata = std::move(meta_or_st.value());
            }

            auto meta_or_st = tablet_mgr->get_tablet_metadata(tablet_id, new_version);
            if (!meta_or_st.ok()) {
                LOG(ERROR) << "get new tablet metadata failed, tablet_id: " << tablet_id
                           << ", status: " << meta_or_st.status().to_string();
                partition_snapshot_rpc_ctx.handle_failure(meta_or_st.status().to_string(), tablet_ids);
                break;
            }
            auto new_tablet_metadata = std::move(meta_or_st.value());

            RowsetIndex pre_rowsets = build_rowset_index(pre_tablet_metadata);
            RowsetIndex new_rowsets = build_rowset_index(new_tablet_metadata);
            FileSet pre_sstable_files = collect_sstable_files(pre_tablet_metadata);
            FileSet new_sstable_files = collect_sstable_files(new_tablet_metadata);
            FileSet pre_dcg_files = collect_dcg_files(pre_tablet_metadata);
            FileSet new_dcg_files = collect_dcg_files(new_tablet_metadata);
            FileSet pre_delvec_files = collect_delvec_files(pre_tablet_metadata);
            FileSet new_delvec_files = collect_delvec_files(new_tablet_metadata);
            // TOOD(zhangqiang)
            // add meta_file
            // add schema_file

            auto* tablet_pb = node_req.add_tablet_snapshots();
            tablet_pb->set_tablet_id(tablet_id);
            for (const auto& file : new_sstable_files) {
                if (pre_sstable_files.find(file) == pre_sstable_files.end()) {
                    tablet_pb->add_new_data_files(file);
                }
            }
            for (const auto& file : new_dcg_files) {
                if (pre_dcg_files.find(file) == pre_dcg_files.end()) {
                    tablet_pb->add_new_data_files(file);
                }
            }
            for (const auto& file : new_delvec_files) {
                if (pre_delvec_files.find(file) == pre_delvec_files.end()) {
                    tablet_pb->add_new_data_files(file);
                }
            }
            for (const auto& [rowset_id, rowset] : new_rowsets) {
                if (pre_rowsets.contains(rowset_id)) {
                    continue;
                }
                for (const auto& segment : rowset->segments()) {
                    auto [it, inserted] = globally_bound_segments.emplace(segment);
                    if (inserted) {
                        tablet_pb->add_new_data_files(segment);
                    }
                }
            }
            for (const auto& file : tablet_pb->new_data_files()) {
                LOG(INFO) << "tablet_id: " << tablet_id << ", new_data_file: " << file;
            }
        }

        if (node_req.tablet_snapshots_size() == 0 || partition_snapshot_rpc_ctx.has_failure()) {
            partition_snapshot_rpc_ctx.count_down();
            continue;
        }

        auto stub = LakeServiceBrpcStubCache::getInstance()->get_stub(backend.host, backend.be_port);
        if (!stub.ok()) {
            LOG(ERROR) << "get stub failed, backend: " << backend.host << ":" << backend.be_port
                       << ", status: " << stub.status().to_string();
            partition_snapshot_rpc_ctx.handle_failure(stub.status().to_string(), tablet_ids);
            partition_snapshot_rpc_ctx.count_down();
            continue;
        }
        auto node_cntl = std::make_unique<brpc::Controller>();
        auto node_resp = std::make_unique<UploadSnapshotFilesResponsePB>();
        node_cntl->set_timeout_ms(kSnapshotRpcTimeoutMs);
        auto rpc_ctx = std::make_unique<BackendSnapshotRpcCtx>(std::move(node_req), std::move(node_cntl),
                                                               std::move(node_resp), &partition_snapshot_rpc_ctx);
        (*stub)->upload_snapshot_files(rpc_ctx->cntl.get(), &rpc_ctx->request, rpc_ctx->response.get(),
                                       brpc::NewCallback(partition_snapshot_rpc_cb, rpc_ctx->cntl.get(),
                                                         rpc_ctx->response.get(), rpc_ctx.get()));
        partition_snapshot_rpc_ctx.add_rpc_context(std::move(rpc_ctx));
    }

    partition_snapshot_rpc_ctx.wait();
    LOG(INFO) << "finish partition snapshot task, status: " << partition_snapshot_rpc_ctx.final_status.to_string();

    // TODO(zhangqiang)
    // write delete txn log
    finish_task(finish_task_request);
    remove_task_info(finish_task_request.task_type, finish_task_request.signature);
}

} // namespace starrocks::lake