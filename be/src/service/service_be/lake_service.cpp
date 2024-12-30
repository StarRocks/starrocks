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

#include "service/service_be/lake_service.h"

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/time.h> // NOLINT

#include "agent/agent_server.h"
#include "common/config.h"
#include "common/status.h"
#include "fs/fs_util.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "runtime/lake_snapshot_loader.h"
#include "runtime/load_channel_mgr.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet.h"
#include "storage/lake/transactions.h"
#include "storage/lake/vacuum.h"
#include "testutil/sync_point.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

namespace {
ThreadPool* get_thread_pool(ExecEnv* env, TTaskType::type type) {
    auto agent = env ? env->agent_server() : nullptr;
    return agent ? agent->get_thread_pool(type) : nullptr;
}

ThreadPool* publish_version_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::PUBLISH_VERSION);
}

ThreadPool* abort_txn_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::MAKE_SNAPSHOT);
}

ThreadPool* delete_tablet_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::CLONE);
}

ThreadPool* drop_table_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::CLONE);
}

ThreadPool* vacuum_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::RELEASE_SNAPSHOT);
}

ThreadPool* get_tablet_stats_thread_pool(ExecEnv* env) {
    return get_thread_pool(env, TTaskType::UPDATE_TABLET_META_INFO);
}

int get_num_publish_queued_tasks(void*) {
#ifndef BE_TEST
    auto tp = publish_version_thread_pool(ExecEnv::GetInstance());
    return tp ? tp->num_queued_tasks() : 0;
#else
    return 0;
#endif
}

int get_num_publish_active_tasks(void*) {
#ifndef BE_TEST
    auto tp = publish_version_thread_pool(ExecEnv::GetInstance());
    return tp ? tp->active_threads() : 0;
#else
    return 0;
#endif
}

int get_num_vacuum_queued_tasks(void*) {
#ifndef BE_TEST
    auto tp = vacuum_thread_pool(ExecEnv::GetInstance());
    return tp ? tp->num_queued_tasks() : 0;
#else
    return 0;
#endif
}

int get_num_vacuum_active_tasks(void*) {
#ifndef BE_TEST
    auto tp = vacuum_thread_pool(ExecEnv::GetInstance());
    return tp ? tp->active_threads() : 0;
#else
    return 0;
#endif
}

bvar::Adder<int64_t> g_publish_version_failed_tasks("lake_publish_version_failed_tasks");
bvar::LatencyRecorder g_publish_tablet_version_latency("lake_publish_tablet_version");
bvar::LatencyRecorder g_publish_tablet_version_queuing_latency("lake_publish_tablet_version_queuing");
bvar::PassiveStatus<int> g_publish_version_queued_tasks("lake_publish_version_queued_tasks",
                                                        get_num_publish_queued_tasks, nullptr);
bvar::PassiveStatus<int> g_publish_version_active_tasks("lake_publish_version_active_tasks",
                                                        get_num_publish_active_tasks, nullptr);
bvar::PassiveStatus<int> g_vacuum_queued_tasks("lake_vacuum_queued_tasks", get_num_vacuum_queued_tasks, nullptr);
bvar::PassiveStatus<int> g_vacuum_active_tasks("lake_vacuum_active_tasks", get_num_vacuum_active_tasks, nullptr);

std::string txn_info_string(const TxnInfoPB& info) {
    return info.DebugString();
}

} // namespace

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

LakeServiceImpl::LakeServiceImpl(ExecEnv* env, lake::TabletManager* tablet_mgr) : _env(env), _tablet_mgr(tablet_mgr) {}

LakeServiceImpl::~LakeServiceImpl() = default;

void LakeServiceImpl::publish_version(::google::protobuf::RpcController* controller,
                                      const ::starrocks::PublishVersionRequest* request,
                                      ::starrocks::PublishVersionResponse* response,
                                      ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_base_version()) {
        cntl->SetFailed("missing base version");
        return;
    }
    if (!request->has_new_version()) {
        cntl->SetFailed("missing new version");
        return;
    }
    if (request->txn_ids_size() == 0 && request->txn_infos_size() == 0) {
        cntl->SetFailed("neither txn_ids nor txn_infos is set, one of them must be set");
        return;
    }
    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    auto timeout_ms = request->has_timeout_ms() ? request->timeout_ms() : kDefaultTimeoutForPublishVersion;
    auto timeout_deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    auto start_ts = butil::gettimeofday_us();
    auto thread_pool = publish_version_thread_pool(_env);
    CHECK(thread_pool != nullptr);
    auto thread_pool_token = ConcurrencyLimitedThreadPoolToken(thread_pool, thread_pool->max_threads() * 2);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;
    scoped_refptr<Trace> trace_gurad = scoped_refptr<Trace>(new Trace());
    Trace* trace = trace_gurad.get();
    TRACE_TO(trace, "got request. txn_ids=$0 base_version=$1 new_version=$2 #tablets=$3",
             JoinInts(request->txn_ids(), ","), request->base_version(), request->new_version(),
             request->tablet_ids_size());

    Status::OK().to_protobuf(response->mutable_status());
    std::unordered_set<int64> rebuild_pindex_tablets;
    for (auto id : request->rebuild_pindex_tablet_ids()) {
        LOG(INFO) << "get rebuild tablet: " << id;
        rebuild_pindex_tablets.insert(id);
    }
    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            DeferOp defer([&] { latch.count_down(); });
            scoped_refptr<Trace> child_trace(new Trace);
            Trace* sub_trace = child_trace.get();
            trace->AddChildTrace("PublishTablet", sub_trace);

            ADOPT_TRACE(sub_trace);
            TRACE("start publish tablet $0 at thread $1", tablet_id, Thread::current_thread()->tid());

            auto run_ts = butil::gettimeofday_us();
            auto queuing_latency = run_ts - start_ts;
            g_publish_tablet_version_queuing_latency << queuing_latency;

            auto base_version = request->base_version();
            auto new_version = request->new_version();
            auto txns = std::vector<TxnInfoPB>();
            if (request->txn_infos_size() > 0) {
                txns.insert(txns.begin(), request->txn_infos().begin(), request->txn_infos().end());
                if (rebuild_pindex_tablets.count(tablet_id) > 0) {
                    for (auto& txn : txns) {
                        txn.set_rebuild_pindex(true);
                    }
                }
            } else { // This is a request from older version FE
                // Construct TxnInfoPB from other fields
                txns.reserve(request->txn_ids_size());
                for (auto i = 0, sz = request->txn_ids_size(); i < sz; i++) {
                    auto& info = txns.emplace_back();
                    info.set_txn_id(request->txn_ids(i));
                    info.set_txn_type(TXN_NORMAL);
                    info.set_combined_txn_log(false);
                    info.set_commit_time(request->commit_time());
                    info.set_force_publish(false);
                    if (rebuild_pindex_tablets.count(tablet_id) > 0) {
                        info.set_rebuild_pindex(true);
                    }
                }
            }

            TRACE_COUNTER_INCREMENT("tablet_id", tablet_id);
            TRACE_COUNTER_INCREMENT("queuing_latency_us", queuing_latency);

            StatusOr<TabletMetadataPtr> res;
            if (std::chrono::system_clock::now() < timeout_deadline) {
                res = lake::publish_version(_tablet_mgr, tablet_id, base_version, new_version, txns);
            } else {
                auto t = MilliSecondsSinceEpochFromTimePoint(timeout_deadline);
                res = Status::TimedOut(fmt::format("reached deadline={}/timeout={}", t, timeout_ms));
            }
            if (res.ok()) {
                auto metadata = std::move(res).value();
                auto score = compaction_score(_tablet_mgr, metadata);
                std::lock_guard l(response_mtx);
                response->mutable_compaction_scores()->insert({tablet_id, score});
            } else {
                g_publish_version_failed_tasks << 1;
                if (res.status().is_resource_busy()) {
                    VLOG(2) << "Fail to publish version: " << res.status() << ". tablet_id=" << tablet_id
                            << " txns=" << JoinMapped(txns, txn_info_string, ",") << " version=" << new_version;
                } else {
                    LOG(WARNING) << "Fail to publish version: " << res.status() << ". tablet_id=" << tablet_id
                                 << " txn_ids=" << JoinMapped(txns, txn_info_string, ",") << " version=" << new_version;
                }
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
                res.status().to_protobuf(response->mutable_status());
            }
            TRACE("finished");
            g_publish_tablet_version_latency << (butil::gettimeofday_us() - run_ts);
        };

        auto st = thread_pool_token.submit_func(std::move(task), timeout_deadline);
        if (!st.ok()) {
            g_publish_version_failed_tasks << 1;
            LOG(WARNING) << "Fail to submit publish version task: " << st << ". tablet_id=" << tablet_id
                         << " txn_ids=" << JoinInts(request->txn_ids(), ",");
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            st.to_protobuf(response->mutable_status());
            latch.count_down();
        }
    }

    latch.wait();
    auto cost = butil::gettimeofday_us() - start_ts;
    auto is_slow = cost >= config::lake_publish_version_slow_log_ms * 1000;
    if (config::lake_enable_publish_version_trace_log && is_slow) {
        LOG(INFO) << "Published txns=" << JoinInts(request->txn_ids(), ",") << ". cost=" << cost << "us\n"
                  << trace->DumpToString();
    } else if (is_slow) {
        LOG(INFO) << "Published txns=" << JoinInts(request->txn_ids(), ",")
                  << ". tablets=" << JoinInts(request->tablet_ids(), ",") << " cost=" << cost
                  << "us, trace: " << trace->MetricsAsJSON();
    }
    TEST_SYNC_POINT("LakeServiceImpl::publish_version:return");
}

void LakeServiceImpl::_submit_publish_log_version_task(const int64_t* tablet_ids, size_t tablet_size,
                                                       std::span<const TxnInfoPB> txn_infos,
                                                       const int64_t* log_versions,
                                                       ::starrocks::PublishLogVersionResponse* response) {
    auto txn_size = txn_infos.size();
    auto thread_pool = publish_version_thread_pool(_env);
    auto latch = BThreadCountDownLatch(tablet_size);
    bthread::Mutex response_mtx;

    for (int i = 0; i < tablet_size; i++) {
        auto tablet_id = tablet_ids[i];
        auto task = [&, tablet_id]() {
            DeferOp defer([&] { latch.count_down(); });
            auto st = lake::publish_log_version(_tablet_mgr, tablet_id, txn_infos, log_versions);
            if (!st.ok()) {
                g_publish_version_failed_tasks << 1;
                LOG(WARNING) << "Fail to publish log version: " << st << " tablet_id=" << tablet_id
                             << " txns=" << JoinMapped(txn_infos, txn_info_string, ",")
                             << " versions=" << JoinElementsIterator(log_versions, log_versions + txn_size, ",");
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
        };

        auto st = thread_pool->submit_func(task);
        if (!st.ok()) {
            g_publish_version_failed_tasks << 1;
            LOG(WARNING) << "Fail to submit publish log version task: " << st << " tablet_id=" << tablet_id
                         << " txns=" << JoinMapped(txn_infos, txn_info_string, ",")
                         << " versions=" << JoinElementsIterator(log_versions, log_versions + txn_size, ",");
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            latch.count_down();
        }
    }

    latch.wait();
}
void LakeServiceImpl::publish_log_version(::google::protobuf::RpcController* controller,
                                          const ::starrocks::PublishLogVersionRequest* request,
                                          ::starrocks::PublishLogVersionResponse* response,
                                          ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }
    if (!request->has_txn_id() && !request->has_txn_info()) {
        cntl->SetFailed("missing txn_id and txn_info");
        return;
    }
    if (!request->has_version()) {
        cntl->SetFailed("missing version");
        return;
    }

    auto tablet_ids = request->tablet_ids().data();
    auto version = int64_t{request->version()};
    if (!request->has_txn_info()) {
        DCHECK(request->has_txn_id());
        auto txn_info = TxnInfoPB();
        txn_info.set_txn_id(request->txn_id());
        txn_info.set_txn_type(TXN_NORMAL);
        txn_info.set_combined_txn_log(false);
        auto txn_infos = std::span<const TxnInfoPB>(&txn_info, 1);
        _submit_publish_log_version_task(tablet_ids, request->tablet_ids_size(), txn_infos, &version, response);
    } else {
        auto txn_infos = std::span<const TxnInfoPB>(&request->txn_info(), 1);
        _submit_publish_log_version_task(tablet_ids, request->tablet_ids_size(), txn_infos, &version, response);
    }
}

void LakeServiceImpl::publish_log_version_batch(::google::protobuf::RpcController* controller,
                                                const ::starrocks::PublishLogVersionBatchRequest* request,
                                                ::starrocks::PublishLogVersionResponse* response,
                                                ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }
    if (request->txn_ids_size() == 0 && request->txn_infos_size() == 0) {
        cntl->SetFailed("neither txn_ids nor txn_infos is set, one of them must be set");
        return;
    }
    if (request->versions_size() == 0) {
        cntl->SetFailed("missing versions");
        return;
    }

    auto txn_infos = std::vector<TxnInfoPB>{};
    auto tablet_ids = request->tablet_ids().data();
    auto versions = request->versions().data();
    if (request->txn_infos_size() == 0) {
        DCHECK_EQ(request->txn_ids_size(), request->versions_size());
        txn_infos.reserve(request->txn_ids_size());
        for (auto txn_id : request->txn_ids()) {
            auto& txn_info = txn_infos.emplace_back();
            txn_info.set_txn_id(txn_id);
            txn_info.set_combined_txn_log(false);
            txn_info.set_txn_type(TXN_NORMAL);
        }
    } else {
        txn_infos.insert(txn_infos.begin(), request->txn_infos().begin(), request->txn_infos().end());
    }

    _submit_publish_log_version_task(tablet_ids, request->tablet_ids_size(), txn_infos, versions, response);
}

void LakeServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::starrocks::AbortTxnRequest* request, ::starrocks::AbortTxnResponse* response,
                                ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    (void)controller;

    LOG(INFO) << "Aborting transactions. request=" << request->DebugString();

    // Cancel active tasks.
    if (LoadChannelMgr* load_mgr = _env->load_channel_mgr(); load_mgr != nullptr) {
        for (auto& txn_id : request->txn_ids()) { // For request sent by and older version FE
            load_mgr->abort_txn(txn_id);
        }
        for (auto& txn_info : request->txn_infos()) { // For request sent by a new version FE
            load_mgr->abort_txn(txn_info.txn_id());
        }
    }

    if (!request->has_skip_cleanup() || request->skip_cleanup()) {
        return;
    }

    auto thread_pool = abort_txn_thread_pool(_env);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        DeferOp defer([&] { latch.count_down(); });
        std::vector<TxnInfoPB> txn_infos;
        if (request->txn_infos_size() > 0) {
            txn_infos.insert(txn_infos.begin(), request->txn_infos().begin(), request->txn_infos().end());
        } else {
            // Construct TxnInfoPB from txn_id and txn_type
            txn_infos.reserve(request->txn_ids_size());
            auto has_txn_type = request->txn_types_size() == request->txn_ids_size();
            for (int i = 0, sz = request->txn_ids_size(); i < sz; i++) {
                auto& info = txn_infos.emplace_back();
                info.set_txn_id(request->txn_ids(i));
                info.set_txn_type(has_txn_type ? request->txn_types(i) : TXN_NORMAL);
                info.set_combined_txn_log(false);
            }
        }
        for (auto tablet_id : request->tablet_ids()) {
            lake::abort_txn(_tablet_mgr, tablet_id, txn_infos);
        }
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit abort transaction task: " << st;
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::delete_tablet(::google::protobuf::RpcController* controller,
                                    const ::starrocks::DeleteTabletRequest* request,
                                    ::starrocks::DeleteTabletResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    auto thread_pool = delete_tablet_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("no thread pool to run task");
        return;
    }
    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        DeferOp defer([&] { latch.count_down(); });
        lake::delete_tablets(_tablet_mgr, *request, response);
    });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit delete tablet task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();

    // Fill failed_tablets for backward compatibility
    if (response->status().status_code() != 0) {
        response->mutable_failed_tablets()->CopyFrom(request->tablet_ids());
    }
}

void LakeServiceImpl::delete_txn_log(::google::protobuf::RpcController* controller,
                                     const ::starrocks::DeleteTxnLogRequest* request,
                                     ::starrocks::DeleteTxnLogResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    if (request->txn_ids_size() == 0 && request->txn_infos_size() == 0) {
        cntl->SetFailed("neither txn_ids nor txn_infos is set, one of them must be set");
        return;
    }

    auto thread_pool = vacuum_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("vacuum thread pool is null when delete txn log");
        return;
    }

    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        DeferOp defer([&] { latch.count_down(); });
        lake::delete_txn_log(_tablet_mgr, *request, response);
    });

    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit vacuum task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    } else {
        Status::OK().to_protobuf(response->mutable_status());
    }

    latch.wait();
}

namespace drop_table_helper {
static bthread::Mutex g_mutex;
static std::unordered_set<std::string> g_paths;

// Returns true if the container already contains a |path|, false otherwise.
bool dedup_path(const std::string& path) {
    if (path.empty()) {
        return false;
    }
    std::lock_guard l(g_mutex);
    return !(g_paths.insert(path).second);
}

void remove_path(const std::string& path) {
    if (path.empty()) {
        return;
    }
    std::lock_guard l(g_mutex);
    g_paths.erase(path);
}

}; // namespace drop_table_helper

void LakeServiceImpl::drop_table(::google::protobuf::RpcController* controller,
                                 const ::starrocks::DropTableRequest* request, ::starrocks::DropTableResponse* response,
                                 ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_tablet_id()) {
        cntl->SetFailed("missing tablet_id");
        return;
    }

    auto thread_pool = drop_table_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("no thread pool to run task");
        return;
    }

    response->mutable_status()->set_status_code(0);

    if (drop_table_helper::dedup_path(request->path())) {
        TEST_SYNC_POINT("LakeService::drop_table:duplicate_path_id");
        LOG(INFO) << "Rejected drop table request because the previous task has not yet finished. tablet_id="
                  << request->tablet_id() << " path=" << request->path();
        response->mutable_status()->set_status_code(TStatusCode::ALREADY_EXIST);
        response->mutable_status()->add_error_msgs("Previous delete task for the same path has not yet finished");
        return;
    }

    DeferOp defer([&]() { drop_table_helper::remove_path(request->path()); });

    LOG(INFO) << "Received drop table request. tablet_id=" << request->tablet_id() << " path=" << request->path()
              << " queued_tasks=" << thread_pool->num_queued_tasks();

    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        TEST_SYNC_POINT("LakeService::drop_table:task_run");
        DeferOp defer([&] { latch.count_down(); });
        auto location = _tablet_mgr->tablet_root_location(request->tablet_id());
        auto st = fs::remove_all(location);
        if (!st.ok() && !st.is_not_found()) {
            LOG(ERROR) << "Fail to remove " << location << ": " << st << ". tablet_id=" << request->tablet_id()
                       << " path=" << request->path();
            st.to_protobuf(response->mutable_status());
        } else {
            LOG(INFO) << "Removed " << location << ". tablet_id=" << request->tablet_id() << " path=" << request->path()
                      << " is_not_found=" << st.is_not_found();
        }
    };

    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit drop table task: " << st << " tablet_id=" << request->tablet_id()
                     << " path=" << request->path();
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::delete_data(::google::protobuf::RpcController* controller,
                                  const ::starrocks::DeleteDataRequest* request,
                                  ::starrocks::DeleteDataResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }
    if (!request->has_txn_id()) {
        cntl->SetFailed("missing txn_id");
        return;
    }
    if (!request->has_delete_predicate()) {
        cntl->SetFailed("missing delete_predicate");
        return;
    }

    auto thread_pool = publish_version_thread_pool(_env);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;
    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            DeferOp defer([&] { latch.count_down(); });
            auto tablet = _tablet_mgr->get_tablet(tablet_id);
            if (!tablet.ok()) {
                LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
                return;
            }
            auto res = tablet->delete_data(request->txn_id(), request->delete_predicate());
            if (!res.ok()) {
                LOG(WARNING) << "Fail to delete data. tablet_id: " << tablet_id << ", txn_id: " << request->txn_id()
                             << ", error: " << res;
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
        };

        auto st = thread_pool->submit_func(task);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit delete data task: " << st;
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::get_tablet_stats(::google::protobuf::RpcController* controller,
                                       const ::starrocks::TabletStatRequest* request,
                                       ::starrocks::TabletStatResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_infos_size() == 0) {
        cntl->SetFailed("missing tablet_infos");
        return;
    }
    auto thread_pool = get_tablet_stats_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("thread pool is null");
        return;
    }
    // The magic number "10" is just a random chosen number, feel free to change it if you have a better choice.
    auto max_pending = std::max(10, thread_pool->max_threads() * 2);
    auto timeout_ms = request->has_timeout_ms() ? request->timeout_ms() : kDefaultTimeoutForGetTabletStat;
    auto timeout_deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
    auto thread_pool_token = ConcurrencyLimitedThreadPoolToken(thread_pool, max_pending);
    auto latch = BThreadCountDownLatch(request->tablet_infos_size());
    bthread::Mutex response_mtx;
    for (const auto& tablet_info : request->tablet_infos()) {
        auto task = [&, tablet_info]() {
            DeferOp defer([&] { latch.count_down(); });

            int64_t tablet_id = tablet_info.tablet_id();
            int64_t version = tablet_info.version();
            if (std::chrono::system_clock::now() >= timeout_deadline) {
                LOG(WARNING) << "Cancelled tablet stat collection task due to timeout exceeded. tablet_id: "
                             << tablet_id << ", version: " << version;
                return;
            }

            auto tablet_metadata = _tablet_mgr->get_tablet_metadata(tablet_id, version, /*fill_cache=*/false);
            if (!tablet_metadata.ok()) {
                LOG(WARNING) << "Fail to get tablet metadata. tablet_id: " << tablet_id << ", version: " << version
                             << ", error: " << tablet_metadata.status();
                return;
            }

            int64_t num_rows = 0;
            int64_t data_size = 0;
            for (const auto& rowset : (*tablet_metadata)->rowsets()) {
                num_rows += rowset.num_rows();
                data_size += rowset.data_size();
            }
            for (const auto& [_, file] : (*tablet_metadata)->delvec_meta().version_to_file()) {
                data_size += file.size();
            }

            std::lock_guard l(response_mtx);
            auto tablet_stat = response->add_tablet_stats();
            tablet_stat->set_tablet_id(tablet_id);
            tablet_stat->set_num_rows(num_rows);
            tablet_stat->set_data_size(data_size);
        };
        TEST_SYNC_POINT_CALLBACK("LakeServiceImpl::get_tablet_stats:before_submit", nullptr);
        if (auto st = thread_pool_token.submit_func(std::move(task), timeout_deadline); !st.ok()) {
            LOG(WARNING) << "Fail to get tablet stats task: " << st;
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::lock_tablet_metadata(::google::protobuf::RpcController* controller,
                                           const ::starrocks::LockTabletMetadataRequest* request,
                                           ::starrocks::LockTabletMetadataResponse* response,
                                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    cntl->SetFailed("does not support lock_tablet_metadata anymore");
}

void LakeServiceImpl::unlock_tablet_metadata(::google::protobuf::RpcController* controller,
                                             const ::starrocks::UnlockTabletMetadataRequest* request,
                                             ::starrocks::UnlockTabletMetadataResponse* response,
                                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    cntl->SetFailed("does not support unlock_tablet_metadata anymore");
}

void LakeServiceImpl::upload_snapshots(::google::protobuf::RpcController* controller,
                                       const ::starrocks::UploadSnapshotsRequest* request,
                                       ::starrocks::UploadSnapshotsResponse* response,
                                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    // TODO: Support fs upload directly
    if (!request->has_broker()) {
        cntl->SetFailed("missing broker");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::UPLOAD);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        DeferOp defer([&] { latch.count_down(); });
        auto loader = std::make_unique<LakeSnapshotLoader>(_env);
        auto st = loader->upload(request);
        if (!st.ok()) {
            cntl->SetFailed("Fail to upload snapshot");
        }
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit upload snapshots task: " << st;
        cntl->SetFailed(std::string(st.message()));
        latch.count_down();
    }
    latch.wait();
}

void LakeServiceImpl::restore_snapshots(::google::protobuf::RpcController* controller,
                                        const ::starrocks::RestoreSnapshotsRequest* request,
                                        ::starrocks::RestoreSnapshotsResponse* response,
                                        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    // TODO: Support fs download and restore directly
    if (!request->has_broker()) {
        cntl->SetFailed("missing broker");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::DOWNLOAD);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        DeferOp defer([&] { latch.count_down(); });
        auto loader = std::make_unique<LakeSnapshotLoader>(_env);
        auto st = loader->restore(request);
        if (!st.ok()) {
            cntl->SetFailed("Fail to restore snapshot");
        }
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit restore snapshots task: " << st;
        cntl->SetFailed(std::string(st.message()));
        latch.count_down();
    }
    latch.wait();
}

void LakeServiceImpl::compact(::google::protobuf::RpcController* controller, const ::starrocks::CompactRequest* request,
                              ::starrocks::CompactResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }
    if (!request->has_txn_id()) {
        cntl->SetFailed("missing txn_id");
        return;
    }
    if (!request->has_version()) {
        cntl->SetFailed("missing version");
        return;
    }

    _tablet_mgr->compaction_scheduler()->compact(controller, request, response, guard.release());
}

void LakeServiceImpl::abort_compaction(::google::protobuf::RpcController* controller,
                                       const ::starrocks::AbortCompactionRequest* request,
                                       ::starrocks::AbortCompactionResponse* response,
                                       ::google::protobuf::Closure* done) {
    TEST_SYNC_POINT("LakeServiceImpl::abort_compaction:enter");

    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_txn_id()) {
        cntl->SetFailed("missing txn_id");
        return;
    }

    auto scheduler = _tablet_mgr->compaction_scheduler();
    auto st = scheduler->abort(request->txn_id());
    TEST_SYNC_POINT("LakeServiceImpl::abort_compaction:aborted");
    st.to_protobuf(response->mutable_status());
}

void LakeServiceImpl::vacuum(::google::protobuf::RpcController* controller, const ::starrocks::VacuumRequest* request,
                             ::starrocks::VacuumResponse* response, ::google::protobuf::Closure* done) {
    static bthread::Mutex s_mtx;
    static std::unordered_set<int64_t> s_vacuuming_partitions;

    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    auto thread_pool = vacuum_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("vacuum thread pool is null");
        return;
    }

    if (request->partition_id() > 0) {
        std::lock_guard l(s_mtx);
        if (!s_vacuuming_partitions.insert(request->partition_id()).second) {
            TEST_SYNC_POINT("LakeServiceImpl::vacuum:1");
            LOG(INFO) << "Ignored duplicate vacuum request of partition " << request->partition_id();
            cntl->SetFailed(fmt::format("duplicated vacuum request of partition {}", request->partition_id()));
            return;
        }
    }

    DeferOp defer([&]() {
        if (request->partition_id() > 0) {
            std::lock_guard l(s_mtx);
            s_vacuuming_partitions.erase(request->partition_id());
        }
    });

    TEST_SYNC_POINT("LakeServiceImpl::vacuum:2");

    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        DeferOp defer([&] { latch.count_down(); });
        lake::vacuum(_tablet_mgr, *request, response);
    });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit vacuum task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::vacuum_full(::google::protobuf::RpcController* controller,
                                  const ::starrocks::VacuumFullRequest* request,
                                  ::starrocks::VacuumFullResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    auto thread_pool = vacuum_thread_pool(_env);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("full vacuum thread pool is null");
        return;
    }
    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        DeferOp defer([&] { latch.count_down(); });
        lake::vacuum_full(_tablet_mgr, *request, response);
    });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit vacuum task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();
}

} // namespace starrocks
