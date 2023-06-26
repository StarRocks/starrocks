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
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "runtime/lake_snapshot_loader.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet.h"
#include "storage/lake/vacuum.h"
#include "testutil/sync_point.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace starrocks {

using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

LakeServiceImpl::LakeServiceImpl(ExecEnv* env) : _env(env) {}

LakeServiceImpl::~LakeServiceImpl() {}

void LakeServiceImpl::publish_version(::google::protobuf::RpcController* controller,
                                      const ::starrocks::lake::PublishVersionRequest* request,
                                      ::starrocks::lake::PublishVersionResponse* response,
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
    if (request->txn_ids_size() == 0) {
        cntl->SetFailed("missing txn_ids");
        return;
    }
    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;

    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            auto base_version = request->base_version();
            auto new_version = request->new_version();
            auto txns = request->txn_ids().data();
            auto txns_size = request->txn_ids().size();
            auto tablet_manager = _env->lake_tablet_manager();

            auto res = tablet_manager->publish_version(tablet_id, base_version, new_version, txns, txns_size);
            if (res.ok()) {
                auto metadata = std::move(res).value();
                auto score = compaction_score(*metadata);
                std::lock_guard l(response_mtx);
                response->mutable_compaction_scores()->insert({tablet_id, score});
            } else {
                LOG(WARNING) << "Fail to publish version: " << res.status() << ". tablet_id=" << tablet_id
                             << " txn_id=" << txns[0];
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
            latch.count_down();
            VLOG(5) << "Published version. tablet_id=" << tablet_id << " txn_id=" << txns[0];
        };

        auto st = thread_pool->submit_func(task, ThreadPool::HIGH_PRIORITY);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit publish version task: " << st << ". tablet_id=" << tablet_id
                         << " txn_id=" << request->txn_ids()[0];
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::publish_log_version(::google::protobuf::RpcController* controller,
                                          const ::starrocks::lake::PublishLogVersionRequest* request,
                                          ::starrocks::lake::PublishLogVersionResponse* response,
                                          ::google::protobuf::Closure* done) {
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

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;

    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            auto txn_id = request->txn_id();
            auto version = request->version();

            auto st = _env->lake_tablet_manager()->publish_log_version(tablet_id, txn_id, version);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to rename txn log. tablet_id=" << tablet_id << " txn_id=" << txn_id << ": " << st;
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
            latch.count_down();
        };

        auto st = thread_pool->submit_func(task, ThreadPool::HIGH_PRIORITY);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit publish log version task: " << st;
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::starrocks::lake::AbortTxnRequest* request,
                                ::starrocks::lake::AbortTxnResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    (void)controller;

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            auto* txn_ids = request->txn_ids().data();
            auto txn_ids_size = request->txn_ids_size();
            _env->lake_tablet_manager()->abort_txn(tablet_id, txn_ids, txn_ids_size);
            latch.count_down();
        };
        auto st = thread_pool->submit_func(task);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit abort txn  task: " << st;
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::delete_tablet(::google::protobuf::RpcController* controller,
                                    const ::starrocks::lake::DeleteTabletRequest* request,
                                    ::starrocks::lake::DeleteTabletResponse* response,
                                    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::DROP);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;
    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            auto res = _env->lake_tablet_manager()->delete_tablet(tablet_id);
            if (!res.ok()) {
                LOG(WARNING) << "Fail to drop tablet " << tablet_id << ": " << res.get_error_msg();
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
            latch.count_down();
        };

        auto st = thread_pool->submit_func(task);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit drop tablet task: " << st;
            std::lock_guard l(response_mtx);
            response->add_failed_tablets(tablet_id);
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::drop_table(::google::protobuf::RpcController* controller,
                                 const ::starrocks::lake::DropTableRequest* request,
                                 ::starrocks::lake::DropTableResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_tablet_id()) {
        cntl->SetFailed("missing tablet_id");
        return;
    }

    // TODO: move the execution to TaskWorkerPool
    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::DROP);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        auto location = _env->lake_tablet_manager()->tablet_root_location(request->tablet_id());
        auto st = fs::remove_all(location);
        if (!st.ok() && !st.is_not_found()) {
            LOG(ERROR) << "Fail to remove " << location << ": " << st;
            cntl->SetFailed("Fail to remove " + location);
        }
        latch.count_down();
    };

    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit drop table task: " << st;
        cntl->SetFailed(st.get_error_msg());
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::delete_data(::google::protobuf::RpcController* controller,
                                  const ::starrocks::lake::DeleteDataRequest* request,
                                  ::starrocks::lake::DeleteDataResponse* response, ::google::protobuf::Closure* done) {
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

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::DROP);
    auto latch = BThreadCountDownLatch(request->tablet_ids_size());
    bthread::Mutex response_mtx;
    for (auto tablet_id : request->tablet_ids()) {
        auto task = [&, tablet_id]() {
            auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
            if (!tablet.ok()) {
                LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
                latch.count_down();
                return;
            }
            auto res = tablet->delete_data(request->txn_id(), request->delete_predicate());
            if (!res.ok()) {
                LOG(WARNING) << "Fail to delete data. tablet_id: " << tablet_id << ", txn_id: " << request->txn_id()
                             << ", error: " << res;
                std::lock_guard l(response_mtx);
                response->add_failed_tablets(tablet_id);
            }
            latch.count_down();
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
                                       const ::starrocks::lake::TabletStatRequest* request,
                                       ::starrocks::lake::TabletStatResponse* response,
                                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_infos_size() == 0) {
        cntl->SetFailed("missing tablet_infos");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    auto latch = BThreadCountDownLatch(request->tablet_infos_size());
    bthread::Mutex response_mtx;
    for (const auto& tablet_info : request->tablet_infos()) {
        auto task = [&, tablet_info]() {
            int64_t tablet_id = tablet_info.tablet_id();
            auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
            if (!tablet.ok()) {
                LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
                latch.count_down();
                return;
            }

            int64_t version = tablet_info.version();
            auto tablet_metadata = tablet->get_metadata(version);
            if (!tablet_metadata.ok()) {
                LOG(WARNING) << "Fail to get tablet metadata. tablet_id: " << tablet_id << ", version: " << version
                             << ", error: " << tablet_metadata.status();
                latch.count_down();
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
            latch.count_down();
        };
        if (auto st = thread_pool->submit_func(std::move(task)); !st.ok()) {
            LOG(WARNING) << "Fail to get tablet stats task: " << st;
            latch.count_down();
        }
    }

    latch.wait();
}

void LakeServiceImpl::lock_tablet_metadata(::google::protobuf::RpcController* controller,
                                           const ::starrocks::lake::LockTabletMetadataRequest* request,
                                           ::starrocks::lake::LockTabletMetadataResponse* response,
                                           ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_version()) {
        cntl->SetFailed("missing version");
        return;
    }
    if (!request->has_tablet_id()) {
        cntl->SetFailed("missing tablet id");
        return;
    }
    if (!request->has_expire_time()) {
        cntl->SetFailed("missing expire time");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        auto tablet = _env->lake_tablet_manager()->get_tablet(request->tablet_id());
        if (!tablet.ok()) {
            LOG(ERROR) << "Fail to get tablet " << request->tablet_id();
            cntl->SetFailed("Fail to get tablet");
            latch.count_down();
            return;
        }
        auto st = tablet->put_tablet_metadata_lock(request->version(), request->expire_time());
        if (!st.ok()) {
            LOG(ERROR) << "Fail to lock tablet metadata, tablet id: " << request->tablet_id()
                       << ", version: " << request->version();
            cntl->SetFailed("Fail to lock tablet metadata");
            latch.count_down();
            return;
        }
        auto tablet_meta = tablet->get_metadata(request->version());
        // If metadata has been deleted, the request should fail.
        if (!tablet_meta.ok()) {
            LOG(ERROR) << "Tablet metadata has been deleted, tablet id: " << request->tablet_id()
                       << ", version: " << request->version();
            cntl->SetFailed("Tablet metadata has been deleted");
        }
        latch.count_down();
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit lock tablet metadata task: " << st;
        cntl->SetFailed(st.get_error_msg());
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::unlock_tablet_metadata(::google::protobuf::RpcController* controller,
                                             const ::starrocks::lake::UnlockTabletMetadataRequest* request,
                                             ::starrocks::lake::UnlockTabletMetadataResponse* response,
                                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);
    if (!request->has_version()) {
        cntl->SetFailed("missing version");
        return;
    }
    if (!request->has_tablet_id()) {
        cntl->SetFailed("missing tablet id");
        return;
    }
    if (!request->has_expire_time()) {
        cntl->SetFailed("missing expire time");
        return;
    }

    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::UPDATE_TABLET_META_INFO);
    auto latch = BThreadCountDownLatch(1);
    auto task = [&]() {
        auto tablet = _env->lake_tablet_manager()->get_tablet(request->tablet_id());
        if (!tablet.ok()) {
            LOG(ERROR) << "Fail to get tablet " << request->tablet_id();
            cntl->SetFailed("Fail to get tablet");
            latch.count_down();
            return;
        }
        auto st = tablet->delete_tablet_metadata_lock(request->version(), request->expire_time());
        if (!st.ok()) {
            LOG(ERROR) << "Fail to unlock tablet metadata, tablet id: " << request->tablet_id()
                       << ", version: " << request->version();
            cntl->SetFailed("Fail to unlock tablet metadata");
        }
        latch.count_down();
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit unlock tablet metadata task: " << st;
        cntl->SetFailed(st.get_error_msg());
        latch.count_down();
    }
    latch.wait();
}

void LakeServiceImpl::upload_snapshots(::google::protobuf::RpcController* controller,
                                       const ::starrocks::lake::UploadSnapshotsRequest* request,
                                       ::starrocks::lake::UploadSnapshotsResponse* response,
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
        auto loader = std::make_unique<LakeSnapshotLoader>(_env);
        auto st = loader->upload(request);
        if (!st.ok()) {
            cntl->SetFailed("Fail to upload snapshot");
        }
        latch.count_down();
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit upload snapshots task: " << st;
        cntl->SetFailed(st.get_error_msg());
        latch.count_down();
    }
    latch.wait();
}

void LakeServiceImpl::restore_snapshots(::google::protobuf::RpcController* controller,
                                        const ::starrocks::lake::RestoreSnapshotsRequest* request,
                                        ::starrocks::lake::RestoreSnapshotsResponse* response,
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
        auto loader = std::make_unique<LakeSnapshotLoader>(_env);
        auto st = loader->restore(request);
        if (!st.ok()) {
            cntl->SetFailed("Fail to restore snapshot");
        }
        latch.count_down();
    };
    auto st = thread_pool->submit_func(task);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit restore snapshots task: " << st;
        cntl->SetFailed(st.get_error_msg());
        latch.count_down();
    }
    latch.wait();
}

void LakeServiceImpl::compact(::google::protobuf::RpcController* controller,
                              const ::starrocks::lake::CompactRequest* request,
                              ::starrocks::lake::CompactResponse* response, ::google::protobuf::Closure* done) {
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

    _env->lake_tablet_manager()->compaction_scheduler()->compact(controller, request, response, guard.release());
}

void LakeServiceImpl::abort_compaction(::google::protobuf::RpcController* controller,
                                       const ::starrocks::lake::AbortCompactionRequest* request,
                                       ::starrocks::lake::AbortCompactionResponse* response,
                                       ::google::protobuf::Closure* done) {
    TEST_SYNC_POINT("LakeServiceImpl::abort_compaction:enter");

    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (!request->has_txn_id()) {
        cntl->SetFailed("missing txn_id");
        return;
    }

    auto scheduler = _env->lake_tablet_manager()->compaction_scheduler();
    auto st = scheduler->abort(request->txn_id());
    TEST_SYNC_POINT("LakeServiceImpl::abort_compaction:aborted");
    st.to_protobuf(response->mutable_status());
}

void LakeServiceImpl::vacuum(::google::protobuf::RpcController* controller,
                             const ::starrocks::lake::VacuumRequest* request,
                             ::starrocks::lake::VacuumResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    // The BE process already has enough threads. I don't want to create more threads, so I directly use
    // the thread pool for creating local tablet's replica.
    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::CREATE);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("vacuum thread pool is null");
        return;
    }
    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        lake::vacuum(_env->lake_tablet_manager(), *request, response);
        latch.count_down();
    });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit vacuum task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();
}

void LakeServiceImpl::vacuum_full(::google::protobuf::RpcController* controller,
                                  const ::starrocks::lake::VacuumFullRequest* request,
                                  ::starrocks::lake::VacuumFullResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    // The BE process already has enough threads. I don't want to create more threads, so I directly use
    // the thread pool for dropping local tablet's replica.
    auto thread_pool = _env->agent_server()->get_thread_pool(TTaskType::DROP);
    if (UNLIKELY(thread_pool == nullptr)) {
        cntl->SetFailed("full vacuum thread pool is null");
        return;
    }
    auto latch = BThreadCountDownLatch(1);
    auto st = thread_pool->submit_func([&]() {
        lake::vacuum_full(_env->lake_tablet_manager(), *request, response);
        latch.count_down();
    });
    if (!st.ok()) {
        LOG(WARNING) << "Fail to submit vacuum task: " << st;
        st.to_protobuf(response->mutable_status());
        latch.count_down();
    }

    latch.wait();
}

} // namespace starrocks
