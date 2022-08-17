// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "service/service_be/lake_service.h"

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <brpc/controller.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include "agent/agent_server.h"
#include "common/status.h"
#include "fs/fs_util.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/tablet.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace starrocks {

template <typename Request, typename Response>
struct RpcContext {
    ::starrocks::ExecEnv* _env;
    const Request* _request;
    Response* _response;
    // response_mtx protects accesses to response.
    bthread::Mutex _response_mtx;
    CountDownLatch _latch;

    RpcContext(::starrocks::ExecEnv* env, const Request* request, Response* response, int latchCount)
            : _env(env), _request(request), _response(response), _response_mtx(), _latch(latchCount) {}

    ~RpcContext() = default;

    void count_down() { _latch.count_down(); }

    void wait() const { _latch.wait(); }
};

using PublishVersionContext = RpcContext<lake::PublishVersionRequest, lake::PublishVersionResponse>;
using PublishLogVersionContext = RpcContext<lake::PublishLogVersionRequest, lake::PublishLogVersionResponse>;

class PublishVersionTask : public Runnable {
public:
    PublishVersionTask(int64_t tablet_id, std::shared_ptr<PublishVersionContext> context)
            : _tablet_id(tablet_id), _context(std::move(context)) {}

    ~PublishVersionTask() override = default;

    void run() override;

    DISALLOW_COPY_AND_MOVE(PublishVersionTask);

private:
    int64_t _tablet_id;
    std::shared_ptr<PublishVersionContext> _context;
};

inline void PublishVersionTask::run() {
    auto base_version = _context->_request->base_version();
    auto new_version = _context->_request->new_version();
    auto txns = _context->_request->txn_ids().data();
    auto txns_size = _context->_request->txn_ids().size();

    auto st = _context->_env->lake_tablet_manager()->publish_version(_tablet_id, base_version, new_version, txns,
                                                                     txns_size);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to publish version for tablet " << _tablet_id << ": " << st;
        std::lock_guard l(_context->_response_mtx);
        _context->_response->add_failed_tablets(_tablet_id);
    }
    _context->count_down();
}

class PublishLogVersionTask : public Runnable {
public:
    PublishLogVersionTask(int64_t tablet_id, std::shared_ptr<PublishLogVersionContext> context)
            : _tablet_id(tablet_id), _context(std::move(context)) {}

    ~PublishLogVersionTask() override = default;

    void run() override;

    DISALLOW_COPY_AND_MOVE(PublishLogVersionTask);

private:
    int64_t _tablet_id;
    std::shared_ptr<PublishLogVersionContext> _context;
};

inline void PublishLogVersionTask::run() {
    auto txn_id = _context->_request->txn_id();
    auto version = _context->_request->version();

    auto st = _context->_env->lake_tablet_manager()->publish_log_version(_tablet_id, txn_id, version);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to rename txn log. tablet_id=" << _tablet_id << " txn_id=" << txn_id << ": " << st;
        std::lock_guard l(_context->_response_mtx);
        _context->_response->add_failed_tablets(_tablet_id);
    }
    _context->count_down();
}

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
    auto context = std::make_shared<PublishVersionContext>(_env, request, response, request->tablet_ids_size());

    for (auto tablet_id : request->tablet_ids()) {
        auto task = std::make_shared<PublishVersionTask>(tablet_id, context);
        auto st = thread_pool->submit(std::move(task));
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit publish version task: " << st;
            std::lock_guard l(context->_response_mtx);
            response->add_failed_tablets(tablet_id);
        }
    }

    context->wait();
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
    auto context = std::make_shared<PublishLogVersionContext>(_env, request, response, request->tablet_ids_size());

    for (auto tablet_id : request->tablet_ids()) {
        auto task = std::make_shared<PublishLogVersionTask>(tablet_id, context);
        auto st = thread_pool->submit(std::move(task));
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit publish log version task: " << st;
            std::lock_guard l(context->_response_mtx);
            response->add_failed_tablets(tablet_id);
        }
    }

    context->wait();
}

void LakeServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::starrocks::lake::AbortTxnRequest* request,
                                ::starrocks::lake::AbortTxnResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    (void)controller;

    // TODO: move the execution to TaskWorkerPool
    auto tablet_mgr = _env->lake_tablet_manager();
    for (auto tablet_id : request->tablet_ids()) {
        tablet_mgr->abort_txn(tablet_id, request->txn_ids().data(), request->txn_ids_size());
    }
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

    for (auto tablet_id : request->tablet_ids()) {
        auto res = _env->lake_tablet_manager()->delete_tablet(tablet_id);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to drop tablet " << tablet_id << ": " << res.get_error_msg();
            response->add_failed_tablets(tablet_id);
        }
    }
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

    // TODO: move the execution to TaskWorkerPool
    for (auto tablet_id : request->tablet_ids()) {
        // TODO: compact tablets in parallel.
        auto res = _env->lake_tablet_manager()->compact(tablet_id, request->version(), request->txn_id());
        if (!res.ok()) {
            LOG(WARNING) << "Fail to create compaction task for tablet " << tablet_id << ": " << res.status();
            response->add_failed_tablets(tablet_id);
            continue;
        }

        lake::CompactionTaskPtr task = std::move(res).value();
        auto st = task->execute();
        if (!st.ok()) {
            LOG(WARNING) << "Fail to compact tablet " << tablet_id << ". version=" << request->version()
                         << " txn_id=" << request->txn_id() << ": " << st;
            response->add_failed_tablets(tablet_id);
        } else {
            LOG(INFO) << "Compacted tablet " << tablet_id << ". version=" << request->version()
                      << " txn_id=" << request->txn_id();
        }
    }
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
    auto location = _env->lake_tablet_manager()->tablet_root_location(request->tablet_id());
    auto st = fs::remove_all(location);
    if (!st.ok() && !st.is_not_found()) {
        LOG(ERROR) << "Fail to remove " << location << ": " << st;
        cntl->SetFailed(st.get_error_msg());
    }
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

    for (auto tablet_id : request->tablet_ids()) {
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        if (!tablet.ok()) {
            LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
            response->add_failed_tablets(tablet_id);
            continue;
        }

        auto res = tablet->delete_data(request->txn_id(), request->delete_predicate());
        if (!res.ok()) {
            LOG(WARNING) << "Fail to delete data. tablet_id: " << tablet_id << ", txn_id: " << request->txn_id()
                         << ", error: " << res;
            response->add_failed_tablets(tablet_id);
        }
    }
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

    for (const auto& tablet_info : request->tablet_infos()) {
        int64_t tablet_id = tablet_info.tablet_id();
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        if (!tablet.ok()) {
            LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
            continue;
        }

        int64_t version = tablet_info.version();
        auto tablet_metadata = tablet->get_metadata(version);
        if (!tablet_metadata.ok()) {
            LOG(WARNING) << "Fail to get tablet metadata. tablet_id: " << tablet_id << ", version: " << version
                         << ", error: " << tablet.status();
            continue;
        }

        int64_t num_rows = 0;
        int64_t data_size = 0;
        for (const auto& rowset : (*tablet_metadata)->rowsets()) {
            num_rows += rowset.num_rows();
            data_size += rowset.data_size();
        }
        auto tablet_stat = response->add_tablet_stats();
        tablet_stat->set_tablet_id(tablet_id);
        tablet_stat->set_num_rows(num_rows);
        tablet_stat->set_data_size(data_size);
    }
}

} // namespace starrocks
