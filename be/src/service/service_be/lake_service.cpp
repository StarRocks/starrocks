// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "service/service_be/lake_service.h"

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <brpc/controller.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include "agent/agent_server.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"

namespace starrocks {

inline Status apply_txn_log(const lake::TxnLog& log, lake::TabletMetadata* metadata) {
    if (log.has_op_write()) {
        if (log.op_write().has_rowset() && log.op_write().rowset().segments_size() > 0) {
            auto rowset = metadata->add_rowsets();
            rowset->CopyFrom(log.op_write().rowset());
            rowset->set_id(metadata->next_rowset_id());
            metadata->set_next_rowset_id(metadata->next_rowset_id() + rowset->segments_size());
        }
    }

    if (log.has_op_compaction()) {
        return Status::NotSupported("does not support apply compaction log yet");
    }

    if (log.has_op_schema_change()) {
        return Status::NotSupported("does not support apply schema change log yet");
    }

    return Status::OK();
}

inline Status publish(lake::Tablet* tablet, const ::starrocks::lake::PublishVersionRequest* request) {
    const auto base_version = request->base_version();
    const auto new_version = request->new_version();

    // Read base version metadata
    auto res = tablet->get_metadata(base_version);
    if (!res.ok()) {
        // Check if the new version metadata exist.
        if (res.status().is_not_found() && tablet->get_metadata(new_version).ok()) {
            // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ optimization, there is no need to invoke `get_metadata` in all
            // circumstances, e.g, network and permission problems.
            return Status::OK();
        }
        LOG(WARNING) << "Fail to get " << tablet->metadata_path(base_version) << ": " << res.status();
        return res.status();
    }
    const lake::TabletMetadataPtr& base_metadata = res.value();

    // make a copy of metadata
    auto new_metadata = std::make_shared<lake::TabletMetadata>(*base_metadata);
    new_metadata->set_version(new_version);

    // Apply txn logs
    for (auto txn_id : request->txn_ids()) {
        auto txnlog = tablet->get_txn_log(txn_id);
        if (txnlog.status().is_not_found() && tablet->get_metadata(new_version).ok()) {
            // txn log does not exist but the new version metadata has been generated, maybe
            // this is a duplicated publish version request.
            return Status::OK();
        } else if (!txnlog.ok()) {
            LOG(WARNING) << "Fail to get " << tablet->txn_log_path(txn_id) << ": " << txnlog.status();
            return txnlog.status();
        }

        auto st = apply_txn_log(**txnlog, new_metadata.get());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to apply " << tablet->txn_log_path(txn_id) << ": " << st;
            return st;
        }
    }

    // Save new metadata
    if (auto st = tablet->put_metadata(new_metadata); !st.ok()) {
        LOG(WARNING) << "Fail to put " << tablet->metadata_path(new_version) << ": " << st;
        return st;
    }

    // Delete txn logs
    for (auto txn_id : request->txn_ids()) {
        auto st = tablet->delete_txn_log(txn_id);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << tablet->txn_log_path(txn_id) << ": " << st;
    }
    return Status::OK();
}

#ifndef BE_TEST
struct PublishVersionContext {
    ::starrocks::ExecEnv* _env;
    ::google::protobuf::Closure* _done;
    const ::starrocks::lake::PublishVersionRequest* _request;
    ::starrocks::lake::PublishVersionResponse* _response;
    // response_mtx protects accesses to response.
    bthread::Mutex _response_mtx;

    PublishVersionContext(::starrocks::ExecEnv* env, ::google::protobuf::Closure* done,
                          const ::starrocks::lake::PublishVersionRequest* request,
                          ::starrocks::lake::PublishVersionResponse* response)
            : _env(env), _done(done), _request(request), _response(response), _response_mtx() {}

    ~PublishVersionContext() { _done->Run(); }
};
#else
// For unit tests
struct PublishVersionContext {
    ::starrocks::ExecEnv* _env;
    const ::starrocks::lake::PublishVersionRequest* _request;
    ::starrocks::lake::PublishVersionResponse* _response;
    // response_mtx protects accesses to response.
    bthread::Mutex _response_mtx;
    CountDownLatch _latch;

    PublishVersionContext(::starrocks::ExecEnv* env, ::google::protobuf::Closure* /*done*/,
                          const ::starrocks::lake::PublishVersionRequest* request,
                          ::starrocks::lake::PublishVersionResponse* response)
            : _env(env), _request(request), _response(response), _response_mtx(), _latch(request->tablet_ids_size()) {}

    ~PublishVersionContext() = default;

    void count_down() { _latch.count_down(); }

    void wait() { _latch.wait(); }
};
#endif // BE_TEST

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
    auto res = _context->_env->lake_tablet_manager()->get_tablet(_tablet_id);
    if (!res.ok()) {
        LOG(WARNING) << "Fail to get tablet " << _tablet_id << ": " << res.status();
        std::lock_guard l(_context->_response_mtx);
        _context->_response->add_failed_tablets(_tablet_id);
    } else {
        auto& tablet = res.value();
        if (auto r = publish(&tablet, _context->_request); !r.ok()) {
            std::lock_guard l(_context->_response_mtx);
            _context->_response->add_failed_tablets(_tablet_id);
        }
    }
#ifdef BE_TEST
    _context->count_down();
#endif
    // Will call `_context->done->Run()` if this is the ref count of _context is 1.
    _context.reset();
}

void LakeServiceImpl::publish_version(::google::protobuf::RpcController* controller,
                                      const ::starrocks::lake::PublishVersionRequest* request,
                                      ::starrocks::lake::PublishVersionResponse* response,
                                      ::google::protobuf::Closure* done) {
    LOG(INFO) << "Publishing version";
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
    auto context = std::make_shared<PublishVersionContext>(_env, guard.release(), request, response);

    for (const auto& tablet_id : request->tablet_ids()) {
        auto task = std::make_shared<PublishVersionTask>(tablet_id, context);
        auto st = thread_pool->submit(std::move(task));
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit publish version task: " << st;
            std::lock_guard l(context->_response_mtx);
            response->add_failed_tablets(tablet_id);
        }
    }

#ifdef BE_TEST
    context->wait();
#endif
}

void LakeServiceImpl::abort_txn(::google::protobuf::RpcController* controller,
                                const ::starrocks::lake::AbortTxnRequest* request,
                                ::starrocks::lake::AbortTxnResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    (void)controller;

    // TODO: move the execution to TaskWorkerPool
    // This rpc never fail.
    for (const auto& tablet_id : request->tablet_ids()) {
        auto tablet = _env->lake_tablet_manager()->get_tablet(tablet_id);
        if (!tablet.ok()) {
            LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << tablet.status();
            continue;
        }
        // TODO: batch deletion
        for (const auto& txn_id : request->txn_ids()) {
            auto st = tablet->delete_txn_log(txn_id);
            LOG_IF(WARNING, !st.ok()) << "Fail to delete " << tablet->txn_log_path(txn_id) << ": " << st;
        }
    }
}

} // namespace starrocks
