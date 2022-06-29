// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "service/service_be/lake_service.h"

#include <brpc/controller.h>

#include "common/status.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

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

Status LakeServiceImpl::publish(lake::Tablet* tablet, const ::starrocks::lake::PublishVersionRequest* request) {
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

    // TODO move the execution to TaskWorkerPool
    for (const auto& tablet_id : request->tablet_ids()) {
        auto res = _env->lake_tablet_manager()->get_tablet(tablet_id);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to get tablet " << tablet_id << ": " << res.status();
            response->add_failed_tablets(tablet_id);
            continue;
        }
        lake::Tablet& tablet = res.value();
        auto st = publish(&tablet, request);
        if (!st.ok()) {
            response->add_failed_tablets(tablet_id);
        }
    }
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

void LakeServiceImpl::drop_tablet(::google::protobuf::RpcController* controller,
                         const ::starrocks::lake::DropTabletRequest* request,
                         ::starrocks::lake::DropTabletResponse* response,
                         ::google::protobuf::Closure* done) {

    brpc::ClosureGuard guard(done);
    auto cntl = static_cast<brpc::Controller*>(controller);

    if (request->tablet_ids_size() == 0) {
        cntl->SetFailed("missing tablet_ids");
        return;
    }

    for (const auto& tablet_id : request->tablet_ids()) {
        // for debug
        LOG(INFO) << " tablet_id in LakeServiceImpl::drop_tablet is " << tablet_id;
        auto res = _env->lake_tablet_manager()->drop_tablet(tablet_id);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to drop tablet " << tablet_id << ": " << res.get_error_msg();
            response->add_failed_tablets(tablet_id);
            continue;
        }
        // for debug
        LOG(INFO) << "drop tablet " << tablet_id << " succ";
    }
} 

} // namespace starrocks
