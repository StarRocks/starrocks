// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/config.h"
#include "gen_cpp/lake_service.pb.h"
#include "util/threadpool.h"

namespace starrocks {

class ExecEnv;

class LakeServiceImpl : public ::starrocks::lake::LakeService {
public:
    explicit LakeServiceImpl(ExecEnv* env) : _env(env) {
        auto st = ThreadPoolBuilder("compact")
                          .set_min_threads(0)
                          .set_max_threads(config::compact_threads)
                          .set_max_queue_size(config::compact_thread_pool_queue_size)
                          .build(&(_compact_thread_pool));
        CHECK(st.ok()) << st;
    }

    ~LakeServiceImpl() override = default;

    void publish_version(::google::protobuf::RpcController* controller,
                         const ::starrocks::lake::PublishVersionRequest* request,
                         ::starrocks::lake::PublishVersionResponse* response,
                         ::google::protobuf::Closure* done) override;

    void abort_txn(::google::protobuf::RpcController* controller, const ::starrocks::lake::AbortTxnRequest* request,
                   ::starrocks::lake::AbortTxnResponse* response, ::google::protobuf::Closure* done) override;

    void delete_tablet(::google::protobuf::RpcController* controller,
                       const ::starrocks::lake::DeleteTabletRequest* request,
                       ::starrocks::lake::DeleteTabletResponse* response, ::google::protobuf::Closure* done) override;

    void compact(::google::protobuf::RpcController* controller, const ::starrocks::lake::CompactRequest* request,
                 ::starrocks::lake::CompactResponse* response, ::google::protobuf::Closure* done) override;

    void drop_table(::google::protobuf::RpcController* controller, const ::starrocks::lake::DropTableRequest* request,
                    ::starrocks::lake::DropTableResponse* response, ::google::protobuf::Closure* done) override;

    void delete_data(::google::protobuf::RpcController* controller, const ::starrocks::lake::DeleteDataRequest* request,
                     ::starrocks::lake::DeleteDataResponse* response, ::google::protobuf::Closure* done) override;

    void get_tablet_stats(::google::protobuf::RpcController* controller,
                          const ::starrocks::lake::TabletStatRequest* request,
                          ::starrocks::lake::TabletStatResponse* response, ::google::protobuf::Closure* done) override;

    void publish_log_version(::google::protobuf::RpcController* controller,
                             const ::starrocks::lake::PublishLogVersionRequest* request,
                             ::starrocks::lake::PublishLogVersionResponse* response,
                             ::google::protobuf::Closure* done) override;

    void lock_tablet_metadata(::google::protobuf::RpcController* controller,
                              const ::starrocks::lake::LockTabletMetadataRequest* request,
                              ::starrocks::lake::LockTabletMetadataResponse* response,
                              ::google::protobuf::Closure* done) override;

    void unlock_tablet_metadata(::google::protobuf::RpcController* controller,
                                const ::starrocks::lake::UnlockTabletMetadataRequest* request,
                                ::starrocks::lake::UnlockTabletMetadataResponse* response,
                                ::google::protobuf::Closure* done) override;

    void upload_snapshots(::google::protobuf::RpcController* controller,
                          const ::starrocks::lake::UploadSnapshotsRequest* request,
                          ::starrocks::lake::UploadSnapshotsResponse* response,
                          ::google::protobuf::Closure* done) override;

    void restore_snapshots(::google::protobuf::RpcController* controller,
                           const ::starrocks::lake::RestoreSnapshotsRequest* request,
                           ::starrocks::lake::RestoreSnapshotsResponse* response,
                           ::google::protobuf::Closure* done) override;

private:
    ExecEnv* _env;

    std::unique_ptr<ThreadPool> _compact_thread_pool;
};

} // namespace starrocks
