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
#include <span>

#include "gen_cpp/lake_service.pb.h"

namespace starrocks {

class ExecEnv;

namespace lake {
class TabletManager;
}

class LakeServiceImpl : public ::starrocks::LakeService {
public:
    explicit LakeServiceImpl(ExecEnv* env, lake::TabletManager* tablet_mgr);

    ~LakeServiceImpl() override;

    void publish_version(::google::protobuf::RpcController* controller,
                         const ::starrocks::PublishVersionRequest* request,
                         ::starrocks::PublishVersionResponse* response, ::google::protobuf::Closure* done) override;

    void abort_txn(::google::protobuf::RpcController* controller, const ::starrocks::AbortTxnRequest* request,
                   ::starrocks::AbortTxnResponse* response, ::google::protobuf::Closure* done) override;

    void delete_tablet(::google::protobuf::RpcController* controller, const ::starrocks::DeleteTabletRequest* request,
                       ::starrocks::DeleteTabletResponse* response, ::google::protobuf::Closure* done) override;

    void delete_txn_log(::google::protobuf::RpcController* controller, const ::starrocks::DeleteTxnLogRequest* request,
                        ::starrocks::DeleteTxnLogResponse* response, ::google::protobuf::Closure* done) override;

    void compact(::google::protobuf::RpcController* controller, const ::starrocks::CompactRequest* request,
                 ::starrocks::CompactResponse* response, ::google::protobuf::Closure* done) override;

    void drop_table(::google::protobuf::RpcController* controller, const ::starrocks::DropTableRequest* request,
                    ::starrocks::DropTableResponse* response, ::google::protobuf::Closure* done) override;

    void delete_data(::google::protobuf::RpcController* controller, const ::starrocks::DeleteDataRequest* request,
                     ::starrocks::DeleteDataResponse* response, ::google::protobuf::Closure* done) override;

    void get_tablet_stats(::google::protobuf::RpcController* controller, const ::starrocks::TabletStatRequest* request,
                          ::starrocks::TabletStatResponse* response, ::google::protobuf::Closure* done) override;

    void publish_log_version(::google::protobuf::RpcController* controller,
                             const ::starrocks::PublishLogVersionRequest* request,
                             ::starrocks::PublishLogVersionResponse* response,
                             ::google::protobuf::Closure* done) override;

    void publish_log_version_batch(::google::protobuf::RpcController* controller,
                                   const ::starrocks::PublishLogVersionBatchRequest* request,
                                   ::starrocks::PublishLogVersionResponse* response,
                                   ::google::protobuf::Closure* done) override;

    void lock_tablet_metadata(::google::protobuf::RpcController* controller,
                              const ::starrocks::LockTabletMetadataRequest* request,
                              ::starrocks::LockTabletMetadataResponse* response,
                              ::google::protobuf::Closure* done) override;

    void unlock_tablet_metadata(::google::protobuf::RpcController* controller,
                                const ::starrocks::UnlockTabletMetadataRequest* request,
                                ::starrocks::UnlockTabletMetadataResponse* response,
                                ::google::protobuf::Closure* done) override;

    void upload_snapshots(::google::protobuf::RpcController* controller,
                          const ::starrocks::UploadSnapshotsRequest* request,
                          ::starrocks::UploadSnapshotsResponse* response, ::google::protobuf::Closure* done) override;

    void restore_snapshots(::google::protobuf::RpcController* controller,
                           const ::starrocks::RestoreSnapshotsRequest* request,
                           ::starrocks::RestoreSnapshotsResponse* response, ::google::protobuf::Closure* done) override;

    void abort_compaction(::google::protobuf::RpcController* controller,
                          const ::starrocks::AbortCompactionRequest* request,
                          ::starrocks::AbortCompactionResponse* response, ::google::protobuf::Closure* done) override;

    void vacuum(::google::protobuf::RpcController* controller, const ::starrocks::VacuumRequest* request,
                ::starrocks::VacuumResponse* response, ::google::protobuf::Closure* done) override;

    void vacuum_full(::google::protobuf::RpcController* controller, const ::starrocks::VacuumFullRequest* request,
                     ::starrocks::VacuumFullResponse* response, ::google::protobuf::Closure* done) override;

private:
    void _submit_publish_log_version_task(const int64_t* tablet_ids, size_t tablet_size,
                                          std::span<const TxnInfoPB> txn_infos, const int64_t* log_versions,
                                          ::starrocks::PublishLogVersionResponse* response);

private:
    static constexpr int64_t kDefaultTimeoutForGetTabletStat = 5 * 60 * 1000L;  // 5 minutes
    static constexpr int64_t kDefaultTimeoutForPublishVersion = 1 * 60 * 1000L; // 1 minute

    ExecEnv* _env;
    lake::TabletManager* _tablet_mgr;
};

} // namespace starrocks
