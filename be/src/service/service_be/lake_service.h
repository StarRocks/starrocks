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

#include "gen_cpp/lake_service.pb.h"

namespace starrocks {

class ExecEnv;

namespace lake {
class TabletManager;
}

class LakeServiceImpl : public ::starrocks::lake::LakeService {
public:
    explicit LakeServiceImpl(ExecEnv* env, lake::TabletManager* tablet_mgr);

    ~LakeServiceImpl() override;

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

    void abort_compaction(::google::protobuf::RpcController* controller,
                          const ::starrocks::lake::AbortCompactionRequest* request,
                          ::starrocks::lake::AbortCompactionResponse* response,
                          ::google::protobuf::Closure* done) override;

    void vacuum(::google::protobuf::RpcController* controller, const ::starrocks::lake::VacuumRequest* request,
                ::starrocks::lake::VacuumResponse* response, ::google::protobuf::Closure* done) override;

    void vacuum_full(::google::protobuf::RpcController* controller, const ::starrocks::lake::VacuumFullRequest* request,
                     ::starrocks::lake::VacuumFullResponse* response, ::google::protobuf::Closure* done) override;

private:
    ExecEnv* _env;
    lake::TabletManager* _tablet_mgr;
};

} // namespace starrocks
