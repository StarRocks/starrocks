// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "gen_cpp/lake_service.pb.h"

namespace starrocks {
class ExecEnv;
class Status;
} // namespace starrocks

namespace starrocks {

namespace lake {
class Tablet;
}

class LakeServiceImpl : public ::starrocks::lake::LakeService {
public:
    explicit LakeServiceImpl(ExecEnv* env) : _env(env) {}

    ~LakeServiceImpl() override = default;

    void publish_version(::google::protobuf::RpcController* controller,
                         const ::starrocks::lake::PublishVersionRequest* request,
                         ::starrocks::lake::PublishVersionResponse* response,
                         ::google::protobuf::Closure* done) override;

    void abort_txn(::google::protobuf::RpcController* controller, const ::starrocks::lake::AbortTxnRequest* request,
                   ::starrocks::lake::AbortTxnResponse* response, ::google::protobuf::Closure* done) override;

    void drop_tablet(::google::protobuf::RpcController* controller, const ::starrocks::lake::DropTabletRequest* request,
                     ::starrocks::lake::DropTabletResponse* response, ::google::protobuf::Closure* done) override;

private:
    ExecEnv* _env;
};

} // namespace starrocks
