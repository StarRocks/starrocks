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

#include "common/brpc/lake_service_recoverable_stub.h"

#include <memory>
#include <utility>

#include "common/config_rpc_client_fwd.h"

namespace starrocks {

namespace {

class LakeRpcInFlightClosure : public google::protobuf::Closure {
public:
    LakeRpcInFlightClosure(BrpcConnectionLoadGuard reservation, google::protobuf::Closure* done)
            : _reservation(std::move(reservation)), _done(done) {}

    void Run() override {
        _reservation.reset();
        _done->Run();
        delete this;
    }

private:
    BrpcConnectionLoadGuard _reservation;
    google::protobuf::Closure* _done;
};

template <typename Request, typename Response>
void call_with_accounting(LakeService_RecoverableStub* owner, const std::shared_ptr<BrpcConnectionLoad>& load,
                          void (LakeService_Stub::*method)(google::protobuf::RpcController*, const Request*, Response*,
                                                           google::protobuf::Closure*),
                          google::protobuf::RpcController* controller, const Request* request, Response* response,
                          google::protobuf::Closure* done) {
    BrpcConnectionLoadGuard reservation;
    google::protobuf::Closure* closure = done;
    if (config::brpc_connection_type == "single") {
        reservation = load->reserve(brpc_request_payload_bytes(request, controller));
        if (done != nullptr) {
            closure = new LakeRpcInFlightClosure(std::move(reservation), done);
        }
    }
    if (done != nullptr) {
        using RecoverableClosureType = RecoverableClosure<LakeService_RecoverableStub>;
        closure = new RecoverableClosureType(owner->shared_from_this(), controller, closure);
    }
    (owner->stub().get()->*method)(controller, request, response, closure);
}

} // namespace

LakeService_RecoverableStub::LakeService_RecoverableStub(const butil::EndPoint& endpoint, std::string protocol,
                                                         int64_t connection_group_seed)
        : _connection_load(get_brpc_connection_load(endpoint, protocol, connection_group_seed)),
          _endpoint(endpoint),
          _connection_group_seed(connection_group_seed),
          _protocol(std::move(protocol)) {}

LakeService_RecoverableStub::~LakeService_RecoverableStub() = default;

Status LakeService_RecoverableStub::reset_channel(int64_t next_connection_group) {
    if (next_connection_group == 0) {
        next_connection_group = _connection_group.load() + 1;
    }
    if (next_connection_group != _connection_group + 1) {
        // need to take int64_t overflow into consideration
        return Status::OK();
    }
    brpc::ChannelOptions options;
    options.connect_timeout_ms = config::rpc_connect_timeout_ms;
    if (!_protocol.empty()) {
        options.protocol = _protocol;
    }
    if (_protocol != "http") {
        // http does not support these.
        options.connection_type = config::brpc_connection_type;
        // The seed keeps sibling stubs on distinct connection_groups (distinct TCP connections under
        // connection_type=single); the epoch suffix lets a single stub obtain a fresh connection when recovering from
        // EHOSTDOWN.
        options.connection_group = std::to_string(_connection_group_seed) + "_" + std::to_string(next_connection_group);
    }
    options.max_retry = 3;
    std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
    if (channel->Init(_endpoint, &options)) {
        LOG(WARNING) << "Fail to init channel " << _endpoint;
        return Status::InternalError("Fail to init channel");
    }
    auto ptr = std::make_unique<LakeService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    std::unique_lock l(_mutex);
    if (next_connection_group == _connection_group.load() + 1) {
        // prevent the underlying _stub been reset again by the same epoch calls
        ++_connection_group;
        _stub.reset(ptr.release());
    }
    return Status::OK();
}

void LakeService_RecoverableStub::publish_version(::google::protobuf::RpcController* controller,
                                                  const ::starrocks::PublishVersionRequest* request,
                                                  ::starrocks::PublishVersionResponse* response,
                                                  ::google::protobuf::Closure* done) {
    call_with_accounting(this, _connection_load, &LakeService_Stub::publish_version, controller, request, response,
                         done);
}

void LakeService_RecoverableStub::compact(::google::protobuf::RpcController* controller,
                                          const ::starrocks::CompactRequest* request,
                                          ::starrocks::CompactResponse* response, ::google::protobuf::Closure* done) {
    call_with_accounting(this, _connection_load, &LakeService_Stub::compact, controller, request, response, done);
}

} // namespace starrocks
