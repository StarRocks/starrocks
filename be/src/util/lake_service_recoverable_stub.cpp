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

#include "util/lake_service_recoverable_stub.h"

#include <utility>

#include "common/config.h"

namespace starrocks {

LakeService_RecoverableStub::LakeService_RecoverableStub(const butil::EndPoint& endpoint, std::string protocol)
        : _endpoint(endpoint), _protocol(std::move(protocol)) {}

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
        options.connection_group = std::to_string(next_connection_group);
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
    using RecoverableClosureType = RecoverableClosure<LakeService_RecoverableStub>;
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->publish_version(controller, request, response, closure);
}

void LakeService_RecoverableStub::compact(::google::protobuf::RpcController* controller,
                                          const ::starrocks::CompactRequest* request,
                                          ::starrocks::CompactResponse* response, ::google::protobuf::Closure* done) {
    using RecoverableClosureType = RecoverableClosure<LakeService_RecoverableStub>;
    auto closure = new RecoverableClosureType(shared_from_this(), controller, done);
    stub()->compact(controller, request, response, closure);
}

} // namespace starrocks