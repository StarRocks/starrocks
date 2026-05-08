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

#include "util/internal_service_recoverable_stub.h"

#include <memory>

#include "common/config.h"

namespace starrocks {

// RecoverableChannel intercepts every RPC call and wraps the user-supplied done
// closure with a RecoverableClosure so that channel errors trigger an automatic
// channel reset on the owning stub.
class RecoverableChannel : public google::protobuf::RpcChannel {
public:
    explicit RecoverableChannel(PInternalService_RecoverableStub* owner) : _owner(owner) {}

    void CallMethod(const google::protobuf::MethodDescriptor* method, google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done) override {
        google::protobuf::Closure* closure = done;
        if (done != nullptr) {
            closure = new PInternalService_RecoverableStub::RecoverableClosureType(_owner->shared_from_this(),
                                                                                   controller, done);
        }
        _owner->stub()->CallMethod(method, controller, request, response, closure);
    }

private:
    PInternalService_RecoverableStub* _owner;
};

PInternalService_RecoverableStub::PInternalService_RecoverableStub(const butil::EndPoint& endpoint,
                                                                   std::string protocol)
        : PInternalService_Stub(new RecoverableChannel(this), google::protobuf::Service::STUB_OWNS_CHANNEL),
          _endpoint(endpoint),
          _protocol(std::move(protocol)) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

Status PInternalService_RecoverableStub::reset_channel(int64_t next_connection_group) {
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
    auto stub =
            std::make_shared<PInternalService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    std::unique_lock l(_mutex);
    if (next_connection_group == _connection_group.load() + 1) {
        // prevent the underlying _stub been reset again by the same epoch calls
        ++_connection_group;
        _stub = std::move(stub);
    }
    return Status::OK();
}

} // namespace starrocks
