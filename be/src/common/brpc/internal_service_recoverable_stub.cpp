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

#include "common/brpc/internal_service_recoverable_stub.h"

#include <memory>

#include "base/utility/defer_op.h"
#include "common/config_network_fwd.h"
#include "common/config_rpc_client_fwd.h"

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
        if (!_owner->try_acquire_inflight()) {
            // Fail the controller but still hand the call to brpc instead of returning here.
            // Channel::CallMethod locks the correlation id and marks the controller used_by_rpc
            // before it notices the failure, then runs its own send-failure path, which destroys
            // the id and completes `done` off this stack. Returning early aborts the process in
            // ~Controller: a controller whose call_id was already taken -- SinkBuffer takes one to
            // join on -- fails a CHECK there unless brpc adopted it.
            // No RecoverableClosure is wrapped around `done` here. The call never reaches a socket,
            // so there is no channel error to recover from, and no slot was taken to release.
            mark_over_inflight_limit(_owner->endpoint(), controller);
            _owner->stub()->CallMethod(method, controller, request, response, done);
            return;
        }
        if (done == nullptr) {
            // Synchronous call: brpc blocks until the RPC ends, so no closure exists to release the
            // slot and it has to be released once the inner call returns.
            DeferOp release([this]() { _owner->release_inflight(); });
            _owner->stub()->CallMethod(method, controller, request, response, nullptr);
            return;
        }
        auto* closure = new PInternalService_RecoverableStub::RecoverableClosureType(_owner->shared_from_this(),
                                                                                     controller, done);
        _owner->stub()->CallMethod(method, controller, request, response, closure);
    }

private:
    PInternalService_RecoverableStub* _owner;
};

PInternalService_RecoverableStub::PInternalService_RecoverableStub(const butil::EndPoint& endpoint,
                                                                   std::string protocol, int64_t connection_group_seed)
        : PInternalService_Stub(new RecoverableChannel(this), google::protobuf::Service::STUB_OWNS_CHANNEL),
          _endpoint(endpoint),
          _connection_group_seed(connection_group_seed),
          _protocol(std::move(protocol)) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

bool PInternalService_RecoverableStub::try_acquire_inflight() {
    // Read the config on every call so the limit can be changed at runtime.
    if (_inflight_limiter.try_acquire(config::brpc_max_inflight_rpc_per_stub)) {
        return true;
    }
    _inflight_limiter.add_rejected();
    return false;
}

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
