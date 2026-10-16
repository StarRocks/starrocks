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
        PInternalService_RecoverableStub::RpcInFlightGuard reservation;
        google::protobuf::Closure* closure = done;
        if (config::brpc_connection_type == "single") {
            reservation = _owner->reserve_rpc(brpc_request_payload_bytes(request, controller));
            if (done != nullptr) {
                closure = new BrpcInFlightClosure(std::move(reservation), done);
            }
        }

        if (done != nullptr) {
            closure = new PInternalService_RecoverableStub::RecoverableClosureType(_owner->shared_from_this(),
                                                                                   controller, closure);
        }
        _owner->stub()->CallMethod(method, controller, request, response, closure);
    }

private:
    PInternalService_RecoverableStub* _owner;
};

PInternalService_RecoverableStub::PInternalService_RecoverableStub(const butil::EndPoint& endpoint,
                                                                   std::string protocol, int64_t connection_group_seed)
        : PInternalService_Stub(new RecoverableChannel(this), google::protobuf::Service::STUB_OWNS_CHANNEL),
          _connection_load(get_brpc_connection_load(endpoint, protocol, connection_group_seed)),
          _endpoint(endpoint),
          _connection_group_seed(connection_group_seed),
          _protocol(std::move(protocol)) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

PInternalService_RecoverableStub::RpcInFlightGuard::RpcInFlightGuard(
        std::shared_ptr<PInternalService_RecoverableStub> stub, int64_t payload_bytes)
        : _stub(std::move(stub)), _load_guard(_stub->_connection_load->reserve(payload_bytes)) {}

PInternalService_RecoverableStub::RpcInFlightGuard::~RpcInFlightGuard() {
    reset();
}

PInternalService_RecoverableStub::RpcInFlightGuard::RpcInFlightGuard(RpcInFlightGuard&& other) noexcept
        : _stub(std::move(other._stub)), _load_guard(std::move(other._load_guard)) {}

PInternalService_RecoverableStub::RpcInFlightGuard& PInternalService_RecoverableStub::RpcInFlightGuard::operator=(
        RpcInFlightGuard&& other) noexcept {
    if (this != &other) {
        reset();
        _stub = std::move(other._stub);
        _load_guard = std::move(other._load_guard);
    }
    return *this;
}

void PInternalService_RecoverableStub::RpcInFlightGuard::reset() {
    if (_stub != nullptr) {
        _load_guard.reset();
        _stub.reset();
    }
}

int64_t PInternalService_RecoverableStub::num_in_flight_rpcs() const {
    return _connection_load->num_in_flight_rpcs();
}

int64_t PInternalService_RecoverableStub::num_in_flight_payload_bytes() const {
    return _connection_load->num_in_flight_payload_bytes();
}

PInternalService_RecoverableStub::RpcInFlightGuard PInternalService_RecoverableStub::reserve_rpc(
        int64_t payload_bytes) {
    return RpcInFlightGuard(shared_from_this(), payload_bytes);
}

void PInternalService_RecoverableStub::transmit_chunk(RpcInFlightGuard reservation,
                                                      google::protobuf::RpcController* controller,
                                                      const PTransmitChunkParams* request,
                                                      PTransmitChunkResult* response, google::protobuf::Closure* done) {
    DCHECK_EQ(reservation.stub(), this);
    auto* accounting = new BrpcInFlightClosure(std::move(reservation), done);
    auto* closure = new RecoverableClosureType(shared_from_this(), controller, accounting);
    stub()->transmit_chunk(controller, request, response, closure);
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
