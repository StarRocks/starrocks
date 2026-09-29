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

#include <limits>
#include <memory>

#include "common/config_rpc_client_fwd.h"

namespace starrocks {

namespace {

int64_t request_payload_bytes(const google::protobuf::Message* request, google::protobuf::RpcController* controller) {
    const size_t request_bytes = request != nullptr ? request->ByteSizeLong() : 0;
    const size_t attachment_bytes = static_cast<brpc::Controller*>(controller)->request_attachment().size();
    constexpr size_t max_payload_bytes = static_cast<size_t>(std::numeric_limits<int64_t>::max());
    if (request_bytes > max_payload_bytes || attachment_bytes > max_payload_bytes - request_bytes) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(request_bytes + attachment_bytes);
}

class RpcInFlightClosure : public google::protobuf::Closure {
public:
    RpcInFlightClosure(PInternalService_RecoverableStub::RpcInFlightGuard reservation, google::protobuf::Closure* done)
            : _reservation(std::move(reservation)), _done(done) {}

    void Run() override {
        _reservation.reset();
        _done->Run();
        delete this;
    }

private:
    PInternalService_RecoverableStub::RpcInFlightGuard _reservation;
    google::protobuf::Closure* _done;
};

} // namespace

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
            reservation = _owner->reserve_rpc(request_payload_bytes(request, controller));
            if (done != nullptr) {
                closure = new RpcInFlightClosure(std::move(reservation), done);
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
          _endpoint(endpoint),
          _connection_group_seed(connection_group_seed),
          _protocol(std::move(protocol)) {}

PInternalService_RecoverableStub::~PInternalService_RecoverableStub() = default;

PInternalService_RecoverableStub::RpcInFlightGuard::RpcInFlightGuard(
        std::shared_ptr<PInternalService_RecoverableStub> stub, int64_t payload_bytes)
        : _stub(std::move(stub)),
          _in_flight_before(_stub->_num_in_flight_rpcs.fetch_add(1)),
          _payload_bytes(payload_bytes) {
    _stub->_in_flight_payload_bytes.fetch_add(_payload_bytes);
}

PInternalService_RecoverableStub::RpcInFlightGuard::~RpcInFlightGuard() {
    reset();
}

PInternalService_RecoverableStub::RpcInFlightGuard::RpcInFlightGuard(RpcInFlightGuard&& other) noexcept
        : _stub(std::move(other._stub)),
          _in_flight_before(other._in_flight_before),
          _payload_bytes(other._payload_bytes) {}

PInternalService_RecoverableStub::RpcInFlightGuard& PInternalService_RecoverableStub::RpcInFlightGuard::operator=(
        RpcInFlightGuard&& other) noexcept {
    if (this != &other) {
        reset();
        _stub = std::move(other._stub);
        _in_flight_before = other._in_flight_before;
        _payload_bytes = other._payload_bytes;
    }
    return *this;
}

void PInternalService_RecoverableStub::RpcInFlightGuard::reset() {
    if (_stub != nullptr) {
        _stub->_num_in_flight_rpcs.fetch_sub(1);
        _stub->_in_flight_payload_bytes.fetch_sub(_payload_bytes);
        _stub.reset();
    }
}

void PInternalService_RecoverableStub::transmit_chunk(RpcInFlightGuard reservation,
                                                      google::protobuf::RpcController* controller,
                                                      const PTransmitChunkParams* request,
                                                      PTransmitChunkResult* response, google::protobuf::Closure* done) {
    DCHECK_EQ(reservation.stub(), this);
    auto* accounting = new RpcInFlightClosure(std::move(reservation), done);
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
