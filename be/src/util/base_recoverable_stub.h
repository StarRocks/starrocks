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

#include <memory>
#include <shared_mutex>

#include "common/status.h"
#include "service/brpc.h"

namespace starrocks {

template <typename StubType>
class RecoverableClosure : public ::google::protobuf::Closure {
public:
    RecoverableClosure(std::shared_ptr<StubType> stub, ::google::protobuf::RpcController* controller,
                       ::google::protobuf::Closure* done)
            : _stub(std::move(stub)),
              _controller(controller),
              _done(done),
              _next_connection_group(_stub->connection_group() + 1) {}

    void Run() override {
        auto* cntl = static_cast<brpc::Controller*>(_controller);
        if (cntl->Failed() && cntl->ErrorCode() == EHOSTDOWN) {
            auto st = _stub->reset_channel(_next_connection_group);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to reset channel: " << st.to_string();
            }
        }
        _done->Run();
        delete this;
    }

private:
    std::shared_ptr<StubType> _stub;
    ::google::protobuf::RpcController* _controller;
    ::google::protobuf::Closure* _done;
    int64_t _next_connection_group;
};

class BaseRecoverableStub {
public:
    BaseRecoverableStub(const butil::EndPoint& endpoint, std::string protocol)
            : _endpoint(endpoint), _protocol(std::move(protocol)), _connection_group(0) {}

    virtual ~BaseRecoverableStub() = default;

    int64_t connection_group() const { return _connection_group.load(); }

    Status reset_channel_base(int64_t next_connection_group,
                              std::function<void(brpc::ChannelOptions&)> options_customizer) {
        if (next_connection_group == 0) {
            next_connection_group = _connection_group.load() + 1;
        }
        if (next_connection_group != _connection_group + 1) {
            return Status::OK();
        }

        brpc::ChannelOptions options;
        options.connect_timeout_ms = config::rpc_connect_timeout_ms;
        options.max_retry = 3;

        if (options_customizer) {
            options_customizer(options);
        }

        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
        if (channel->Init(_endpoint, &options)) {
            LOG(WARNING) << "Fail to init channel " << _endpoint;
            return Status::InternalError("Fail to init channel");
        }

        return reset_channel_impl(std::move(channel), next_connection_group);
    }

protected:
    virtual Status reset_channel_impl(std::unique_ptr<brpc::Channel> channel, int64_t next_connection_group) = 0;

    butil::EndPoint _endpoint;
    std::string _protocol;
    std::atomic<int64_t> _connection_group;
    mutable std::shared_mutex _mutex;
};

} // namespace starrocks