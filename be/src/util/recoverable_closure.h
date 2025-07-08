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

} // namespace starrocks