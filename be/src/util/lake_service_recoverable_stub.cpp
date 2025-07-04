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
        : BaseRecoverableStub(endpoint, std::move(protocol)) {}

LakeService_RecoverableStub::~LakeService_RecoverableStub() = default;

Status LakeService_RecoverableStub::reset_channel(int64_t next_connection_group) {
    return reset_channel_base(next_connection_group, nullptr);
}

Status LakeService_RecoverableStub::reset_channel_impl(std::unique_ptr<brpc::Channel> channel,
                                                       int64_t next_connection_group) {
    auto ptr = std::make_shared<LakeService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    std::unique_lock l(_mutex);
    if (next_connection_group == _connection_group.load() + 1) {
        ++_connection_group;
        _stub = ptr;
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

} // namespace starrocks