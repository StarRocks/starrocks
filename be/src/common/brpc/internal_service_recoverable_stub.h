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

#include "base/brpc/recoverable_closure.h"
#include "base/status.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

class PInternalService_RecoverableStub : public PInternalService_Stub,
                                         public std::enable_shared_from_this<PInternalService_RecoverableStub> {
public:
    using RecoverableClosureType = RecoverableClosure<PInternalService_RecoverableStub>;

    PInternalService_RecoverableStub(const butil::EndPoint& endpoint, std::string protocol = "");
    ~PInternalService_RecoverableStub() override;

    Status reset_channel(int64_t next_connection_group = 0);

    std::shared_ptr<starrocks::PInternalService_Stub> stub() const {
        std::shared_lock l(_mutex);
        return _stub;
    }

    int64_t connection_group() const { return _connection_group.load(); }

private:
    std::shared_ptr<starrocks::PInternalService_Stub> _stub;
    const butil::EndPoint _endpoint;
    std::atomic<int64_t> _connection_group = 0;
    mutable std::shared_mutex _mutex;
    std::string _protocol;

    GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(PInternalService_RecoverableStub);
};

} // namespace starrocks
