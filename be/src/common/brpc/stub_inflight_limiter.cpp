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

#include "common/brpc/stub_inflight_limiter.h"

#include "base/brpc/brpc.h"
#include "base/bthreads/util.h"
#include "base/logging.h"
#include "common/config_network_fwd.h"

namespace starrocks {

void reject_over_inflight_limit(const butil::EndPoint& endpoint, google::protobuf::RpcController* controller,
                                google::protobuf::Closure* done) {
    auto* cntl = static_cast<brpc::Controller*>(controller);
    cntl->SetFailed(EAGAIN, "%s has too many in-flight RPCs, raise brpc_max_inflight_rpc_per_stub (now %d)",
                    butil::endpoint2str(endpoint).c_str(), config::brpc_max_inflight_rpc_per_stub);
    if (done == nullptr) {
        return;
    }
    auto res = bthreads::start_bthread([done]() { done->Run(); });
    if (!res.ok()) {
        LOG(WARNING) << "Fail to run closure in a new bthread, running it in place: " << res.status();
        done->Run();
    }
}

} // namespace starrocks
