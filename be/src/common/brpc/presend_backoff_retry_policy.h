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

#include <brpc/retry_policy.h>

#include "common/config_rpc_client_fwd.h"

namespace starrocks {

// Gives brpc's retries time to see a peer's re-established connection.
//
// A restarted peer keeps the cached socket marked failed until the background health check
// succeeds (`-health_check_interval`, 3s by default). brpc retries immediately by default, so all
// `max_retry` attempts of an RPC that spans the restart are spent within microseconds and the RPC
// fails with EHOSTDOWN although the peer is already listening again. Backing off between attempts
// lets the health check repair the socket before the attempts run out.
//
// Only the errors raised before the request is written (see `Controller::IssueRPC`) are backed off:
// failing fast is still the best answer to a dead peer, and errors raised after the request may
// have reached the peer must not be delayed.
class PreSendBackoffRetryPolicy final : public brpc::RpcRetryPolicy {
public:
    int32_t GetBackoffTimeMs(const brpc::Controller* controller) const override {
        const int error_code = controller->ErrorCode();
        if (error_code != EHOSTDOWN && error_code != ECONNREFUSED) {
            return 0;
        }
        return config::brpc_retry_backoff_ms > 0 ? config::brpc_retry_backoff_ms : 0;
    }

    // Backoff is skipped inside a pthread unless it is allowed explicitly.
    bool CanRetryBackoffInPthread() const override { return true; }
};

// The policy holds no state (it reads `brpc_retry_backoff_ms` on every call), so one instance serves
// all channels. Install it unconditionally: a channel created while the backoff is disabled must
// still pick up the backoff once the setting is changed at runtime, and the policy returns 0 then.
inline const brpc::RetryPolicy* pre_send_backoff_retry_policy() {
    static const PreSendBackoffRetryPolicy policy;
    return &policy;
}

} // namespace starrocks
