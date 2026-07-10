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

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/hash/hash_std.hpp"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

struct ExpiredRemoteScanToken {
    std::string scan_token;
    TUniqueId fragment_instance_id;
    TStarRocksScanTransport::type transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
};

class RemoteScanTokenMgr {
public:
    struct TokenInfo {
        TUniqueId fragment_instance_id;
        TStarRocksScanTransport::type transport = TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
        int64_t expire_ms = 0;
        bool completed = false;
    };

    Status register_token(std::string token, const TUniqueId& fragment_instance_id,
                          TStarRocksScanTransport::type transport, int64_t expire_ms);
    // Marks a registered token as having cleanly produced its full result (EOS published).
    // Lets a later fetch tell a legitimately drained-and-erased queue (completed) apart from a
    // queue that vanished before the stream ever finished (an anomaly).
    Status mark_completed(const std::string& token);
    Status lookup(const std::string& token, TStarRocksScanTransport::type expected_transport,
                  TUniqueId* fragment_instance_id, bool* completed = nullptr);
    std::vector<ExpiredRemoteScanToken> cleanup_expired_tokens(int64_t now_ms);
    Status remove(const std::string& token);
    size_t size() const;

private:
    mutable std::mutex _lock;
    std::unordered_map<std::string, TokenInfo> _tokens;
};

} // namespace starrocks
