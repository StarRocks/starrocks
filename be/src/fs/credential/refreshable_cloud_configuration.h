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

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "common/statusor.h"
#include "gen_cpp/CloudConfiguration_types.h"

namespace starrocks {

// A scan node's cloud configuration. When the FE marked it refreshable (refresh_after_ms is set, for vended
// credentials that expire), the first file open after that time fetches a newer one from the coordinator and
// every later open uses it.
//
// Most configurations are not refreshable; those are returned without taking any lock. Once a refreshable one is
// due, file opens wait for the single fetch in flight instead of opening with credentials that may have expired.
// A failed fetch keeps the current configuration and is retried after kRetryIntervalMs, which is how every scan
// behaved before credentials could be refreshed at all.
class RefreshableCloudConfiguration {
public:
    // Given the configuration being replaced, whose refresh_token the request must carry.
    using Fetcher = std::function<StatusOr<TCloudConfiguration>(const TCloudConfiguration& current)>;

    // Wall-clock epoch millis, compared against the FE's refresh_after_ms.
    using Clock = std::function<int64_t()>;

    static constexpr int64_t kRetryIntervalMs = 60 * 1000;

    // `initial` may be null: the scan has no cloud configuration and there is nothing to refresh. When
    // `refresh_enabled` is false, `initial` is used as is, however the FE marked it.
    explicit RefreshableCloudConfiguration(const TCloudConfiguration* initial, bool refresh_enabled = true);

    // The configuration to open a file with, or null when there is none. The clock is read again after a fetch:
    // a slow failure must still back off a full kRetryIntervalMs from when it ended.
    std::shared_ptr<const TCloudConfiguration> get(const Fetcher& fetch, const Clock& clock) const;

private:
    // Fixed at construction. When false, _current never changes and get() reads it without locking.
    const bool _refreshable;
    // Serializes fetches; callers that are due wait here for the one in flight.
    mutable std::mutex _fetch_mutex;
    // Guards the two fields below.
    mutable std::mutex _mutex;
    mutable std::shared_ptr<const TCloudConfiguration> _current;
    mutable int64_t _next_fetch_ms;
};

} // namespace starrocks
