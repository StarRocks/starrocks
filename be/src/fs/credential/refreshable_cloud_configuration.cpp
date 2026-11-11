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

#include "fs/credential/refreshable_cloud_configuration.h"

#include <limits>

#include "common/logging.h"

namespace starrocks {

namespace {
int64_t next_fetch_of(const TCloudConfiguration& conf) {
    return conf.__isset.refresh_after_ms ? conf.refresh_after_ms : std::numeric_limits<int64_t>::max();
}
} // namespace

RefreshableCloudConfiguration::RefreshableCloudConfiguration(const TCloudConfiguration* initial, bool refresh_enabled)
        : _refreshable(refresh_enabled && initial != nullptr && initial->__isset.refresh_after_ms),
          _current(initial == nullptr ? nullptr : std::make_shared<const TCloudConfiguration>(*initial)),
          _next_fetch_ms(initial == nullptr ? std::numeric_limits<int64_t>::max() : next_fetch_of(*initial)) {}

std::shared_ptr<const TCloudConfiguration> RefreshableCloudConfiguration::get(const Fetcher& fetch,
                                                                              const Clock& clock) const {
    if (!_refreshable) {
        return _current;
    }
    const int64_t now_ms = clock();
    {
        std::lock_guard l(_mutex);
        if (now_ms < _next_fetch_ms) {
            return _current;
        }
    }
    std::lock_guard fetching(_fetch_mutex);
    std::shared_ptr<const TCloudConfiguration> current;
    {
        // A fetch that finished while this caller waited, successful or not, has moved _next_fetch_ms on.
        std::lock_guard l(_mutex);
        if (now_ms < _next_fetch_ms) {
            return _current;
        }
        current = _current;
    }

    auto fetched = fetch(*current);
    const int64_t done_ms = clock();

    std::lock_guard l(_mutex);
    if (fetched.ok()) {
        int64_t next = next_fetch_of(fetched.value());
        _current = std::make_shared<const TCloudConfiguration>(std::move(fetched).value());
        // A renewal that is already due again would be fetched on every open; wait like after a failure.
        _next_fetch_ms = next > done_ms ? next : done_ms + kRetryIntervalMs;
        LOG(INFO) << "Refreshed scan credentials, next refresh after " << _next_fetch_ms;
    } else {
        LOG(WARNING) << "Failed to refresh scan credentials, keep using the current ones and retry in "
                     << kRetryIntervalMs / 1000 << "s: " << fetched.status();
        _next_fetch_ms = done_ms + kRetryIntervalMs;
    }
    return _current;
}

} // namespace starrocks
