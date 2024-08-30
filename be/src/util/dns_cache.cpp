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

#include "util/dns_cache.h"

#include <mutex>
#include <unordered_set>

#include "service/backend_options.h"
#include "util/network_util.h"

namespace starrocks {

DNSCache::DNSCache() {}

DNSCache::~DNSCache() {}

StatusOr<std::string> DNSCache::get(const std::string& hostname) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = cache.find(hostname);
        if (it != cache.end()) {
            return it->second;
        }
    }
    // Update if not found
    RETURN_IF_ERROR(update(hostname));
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return cache[hostname];
    }
}

Status DNSCache::update(const std::string& hostname) {
    std::string ip = "";
    RETURN_IF_ERROR(hostname_to_ip(hostname, ip, BackendOptions::is_bind_ipv6()));
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = cache.find(hostname);
    if (it == cache.end() || it->second != ip) {
        cache[hostname] = ip;
        LOG(INFO) << "update hostname " << hostname << "'s ip to " << ip;
    }
    return Status::OK();
}

void DNSCache::refresh() {
    std::unordered_set<std::string> keys;
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        std::transform(cache.begin(), cache.end(), std::inserter(keys, keys.end()),
                       [](const auto& pair) { return pair.first; });
    }
    for (auto& key : keys) {
        auto st = update(key);
        if (!st.ok()) {
            LOG(WARNING) << "update hostname " << key << " failed when refreshing dns cache";
        }
    }
}

} // namespace starrocks