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

#include <chrono>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/statusor.h"

namespace starrocks {

class DNSCache {
public:
    explicit DNSCache();
    ~DNSCache();

    // get ip by hostname
    StatusOr<std::string> get(const std::string& hostname);
    // update the ip of hostname in cache
    Status update(const std::string& hostname);
    // background update host to ip mapping
    void refresh();

private:
    // hostname -> ip
    std::unordered_map<std::string, std::string> cache;
    mutable std::shared_mutex mutex;
};

} // namespace starrocks