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

namespace starrocks::fs {

// AWS SDK clients created on this thread while an instance is alive get the SDK's own curl HTTP client, also
// when object storage fast retry is enabled (enable_object_storage_fast_retry). BE creates the clients for its
// own object storage access (external tables, unload, backup) in such a scope, so fast retry only applies to
// the clients that starlet creates for shared-data storage.
class ScopedPlainAwsHttpClient {
public:
    ScopedPlainAwsHttpClient() : _outer(active_flag()) { active_flag() = true; }
    ~ScopedPlainAwsHttpClient() { active_flag() = _outer; }

    ScopedPlainAwsHttpClient(const ScopedPlainAwsHttpClient&) = delete;
    ScopedPlainAwsHttpClient& operator=(const ScopedPlainAwsHttpClient&) = delete;

    static bool active() { return active_flag(); }

private:
    static bool& active_flag() {
        static thread_local bool active = false;
        return active;
    }

    const bool _outer;
};

} // namespace starrocks::fs
