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

namespace starrocks {

// RAII guard around curl_global_init / curl_global_cleanup. Call init() once
// at process startup; failure to initialize is reported via the return code so
// the caller can choose how to fail. The destructor runs curl_global_cleanup
// only if init() succeeded.
class CurlGlobalGuard {
public:
    CurlGlobalGuard() = default;
    ~CurlGlobalGuard();

    CurlGlobalGuard(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard& operator=(const CurlGlobalGuard&) = delete;
    CurlGlobalGuard(CurlGlobalGuard&&) = delete;
    CurlGlobalGuard& operator=(CurlGlobalGuard&&) = delete;

    // Returns the underlying CURLcode (0 on success).
    int init();

private:
    bool _initialized = false;
};

} // namespace starrocks
