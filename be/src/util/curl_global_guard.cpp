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

#include "util/curl_global_guard.h"

#include <curl/curl.h>

namespace starrocks {

int CurlGlobalGuard::init() {
    auto ret = curl_global_init(CURL_GLOBAL_ALL);
    _initialized = (ret == CURLE_OK);
    return ret;
}

CurlGlobalGuard::~CurlGlobalGuard() {
    if (_initialized) {
        curl_global_cleanup();
    }
}

} // namespace starrocks
