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

#include <curl/curl.h>
#include <gtest/gtest.h>

#include <cstdio>

#include "base/utility/defer_op.h"
#include "common/configbase.h"

int main(int argc, char** argv) {
    if (!starrocks::config::init(nullptr)) {
        std::fprintf(stderr, "failed to initialize config defaults\n");
        return 1;
    }
    const CURLcode curl_status = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_status != CURLE_OK) {
        std::fprintf(stderr, "failed to initialize libcurl, curl_status=%d\n", static_cast<int>(curl_status));
        return 1;
    }
    starrocks::DeferOp curl_cleanup([] { curl_global_cleanup(); });

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
