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

#include "util/misc.h"

#include "util/await.h"

namespace starrocks {

#ifdef BE_TEST
static constexpr int sleep_interval = 100 * 1000; // 100ms
#else
static constexpr int sleep_interval = 1 * 1000 * 1000; // 1 seconds
#endif

void nap_sleep(int32_t sleep_secs, const std::function<bool()>& stop_condition) {
    Awaitility await;
    await.timeout(sleep_secs * 1000LL * 1000LL).interval(sleep_interval);
    await.until(stop_condition);
}

} // namespace starrocks
