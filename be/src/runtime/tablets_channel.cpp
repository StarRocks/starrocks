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

#include "runtime/tablets_channel.h"

#include "glog/logging.h"
#include "util/await.h"

namespace starrocks {

bool TabletsChannel::drain_senders(int64_t timeout, const std::string& log_msg) {
    int64_t check_interval = 10 * 1000;       // 10ms
    auto duration = std::chrono::seconds(60); // 60 seconds;
    auto start = std::chrono::steady_clock::now();
    auto next_logging = start + duration;

    auto cb = [&](int) {
        auto current = std::chrono::steady_clock::now();
        if (current > next_logging) {
            LOG(INFO) << log_msg << ", wait all sender close already "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(current - start).count()
                      << "ms still has " << _num_remaining_senders << " sender";
            next_logging += duration;
        }
    };

    Awaitility wait;
    auto cond = [&]() { return _num_remaining_senders.load(std::memory_order_acquire) == 0; };
    auto ret = wait.timeout(timeout).interval(check_interval).interval_callback(cb).until(cond);
    if (!ret) {
        LOG(INFO) << log_msg << " wait all sender close timeout " << timeout / 1000 << "ms still has "
                  << _num_remaining_senders << " sender";
    }
    return ret;
}

} // namespace starrocks
