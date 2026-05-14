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

#include "io/s3_global_throttle.h"

#include <chrono>
#include <random>
#include <thread>

#include "common/logging.h"

namespace starrocks::io {

std::atomic<uint64_t> S3GlobalThrottle::_next_retry_time_ms{0};

static uint64_t now_ms() {
    return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
                    .count());
}

void S3GlobalThrottle::record_backoff(uint64_t delay_ms) {
    uint64_t new_time = now_ms() + delay_ms;
    uint64_t current = _next_retry_time_ms.load();

    // Only update if the new deadline is later.
    while (current < new_time) {
        if (_next_retry_time_ms.compare_exchange_weak(current, new_time)) {
            VLOG(12) << "Global S3 throttle updated, waiting " << delay_ms << " ms";
            break;
        }
    }
}

void S3GlobalThrottle::wait() {
    uint64_t next_time = _next_retry_time_ms.load();
    if (next_time == 0) return;

    uint64_t current = now_ms();
    if (current < next_time) {
        uint64_t wait_ms = next_time - current;

        // Add small jitter (0-10%) to prevent synchronized retries.
        // Use thread_local RNG to avoid data races and repeated initialization.
        thread_local std::mt19937 tl_rng{std::random_device{}()};
        std::uniform_real_distribution<double> dist(1.0, 1.1);
        wait_ms = static_cast<uint64_t>(wait_ms * dist(tl_rng));

        VLOG(12) << "Waiting " << wait_ms << " ms due to global S3 throttle";
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }
}

} // namespace starrocks::io
