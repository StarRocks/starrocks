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

#include <atomic>
#include <cstdint>

namespace starrocks::io {

// Process-global S3 throttle window, shared across all S3 clients and streams.
//
// Producer: the fs/s3 retry strategy pushes the window out when S3 answers 429/503.
// Consumer: S3 input streams wait out the window before issuing a request, so every
// thread backs off together instead of hammering a throttling endpoint.
//
// Lives in the io layer so both libIO (s3_input_stream, the consumer) and libFS
// (S3RetryStrategy, the producer; fs depends on io) resolve the same single instance.
// A header-only/static-inline version would give each shared object its own copy and
// break the cross-client sharing this is meant to provide.
class S3GlobalThrottle {
public:
    // Wait out any active throttle window. Call before issuing an S3 request.
    static void wait();

    // Push the throttle deadline out to now + delay_ms, keeping the latest deadline.
    static void record_backoff(uint64_t delay_ms);

private:
    static std::atomic<uint64_t> _next_retry_time_ms;
};

} // namespace starrocks::io
