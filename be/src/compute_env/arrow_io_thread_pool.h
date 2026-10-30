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

#include <cstdint>

#include "common/status.h"

namespace starrocks {

// Largest value accepted for the arrow_io_thread_pool_size config. Arrow starts a worker only for a
// queued task, so this bounds the threads it may create under load. Arrow does not handle a failed
// thread start (std::thread throws std::system_error out of the task submission), so stay well
// below the 65535 threads per user that bin/common.sh allows the BE, which its own threads share.
inline constexpr int32_t kMaxArrowIOThreadPoolSize = 32768;

// The capacity Arrow gave its process-wide IO pool when it created it: ARROW_IO_THREADS when that
// holds a positive integer, 8 otherwise. Read before this process first resizes the pool, which
// only apply_arrow_io_thread_pool_size() does, so later resizes do not change it.
int arrow_io_thread_pool_default_capacity();

// Resizes Arrow's process-wide IO pool to `size` threads, or back to
// arrow_io_thread_pool_default_capacity() when `size` is 0. A negative size or one above
// kMaxArrowIOThreadPoolSize is rejected without touching the pool.
//
// Arrow applies the new capacity right away: growing starts workers for queued tasks at once,
// and shrinking lets each excess worker exit after the task it is running.
Status apply_arrow_io_thread_pool_size(int32_t size);

} // namespace starrocks
