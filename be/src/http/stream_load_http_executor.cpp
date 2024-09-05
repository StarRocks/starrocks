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

#include "http/stream_load_http_executor.h"

#include "util/starrocks_metrics.h"

namespace starrocks {

Status StreamLoadHttpExecutor::init() {
    Status st = ThreadPoolBuilder("stream_load_http")
                        .set_min_threads(config::stream_load_async_handle_thread_pool_num_min)
                        .set_max_threads(config::stream_load_async_handle_thread_pool_num_max)
                        .set_max_queue_size(config::stream_load_async_handle_thread_pool_queue_size)
                        .set_idle_timeout(
                                MonoDelta::FromMilliseconds(config::stream_load_async_handle_thread_pool_idle_time_ms))
                        .build(&_thread_pool);
    if (st.ok()) {
        REGISTER_THREAD_POOL_METRICS(stream_load_http, _thread_pool.get());
    }
    return st;
}

void StreamLoadHttpExecutor::stop() {
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
}

} // namespace starrocks