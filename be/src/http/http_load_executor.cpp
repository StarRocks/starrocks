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

#include "http/http_load_executor.h"

namespace starrocks {

Status HttpLoadExecutor::init() {
    return ThreadPoolBuilder("http_load_io")
            .set_min_threads(config::http_load_thread_pool_num_min)
            .set_max_threads(INT32_MAX)
            .set_max_queue_size(INT32_MAX)
            .set_idle_timeout(MonoDelta::FromMilliseconds(config::http_load_thread_pool_idle_time_ms))
            .build(&_thread_pool);
}

void HttpLoadExecutor::stop() {
    if (_thread_pool) {
        _thread_pool->shutdown();
    }
}

} // namespace starrocks
