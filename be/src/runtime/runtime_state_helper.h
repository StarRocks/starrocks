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
#include <memory>

#include "common/status.h"

namespace starrocks {

class ObjectPool;
class QueryStatisticsRecvr;
class RuntimeState;
struct TReportExecStatusParams;

class RuntimeStateHelper {
public:
    static void init_runtime_filter_port(RuntimeState* state);
    static ObjectPool* global_obj_pool(const RuntimeState* state);

    static Status create_error_log_file(RuntimeState* state);
    static Status create_rejected_record_file(RuntimeState* state);

    static std::shared_ptr<QueryStatisticsRecvr> query_recv(RuntimeState* state);
    static std::atomic_int64_t* mutable_total_spill_bytes(RuntimeState* state);

    static bool is_jit_enabled(const RuntimeState* state);
    static void update_load_datacache_metrics(const RuntimeState* state, TReportExecStatusParams* load_params);
};

} // namespace starrocks
