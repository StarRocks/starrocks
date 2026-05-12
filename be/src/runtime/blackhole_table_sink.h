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

#include "exec/data_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class BlackHoleTableSink final : public DataSink {
public:
    explicit BlackHoleTableSink(ObjectPool* pool) : _pool(pool){};
    ~BlackHoleTableSink() override = default;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override { return Status::OK(); }

<<<<<<< HEAD:be/src/runtime/blackhole_table_sink.h
    RuntimeProfile* profile() override { return _profile; }
=======
    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_failed_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_GAUGE(staros_shard_count, MetricUnit::NOUNIT);
>>>>>>> 1ebdffab01 ([Enhancement] Expose staros_shard_count metric on BE StarOSWorker (#73096)):be/src/staros_integration/staros_worker_metrics.h

private:
    ObjectPool* _pool;
    RuntimeProfile* _profile = nullptr;
};
} // namespace starrocks
