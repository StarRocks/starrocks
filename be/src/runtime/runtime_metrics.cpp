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

#include "runtime/runtime_metrics.h"

#include "gutil/macros.h"

namespace starrocks {

RuntimeMetrics* RuntimeMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new RuntimeMetrics();
    return instance;
}

void RuntimeMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_RUNTIME_METRIC(name) registry->register_metric(#name, &name)

    REGISTER_RUNTIME_METRIC(fragment_requests_total);
    REGISTER_RUNTIME_METRIC(fragment_request_duration_us);

    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_total);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_eos_total);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_memtable_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_writer_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_replica_duration_us);

    REGISTER_RUNTIME_METRIC(memory_pool_bytes_total);
    REGISTER_RUNTIME_METRIC(process_thread_num);
    REGISTER_RUNTIME_METRIC(process_fd_num_used);
    REGISTER_RUNTIME_METRIC(process_fd_num_limit_soft);
    REGISTER_RUNTIME_METRIC(process_fd_num_limit_hard);

    REGISTER_RUNTIME_METRIC(max_disk_io_util_percent);
    REGISTER_RUNTIME_METRIC(max_network_send_bytes_rate);
    REGISTER_RUNTIME_METRIC(max_network_receive_bytes_rate);

    REGISTER_RUNTIME_METRIC(exec_runtime_memory_size);

#undef REGISTER_RUNTIME_METRIC
}

} // namespace starrocks
