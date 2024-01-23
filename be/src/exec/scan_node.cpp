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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/scan_node.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/scan_node.h"

#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/morsel.h"

namespace starrocks {

const std::string ScanNode::_s_bytes_read_counter = "BytesRead";
const std::string ScanNode::_s_rows_read_counter = "RowsRead";
const std::string ScanNode::_s_total_read_timer = "TotalRawReadTime(*)";
const std::string ScanNode::_s_total_throughput_counter = "TotalReadThroughput";
const std::string ScanNode::_s_materialize_tuple_timer = "MaterializeTupleTime(*)";
const std::string ScanNode::_s_num_disks_accessed_counter = "NumDiskAccess";
const std::string ScanNode::_s_scanner_thread_counters_prefix = "ScannerThreads";
const std::string ScanNode::_s_scanner_thread_total_wallclock_time = "ScannerThreadsTotalWallClockTime";

const string ScanNode::_s_num_scanner_threads_started = "NumScannerThreadsStarted";

Status ScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const TQueryOptions& options = state->query_options();
    if (options.__isset.io_tasks_per_scan_operator) {
        _io_tasks_per_scan_operator = options.io_tasks_per_scan_operator;
    }
    double mem_ratio = config::scan_use_query_mem_ratio;
    if (options.__isset.scan_use_query_mem_ratio) {
        mem_ratio = options.scan_use_query_mem_ratio;
    }
    if (runtime_state()->query_ctx()) {
        // Used in pipeline-engine
        _mem_limit = state->query_ctx()->get_static_query_mem_limit() * mem_ratio;
    } else if (runtime_state()->query_mem_tracker_ptr()) {
        // Fallback in non-pipeline
        _mem_limit = state->query_mem_tracker_ptr()->limit() * mem_ratio;
    }

    return Status::OK();
}

Status ScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _scanner_thread_counters = ADD_THREAD_COUNTERS(runtime_profile(), _s_scanner_thread_counters_prefix);
    _bytes_read_counter = ADD_COUNTER(runtime_profile(), _s_bytes_read_counter, TUnit::BYTES);
    //TODO: The _rows_read_counter == RowsReturned counter in exec node, there is no need to keep both of them
    _rows_read_counter = ADD_COUNTER(runtime_profile(), _s_rows_read_counter, TUnit::UNIT);
    _read_timer = ADD_TIMER(runtime_profile(), _s_total_read_timer);
#ifndef BE_TEST
    _total_throughput_counter = runtime_profile()->add_derived_counter(
            _s_total_throughput_counter, TUnit::BYTES_PER_SECOND,
            [bytes_read_counter = _bytes_read_counter, total_time_countger = runtime_profile()->total_time_counter()] {
                return RuntimeProfile::units_per_second(bytes_read_counter, total_time_countger);
            },
            "");
#endif
    _materialize_tuple_timer =
            ADD_CHILD_TIMER(runtime_profile(), _s_materialize_tuple_timer, _s_scanner_thread_total_wallclock_time);
    _num_disks_accessed_counter = ADD_COUNTER(runtime_profile(), _s_num_disks_accessed_counter, TUnit::UNIT);

    return Status::OK();
}

// Distribute morsels from a single queue to multiple queues
static std::map<int, pipeline::MorselQueuePtr> uniform_distribute_morsels(pipeline::MorselQueuePtr morsel_queue,
                                                                          int dop) {
    std::map<int, pipeline::Morsels> morsels_per_driver;
    int driver_seq = 0;
    while (!morsel_queue->empty()) {
        auto maybe_morsel = morsel_queue->try_get();
        DCHECK(maybe_morsel.ok());
        morsels_per_driver[driver_seq].push_back(std::move(maybe_morsel.value()));
        driver_seq = (driver_seq + 1) % dop;
    }
    std::map<int, pipeline::MorselQueuePtr> queue_per_driver;
    for (auto& [operator_seq, morsels] : morsels_per_driver) {
        queue_per_driver.emplace(operator_seq, std::make_unique<pipeline::FixedMorselQueue>(std::move(morsels)));
    }
    return queue_per_driver;
}

StatusOr<pipeline::MorselQueueFactoryPtr> ScanNode::convert_scan_range_to_morsel_queue_factory(
        const std::vector<TScanRangeParams>& global_scan_ranges,
        const std::map<int32_t, std::vector<TScanRangeParams>>& scan_ranges_per_driver_seq, int node_id,
        int pipeline_dop, bool enable_tablet_internal_parallel,
        TTabletInternalParallelMode::type tablet_internal_parallel_mode) {
    // if scan range is empty, we don't have to check for per-bucket-optimize
    // if we enable per-bucket-optimize, each scan_operator should be assign scan range by FE planner
    DCHECK(global_scan_ranges.empty() || !output_chunk_by_bucket() || !scan_ranges_per_driver_seq.empty());
    if (scan_ranges_per_driver_seq.empty()) {
        ASSIGN_OR_RETURN(auto morsel_queue,
                         convert_scan_range_to_morsel_queue(global_scan_ranges, node_id, pipeline_dop,
                                                            enable_tablet_internal_parallel,
                                                            tablet_internal_parallel_mode, global_scan_ranges.size()));
        int scan_dop = std::min<int>(std::max<int>(1, morsel_queue->max_degree_of_parallelism()), pipeline_dop);
        int io_parallelism = scan_dop * io_tasks_per_scan_operator();

        // If not so much morsels, try to assign morsel uniformly among operators to avoid data skew
        if (!always_shared_scan() && scan_dop > 1 && dynamic_cast<pipeline::FixedMorselQueue*>(morsel_queue.get()) &&
            morsel_queue->num_original_morsels() <= io_parallelism) {
            auto morsel_queue_map = uniform_distribute_morsels(std::move(morsel_queue), scan_dop);
            return std::make_unique<pipeline::IndividualMorselQueueFactory>(std::move(morsel_queue_map),
                                                                            /*could_local_shuffle*/ true);
        } else {
            return std::make_unique<pipeline::SharedMorselQueueFactory>(std::move(morsel_queue), scan_dop);
        }
    } else {
        size_t num_total_scan_ranges = 0;
        for (const auto& [_, scan_ranges] : scan_ranges_per_driver_seq) {
            num_total_scan_ranges += scan_ranges.size();
        }

        std::map<int, pipeline::MorselQueuePtr> queue_per_driver_seq;
        for (const auto& [dop, scan_ranges] : scan_ranges_per_driver_seq) {
            ASSIGN_OR_RETURN(auto queue, convert_scan_range_to_morsel_queue(
                                                 scan_ranges, node_id, pipeline_dop, enable_tablet_internal_parallel,
                                                 tablet_internal_parallel_mode, num_total_scan_ranges));
            queue_per_driver_seq.emplace(dop, std::move(queue));
        }

        if (output_chunk_by_bucket()) {
            return std::make_unique<pipeline::BucketSequenceMorselQueueFactory>(std::move(queue_per_driver_seq),
                                                                                /*could_local_shuffle*/ false);
        } else {
            return std::make_unique<pipeline::IndividualMorselQueueFactory>(std::move(queue_per_driver_seq),
                                                                            /*could_local_shuffle*/ false);
        }
    }
}

StatusOr<pipeline::MorselQueuePtr> ScanNode::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
        bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
        size_t num_total_scan_ranges) {
    pipeline::Morsels morsels;
    // If this scan node does not accept non-empty scan ranges, create a placeholder one.
    if (!accept_empty_scan_ranges() && scan_ranges.empty()) {
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, TScanRangeParams()));
    } else {
        for (const auto& scan_range : scan_ranges) {
            morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, scan_range));
        }
    }

    return std::make_unique<pipeline::FixedMorselQueue>(std::move(morsels));
}

void ScanNode::enable_shared_scan(bool enable) {
    _enable_shared_scan = enable;
}

bool ScanNode::is_shared_scan_enabled() const {
    return _enable_shared_scan;
}
} // namespace starrocks
