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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/scan_node.h

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

#pragma once

#include <cstddef>
#include <optional>
#include <string>

#include "column/column_access_path.h"
#include "exec/exec_node.h"
#include "gen_cpp/InternalService_types.h"
#include "util/runtime_profile.h"

namespace starrocks {

namespace pipeline {
class MorselQueue;
using MorselQueuePtr = std::unique_ptr<MorselQueue>;
class MorselQueueFactory;
using MorselQueueFactoryPtr = std::unique_ptr<MorselQueueFactory>;
} // namespace pipeline

class TScanRange;

// Abstract base class of all scan nodes; introduces set_scan_range().
//
// Includes ScanNode common counters:
//   BytesRead - total bytes read by this scan node
//
//   NumDisksAccessed - number of disks accessed.

//   AverageScannerThreadConcurrency - the average number of active scanner threads. A
//     scanner thread is considered active if it is not blocked by IO. This number would
//     be low (less than 1) for IO bounded queries. For cpu bounded queries, this number
//     would be close to the max scanner threads allowed.
//
//   ScanRangesComplete - number of scan ranges completed
//
//   ScannerThreadsTotalWallClockTime - total time spent in all scanner threads.
//
class ScanNode : public ExecNode {
public:
    ScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) : ExecNode(pool, tnode, descs) {}
    ~ScanNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    // Set up counters
    Status prepare(RuntimeState* state) override;

    // Convert scan_ranges into node-specific scan restrictions.  This should be
    // called after prepare()
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) = 0;
    StatusOr<pipeline::MorselQueueFactoryPtr> convert_scan_range_to_morsel_queue_factory(
            const std::vector<TScanRangeParams>& scan_ranges,
            const std::map<int32_t, std::vector<TScanRangeParams>>& scan_ranges_per_driver_seq, int node_id,
            int pipeline_dop, bool enable_tablet_internal_parallel,
            TTabletInternalParallelMode::type tablet_internal_parallel_mode);
    virtual StatusOr<pipeline::MorselQueuePtr> convert_scan_range_to_morsel_queue(
            const std::vector<TScanRangeParams>& scan_ranges, int node_id, int32_t pipeline_dop,
            bool enable_tablet_internal_parallel, TTabletInternalParallelMode::type tablet_internal_parallel_mode,
            size_t num_total_scan_ranges);

    // If this scan node accept empty scan ranges.
    virtual bool accept_empty_scan_ranges() const { return true; }

    bool is_scan_node() const override { return true; }

    RuntimeProfile::Counter* bytes_read_counter() const { return _bytes_read_counter; }
    RuntimeProfile::Counter* rows_read_counter() const { return _rows_read_counter; }
    RuntimeProfile::Counter* read_timer() const { return _read_timer; }
    RuntimeProfile::Counter* total_throughput_counter() const { return _total_throughput_counter; }
    RuntimeProfile::Counter* materialize_tuple_timer() const { return _materialize_tuple_timer; }
    RuntimeProfile::ThreadCounters* scanner_thread_counters() const { return _scanner_thread_counters; }

    // names of ScanNode common counters
    static const std::string _s_bytes_read_counter;
    static const std::string _s_rows_read_counter;
    static const std::string _s_total_read_timer;
    static const std::string _s_total_throughput_counter;
    static const std::string _s_num_disks_accessed_counter;
    static const std::string _s_materialize_tuple_timer;
    static const std::string _s_scanner_thread_counters_prefix;
    static const std::string _s_scanner_thread_total_wallclock_time;
    static const std::string _s_average_io_mgr_queue_capacity;
    static const std::string _s_num_scanner_threads_started;

    const std::string& name() const { return _name; }

    virtual int io_tasks_per_scan_operator() const { return _io_tasks_per_scan_operator; }
    virtual bool always_shared_scan() const { return false; }
    virtual bool output_chunk_by_bucket() const { return false; }
    virtual bool is_asc_hint() const { return true; }
    virtual std::optional<bool> partition_order_hint() const { return std::nullopt; }

    // TODO: support more share_scan strategy
    void enable_shared_scan(bool enable);
    bool is_shared_scan_enabled() const;

    const std::vector<ColumnAccessPathPtr>& column_access_paths() const { return _column_access_paths; }

protected:
    RuntimeProfile::Counter* _bytes_read_counter = nullptr; // # bytes read from the scanner
    // # rows/tuples read from the scanner (including those discarded by eval_conjucts())
    RuntimeProfile::Counter* _rows_read_counter = nullptr;
    RuntimeProfile::Counter* _read_timer = nullptr; // total read time
    // Wall based aggregate read throughput [bytes/sec]
    RuntimeProfile::Counter* _total_throughput_counter = nullptr;
    // Per thread read throughput [bytes/sec]
    RuntimeProfile::Counter* _num_disks_accessed_counter = nullptr;
    RuntimeProfile::Counter* _materialize_tuple_timer = nullptr; // time writing tuple slots
    // Aggregated scanner thread counters
    RuntimeProfile::ThreadCounters* _scanner_thread_counters = nullptr;
    RuntimeProfile::Counter* _num_scanner_threads_started_counter = nullptr;
    std::string _name;
    bool _enable_shared_scan = false;
    int64_t _mem_limit = 0;
    int32_t _io_tasks_per_scan_operator = config::io_tasks_per_scan_operator;

    std::vector<ColumnAccessPathPtr> _column_access_paths;
};

} // namespace starrocks
