// This file is made available under Elastic License 2.0.
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

#include "exec/pipeline/scan/morsel.h"
#include "runtime/exec_env.h"

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

Status ScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _scanner_thread_counters = ADD_THREAD_COUNTERS(runtime_profile(), _s_scanner_thread_counters_prefix);
    _bytes_read_counter = ADD_COUNTER(runtime_profile(), _s_bytes_read_counter, TUnit::BYTES);
    //TODO: The _rows_read_counter == RowsReturned counter in exec node, there is no need to keep both of them
    _rows_read_counter = ADD_COUNTER(runtime_profile(), _s_rows_read_counter, TUnit::UNIT);
    _read_timer = ADD_TIMER(runtime_profile(), _s_total_read_timer);
#ifndef BE_TEST
    _total_throughput_counter = runtime_profile()->add_rate_counter(_s_total_throughput_counter, _bytes_read_counter);
#endif
    _materialize_tuple_timer =
            ADD_CHILD_TIMER(runtime_profile(), _s_materialize_tuple_timer, _s_scanner_thread_total_wallclock_time);
    _num_disks_accessed_counter = ADD_COUNTER(runtime_profile(), _s_num_disks_accessed_counter, TUnit::UNIT);

    return Status::OK();
}

int ScanNode::_scan_dop(const std::vector<TScanRangeParams>& scan_ranges,
                        const TExecPlanFragmentParams& request) const {
    int dop = request.__isset.pipeline_dop ? request.pipeline_dop : 0;
    dop = ExecEnv::GetInstance()->calc_pipeline_dop(dop);
    dop = std::max(1, std::min((int)scan_ranges.size(), dop));
    return dop;
}

StatusOr<pipeline::MorselQueuePtr> ScanNode::convert_scan_range_to_morsel_queue(
        const std::vector<TScanRangeParams>& scan_ranges, int node_id, const TExecPlanFragmentParams& request) {
    pipeline::Morsels morsels;
    // If this scan node does not accept non-empty scan ranges, create a placeholder one.
    if (!accept_empty_scan_ranges() && scan_ranges.empty()) {
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, TScanRangeParams()));
    } else {
        for (const auto& scan_range : scan_ranges) {
            morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(node_id, scan_range));
        }
    }

    int dop = _scan_dop(scan_ranges, request);
    return std::make_unique<pipeline::FixedMorselQueue>(std::move(morsels), dop);
}

} // namespace starrocks
