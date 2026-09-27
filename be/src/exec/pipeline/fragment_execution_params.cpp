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

#include "exec/pipeline/fragment_execution_params.h"

#include "gutil/map_util.h"

namespace starrocks::pipeline {

const std::vector<TScanRangeParams> UnifiedExecPlanFragmentParams::_no_scan_ranges;
const PerDriverScanRangesMap UnifiedExecPlanFragmentParams::_no_scan_ranges_per_driver_seq;

const std::vector<TScanRangeParams>& UnifiedExecPlanFragmentParams::scan_ranges_of_node(TPlanNodeId node_id) const {
    return FindWithDefault(_unique_request.params.per_node_scan_ranges, node_id, _no_scan_ranges);
}

const PerDriverScanRangesMap& UnifiedExecPlanFragmentParams::per_driver_seq_scan_ranges_of_node(
        TPlanNodeId node_id) const {
    if (!_unique_request.params.__isset.node_to_per_driver_seq_scan_ranges) {
        return _no_scan_ranges_per_driver_seq;
    }

    return FindWithDefault(_unique_request.params.node_to_per_driver_seq_scan_ranges, node_id,
                           _no_scan_ranges_per_driver_seq);
}

const TDataSink& UnifiedExecPlanFragmentParams::output_sink() const {
    if (_unique_request.fragment.__isset.output_sink) {
        return _unique_request.fragment.output_sink;
    }
    return _common_request.fragment.output_sink;
}

} // namespace starrocks::pipeline
