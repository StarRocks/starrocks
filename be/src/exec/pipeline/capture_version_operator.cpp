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

#include "exec/pipeline/capture_version_operator.h"

#include "common/logging.h"
#include "exec/olap_scan_node.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {
Status CaptureVersionOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _capture_tablet_rowsets_timer = ADD_TIMER(_unique_metrics, "CaptureTabletRowsetsTime");

    return Status::OK();
}

StatusOr<ChunkPtr> CaptureVersionOperator::pull_chunk(RuntimeState* state) {
    // do capture rowset here
    auto defer = DeferOp([this]() { _is_finished = true; });
    std::vector<std::vector<RowsetSharedPtr>> tablet_rowsets;

    {
        SCOPED_TIMER(_capture_tablet_rowsets_timer);
        auto scan_rages = _morsel_queue->prepare_olap_scan_ranges();
        std::vector<std::vector<RowsetSharedPtr>> tablet_rowsets;
        VLOG_QUERY << "capture rowset scan_ranges nums:" << scan_rages.size();
        for (auto& scan_range : scan_rages) {
            ASSIGN_OR_RETURN(TabletSharedPtr tablet, OlapScanNode::get_tablet(scan_range));
            ASSIGN_OR_RETURN(auto rowset, OlapScanNode::capture_tablet_rowsets(tablet, scan_range));
            tablet_rowsets.emplace_back(std::move(rowset));
        }
        //
        _rowset_release_guard = MultiRowsetReleaseGuard(std::move(tablet_rowsets), adopt_acquire_t{});
    }
    return nullptr;
}

} // namespace starrocks::pipeline
