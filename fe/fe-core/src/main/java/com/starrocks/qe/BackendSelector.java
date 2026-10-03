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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;

public interface BackendSelector {
    void computeScanRangeAssignment() throws StarRocksException;

    /**
     * Appends the incremental-scan-range sentinel (an empty scan range carrying the has_more flag)
     * to every worker's assignment, so running instances learn whether more batches will follow.
     */
    static void appendIncrementalScanRangeSentinel(ScanNode scanNode, WorkerProvider workerProvider,
                                                   FragmentScanRangeAssignment assignment) {
        TScanRangeParams end = new TScanRangeParams();
        end.setScan_range(new TScanRange());
        end.setEmpty(true);
        end.setHas_more(scanNode.hasMoreScanRanges());
        for (ComputeNode computeNode : workerProvider.getAllWorkers()) {
            assignment.put(computeNode.getId(), scanNode.getId().asInt(), end);
        }
    }
}
