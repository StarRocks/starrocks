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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TKeyRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.UtFrameUtils;

import java.util.List;

/**
 * Plans a statement and collects the partition column ranges FE attaches to every OLAP scan range,
 * paired with the physical partition id the scan range belongs to. Shared by the tests that pin down
 * what dynamic partition pruning hands to BE.
 */
final class ScanRangeKeyRanges {
    private ScanRangeKeyRanges() {
    }

    /** Every (physical partition id, key range) pair in the plan, in scan range order. */
    static List<Pair<Long, TKeyRange>> collect(ConnectContext connectContext, String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        List<Pair<Long, TKeyRange>> ranges = Lists.newArrayList();
        for (ScanNode scanNode : plan.second.getScanNodes()) {
            for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
                TInternalScanRange range = locations.getScan_range().getInternal_scan_range();
                if (range == null || range.getPartition_column_ranges() == null) {
                    continue;
                }
                for (TKeyRange keyRange : range.getPartition_column_ranges()) {
                    ranges.add(Pair.create(range.getPartition_id(), keyRange));
                }
            }
        }
        return ranges;
    }
}
