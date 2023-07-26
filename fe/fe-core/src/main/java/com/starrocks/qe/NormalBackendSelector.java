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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;

public class NormalBackendSelector implements BackendSelector {
    private static final Logger LOG = LogManager.getLogger(NormalBackendSelector.class);

    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;

    private final WorkerProvider workerProvider;
    private final boolean isLoad;

    public NormalBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                 FragmentScanRangeAssignment assignment, WorkerProvider workerProvider,
                                 boolean isLoad) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.isLoad = isLoad;
    }

    private boolean isEnableScheduleByRowCnt(TScanRangeLocations scanRangeLocations) {
        // only enable for load now, The insert into select performance problem caused by data skew is the most serious
        return Config.enable_schedule_insert_query_by_row_count && isLoad
                && scanRangeLocations.getScan_range().isSetInternal_scan_range()
                && scanRangeLocations.getScan_range().getInternal_scan_range().isSetRow_count()
                && scanRangeLocations.getScan_range().getInternal_scan_range().getRow_count() > 0;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        HashMap<TNetworkAddress, Long> assignedRowCountPerHost = Maps.newHashMap();
        // sort the scan ranges by row count
        // only sort the scan range when it is load job
        // but when there are too many scan ranges, we will not sort them since performance issue
        if (locations.size() < 10240 && !locations.isEmpty() && isEnableScheduleByRowCnt(locations.get(0))) {
            locations.sort((lhs, rhs) -> Long.compare(
                    rhs.getScan_range().getInternal_scan_range().getRow_count(),
                    lhs.getScan_range().getInternal_scan_range().getRow_count()));
        }

        for (TScanRangeLocations scanRangeLocations : locations) {
            // assign this scan range to the host w/ the fewest assigned row count
            Long minRowCount = Long.MAX_VALUE;
            TScanRangeLocation minLocation = null;
            for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                if (!workerProvider.isDataNodeAvailable(location.getBackend_id())) {
                    continue;
                }

                Long assignedBytes = assignedRowCountPerHost.getOrDefault(location.server, 0L);
                if (assignedBytes < minRowCount) {
                    minRowCount = assignedBytes;
                    minLocation = location;
                }
            }

            if (minLocation == null) {
                workerProvider.reportDataNodeNotFoundException();
            }
            Preconditions.checkNotNull(minLocation);

            // only enable for load now, The insert into select performance problem caused by data skew is the most serious
            long curRowCount;
            if (isEnableScheduleByRowCnt(scanRangeLocations)) {
                curRowCount = Math.max(1L, scanRangeLocations.getScan_range().getInternal_scan_range().getRow_count());
            } else {
                curRowCount = 1L;
            }
            assignedRowCountPerHost.put(minLocation.server, minRowCount + curRowCount);
            workerProvider.selectWorker(minLocation.backend_id);
            // add scan range
            TScanRangeParams scanRangeParams = new TScanRangeParams(scanRangeLocations.scan_range);
            assignment.put(minLocation.backend_id, scanNode.getId().asInt(), scanRangeParams);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("assignedRowCountPerHost: {}", assignedRowCountPerHost);
        }
    }
}