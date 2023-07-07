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

import com.google.common.collect.Lists;
import com.starrocks.planner.ScanNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.List;
import java.util.Map;

public class ReplicatedBackendSelector implements BackendSelector {
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final ColocatedBackendSelector.Assignment colocatedAssignment;

    public ReplicatedBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                     FragmentScanRangeAssignment assignment,
                                     ColocatedBackendSelector.Assignment colocatedAssignment) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.colocatedAssignment = colocatedAssignment;
    }

    @Override
    public void computeScanRangeAssignment() {
        for (TScanRangeLocations scanRangeLocations : locations) {
            for (Map.Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> kv : assignment.entrySet()) {
                Map<Integer, List<TScanRangeParams>> scanRanges = kv.getValue();
                List<TScanRangeParams> scanRangeParamsList =
                        scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
                // add scan range
                TScanRangeParams scanRangeParams = new TScanRangeParams(scanRangeLocations.scan_range);
                scanRangeParamsList.add(scanRangeParams);
            }
        }
        // If this fragment has bucket/colocate join, there need to fill fragmentIdBucketSeqToScanRangeMap here.
        // For example:
        //                       join(replicated)
        //                    /                    \
        //            join(bucket/colocate)       scan(C)
        //              /           \
        //            scan(A)         scan(B)
        // There are replicate join and bucket/colocate join in same fragment. for each bucket A,B used, we need to
        // add table C all tablet because of the character of the replicate join.
        ColocatedBackendSelector.BucketSeqToScanRange
                bucketSeqToScanRange = colocatedAssignment == null ? null : colocatedAssignment.getSeqToScanRange();
        if (bucketSeqToScanRange != null && !bucketSeqToScanRange.isEmpty()) {
            for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> entry : bucketSeqToScanRange.entrySet()) {
                for (TScanRangeLocations scanRangeLocations : locations) {

                    List<TScanRangeParams> scanRangeParamsList =
                            entry.getValue().computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
                    // add scan range
                    TScanRangeParams scanRangeParams = new TScanRangeParams(scanRangeLocations.scan_range);
                    scanRangeParamsList.add(scanRangeParams);
                }
            }
        }
    }
}