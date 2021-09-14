// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/BackendProcNode.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;

import java.util.List;
import java.util.Map;

public class BackendProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("RootPath").add("DataUsedCapacity").add("OtherUsedCapacity").add("AvailCapacity")
            .add("TotalCapacity").add("TotalUsedPct").add("State").add("PathHash").add("StorageMedium")
            .add("TabletNum").build();

    private Backend backend;

    public BackendProcNode(Backend backend) {
        this.backend = backend;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(backend);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (Map.Entry<String, DiskInfo> entry : backend.getDisks().entrySet()) {
            DiskInfo diskInfo = entry.getValue();

            List<String> info = Lists.newArrayList();
            info.add(entry.getKey());

            // data used
            long dataUsedB = diskInfo.getDataUsedCapacityB();
            Pair<Double, String> dataUsedUnitPair = DebugUtil.getByteUint(dataUsedB);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataUsedUnitPair.first) + " "
                    + dataUsedUnitPair.second);

            // avail
            long availB = diskInfo.getAvailableCapacityB();
            Pair<Double, String> availUnitPair = DebugUtil.getByteUint(availB);
            // total
            long totalB = diskInfo.getTotalCapacityB();
            Pair<Double, String> totalUnitPair = DebugUtil.getByteUint(totalB);
            // other
            long otherB = totalB - availB;
            Pair<Double, String> otherUnitPair = DebugUtil.getByteUint(otherB);

            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(otherUnitPair.first) + " " + otherUnitPair.second);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availUnitPair.first) + " " + availUnitPair.second);
            info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUnitPair.first) + " " + totalUnitPair.second);

            // total used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) otherB * 100 / totalB;
            }
            info.add(String.format("%.2f", used) + " %");

            info.add(diskInfo.getState().name());
            info.add(String.valueOf(diskInfo.getPathHash()));

            // medium
            TStorageMedium medium = diskInfo.getStorageMedium();
            if (medium == null) {
                info.add("N/A");
            } else {
                info.add(medium.name());
            }

            // tablet num
            info.add(String.valueOf(Catalog.getCurrentInvertedIndex().getTabletNumByBackendIdAndPathHash(
                    backend.getId(), diskInfo.getPathHash())));

            result.addRow(info);
        }

        return result;
    }

}
