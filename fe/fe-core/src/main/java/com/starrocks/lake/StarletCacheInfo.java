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

package com.starrocks.lake;

import autovalue.shaded.com.google.common.common.collect.Lists;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.thrift.TStorageMedium;

import java.util.List;

public class StarletCacheInfo {
    private String cachePath;

    private long totalCapacityB;
    private long usedCapacityB;
    private long availableCapacityB;

    private TStorageMedium storageMedium;

    private static final long DEFAULT_CAPACITY_B = 1024 * 1024 * 1024 * 1024L; // 1T

    public StarletCacheInfo(String cachePath) {
        this.cachePath = cachePath;
        this.totalCapacityB = DEFAULT_CAPACITY_B;
        this.usedCapacityB = 0;
        this.availableCapacityB = totalCapacityB;
        this.storageMedium = TStorageMedium.HDD;
    }

    public long getUsedCapacityB() {
        return usedCapacityB;
    }

    public void setUsedCapacityB(long usedCapacityB) {
        this.usedCapacityB = usedCapacityB;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public void setTotalCapacityB(long totalCapacityB) {
        this.totalCapacityB = totalCapacityB;
    }

    public long getAvailableCapacityB() {
        return availableCapacityB;
    }

    public void setAvailableCapacityB(long availableCapacityB) {
        this.availableCapacityB = availableCapacityB;
    }

    public long getDataTotalCapacityB() {
        return usedCapacityB + availableCapacityB;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    public List<String> toBackendProcNodeInfo() {
        List<String> info = Lists.newArrayList();
        // path
        info.add(cachePath);

        // data used
        Pair<Double, String> dataUsedUnitPair = DebugUtil.getByteUint(usedCapacityB);
        info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataUsedUnitPair.first) + " " + dataUsedUnitPair.second);

        // other used
        long otherUsedB = totalCapacityB - availableCapacityB - usedCapacityB;
        Pair<Double, String> otherUsedUnitPair = DebugUtil.getByteUint(otherUsedB);
        info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(otherUsedUnitPair.first) + " " + otherUsedUnitPair.second);

        // avail
        Pair<Double, String> availUnitPair = DebugUtil.getByteUint(availableCapacityB);
        info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availUnitPair.first) + " " + availUnitPair.second);

        // total
        Pair<Double, String> totalUnitPair = DebugUtil.getByteUint(totalCapacityB);
        info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUnitPair.first) + " " + totalUnitPair.second);

        // total used percent
        double used = 0.0;
        if (totalCapacityB > 0) {
            used = (double) (totalCapacityB - availableCapacityB) * 100 / totalCapacityB;
        }
        info.add(String.format("%.2f", used) + " %");

        // state
        info.add(DiskInfo.DiskState.ONLINE.name());

        // path hash
        info.add("");

        // medium
        if (storageMedium == null) {
            info.add("N/A");
        } else {
            info.add(storageMedium.name());
        }

        // tablet num
        info.add("0");

        // data total
        long dataTotalB = getDataTotalCapacityB();
        Pair<Double, String> dataTotalUnitPair = DebugUtil.getByteUint(dataTotalB);
        info.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataTotalUnitPair.first) + " " + dataTotalUnitPair.second);

        // data used percent
        double dataUsed = 0.0;
        if (dataTotalB > 0) {
            dataUsed = (double) usedCapacityB * 100 / dataTotalB;
        }
        info.add(String.format("%.2f", dataUsed) + " %");

        return info;
    }
}
