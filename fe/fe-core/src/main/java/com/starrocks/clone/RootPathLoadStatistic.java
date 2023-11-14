// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/RootPathLoadStatistic.java

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

package com.starrocks.clone;

import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.clone.BackendLoadStatistic.Classification;
import com.starrocks.clone.BalanceStatus.ErrCode;
import com.starrocks.thrift.TStorageMedium;

public class RootPathLoadStatistic implements Comparable<RootPathLoadStatistic> {

    private long beId;
    private String path;
    private Long pathHash;
    private TStorageMedium storageMedium;
    private long capacityB;
    private long usedCapacityB;
    private DiskState diskState;

    private Classification clazz = Classification.INIT;

    public RootPathLoadStatistic(long beId, String path, Long pathHash, TStorageMedium storageMedium,
                                 long capacityB, long usedCapacityB, DiskState diskState) {
        this.beId = beId;
        this.path = path;
        this.pathHash = pathHash;
        this.storageMedium = storageMedium;
        this.capacityB = capacityB <= 0 ? 1 : capacityB;
        this.usedCapacityB = usedCapacityB;
        this.diskState = diskState;
    }

    public long getBeId() {
        return beId;
    }

    public String getPath() {
        return path;
    }

    public long getPathHash() {
        return pathHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCapacityB() {
        return capacityB;
    }

    public long getUsedCapacityB() {
        return usedCapacityB;
    }

    public double getUsedPercent() {
        return capacityB <= 0 ? 0.0 : usedCapacityB / (double) capacityB;
    }

    public void setClazz(Classification clazz) {
        this.clazz = clazz;
    }

    public Classification getClazz() {
        return clazz;
    }

    public DiskState getDiskState() {
        return diskState;
    }

    public BalanceStatus isFit(long tabletSize) {
        if (diskState == DiskState.OFFLINE) {
            return new BalanceStatus(ErrCode.COMMON_ERROR,
                    toString() + " does not fit tablet with size: " + tabletSize + ", offline");
        }

        if (DiskInfo.exceedLimit(capacityB - usedCapacityB - tabletSize, capacityB, false)) {
            return new BalanceStatus(ErrCode.COMMON_ERROR,
                    toString() + " does not fit tablet with size: " + tabletSize);
        }

        return BalanceStatus.OK;
    }

    // path with lower usage percent rank ahead
    @Override
    public int compareTo(RootPathLoadStatistic o) {
        double myPercent = getUsedPercent();
        double otherPercent = o.getUsedPercent();
        return Double.compare(myPercent, otherPercent);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("path: ").append(path).append(", path hash: ").append(pathHash).append(", be: ").append(beId);
        sb.append(", medium: ").append(storageMedium).append(", used: ").append(usedCapacityB);
        sb.append(", total: ").append(capacityB);
        return sb.toString();
    }
}
