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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/DiskInfo.java

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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.proto.DiskInfoPB;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskInfo implements Writable {
    public enum DiskState {
        ONLINE,
        // Reported from BE, if disk is detected as unavailable, state will be OFFLINE.
        // Tablets on OFFLINE disk will be dropped.
        OFFLINE,
        // Set by user, tablets on DISABLED disk will be dropped.
        DISABLED,
        // Set by user, tablets on DECOMMISSIONED disk will be cloned to other backends.
        // Before the decommission finish, the disk is still usable, i.e. can provide read and write capability.
        DECOMMISSIONED
    }

    private static final Logger LOG = LogManager.getLogger(DiskInfo.class);
    private static final long DEFAULT_CAPACITY_B = 1024 * 1024 * 1024 * 1024L; // 1T

    @SerializedName(value = "r")
    private String rootPath;
    @SerializedName(value = "s")
    private DiskState state;

    // The following properties are reported from Backend and only used in leader node, so no need to persist
    private long totalCapacityB;
    private long dataUsedCapacityB;
    private long diskAvailableCapacityB;
    private long pathHash = 0;
    private TStorageMedium storageMedium;

    private DiskInfo() {
        // for persist
    }

    public DiskInfo(String rootPath) {
        this.rootPath = rootPath;
        this.totalCapacityB = DEFAULT_CAPACITY_B;
        this.dataUsedCapacityB = 0;
        this.diskAvailableCapacityB = totalCapacityB;
        this.state = DiskState.ONLINE;
        this.pathHash = 0;
        this.storageMedium = TStorageMedium.HDD;
    }

    public String getRootPath() {
        return rootPath;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public void setTotalCapacityB(long totalCapacityB) {
        this.totalCapacityB = totalCapacityB;
    }

    /**
     * OtherUsed (totalCapacityB - diskAvailableCapacityB - dataUsedCapacityB) may hold a lot of disk space,
     * disk usage percent = DataUsedCapacityB / TotalCapacityB in balance,
     * using dataUsedCapacityB + diskAvailableCapacityB as total capacity is more reasonable.
     */
    public long getDataTotalCapacityB() {
        return dataUsedCapacityB + diskAvailableCapacityB;
    }

    public long getDataUsedCapacityB() {
        return dataUsedCapacityB;
    }

    public void setDataUsedCapacityB(long dataUsedCapacityB) {
        this.dataUsedCapacityB = dataUsedCapacityB;
    }

    public long getAvailableCapacityB() {
        return diskAvailableCapacityB;
    }

    public void setAvailableCapacityB(long availableCapacityB) {
        this.diskAvailableCapacityB = availableCapacityB;
    }

    public double getUsedPct() {
        return (totalCapacityB - diskAvailableCapacityB) / (double) (totalCapacityB <= 0 ? 1 : totalCapacityB);
    }

    public DiskState getState() {
        return state;
    }

    public boolean canReadWrite() {
        return state == DiskState.ONLINE || state == DiskState.DECOMMISSIONED;
    }

    public boolean canCreateTablet() {
        return state == DiskState.ONLINE;
    }

    public void setState(DiskState state) {
        this.state = state;
    }

    public long getPathHash() {
        return pathHash;
    }

    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    public boolean hasPathHash() {
        return pathHash != 0;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    /*
     * Check if this disk's capacity reach the limit. Return true if yes.
     * If usingHardLimit is true, use usingHardLimit threshold to check.
     */
    public static boolean exceedLimit(long currentAvailCapacityB, long totalCapacityB, boolean usingHardLimit) {
        if (usingHardLimit) {
            return currentAvailCapacityB < Config.storage_usage_hard_limit_reserve_bytes &&
                    (double) (totalCapacityB - currentAvailCapacityB) / totalCapacityB >
                            (Config.storage_usage_hard_limit_percent / 100.0);
        } else {
            return currentAvailCapacityB < Config.storage_usage_soft_limit_reserve_bytes &&
                    (double) (totalCapacityB - currentAvailCapacityB) / totalCapacityB >
                            (Config.storage_usage_soft_limit_percent / 100.0);
        }
    }

    public boolean exceedLimit(boolean usingHardLimit) {
        LOG.debug("using hard limit: {}, diskAvailableCapacityB: {}, totalCapacityB: {}",
                usingHardLimit, diskAvailableCapacityB, totalCapacityB);
        return DiskInfo.exceedLimit(diskAvailableCapacityB, totalCapacityB, usingHardLimit);
    }

    @Override
    public String toString() {
        return "DiskInfo [rootPath=" + rootPath + "(" + pathHash + "), totalCapacityB=" + totalCapacityB
                + ", dataTotalCapacityB=" + getDataTotalCapacityB() + ", dataUsedCapacityB=" + dataUsedCapacityB
                + ", diskAvailableCapacityB=" + diskAvailableCapacityB + ", state=" + state
                + ", medium: " + storageMedium + "]";
    }


    // ---- image v3 -------------------------------------------------------------------------------

    public DiskInfoPB toProto() {
        DiskInfoPB.Builder builder = DiskInfoPB.newBuilder().setState(toProto(state));
        if (rootPath != null) {
            builder.setRootPath(rootPath);
        }
        return builder.build();
    }

    public static DiskInfo fromProto(DiskInfoPB pb) {
        // same constructor GSON uses, so the non-persisted fields start out identically
        DiskInfo disk = new DiskInfo();
        disk.rootPath = pb.getRootPath();
        disk.state = fromProto(pb.getState());
        return disk;
    }

    private static DiskInfoPB.State toProto(DiskState state) {
        if (state == null) {
            return DiskInfoPB.State.UNSPECIFIED;
        }
        switch (state) {
            case ONLINE:
                return DiskInfoPB.State.ONLINE;
            case OFFLINE:
                return DiskInfoPB.State.OFFLINE;
            case DISABLED:
                return DiskInfoPB.State.DISABLED;
            default:
                throw new IllegalArgumentException("unmapped disk state " + state);
        }
    }

    private static DiskState fromProto(DiskInfoPB.State state) {
        switch (state) {
            case UNSPECIFIED:
                return null;
            case ONLINE:
                return DiskState.ONLINE;
            case OFFLINE:
                return DiskState.OFFLINE;
            case DISABLED:
                return DiskState.DISABLED;
            default:
                throw new IllegalArgumentException("unknown disk state " + state.getNumber());
        }
    }
}
