// This file is made available under Elastic License 2.0.
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

import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiskInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(DiskInfo.class);

    public enum DiskState {
        ONLINE,
        OFFLINE
    }

    private static final long DEFAULT_CAPACITY_B = 1024 * 1024 * 1024 * 1024L; // 1T

    private String rootPath;
    // disk capacity
    private long totalCapacityB;
    private long dataUsedCapacityB;
    private long diskAvailableCapacityB;
    private DiskState state;

    // path hash and storage medium are reported from Backend and no need to persist
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

    // return true if changed
    public boolean setState(DiskState state) {
        if (this.state != state) {
            this.state = state;
            return true;
        }
        return false;
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

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, rootPath);
        out.writeLong(totalCapacityB);
        out.writeLong(dataUsedCapacityB);
        out.writeLong(diskAvailableCapacityB);
        Text.writeString(out, state.name());
    }

    public void readFields(DataInput in) throws IOException {
        this.rootPath = Text.readString(in);
        this.totalCapacityB = in.readLong();
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_36) {
            this.dataUsedCapacityB = in.readLong();
            this.diskAvailableCapacityB = in.readLong();
        } else {
            long availableCapacityB = in.readLong();
            this.dataUsedCapacityB = this.totalCapacityB - availableCapacityB;
            this.diskAvailableCapacityB = availableCapacityB;
        }
        this.state = DiskState.valueOf(Text.readString(in));
    }

    public static DiskInfo read(DataInput in) throws IOException {
        DiskInfo diskInfo = new DiskInfo();
        diskInfo.readFields(in);
        return diskInfo;
    }
}
