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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/Backend.java

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

package com.starrocks.system;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class extends the primary identifier of a Backend with ephemeral state,
 * e.g. usage information, current administrative state etc.
 */
public class Backend extends ComputeNode {

    public enum BackendState {
        using, /* backend belongs to a cluster*/
        offline,
        free /* backend is not belong to any clusters */
    }

    private static final Logger LOG = LogManager.getLogger(Backend.class);

    // rootPath -> DiskInfo
    @SerializedName(value = "d")
    private volatile ConcurrentHashMap<String, DiskInfo> disksRef;

    // This is used for the first time we initiate pathHashToDishInfo in SystemInfoService.
    // after initiating it, this variable is set to true.
    private boolean initPathInfo = false;

    // the max tablet compaction score of this backend.
    // this field is set by tablet report, and just for metric monitor, no need to persist.
    private volatile long tabletMaxCompactionScore = 0;

    // additional backendStatus information for BE, display in JSON format
    private final BackendStatus backendStatus = new BackendStatus();

    public Backend() {
        super();
        this.disksRef = new ConcurrentHashMap<>();
    }

    public Backend(long id, String host, int heartbeatPort) {
        super(id, host, heartbeatPort);
        this.disksRef = new ConcurrentHashMap<>();
    }

    public void setDisks(ImmutableMap<String, DiskInfo> disks) {
        this.disksRef = new ConcurrentHashMap<>(disks);
    }

    public ImmutableMap<String, DiskInfo> getDisks() {
        return ImmutableMap.copyOf(this.disksRef);
    }

    public boolean hasPathHash() {
        return disksRef.values().stream().allMatch(DiskInfo::hasPathHash);
    }

    public TNetworkAddress getAddress() {
        return new TNetworkAddress(getHost(), getBePort());
    }

    public long getTotalCapacityB() {
        long totalCapacityB = 0L;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalCapacityB += diskInfo.getTotalCapacityB();
            }
        }
        return totalCapacityB;
    }

    public long getDataTotalCapacityB() {
        long dataTotalCapacityB = 0L;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataTotalCapacityB += diskInfo.getDataTotalCapacityB();
            }
        }
        return dataTotalCapacityB;
    }

    public long getAvailableCapacityB() {
        // when cluster init, disks is empty, return 1L.
        long availableCapacityB = 1L;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                availableCapacityB += diskInfo.getAvailableCapacityB();
            }
        }
        return availableCapacityB;
    }

    public long getDataUsedCapacityB() {
        long dataUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }
        }
        return dataUsedCapacityB;
    }

    public double getMaxDiskUsedPct() {
        double maxPct = 0.0;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                double percent = diskInfo.getUsedPct();
                if (percent > maxPct) {
                    maxPct = percent;
                }
            }
        }
        return maxPct;
    }

    public boolean diskExceedLimitByStorageMedium(TStorageMedium storageMedium) {
        if (getDiskNumByStorageMedium(storageMedium) <= 0) {
            return true;
        }
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && diskInfo.getStorageMedium() == storageMedium &&
                    !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public boolean diskExceedLimit() {
        if (getDiskNum() <= 0) {
            return true;
        }
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : disksRef.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public void updateDisks(Map<String, TDisk> backendDisks) {
        // The very first time to init the path info
        if (!initPathInfo) {
            boolean allPathHashUpdated = true;
            for (DiskInfo diskInfo : disksRef.values()) {
                if (diskInfo.getPathHash() == 0) {
                    allPathHashUpdated = false;
                    break;
                }
            }
            if (allPathHashUpdated) {
                initPathInfo = true;
                GlobalStateMgr.getCurrentSystemInfo()
                        .updatePathInfo(new ArrayList<>(disksRef.values()), Lists.newArrayList());
            }
        }

        // update status or add new diskInfo
        Map<String, DiskInfo> newDiskInfos = Maps.newHashMap();
        List<DiskInfo> addedDisks = Lists.newArrayList();
        List<DiskInfo> removedDisks = Lists.newArrayList();
        /*
         * set isChanged to true only if new disk is added or old disk is dropped.
         * we ignore the change of capacity, because capacity info is only used in master FE.
         */
        boolean isChanged = false;
        for (TDisk tDisk : backendDisks.values()) {
            String rootPath = tDisk.getRoot_path();
            long totalCapacityB = tDisk.getDisk_total_capacity();
            long dataUsedCapacityB = tDisk.getData_used_capacity();
            long diskAvailableCapacityB = tDisk.getDisk_available_capacity();
            boolean isUsed = tDisk.isUsed();

            DiskInfo diskInfo = disksRef.get(rootPath);
            if (diskInfo == null) {
                diskInfo = new DiskInfo(rootPath);
                addedDisks.add(diskInfo);
                isChanged = true;
                LOG.info("add new disk info. backendId: {}, rootPath: {}", getId(), rootPath);
            }
            newDiskInfos.put(rootPath, diskInfo);

            diskInfo.setTotalCapacityB(totalCapacityB);
            diskInfo.setDataUsedCapacityB(dataUsedCapacityB);
            diskInfo.setAvailableCapacityB(diskAvailableCapacityB);
            if (tDisk.isSetPath_hash()) {
                diskInfo.setPathHash(tDisk.getPath_hash());
            }

            if (tDisk.isSetStorage_medium()) {
                diskInfo.setStorageMedium(tDisk.getStorage_medium());
            }

            if (isUsed) {
                if (diskInfo.setState(DiskState.ONLINE)) {
                    isChanged = true;
                }
            } else {
                if (diskInfo.setState(DiskState.OFFLINE)) {
                    isChanged = true;
                }
            }
            LOG.debug("update disk info. backendId: {}, diskInfo: {}", getId(), diskInfo.toString());
        }

        // remove not exist rootPath in backend
        for (DiskInfo diskInfo : disksRef.values()) {
            String rootPath = diskInfo.getRootPath();
            if (!backendDisks.containsKey(rootPath)) {
                removedDisks.add(diskInfo);
                isChanged = true;
                LOG.warn("remove not exist rootPath. backendId: {}, rootPath: {}", getId(), rootPath);
            }
        }

        if (isChanged) {
            // update disksRef
            disksRef = new ConcurrentHashMap<>(newDiskInfos);
            GlobalStateMgr.getCurrentSystemInfo().updatePathInfo(addedDisks, removedDisks);
            // log disk changing
            GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(this);
        }
    }

    public void setStorageMediumForAllDisks(TStorageMedium m) {
        for (DiskInfo diskInfo : disksRef.values()) {
            diskInfo.setStorageMedium(m);
        }
    }

    public BackendStatus getBackendStatus() {
        return backendStatus;
    }

    @Override
    public boolean isSetStoragePath() {
        return true;
    }

    public static Backend read(DataInput in) throws IOException {
        Backend backend = new Backend();
        backend.readFields(in);
        return backend;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getId());
        Text.writeString(out, getHost());
        out.writeInt(getHeartbeatPort());
        out.writeInt(getBePort());
        out.writeInt(getHttpPort());
        out.writeInt(getBeRpcPort());
        out.writeBoolean(getIsAlive().get());
        out.writeBoolean(isDecommissioned());
        out.writeLong(getLastUpdateMs());

        out.writeLong(getLastStartTime());

        out.writeInt(disksRef.size());
        for (Map.Entry<String, DiskInfo> entry : disksRef.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }

        Text.writeString(out, SystemInfoService.DEFAULT_CLUSTER);
        out.writeInt(getBackendState().ordinal());
        out.writeInt(getDecommissionType().ordinal());

        out.writeInt(getBrpcPort());
    }

    public void readFields(DataInput in) throws IOException {
        setId(in.readLong());
        setHost(Text.readString(in));
        setHeartbeatPort(in.readInt());
        setBePort(in.readInt());
        setHttpPort(in.readInt());
        setBeRpcPort(in.readInt());
        setAlive(in.readBoolean());
        setDecommissioned(in.readBoolean());

        setLastUpdateMs(in.readLong());

        setLastStartTime(in.readLong());

        Map<String, DiskInfo> disks = Maps.newHashMap();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String rootPath = Text.readString(in);
            DiskInfo diskInfo = DiskInfo.read(in);
            disks.put(rootPath, diskInfo);
        }

        disksRef = new ConcurrentHashMap<>(disks);
        // ignore clusterName
        Text.readString(in);
        setBackendState(in.readInt());
        setDecommissionType(in.readInt());

        setBrpcPort(in.readInt());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Backend)) {
            return false;
        }

        Backend backend = (Backend) obj;

        return (getId() == backend.getId()) && (getHost().equals(backend.getHost()))
                && (getHeartbeatPort() == backend.getHeartbeatPort())
                && (getBePort() == backend.getBePort()) && (getIsAlive().get() == backend.getIsAlive().get());
    }

    @Override
    public String toString() {
        return "Backend [id=" + getId() + ", host=" + getHost() + ", heartbeatPort=" + getHeartbeatPort()
                + ", alive=" + getIsAlive().get() + "]";
    }

    public void setTabletMaxCompactionScore(long compactionScore) {
        tabletMaxCompactionScore = compactionScore;
    }

    public long getTabletMaxCompactionScore() {
        return tabletMaxCompactionScore;
    }

    private long getDiskNumByStorageMedium(TStorageMedium storageMedium) {
        return disksRef.values().stream().filter(v -> v.getStorageMedium() == storageMedium).count();
    }

    public int getAvailableBackendStorageTypeCnt() {
        if (!getIsAlive().get()) {
            return 0;
        }
        ImmutableMap<String, DiskInfo> disks = this.getDisks();
        Set<TStorageMedium> set = Sets.newHashSet();
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                set.add(diskInfo.getStorageMedium());
            }
        }
        return set.size();
    }

    private int getDiskNum() {
        return disksRef.size();
    }

    /**
     * Note: This class must be a POJO in order to display in JSON format
     * Add additional information in the class to show in `show backends`
     * if just change new added backendStatus, you can do like following
     * BackendStatus status = Backend.getBackendStatus();
     * status.newItem = xxx;
     */
    public static class BackendStatus {
        // this will be output as json, so not using FeConstants.null_string;
        public String lastSuccessReportTabletsTime = "N/A";
    }
}

