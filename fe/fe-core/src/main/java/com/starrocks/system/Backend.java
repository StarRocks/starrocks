// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.alter.DecommissionType;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDisk;
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
    private volatile ImmutableMap<String, DiskInfo> disksRef;

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
        this.disksRef = ImmutableMap.of();
    }

    public Backend(long id, String host, int heartbeatPort) {
        super(id, host, heartbeatPort);
        this.disksRef = ImmutableMap.of();
    }

    public void setDisks(ImmutableMap<String, DiskInfo> disks) {
        this.disksRef = disks;
    }

    public ImmutableMap<String, DiskInfo> getDisks() {
        return this.disksRef;
    }

    public boolean hasPathHash() {
        return disksRef.values().stream().allMatch(DiskInfo::hasPathHash);
    }

    public long getTotalCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long totalCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalCapacityB += diskInfo.getTotalCapacityB();
            }
        }
        return totalCapacityB;
    }

    public long getDataTotalCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long dataTotalCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataTotalCapacityB += diskInfo.getDataTotalCapacityB();
            }
        }
        return dataTotalCapacityB;
    }

    public long getAvailableCapacityB() {
        // when cluster init, disks is empty, return 1L.
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long availableCapacityB = 1L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                availableCapacityB += diskInfo.getAvailableCapacityB();
            }
        }
        return availableCapacityB;
    }

    public long getDataUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long dataUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }
        }
        return dataUsedCapacityB;
    }

    public double getMaxDiskUsedPct() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        double maxPct = 0.0;
        for (DiskInfo diskInfo : disks.values()) {
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
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
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
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public void updateDisks(Map<String, TDisk> backendDisks) {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        // The very first time to init the path info
        if (!initPathInfo) {
            boolean allPathHashUpdated = true;
            for (DiskInfo diskInfo : disks.values()) {
                if (diskInfo.getPathHash() == 0) {
                    allPathHashUpdated = false;
                    break;
                }
            }
            if (allPathHashUpdated) {
                initPathInfo = true;
                GlobalStateMgr.getCurrentSystemInfo()
                        .updatePathInfo(new ArrayList<>(disks.values()), Lists.newArrayList());
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

            DiskInfo diskInfo = disks.get(rootPath);
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
        for (DiskInfo diskInfo : disks.values()) {
            String rootPath = diskInfo.getRootPath();
            if (!backendDisks.containsKey(rootPath)) {
                removedDisks.add(diskInfo);
                isChanged = true;
                LOG.warn("remove not exist rootPath. backendId: {}, rootPath: {}", getId(), rootPath);
            }
        }

        if (isChanged) {
            // update disksRef
            disksRef = ImmutableMap.copyOf(newDiskInfos);
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

        ImmutableMap<String, DiskInfo> disks = disksRef;
        out.writeInt(disks.size());
        for (Map.Entry<String, DiskInfo> entry : disks.entrySet()) {
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
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_31) {
            setBeRpcPort(in.readInt());
        }
        setAlive(in.readBoolean());

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= 5) {
            setDecommissioned(in.readBoolean());
        }

        setLastUpdateMs(in.readLong());

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= 2) {
            setLastStartTime(in.readLong());

            Map<String, DiskInfo> disks = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String rootPath = Text.readString(in);
                DiskInfo diskInfo = DiskInfo.read(in);
                disks.put(rootPath, diskInfo);
            }

            disksRef = ImmutableMap.copyOf(disks);
        }
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_30) {
            // ignore clusterName
            Text.readString(in);
            setBackendState(in.readInt());
            setDecommissionType(in.readInt());
        } else {
            setBackendState(BackendState.using.ordinal());
            setDecommissionType(DecommissionType.SystemDecommission.ordinal());
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_40) {
            setBrpcPort(in.readInt());
        }
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

