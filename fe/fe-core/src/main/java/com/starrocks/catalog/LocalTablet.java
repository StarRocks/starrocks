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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Tablet.java

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

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletSchedCtx.Priority;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TxnFinishState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents the local olap tablet related metadata.
 * LocalTablet is based on local disk storage and replicas are managed by StarRocks.
 */
public class LocalTablet extends Tablet implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

    public enum TabletHealthStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num.
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing.
        REPLICA_RELOCATING, // replica is healthy, but is under relocating (e.g. BE is decommission).
        REDUNDANT, // too much replicas.
        FORCE_REDUNDANT, // some replica is missing or bad, but there is no other backends for repair,
        // at least one replica has to be deleted first to make room for new replica.
        COLOCATE_MISMATCH, // replicas do not all locate in right colocate backends set.
        COLOCATE_REDUNDANT, // replicas match the colocate backends set, but redundant.
        NEED_FURTHER_REPAIR, // one of replicas need a definite repair.
        DISK_MIGRATION, // The disk where the replica is located is decommissioned.
        LOCATION_MISMATCH // The location of replica doesn't match the location specified in table property.
    }

    // Most read only accesses to replicas should acquire db lock, to prevent
    // modification to replicas list during read access.
    // This method avoids acquiring db lock by acquiring replicas object lock
    // instead, so the lock granularity is reduced.
    // To achieve this goal, all write operations to replicas object should
    // also acquire object lock.
    @SerializedName(value = "replicas")
    private List<Replica> replicas;
    private List<Replica> immutableReplicas;
    @SerializedName(value = "checkedVersion")
    private long checkedVersion;
    @SerializedName(value = "isConsistent")
    private boolean isConsistent;

    // last time that the TabletChecker checks this tablet.
    // no need to persist
    private long lastStatusCheckTime = -1;

    private long lastFullCloneFinishedTimeMs = -1;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public LocalTablet() {
        this(0L, new ArrayList<>());
    }

    public LocalTablet(long id) {
        this(id, new ArrayList<>());
    }

    public LocalTablet(long id, List<Replica> replicas) {
        super(id);
        this.replicas = replicas;
        if (this.replicas == null) {
            this.replicas = new ArrayList<>();
        }

        checkedVersion = -1L;

        isConsistent = true;
        assert replicas != null;
        this.immutableReplicas = Collections.unmodifiableList(replicas);
    }

    public long getCheckedVersion() {
        return this.checkedVersion;
    }

    public void setCheckedVersion(long checkedVersion) {
        this.checkedVersion = checkedVersion;
    }

    public void setIsConsistent(boolean good) {
        this.isConsistent = good;
    }

    public boolean isConsistent() {
        return isConsistent;
    }

    private boolean deleteRedundantReplica(long backendId, long version) {
        boolean delete = false;
        boolean hasBackend = false;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
            Iterator<Replica> iterator = replicas.iterator();
            while (iterator.hasNext()) {
                Replica replica = iterator.next();
                if (replica.getBackendId() == backendId) {
                    hasBackend = true;
                    if (replica.getVersion() <= version) {
                        iterator.remove();
                        delete = true;
                    }
                }
            }
        }
        return delete || !hasBackend;
    }

    public void addReplica(Replica replica, boolean updateInvertedIndex) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
            if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
                replicas.add(replica);
                if (updateInvertedIndex) {
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(id, replica);
                }
            }
        }
    }

    public void addReplica(Replica replica) {
        addReplica(replica, true);
    }

    public int getErrorStateReplicaNum() {
        int num = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.isErrorState()) {
                    num++;
                }
            }
        }
        return num;
    }

    public Lock getReadLock() {
        return rwLock.readLock();
    }

    @Override
    public List<Replica> getAllReplicas() {
        return replicas;
    }

    /**
     * @return Immutable list of replicas
     * notice: the list is immutable, not replica
     */
    public List<Replica> getImmutableReplicas() {
        return immutableReplicas;
    }

    public Replica getSingleReplica() {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            return replicas.get(0);
        }
    }

    @Override
    public Set<Long> getBackendIds() {
        Set<Long> beIds = Sets.newHashSet();
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                beIds.add(replica.getBackendId());
            }
        }
        return beIds;
    }

    public List<String> getBackends() {
        List<String> backends = new ArrayList<>();
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                Backend backend = infoService.getBackend(replica.getBackendId());
                if (backend == null) {
                    continue;
                }
                backends.add(backend.getHost());
            }
        }
        return backends;
    }

    // for loading data
    public List<Long> getNormalReplicaBackendIds() {
        List<Long> beIds = Lists.newArrayList();
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.isBad()) {
                    continue;
                }

                ReplicaState state = replica.getState();
                if (infoService.checkBackendAlive(replica.getBackendId()) && state.canLoad()) {
                    beIds.add(replica.getBackendId());
                }
            }
        }
        return beIds;
    }

    // return map of (BE id -> path hash) of normal replicas
    public Multimap<Replica, Long> getNormalReplicaBackendPathMap(int clusterId) {
        Multimap<Replica, Long> map = LinkedHashMultimap.create();
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getOrCreateSystemInfo(clusterId);
            for (Replica replica : replicas) {
                if (replica.isBad()) {
                    continue;
                }

                ReplicaState state = replica.getState();
                if (infoService.checkBackendAlive(replica.getBackendId())
                        && (state == ReplicaState.NORMAL || state == ReplicaState.ALTER)) {
                    map.put(replica, replica.getPathHash());
                }
            }
        }
        return map;
    }

    // for query
    @Override
    public void getQueryableReplicas(List<Replica> allQueryableReplicas, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.isBad()) {
                    continue;
                }

                // Skip the missing version replica
                if (replica.getLastFailedVersion() > 0) {
                    continue;
                }

                ReplicaState state = replica.getState();
                if (state.canQuery()) {
                    // replica.getSchemaHash() == -1 is for compatibility
                    if (replica.checkVersionCatchUp(visibleVersion, false)
                            && replica.getMinReadableVersion() <= visibleVersion
                            && (replica.getSchemaHash() == -1 || replica.getSchemaHash() == schemaHash)) {
                        allQueryableReplicas.add(replica);
                        if (localBeId != -1 && replica.getBackendId() == localBeId) {
                            localReplicas.add(replica);
                        }
                    }
                }
            }
        }
    }

    public int getQueryableReplicasSize(long visibleVersion, int schemaHash) {
        int size = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.isBad()) {
                    continue;
                }

                // Skip the missing version replica
                if (replica.getLastFailedVersion() > 0) {
                    continue;
                }

                ReplicaState state = replica.getState();
                if (state.canQuery()) {
                    // replica.getSchemaHash() == -1 is for compatibility
                    if (replica.checkVersionCatchUp(visibleVersion, false)
                            && replica.getMinReadableVersion() <= visibleVersion
                            && (replica.getSchemaHash() == -1 || replica.getSchemaHash() == schemaHash)) {
                        size++;
                    }
                }
            }
        }
        return size;
    }

    public Replica getReplicaById(long replicaId) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.getId() == replicaId) {
                    return replica;
                }
            }
        }
        return null;
    }

    public Replica getReplicaByBackendId(long backendId) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.getBackendId() == backendId) {
                    return replica;
                }
            }
        }
        return null;
    }

    public boolean deleteReplica(Replica replica) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
            if (replicas.contains(replica)) {
                replicas.remove(replica);
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteReplica(id, replica.getBackendId());
                return true;
            }
        }
        return false;
    }

    public boolean deleteReplicaByBackendId(long backendId) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
            Iterator<Replica> iterator = replicas.iterator();
            while (iterator.hasNext()) {
                Replica replica = iterator.next();
                if (replica.getBackendId() == backendId) {
                    iterator.remove();
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteReplica(id, backendId);
                    return true;
                }
            }
        }
        return false;
    }

    // for test,
    // and for some replay cases
    public void clearReplica() {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
            this.replicas.clear();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        int replicaCount = replicas.size();
        out.writeInt(replicaCount);
        for (int i = 0; i < replicaCount; ++i) {
            replicas.get(i).write(out);
        }

        out.writeLong(checkedVersion);
        out.writeLong(0); // write a version_hash for compatibility
        out.writeBoolean(isConsistent);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        int replicaCount = in.readInt();
        for (int i = 0; i < replicaCount; ++i) {
            Replica replica = Replica.read(in);
            if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
                // do not need to update immutableReplicas, because it is a view of replicas
                replicas.add(replica);
            }
        }

        checkedVersion = in.readLong();
        in.readLong(); // read a version_hash for compatibility
        isConsistent = in.readBoolean();
    }

    public static LocalTablet read(DataInput in) throws IOException {
        LocalTablet tablet = new LocalTablet();
        tablet.readFields(in);
        return tablet;
    }

    @Override
    public void gsonPostProcess() {
        // we need to update immutableReplicas, because replicas after deserialization from a json string
        // will be different from the replicas initiated in the constructor
        immutableReplicas = Collections.unmodifiableList(replicas);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LocalTablet)) {
            return false;
        }

        LocalTablet tablet = (LocalTablet) obj;

        if (replicas != tablet.replicas) {
            if (replicas.size() != tablet.replicas.size()) {
                return false;
            }
            int size = replicas.size();
            for (int i = 0; i < size; i++) {
                if (!tablet.replicas.contains(replicas.get(i))) {
                    return false;
                }
            }
        }
        return id == tablet.id;
    }

    // Get total data size of all replicas if singleReplica is true, else get max replica data size.
    // Replica state must be NORMAL or SCHEMA_CHANGE.
    @Override
    public long getDataSize(boolean singleReplica) {
        long dataSize = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.getState() == ReplicaState.NORMAL || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                    if (singleReplica) {
                        long replicaDataSize = replica.getDataSize();
                        if (replicaDataSize > dataSize) {
                            dataSize = replicaDataSize;
                        }
                    } else {
                        dataSize += replica.getDataSize();
                    }
                }
            }
        }
        return dataSize;
    }

    // Get max row count of all replicas which version catches up.
    @Override
    public long getRowCount(long version) {
        long tabletRowCount = 0L;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                if (replica.checkVersionCatchUp(version, false) && replica.getRowCount() > tabletRowCount) {
                    tabletRowCount = replica.getRowCount();
                }
            }
        }
        return tabletRowCount;
    }

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORMAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    public boolean readyToBeRepaired(TabletHealthStatus status, TabletSchedCtx.Priority priority) {
        if (priority == Priority.VERY_HIGH ||
                status == TabletHealthStatus.VERSION_INCOMPLETE ||
                status == TabletHealthStatus.NEED_FURTHER_REPAIR) {
            return true;
        }

        long currentTime = System.currentTimeMillis();

        // first check, wait for next round
        if (lastStatusCheckTime == -1) {
            lastStatusCheckTime = currentTime;
            return false;
        }

        boolean ready = false;
        switch (priority) {
            case HIGH:
                ready = currentTime - lastStatusCheckTime > Config.tablet_sched_repair_delay_factor_second * 1000;
                break;
            case NORMAL:
                ready = currentTime - lastStatusCheckTime > Config.tablet_sched_repair_delay_factor_second * 1000 * 2;
                break;
            case LOW:
                ready = currentTime - lastStatusCheckTime > Config.tablet_sched_repair_delay_factor_second * 1000 * 3;
                break;
            default:
                break;
        }

        return ready;
    }

    public void setLastStatusCheckTime(long lastStatusCheckTime) {
        this.lastStatusCheckTime = lastStatusCheckTime;
    }

    private String getReplicaBackendState(long backendId) {
        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        Backend backend = infoService.getBackend(backendId);
        if (backend == null) {
            return "NIL";
        } else if (!backend.isAlive()) {
            return "DEAD";
        } else if (backend.isDecommissioned()) {
            return "DECOMM";
        } else {
            return "ALIVE";
        }
    }

    public String getReplicaInfos() {
        StringBuilder sb = new StringBuilder();
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                sb.append(String.format("%d:%d/%d/%d/%d:%s:%s,", replica.getBackendId(), replica.getVersion(),
                        replica.getLastFailedVersion(), replica.getLastSuccessVersion(), replica.getMinReadableVersion(),
                        replica.isBad() ? "BAD" : replica.getState(), getReplicaBackendState(replica.getBackendId())));
            }
        }
        return sb.toString();
    }

    // Note: this method does not require db lock to be held
    public boolean quorumReachVersion(long version, long quorum, TxnFinishState finishState) {
        long valid = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            for (Replica replica : replicas) {
                long replicaId = replica.getId();
                long replicaVersion = replica.getVersion();
                if (replicaVersion > version) {
                    valid++;
                    finishState.normalReplicas.remove(replicaId);
                    finishState.abnormalReplicasWithVersion.put(replica.getId(), replicaVersion);
                } else if (replicaVersion == version) {
                    valid++;
                    finishState.normalReplicas.add(replicaId);
                    finishState.abnormalReplicasWithVersion.remove(replicaId);
                } else {
                    if (replica.getState() == ReplicaState.ALTER && replicaVersion <= Partition.PARTITION_INIT_VERSION) {
                        valid++;
                    }
                    finishState.normalReplicas.remove(replicaId);
                    finishState.abnormalReplicasWithVersion.put(replicaId, replicaVersion);
                }
            }
        }
        return valid >= quorum;
    }

    public void getAbnormalReplicaInfos(long version, long quorum, StringBuilder sb) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            boolean empty = true;
            for (Replica replica : replicas) {
                long replicaVersion = replica.getVersion();
                if (replicaVersion < version) {
                    if (empty) {
                        sb.append(String.format(" {tablet:%d quorum:%d version:%d #replica:%d err:", id, quorum, version,
                                replicas.size()));
                        empty = false;
                    }
                    Backend backend =
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(replica.getBackendId());
                    sb.append(String.format(" %s:%d%s",
                            backend == null ? Long.toString(replica.getBackendId()) : backend.getHost(), replicaVersion,
                            replica.getState() == ReplicaState.ALTER ? "ALTER" : ""));
                }
            }
            if (!empty) {
                sb.append("}");
            }
        }
    }

    public long getLastFullCloneFinishedTimeMs() {
        return lastFullCloneFinishedTimeMs;
    }

    public void setLastFullCloneFinishedTimeMs(long lastFullCloneFinishedTimeMs) {
        this.lastFullCloneFinishedTimeMs = lastFullCloneFinishedTimeMs;
    }
}
