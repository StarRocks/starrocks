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
import com.starrocks.common.Pair;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This class represents the local olap tablet related metadata.
 * LocalTablet is based on local disk storage and replicas are managed by StarRocks.
 */
public class LocalTablet extends Tablet implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

    public enum TabletStatus {
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
                    GlobalStateMgr.getCurrentInvertedIndex().addReplica(id, replica);
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
        List<String> backends = new ArrayList<String>();
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
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
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
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
            SystemInfoService infoService = GlobalStateMgr.getCurrentState().getOrCreateSystemInfo(clusterId);
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
                GlobalStateMgr.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendId());
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
                    GlobalStateMgr.getCurrentInvertedIndex().deleteReplica(id, backendId);
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

    private Pair<TabletStatus, TabletSchedCtx.Priority> createRedundantSchedCtx(
            TabletStatus status, Priority prio, Replica needFurtherRepairReplica) {
        if (needFurtherRepairReplica != null) {
            return Pair.create(TabletStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
        }
        return Pair.create(status, prio);
    }

    /**
     * For certain deployment, like k8s pods + pvc, the replica is not lost even the
     * corresponding backend is detected as dead, because the replica data is persisted
     * on a pvc which is backed by a remote storage service, such as AWS EBS. And later,
     * k8s control place will schedule a new pod and attach the pvc to it which will
     * restore the replica to a {@link ReplicaState#NORMAL} state immediately. But normally
     * the {@link com.starrocks.clone.TabletScheduler} of Starrocks will start to schedule
     * {@link TabletStatus#REPLICA_MISSING} tasks and create new replicas in a short time.
     * After new pod scheduling is completed, {@link com.starrocks.clone.TabletScheduler} has
     * to delete the redundant healthy replica which cause resource waste and may also affect
     * the loading process.
     *
     * <p>This method checks whether the corresponding backend of tablet replica is dead or not.
     * Only when the backend has been dead for {@link Config#tablet_sched_be_down_tolerate_time_s}
     * seconds, will this method returns true.
     */
    private boolean isReplicaBackendDead(Backend backend) {
        long currentTimeMs = System.currentTimeMillis();
        assert backend != null;
        return !backend.isAlive() &&
                (currentTimeMs - backend.getLastUpdateMs() > Config.tablet_sched_be_down_tolerate_time_s * 1000);
    }

    private boolean isReplicaBackendDropped(Backend backend) {
        return backend == null;
    }

    private boolean isReplicaStateAbnormal(Replica replica, Backend backend, Set<String> replicaHostSet) {
        assert backend != null && replica != null;
        return replica.getState() == ReplicaState.CLONE
                || replica.getState() == ReplicaState.DECOMMISSION
                || replica.isBad()
                || !replicaHostSet.add(backend.getHost());
    }

    private boolean needRecoverWithEmptyTablet(SystemInfoService systemInfoService) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            if (Config.recover_with_empty_tablet && replicas.size() > 1) {
                int numReplicaLostForever = 0;
                int numReplicaRecoverable = 0;
                for (Replica replica : replicas) {
                    if (replica.isBad() || systemInfoService.getBackend(replica.getBackendId()) == null) {
                        numReplicaLostForever++;
                    } else {
                        numReplicaRecoverable++;
                    }
                }

                return numReplicaLostForever > 0 && numReplicaRecoverable == 0;
            }
        }

        return false;
    }

    public Pair<TabletStatus, TabletSchedCtx.Priority> getHealthStatusWithPriority(
            SystemInfoService systemInfoService,
            long visibleVersion, int replicationNum,
            List<Long> aliveBeIdsInCluster) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            return getHealthStatusWithPriorityUnlocked(systemInfoService, visibleVersion,
                    replicationNum, aliveBeIdsInCluster);
        }
    }

    /**
     * A replica is healthy only if
     * 1. the backend is available
     * 2. replica version is caught up, and last failed version is -1
     * <p>
     * A tablet is healthy only if
     * 1. healthy replica num is equal to replicationNum
     * 2. all healthy replicas are in right cluster
     */
    private Pair<TabletStatus, TabletSchedCtx.Priority> getHealthStatusWithPriorityUnlocked(
            SystemInfoService systemInfoService,
            long visibleVersion, int replicationNum,
            List<Long> aliveBeIdsInCluster) {

        int alive = 0;
        int aliveAndVersionComplete = 0;
        int stable = 0;

        Replica needFurtherRepairReplica = null;
        Set<String> hosts = Sets.newHashSet();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (isReplicaBackendDropped(backend)
                    || isReplicaBackendDead(backend)
                    || isReplicaStateAbnormal(replica, backend, hosts)) {
                // this replica is not alive,
                // or if this replica is on same host with another replica, we also treat it as 'dead',
                // so that Tablet Scheduler will create a new replica on different host.
                // ATTN: Replicas on same host is a bug of previous StarRocks version, so we fix it by this way.
                continue;
            }
            alive++;

            if (replica.needFurtherRepair() && needFurtherRepairReplica == null) {
                needFurtherRepairReplica = replica;
            }

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                continue;
            }
            aliveAndVersionComplete++;

            if (backend.isDecommissioned()) {
                // this replica is alive, version complete, but backend is not available
                continue;
            }
            stable++;
        }

        // 1. alive replicas are not enough
        int aliveBackendsNum = aliveBeIdsInCluster.size();
        // check whether we need to forcefully recover with an empty tablet first
        // we use a FORCE_REDUNDANT task to drop the invalid replica first and
        // then REPLICA_MISSING task will try to create that empty tablet
        if (needRecoverWithEmptyTablet(systemInfoService)) {
            LOG.info("need to forcefully recover with empty tablet for {}, replica info:{}",
                    id, getReplicaInfos());
            return createRedundantSchedCtx(TabletStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    needFurtherRepairReplica);
        }

        if (alive < replicationNum && replicas.size() >= aliveBackendsNum
                && aliveBackendsNum >= replicationNum && replicationNum > 1) {
            // there is no enough backend for us to create a new replica, so we have to delete an existing replica,
            // so there can be available backend for us to create a new replica.
            // And if there is only one replica, we will not handle it(maybe need human interference)
            // condition explain:
            // 1. alive < replicationNum: replica is missing or bad
            // 2. replicas.size() >= aliveBackendsNum: the existing replicas occupies all available backends
            // 3. aliveBackendsNum >= replicationNum: make sure after deletion, there will be
            //    at least one backend for new replica.
            // 4. replicationNum > 1: if replication num is set to 1, do not delete any replica, for safety reason
            // For example: 3 replica, 3 be, one set bad, we need to forcefully delete one first
            return createRedundantSchedCtx(TabletStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    needFurtherRepairReplica);
        } else {
            List<Long> availableBEs = systemInfoService.getAvailableBackendIds();
            // We create `REPLICA_MISSING` type task only when there exists enough available BEs which
            // we can choose to clone data to, if not we should check if we can create `VERSION_INCOMPLETE` task,
            // so that repair of replica with incomplete version won't be blocked and hence version publish process
            // of load task won't be blocked either.
            if (availableBEs.size() > alive) {
                if (alive < (replicationNum / 2) + 1) {
                    return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.HIGH);
                } else if (alive < replicationNum) {
                    return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.NORMAL);
                }
            }
        }

        // 2. version complete replicas are not enough
        if (aliveAndVersionComplete < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.HIGH);
        } else if (aliveAndVersionComplete < replicationNum) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.NORMAL);
        } else if (aliveAndVersionComplete > replicationNum) {
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return createRedundantSchedCtx(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    needFurtherRepairReplica);
        }

        // 3. replica is under relocating
        if (stable < replicationNum) {
            List<Long> replicaBeIds = replicas.stream()
                    .map(Replica::getBackendId).collect(Collectors.toList());
            List<Long> availableBeIds = aliveBeIdsInCluster.stream()
                    .filter(systemInfoService::checkBackendAvailable)
                    .collect(Collectors.toList());
            if (replicaBeIds.containsAll(availableBeIds)
                    && availableBeIds.size() >= replicationNum
                    && replicationNum > 1) { // Doesn't have any BE that can be chosen to create a new replica
                return createRedundantSchedCtx(TabletStatus.FORCE_REDUNDANT,
                        stable < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.NORMAL :
                                TabletSchedCtx.Priority.LOW, needFurtherRepairReplica);
            }
            if (stable < (replicationNum / 2) + 1) {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.NORMAL);
            } else {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, Priority.LOW);
            }
        }

        // 4. replica redundant
        if (replicas.size() > replicationNum) {
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return createRedundantSchedCtx(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH,
                    needFurtherRepairReplica);
        }

        // 5. healthy
        return Pair.create(TabletStatus.HEALTHY, TabletSchedCtx.Priority.NORMAL);
    }

    public TabletStatus getColocateHealthStatus(long visibleVersion,
                                                int replicationNum, Set<Long> backendsSet) {
        try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
            return getColocateHealthStatusUnlocked(visibleVersion, replicationNum, backendsSet);
        }
    }

    /**
     * Check colocate table's tablet health
     * <p>
     * 1. Mismatch:<p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,5
     * <p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2
     * <p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,4,5
     * <p>
     * 2. Version incomplete:<p>
     * backend matched, but some replica's version is incomplete
     * <p>
     * 3. Redundant:<p>
     * backends set:       1,2,3<p>
     * tablet replicas:    1,2,3,4
     * <p>
     * 4. Replica bad:<p>
     * If a replica is marked bad, we need to migrate it and all the other replicas corresponding to the same bucket
     * index in the colocate group to another backend, but the backend where the bad replica sits on may still be
     * available, so the backend set won't be changed by ColocateBalancer, so we need to learn this state and update
     * the backend set to replace the backend that has the bad replica. In order to be in consistent with the current
     * logic, we return COLOCATE_MISMATCH not REPLICA_MISSING.
     * <p></p>
     * No need to check if backend is available. We consider all backends in 'backendsSet' are available,
     * If not, unavailable backends will be relocated by ColocateTableBalancer first.
     */
    private TabletStatus getColocateHealthStatusUnlocked(long visibleVersion,
                                                int replicationNum, Set<Long> backendsSet) {
        // 1. check if replicas' backends are mismatch
        Set<Long> replicaBackendIds = getBackendIds();
        for (Long backendId : backendsSet) {
            if (!replicaBackendIds.contains(backendId)
                    && containsAnyHighPrioBackend(replicaBackendIds, Config.tablet_sched_colocate_balance_high_prio_backends)) {
                return TabletStatus.COLOCATE_MISMATCH;
            }
        }

        // 2. check version completeness
        for (Replica replica : replicas) {
            // do not check the replica that is not in the colocate backend set,
            // this kind of replica should be dropped.
            if (!backendsSet.contains(replica.getBackendId())) {
                continue;
            }

            if (replica.isBad()) {
                LOG.debug("colocate tablet {} has bad replica, need to drop-then-repair, " +
                        "current backend set: {}, visible version: {}", id, backendsSet, visibleVersion);
                // we use `TabletScheduler#handleColocateRedundant()` to drop bad replica forcefully.
                return TabletStatus.COLOCATE_REDUNDANT;
            }

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                return TabletStatus.VERSION_INCOMPLETE;
            }
        }

        // 3. check redundant
        if (replicas.size() > replicationNum) {
            return TabletStatus.COLOCATE_REDUNDANT;
        }

        return TabletStatus.HEALTHY;
    }

    private boolean containsAnyHighPrioBackend(Set<Long> backendIds, long[] highPriorityBackendIds) {
        if (highPriorityBackendIds == null || highPriorityBackendIds.length == 0) {
            return true;
        }

        for (long beId : highPriorityBackendIds) {
            if (backendIds.contains(beId)) {
                return true;
            }
        }

        return false;
    }

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORMAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    public boolean readyToBeRepaired(TabletStatus status, TabletSchedCtx.Priority priority) {
        if (priority == Priority.VERY_HIGH ||
                status == TabletStatus.VERSION_INCOMPLETE ||
                status == TabletStatus.NEED_FURTHER_REPAIR) {
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
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
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
<<<<<<< HEAD
                        replica.getState(), getReplicaBackendState(replica.getBackendId())));

=======
                        replica.isBad() ? "BAD" : replica.getState(), getReplicaBackendState(replica.getBackendId())));
>>>>>>> 7e69d6a195 ([Enhancement] Replica state should reflect whether it's bad when logging (#40071))
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
                    Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(replica.getBackendId());
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
