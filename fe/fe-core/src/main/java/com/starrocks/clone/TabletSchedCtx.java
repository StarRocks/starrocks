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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/TabletSchedCtx.java

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletHealthStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.clone.DiskAndTabletLoadReBalancer.BalanceType;
import com.starrocks.clone.SchedException.Status;
import com.starrocks.clone.TabletScheduler.PathSlot;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CloneTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.CreateReplicaTask.RecoverySource;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.thrift.TTabletSchedule;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * TabletSchedCtx contains all information which is created during tablet scheduler processing.
 */
public class TabletSchedCtx implements Comparable<TabletSchedCtx> {
    private static final Logger LOG = LogManager.getLogger(TabletSchedCtx.class);

    /*
     *  A clone task timeout is between Config.min_clone_task_timeout_sec and Config.max_clone_task_timeout_sec,
     *  estimated by tablet size / MIN_CLONE_SPEED_MB_PER_SECOND.
     */
    private static final long MIN_CLONE_SPEED_MB_PER_SECOND = 5; // 5MB/sec

    /*
     * If a clone task is failed to run more than RUNNING_FAILED_COUNTER_THRESHOLD, it will be removed
     * from the tablet scheduler.
     */
    private static final int RUNNING_FAILED_COUNTER_THRESHOLD = 3;

    public enum Type {
        BALANCE, REPAIR
    }

    public enum Priority {
        LOW,
        NORMAL,
        HIGH,
        VERY_HIGH;

        // try to upgrade the priority
        // LOW can only be upgraded to HIGH
        public Priority adjust(Priority origPriority) {
            switch (this) {
                case VERY_HIGH:
                    return VERY_HIGH;
                case HIGH:
                    return origPriority == LOW ? HIGH : VERY_HIGH;
                case NORMAL:
                    return HIGH;
                default:
                    return NORMAL;
            }
        }

    }

    public enum State {
        PENDING, // tablet is not being scheduled
        RUNNING, // tablet is being scheduled
        FINISHED, // task is finished
        CANCELLED, // task is failed
        TIMEOUT, // task is timeout
        UNEXPECTED // other unexpected errors
    }

    private Type type;

    /*
     * origPriority is the origin priority being set when this tablet being added to scheduler.
     * dynamicPriority will be set during tablet schedule processing, it will not be prior than origin priority.
     * And dynamic priority is also used in priority queue compare in tablet scheduler.
     */
    private Priority origPriority;
    private Priority dynamicPriority;

    // we change the dynamic priority based on how many times it fails to be scheduled
    private int failedSchedCounter = 0;
    // clone task failed counter
    private int failedRunningCounter = 0;

    // last time this tablet being scheduled
    private long lastSchedTime = 0;
    // last time the dynamic priority being adjusted
    private long lastAdjustPrioTime = 0;

    // last time this tablet being visited.
    // being visited means:
    // 1. being visited in TabletScheduler.schedulePendingTablets()
    // 2. being visited in finishCloneTask()
    //
    // This time is used to observer when this tablet being visited, for debug tracing.
    // It does not same as 'lastSchedTime', which is used for adjusting priority.
    private long lastVisitedTime = -1;

    // an approximate timeout of this task, only be set when sending clone task.
    private long taskTimeoutMs = 0;

    private State state;
    private TabletHealthStatus tabletHealthStatus;

    private final long dbId;
    private final long tblId;
    private final long partitionId;
    private final long physicalPartitionId;
    private final long indexId;
    private final long tabletId;
    private int schemaHash;
    private TStorageMedium storageMedium;

    private final long createTime;
    private long finishedTime = -1;

    private LocalTablet tablet = null;
    private long visibleVersion = -1;
    private long visibleTxnId = -1;
    private long committedVersion = -1;

    private Replica srcReplica = null;
    private long srcPathHash = -1;
    private long destBackendId = -1;
    private long destPathHash = -1;
    private String errMsg = null;

    private CloneTask cloneTask = null;

    // statistics gathered from clone task report
    // the total size of clone files and the total cost time in ms.
    private long copySize = 0;
    private long copyTimeMs = 0;

    private Set<Long> colocateBackendsSet = null;
    private int tabletOrderIdx = -1;
    // ID of colocate group that this tablet belongs to
    private GroupId colocateGroupId = null;
    private boolean relocationForRepair = false;

    private final SystemInfoService infoService;

    // for DiskAndTabletLoadReBalancer to identify balance type
    private BalanceType balanceType;
    // for DiskAndTabletLoadBalancer to identify whether to release disk path resource
    private boolean srcPathResourceHold = false;
    private boolean destPathResourceHold = false;

    private Replica decommissionedReplica;
    private ReplicaState decommissionedReplicaPreviousState;

    private Multimap<String, String> requiredLocation;

    /**
     * The number of replicas set for this tablet when creating the corresponding table.
     */
    private int replicaNum;

    public TabletSchedCtx(Type type, long dbId, long tblId, long partId, long physicalPartitionId,
                          long idxId, long tabletId, long createTime) {
        this.type = type;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.physicalPartitionId = physicalPartitionId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.createTime = createTime;
        this.infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        this.state = State.PENDING;
    }

    @VisibleForTesting
    public TabletSchedCtx(Type type, long dbId, long tblId, long partId,
                          long idxId, long tabletId, long createTime) {
        this(type, dbId, tblId, partId, partId, idxId, tabletId, createTime);
    }

    @VisibleForTesting
    public TabletSchedCtx(Type type, long dbId, long tblId, long partId,
                          long idxId, long tabletId, long createTime, SystemInfoService infoService) {
        this.type = type;
        this.dbId = dbId;
        this.tblId = tblId;
        this.partitionId = partId;
        this.physicalPartitionId = partId;
        this.indexId = idxId;
        this.tabletId = tabletId;
        this.createTime = createTime;
        this.infoService = infoService;
        this.state = State.PENDING;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public void setOrigPriority(Priority origPriority) {
        this.origPriority = origPriority;
        // reset dynamic priority along with the origin priority being set.
        this.dynamicPriority = origPriority;
        this.failedSchedCounter = 0;
        this.lastSchedTime = 0;
        this.lastAdjustPrioTime = 0;
    }

    public Priority getDynamicPriority() {
        return dynamicPriority;
    }

    public void increaseFailedSchedCounter() {
        ++failedSchedCounter;
    }

    public int getFailedSchedCounter() {
        return failedSchedCounter;
    }

    public void increaseFailedRunningCounter() {
        ++failedRunningCounter;
    }

    public void setLastSchedTime(long lastSchedTime) {
        this.lastSchedTime = lastSchedTime;
    }

    public void setLastVisitedTime(long lastVisitedTime) {
        this.lastVisitedTime = lastVisitedTime;
    }

    public void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setTabletStatus(TabletHealthStatus tabletHealthStatus) {
        this.tabletHealthStatus = tabletHealthStatus;
    }

    public TabletHealthStatus getTabletHealthStatus() {
        return tabletHealthStatus;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTblId() {
        return tblId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public void setSchemaHash(int schemaHash) {
        this.schemaHash = schemaHash;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    public void setTablet(LocalTablet tablet) {
        this.tablet = tablet;
    }

    public LocalTablet getTablet() {
        return tablet;
    }

    // database lock should be held.
    public List<Replica> getReplicas() {
        return tablet.getImmutableReplicas();
    }

    public void setVersionInfo(long visibleVersion,
                               long committedVersion, long visibleTxnId) {
        this.visibleVersion = visibleVersion;
        this.committedVersion = committedVersion;
        this.visibleTxnId = visibleTxnId;
    }

    public long getVisibleTxnId() {
        return visibleTxnId;
    }

    public void setDest(Long destBeId, long destPathHash) {
        this.destBackendId = destBeId;
        this.destPathHash = destPathHash;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public long getCopySize() {
        return copySize;
    }

    public long getCopyTimeMs() {
        return copyTimeMs;
    }

    public long getSrcBackendId() {
        if (srcReplica != null) {
            return srcReplica.getBackendId();
        } else {
            return -1;
        }
    }

    public long getSrcPathHash() {
        return srcPathHash;
    }

    public void setSrc(Replica srcReplica) {
        this.srcReplica = srcReplica;
        this.srcPathHash = srcReplica.getPathHash();
    }

    public Replica getSrcReplica() {
        return this.srcReplica;
    }

    public long getDestBackendId() {
        return destBackendId;
    }

    public long getDestPathHash() {
        return destPathHash;
    }

    // database lock should be held.
    public long getTabletSize() {
        long max = Long.MIN_VALUE;
        for (Replica replica : tablet.getImmutableReplicas()) {
            if (replica.getDataSize() > max) {
                max = replica.getDataSize();
            }
        }
        return max;
    }

    /*
     * check if existing replicas are on same BE.
     * database lock should be held.
     */
    public boolean containsBE(long beId, boolean forColocate) {
        String host = infoService.getBackend(beId).getHost();
        for (Replica replica : tablet.getImmutableReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // BE has been dropped, skip it
                continue;
            }

            // For colocate table, replica on same host is intermediate state,
            // ColocateTabletScheduler make sure no replica on same host in steady state
            if (forColocate) {
                if (replica.getBackendId() == beId) {
                    return true;
                }
            } else {
                if (host.equals(be.getHost())) {
                    return true;
                }
            }
        }
        return false;
    }

    public void setColocateGroupBackendIds(Set<Long> backendsSet) {
        this.colocateBackendsSet = backendsSet;
    }

    public Set<Long> getColocateBackendsSet() {
        return colocateBackendsSet;
    }

    public void setColocateGroupId(GroupId gid) {
        colocateGroupId = gid;
    }

    public GroupId getColocateGroupId() {
        return colocateGroupId;
    }

    public void setRelocationForRepair(boolean isRepair) {
        relocationForRepair = isRepair;
    }

    public Multimap<String, String> getRequiredLocation() {
        return requiredLocation;
    }

    public void setRequiredLocation(Multimap<String, String> requiredLocation) {
        this.requiredLocation = requiredLocation;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public boolean isRelocationForRepair() {
        return relocationForRepair;
    }

    public void setTabletOrderIdx(int idx) {
        this.tabletOrderIdx = idx;
    }

    public int getTabletOrderIdx() {
        return tabletOrderIdx;
    }

    public BalanceType getBalanceType() {
        return balanceType;
    }

    public void setBalanceType(BalanceType balanceType) {
        this.balanceType = balanceType;
    }

    public void setSrcPathResourceHold() {
        this.srcPathResourceHold = true;
    }

    public boolean isSrcPathResourceHold() {
        return this.srcPathResourceHold;
    }

    public void setDestPathResourceHold() {
        this.destPathResourceHold = true;
    }

    public boolean isDestPathResourceHold() {
        return this.destPathResourceHold;
    }

    public boolean needCloneFromSource() {
        return tabletHealthStatus == TabletHealthStatus.REPLICA_MISSING ||
                tabletHealthStatus == TabletHealthStatus.VERSION_INCOMPLETE ||
                tabletHealthStatus == TabletHealthStatus.REPLICA_RELOCATING ||
                tabletHealthStatus == TabletHealthStatus.LOCATION_MISMATCH ||
                tabletHealthStatus == TabletHealthStatus.COLOCATE_MISMATCH ||
                tabletHealthStatus == TabletHealthStatus.NEED_FURTHER_REPAIR;
    }

    public List<Replica> getHealthyReplicas() {
        List<Replica> candidates = Lists.newArrayList();
        for (Replica replica : tablet.getImmutableReplicas()) {
            if (replica.isBad()
                    || replica.getState() == ReplicaState.DECOMMISSION
                    || replica.getState() == ReplicaState.RECOVER) {
                continue;
            }

            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null || !be.isAlive()) {
                // backend which is in decommission can still be the source backend
                continue;
            }

            if (replica.getLastFailedVersion() > 0) {
                continue;
            }

            if (!replica.checkVersionCatchUp(visibleVersion, false)) {
                continue;
            }

            candidates.add(replica);
        }

        return candidates;
    }

    public Replica getDecommissionedReplica() {
        return decommissionedReplica;
    }

    // database lock should be held.
    public void chooseSrcReplica(Map<Long, PathSlot> backendsWorkingSlots) throws SchedException {
        /*
         * get all candidate source replicas
         * 1. source replica should be healthy.
         * 2. slot of this source replica is available.
         */
        List<Replica> candidates = getHealthyReplicas();
        if (candidates.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find source replica");
        }

        // Shuffle the candidate list first so that we won't always choose the same replica with
        // the highest version to avoid continuous clone failure
        Collections.shuffle(candidates);
        // Sort the candidates by version in desc order, so that replica with higher version
        // can be used as clone source first.
        candidates.sort((c1, c2) -> (int) (c2.getVersion() - c1.getVersion()));

        // choose a replica which slot is available from candidates.
        for (Replica srcReplica : candidates) {
            PathSlot slot = backendsWorkingSlots.get(srcReplica.getBackendId());
            if (slot == null) {
                continue;
            }

            long srcPathHash;
            try {
                srcPathHash = slot.takeSlot(srcReplica.getPathHash());
            } catch (SchedException e) {
                LOG.info("take slot from replica {}(belonged backend {}) failed, {}", srcReplica.getId(),
                        srcReplica.getBackendId(), e.getMessage());
                continue;
            }
            if (srcPathHash != -1) {
                setSrc(srcReplica);
                return;
            }
        }

        throw new SchedException(Status.SCHEDULE_RETRY, "path busy, wait for next round");
    }

    /*
     * Same rules as choosing source replica for supplement.
     * But we need to check that we can not choose the same replica as dest replica,
     * because replica info is changing all the time.
     */
    public void chooseSrcReplicaForVersionIncomplete(Map<Long, PathSlot> backendsWorkingSlots)
            throws SchedException {
        chooseSrcReplica(backendsWorkingSlots);
        if (srcReplica.getBackendId() == destBackendId) {
            throw new SchedException(Status.UNRECOVERABLE, "the chosen source replica is in dest backend");
        }
    }

    /*
     * Rules to choose a destination replica for version incomplete
     * 1. replica's last failed version > 0
     * 2. better to choose a replica which has a lower last failed version
     * 3. best to choose a replica if its last success version > last failed version
     * 4. if there's replica which needs further repair, choose that replica.
     *
     * database lock should be held.
     */
    public boolean chooseDestReplicaForVersionIncomplete(Map<Long, PathSlot> backendsWorkingSlots)
            throws SchedException {
        Replica chosenReplica = null;
        boolean needFurtherRepair = false;

        for (Replica replica : tablet.getImmutableReplicas()) {
            if (replica.isBad()) {
                continue;
            }

            Backend be = infoService.getBackend(replica.getBackendId());
            // NOTE: be in decommission can also be the destination,
            // or the version incomplete replica will block the decommission process
            if (be == null || !be.isAlive()) {
                continue;
            }

            if (replica.getLastFailedVersion() <= 0 && replica.getVersion() >= visibleVersion) {
                // skip healthy replica
                continue;
            }

            if (replica.needFurtherRepair()) {
                chosenReplica = replica;
                needFurtherRepair = true;
                break;
            }

            if (chosenReplica == null) {
                chosenReplica = replica;
            } else if (replica.getLastSuccessVersion() > replica.getLastFailedVersion()) {
                chosenReplica = replica;
                break;
            } else if (replica.getLastFailedVersion() < chosenReplica.getLastFailedVersion()) {
                // it's better to select a low last failed version replica
                chosenReplica = replica;
            }
        }

        if (chosenReplica == null) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to choose dest replica(maybe no incomplete replica");
        }

        // check if the dest replica has available slot
        PathSlot slot = backendsWorkingSlots.get(chosenReplica.getBackendId());
        if (slot == null) {
            throw new SchedException(Status.UNRECOVERABLE, "working slots not exist for be: "
                    + chosenReplica.getBackendId());
        }

        long destPathHash = slot.takeSlot(chosenReplica.getPathHash());
        if (destPathHash == -1) {
            throw new SchedException(Status.SCHEDULE_RETRY, "path busy, wait for next round");
        }

        setDest(chosenReplica.getBackendId(), chosenReplica.getPathHash());

        return needFurtherRepair;
    }

    public void releaseResource(TabletScheduler tabletScheduler) {
        releaseResource(tabletScheduler, false);
    }

    /*
     * release all resources before finishing this task.
     * if reserveTablet is true, the tablet object in this ctx will not be set to null after calling reset().
     */
    public void releaseResource(TabletScheduler tabletScheduler, boolean reserveTablet) {
        if (srcReplica != null) {
            Preconditions.checkState(srcPathHash != -1);
            PathSlot slot = tabletScheduler.getBackendsWorkingSlots().get(srcReplica.getBackendId());
            if (slot != null) {
                if (type == Type.REPAIR || isSrcPathResourceHold()) {
                    slot.freeSlot(srcPathHash);
                }
            }
        }

        if (destPathHash != -1) {
            PathSlot slot = tabletScheduler.getBackendsWorkingSlots().get(destBackendId);
            if (slot != null) {
                if (type == Type.REPAIR || isDestPathResourceHold()) {
                    slot.freeSlot(destPathHash);
                }
            }
        }

        if (cloneTask != null) {
            AgentTaskQueue.removeTask(cloneTask.getBackendId(), TTaskType.CLONE, cloneTask.getSignature());

            List<Replica> cloneReplicas = Lists.newArrayList();
            tablet.getImmutableReplicas().stream().filter(r -> r.getState() == ReplicaState.CLONE).forEach(
                    cloneReplicas::add);

            for (Replica cloneReplica : cloneReplicas) {
                tablet.deleteReplica(cloneReplica);
            }
        }

        reset(reserveTablet);
    }

    // reset to save memory after state is done
    private void reset(boolean reserveTablet) {
        /*
         * If state is PENDING, these fields will be reset when being rescheduled.
         * if state is FINISHED/CANCELLED/TIMEOUT, leave these fields for show.
         *
         * condition explain:
         * 1. only for PENDING task
         * 2. repair task or balance task that does not adopt strategy of TABLET_BALANCER_STRATEGY_DISK_AND_TABLET
         */
        if (state == State.PENDING
                && (type == Type.REPAIR)) {
            if (!reserveTablet) {
                this.tablet = null;
            }
            this.srcReplica = null;
            this.srcPathHash = -1;
            this.destBackendId = -1;
            this.destPathHash = -1;
            this.cloneTask = null;
        }
    }

    public void resetDecommissionedReplicaState() {
        if (decommissionedReplica != null
                && decommissionedReplica.getState() == ReplicaState.DECOMMISSION
                && decommissionedReplicaPreviousState != null) {
            decommissionedReplica.setState(decommissionedReplicaPreviousState);
            decommissionedReplica.setWatermarkTxnId(-1);
            LOG.debug("reset replica {} on backend {} with state from DECOMMISSION to {}, tablet: {}",
                    decommissionedReplica.getId(), decommissionedReplica.getBackendId(),
                    decommissionedReplica.getState(), tabletId);
        }
    }

    public void setDecommissionedReplica(Replica replica) {
        this.decommissionedReplica = replica;
        this.decommissionedReplicaPreviousState = replica.getState();
    }

    public boolean deleteReplica(Replica replica) {
        return tablet.deleteReplicaByBackendId(replica.getBackendId());
    }

    public CloneTask createCloneReplicaAndTask() throws SchedException {
        Backend srcBe = infoService.getBackend(srcReplica.getBackendId());
        if (srcBe == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    "src backend " + srcReplica.getBackendId() + " does not exist");
        }

        Backend destBe = infoService.getBackend(destBackendId);
        if (destBe == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    "dest backend " + destBackendId + " does not exist");
        }

        taskTimeoutMs = getApproximateTimeoutMs();

        // create the clone task and clone replica.
        // we use visible version in clone task, but set the clone replica's last failed version to
        // committed version.
        // because the clone task can only clone the data with visible version.
        // so after clone is finished, the clone replica's version is visible version, but its last
        // failed version is committed version, which is larger than visible version.
        // So after clone, this clone replica is still version incomplete, and it will be repaired in
        // another clone task.
        // That is, we may need to use 2 clone tasks to create a new replica. It is inefficient,
        // but there is no other way now.
        TBackend tSrcBe = new TBackend(srcBe.getHost(), srcBe.getBePort(), srcBe.getHttpPort());
        cloneTask = new CloneTask(destBackendId, dbId, tblId, partitionId, indexId,
                tabletId, schemaHash, Lists.newArrayList(tSrcBe), storageMedium,
                visibleVersion, (int) (taskTimeoutMs / 1000));
        cloneTask.setPathHash(srcPathHash, destPathHash);
        cloneTask.setIsLocal(srcReplica.getBackendId() == destBackendId);

        // if this is a balance task, or this is a repair task with REPLICA_MISSING/REPLICA_RELOCATING,
        // we create a new replica with state CLONE
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db " + dbId + " not exist");
        }
        Locker locker = new Locker();
        try {
            locker.lockDatabase(db, LockType.WRITE);
            if (tabletHealthStatus == TabletHealthStatus.REPLICA_MISSING
                    || tabletHealthStatus == TabletHealthStatus.REPLICA_RELOCATING
                    || tabletHealthStatus == TabletHealthStatus.LOCATION_MISMATCH
                    || tabletHealthStatus == TabletHealthStatus.COLOCATE_MISMATCH
                    || (type == Type.BALANCE && !cloneTask.isLocal())) {
                // We should avoid full clone task to run concurrently with replica drop task,
                // otherwise it may cause replica lost in the following scenario:
                // we have a tablet T with only one replica,
                // t1: balancer move T from backend 10001 to backend 10002
                // t2: after clone finished, redundant replica on 10001 will be dropped,
                //     but this will only clean the meta from FE, replica is physically
                //     dropped until next tablet report from BE
                // t3: balancer schedule a task to move back T from backend 10002 to backend 10001
                // t4: tablet report from 10001 received, and a replica drop task is sent to backend 10001
                // t5: clone on backend 10001 finished, and a new replica of T is created on FE
                // t6: replica drop task has been executed on backend 10001
                // t7: after the second clone finished, redundant replica on 10002 will be dropped
                // t8: replica of T is physically lost with only meta of replica on 10001 is left on FE
                //
                // If the timing of consecutive full clones is too close, we will delay the next full clone
                // so that report handler will delete the src replica physically in time.
                if (System.currentTimeMillis() - tablet.getLastFullCloneFinishedTimeMs() <
                        Config.tablet_sched_consecutive_full_clone_delay_sec * 1000) {
                    throw new SchedException(Status.SCHEDULE_RETRY, "consecutive full clone needs to delay");
                }

                Replica cloneReplica = new Replica(
                        GlobalStateMgr.getCurrentState().getNextId(), destBackendId,
                        -1 /* version */, schemaHash,
                        -1 /* data size */, -1 /* row count */,
                        ReplicaState.CLONE,
                        committedVersion,
                        -1 /* last success version */);

                // addReplica() method will add this replica to tablet inverted index too.
                tablet.addReplica(cloneReplica);
            } else if (tabletHealthStatus == TabletHealthStatus.VERSION_INCOMPLETE) {
                Preconditions.checkState(type == Type.REPAIR, type);
                // double check
                Replica replica = tablet.getReplicaByBackendId(destBackendId);
                if (replica == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "dest replica does not exist on BE " + destBackendId);
                }

                if (replica.getPathHash() != destPathHash) {
                    throw new SchedException(Status.UNRECOVERABLE, "dest replica's path hash is changed. "
                            + "current: " + replica.getPathHash() + ", scheduled: " + destPathHash);
                }
            } else if (type == Type.BALANCE && cloneTask.isLocal()) {
                if (tabletHealthStatus != TabletHealthStatus.HEALTHY) {
                    throw new SchedException(Status.SCHEDULE_RETRY, "tablet " + tabletId + " is not healthy");
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        this.state = State.RUNNING;
        return cloneTask;
    }

    public CreateReplicaTask createEmptyReplicaAndTask() throws SchedException {
        // only create this task if force recovery is true
        LOG.warn("tablet {} has only one replica {} on backend {}"
                        + " and it is lost. create an empty replica to recover it on backend {}",
                tabletId, tablet.getSingleReplica(), tablet.getSingleReplica().getBackendId(), destBackendId);

        final GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db " + dbId + " does not exist");
        }
        Locker locker = new Locker();
        try {
            locker.lockDatabase(db, LockType.WRITE);
            OlapTable olapTable = (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(
                    globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId),
                    tblId);
            if (olapTable == null) {
                throw new SchedException(Status.UNRECOVERABLE, "table " + tblId + " does not exist");
            }
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
            if (indexMeta == null) {
                throw new SchedException(Status.UNRECOVERABLE, "materialized view " + indexId + " does not exist");
            }
            CreateReplicaTask createReplicaTask = new CreateReplicaTask(destBackendId, dbId,
                    tblId, partitionId, indexId, tabletId, indexMeta.getShortKeyColumnCount(),
                    indexMeta.getSchemaHash(), indexMeta.getSchemaVersion(), visibleVersion,
                    indexMeta.getKeysType(),
                    indexMeta.getStorageType(),
                    TStorageMedium.HDD, indexMeta.getSchema(), olapTable.getCopiedBfColumns(), olapTable.getBfFpp(), null,
                    olapTable.getCopiedIndexes(),
                    olapTable.isInMemory(),
                    olapTable.enablePersistentIndex(),
                    olapTable.primaryIndexCacheExpireSec(),
                    olapTable.getPartitionInfo().getTabletType(partitionId),
                    olapTable.getCompressionType(), indexMeta.getSortKeyIdxes(),
                    indexMeta.getSortKeyUniqueIds());
            createReplicaTask.setRecoverySource(RecoverySource.SCHEDULER);
            taskTimeoutMs = Config.tablet_sched_min_clone_task_timeout_sec * 1000;

            Replica emptyReplica =
                    new Replica(tablet.getSingleReplica().getId(), destBackendId, ReplicaState.RECOVER, visibleVersion,
                            indexMeta.getSchemaHash());
            // addReplica() method will add this replica to tablet inverted index too.
            tablet.addReplica(emptyReplica);
            state = State.RUNNING;
            return createReplicaTask;
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }
    }

    // timeout is between MIN_CLONE_TASK_TIMEOUT_MS and MAX_CLONE_TASK_TIMEOUT_MS
    private long getApproximateTimeoutMs() {
        long tabletSize = getTabletSize();
        long timeoutMs = tabletSize / 1024 / 1024 / MIN_CLONE_SPEED_MB_PER_SECOND * 1000;
        timeoutMs = Math.max(timeoutMs, Config.tablet_sched_min_clone_task_timeout_sec * 1000);
        timeoutMs = Math.min(timeoutMs, Config.tablet_sched_max_clone_task_timeout_sec * 1000);
        return timeoutMs;
    }

    /*
     * 1. Check if the tablet is already healthy. If yes, ignore the clone task report, and take it as FINISHED.
     * 2. If not, check the reported clone replica, and try to make it effective.
     *
     * Throw SchedException if error happens
     * 1. SCHEDULE_FAILED: will keep the tablet RUNNING.
     * 2. UNRECOVERABLE: will remove the tablet from runningTablets.
     */
    public void finishCloneTask(CloneTask cloneTask, TFinishTaskRequest request)
            throws SchedException {
        Preconditions.checkState(state == State.RUNNING, state);
        Preconditions.checkArgument(cloneTask.getTaskVersion() == CloneTask.VERSION_2);
        setLastVisitedTime(System.currentTimeMillis());
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        // check if clone task success
        if (request.getTask_status().getStatus_code() != TStatusCode.OK) {
            throw new SchedException(Status.UNRECOVERABLE, request.getTask_status().getError_msgs().get(0));
        }

        // check tablet info is set
        if (!request.isSetFinish_tablet_infos() || request.getFinish_tablet_infos().isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "tablet info is not set in task report request");
        }

        // check task report
        if (dbId != cloneTask.getDbId() || tblId != cloneTask.getTableId()
                || partitionId != cloneTask.getPartitionId() || indexId != cloneTask.getIndexId()
                || tabletId != cloneTask.getTabletId() || destBackendId != cloneTask.getBackendId()) {
            String msg = String.format("clone task does not match the tablet info"
                            + ". clone task %d-%d-%d-%d-%d-%d"
                            + ", tablet info: %d-%d-%d-%d-%d-%d",
                    cloneTask.getDbId(), cloneTask.getTableId(), cloneTask.getPartitionId(),
                    cloneTask.getIndexId(), cloneTask.getTabletId(), cloneTask.getBackendId(),
                    dbId, tblId, partitionId, indexId, tablet.getId(), destBackendId);
            throw new SchedException(Status.UNRECOVERABLE, msg);
        }

        // 1. check the tablet status first
        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
        }
        Locker locker = new Locker();
        try {
            locker.lockDatabase(db, LockType.WRITE);
            OlapTable olapTable = (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tblId);
            if (olapTable == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
            }

            Partition partition = globalStateMgr.getLocalMetastore().getPartitionIncludeRecycleBin(olapTable, partitionId);
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }

            short replicationNum =
                    globalStateMgr.getLocalMetastore()
                            .getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(), partitionId);
            if (replicationNum == (short) -1) {
                throw new SchedException(Status.UNRECOVERABLE, "invalid replication number");
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
            }

            if (schemaHash != olapTable.getSchemaHashByIndexId(indexId)) {
                throw new SchedException(Status.UNRECOVERABLE, "schema hash is not consistent. index's: "
                        + olapTable.getSchemaHashByIndexId(indexId)
                        + ", task's: " + schemaHash);
            }

            LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
            if (tablet == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet does not exist");
            }

            String finishInfo = "";
            if (cloneTask.isLocal()) {
                unprotectedFinishLocalMigration(request);
            } else {
                finishInfo = unprotectedFinishClone(request, partition, replicationNum, olapTable.getLocation());
            }

            state = State.FINISHED;
            LOG.info("clone finished: {} {}", this, finishInfo);
        } catch (SchedException e) {
            // if failed to too many times, remove this task
            ++failedRunningCounter;
            if (failedRunningCounter > RUNNING_FAILED_COUNTER_THRESHOLD) {
                throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
            }
            throw e;
        } finally {
            locker.unLockDatabase(db, LockType.WRITE);
        }

        if (request.isSetCopy_size()) {
            this.copySize = request.getCopy_size();
        }

        if (request.isSetCopy_time_ms()) {
            this.copyTimeMs = request.getCopy_time_ms();
        }
    }

    private void unprotectedFinishLocalMigration(TFinishTaskRequest request) throws SchedException {
        Replica replica = tablet.getReplicaByBackendId(destBackendId);
        if (replica == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    "replica does not exist. backend id: " + destBackendId);
        }

        // local migration only needs set path hash
        TTabletInfo reportedTablet = request.getFinish_tablet_infos().get(0);
        Preconditions.checkArgument(reportedTablet.isSetPath_hash());
        replica.setPathHash(reportedTablet.getPath_hash());
        replica.updateVersion(reportedTablet.version);
    }

    private String unprotectedFinishClone(TFinishTaskRequest request, Partition partition,
                                          short replicationNum, Multimap<String, String> location) throws SchedException {
        List<Long> aliveBeIdsInCluster = infoService.getBackendIds(true);
        Pair<TabletHealthStatus, TabletSchedCtx.Priority> pair = TabletChecker.getTabletHealthStatusWithPriority(
                tablet, infoService, visibleVersion, replicationNum,
                aliveBeIdsInCluster, location);
        if (pair.first == TabletHealthStatus.HEALTHY) {
            throw new SchedException(Status.FINISHED, "tablet is healthy");
        }

        // tablet is unhealthy, go on

        // Here we do not check if the clone version is equal to the partition's visible version.
        // Because in case of high frequency loading, clone version always lags behind the visible version,
        // But we will check if the clone replica's version is larger than or equal to the task's visible version.
        // (which is 'visibleVersion[Hash]' saved)
        // We should discard the clone replica with stale version.
        TTabletInfo reportedTablet = request.getFinish_tablet_infos().get(0);
        if (reportedTablet.getVersion() < visibleVersion) {
            String msg = String.format("the clone replica's version is stale. %d, task visible version: %d",
                    reportedTablet.getVersion(),
                    visibleVersion);
            throw new SchedException(Status.UNRECOVERABLE, msg);
        }

        // check if replica exist
        Replica replica = tablet.getReplicaByBackendId(destBackendId);
        if (replica == null) {
            throw new SchedException(Status.UNRECOVERABLE,
                    "replica does not exist. backend id: " + destBackendId);
        }

        long beVisibleVersion = Config.enable_sync_publish ? reportedTablet.getMax_readable_version() :
                reportedTablet.getVersion();

        replica.updateRowCount(beVisibleVersion, reportedTablet.getMin_readable_version(),
                reportedTablet.getData_size(), reportedTablet.getRow_count());
        if (reportedTablet.isSetPath_hash()) {
            replica.setPathHash(reportedTablet.getPath_hash());
        }

        if (this.type == Type.BALANCE) {
            long partitionVisibleVersion = partition.getVisibleVersion();
            if (replica.getVersion() < partitionVisibleVersion) {
                // see comment 'needFurtherRepair' of Replica for explanation.
                // no need to persist this info. If FE restart, just do it again.
                replica.setNeedFurtherRepair(true);
            }
        } else {
            replica.setNeedFurtherRepair(false);
        }

        ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(dbId, tblId, partitionId, indexId,
                tabletId, destBackendId, replica.getId(),
                replica.getVersion(),
                reportedTablet.getSchema_hash(),
                reportedTablet.getData_size(),
                reportedTablet.getRow_count(),
                replica.getLastFailedVersion(),
                replica.getLastSuccessVersion(),
                reportedTablet.getMin_readable_version());

        if (replica.getState() == ReplicaState.CLONE) {
            replica.setState(ReplicaState.NORMAL);
            tablet.setLastFullCloneFinishedTimeMs(System.currentTimeMillis());
            GlobalStateMgr.getCurrentState().getEditLog().logAddReplica(info);
        } else {
            // if in VERSION_INCOMPLETE, replica is not newly created, thus the state is not CLONE
            // so, we keep it state unchanged, and log update replica
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateReplica(info);
        }
        return String.format("version:%d min_readable_version:%d", reportedTablet.getVersion(),
                reportedTablet.getMin_readable_version());
    }

    /**
     * try to upgrade the priority if this tablet has not been scheduled for a long time
     */
    public boolean adjustPriority(TabletSchedulerStat stat) {
        long currentTime = System.currentTimeMillis();
        if (lastAdjustPrioTime == 0) {
            // skip the first time we adjust this priority
            lastAdjustPrioTime = currentTime;
            return false;
        } else {
            if (currentTime - lastAdjustPrioTime < Config.tablet_sched_max_not_being_scheduled_interval_ms) {
                return false;
            }
        }

        lastAdjustPrioTime = System.currentTimeMillis();

        Priority originDynamicPriority = dynamicPriority;
        dynamicPriority = dynamicPriority.adjust(origPriority);
        if (originDynamicPriority != dynamicPriority) {
            LOG.debug("upgrade dynamic priority from {} to {}, origin: {}, tablet: {}",
                    originDynamicPriority.name(), dynamicPriority.name(), origPriority.name(), tabletId);
            stat.counterTabletPrioUpgraded.incrementAndGet();
            return true;
        }
        return false;
    }

    public boolean isTimeout() {
        if (state != TabletSchedCtx.State.RUNNING) {
            return false;
        }

        Preconditions.checkState(lastSchedTime != 0 && taskTimeoutMs != 0, lastSchedTime + "-" + taskTimeoutMs);
        return System.currentTimeMillis() - lastSchedTime > taskTimeoutMs;
    }

    public List<String> getBrief() {
        List<String> result = Lists.newArrayList();
        result.add(String.valueOf(tabletId));
        result.add(type.name());
        result.add(storageMedium == null ? FeConstants.NULL_STRING : storageMedium.name());
        result.add(tabletHealthStatus == null ? FeConstants.NULL_STRING : tabletHealthStatus.name());
        result.add(state.name());
        result.add(origPriority.name());
        result.add(dynamicPriority.name());
        result.add(srcReplica == null ? "-1" : String.valueOf(srcReplica.getBackendId()));
        result.add(String.valueOf(srcPathHash));
        result.add(String.valueOf(destBackendId));
        result.add(String.valueOf(destPathHash));
        result.add(String.valueOf(taskTimeoutMs));
        result.add(TimeUtils.longToTimeString(createTime));
        result.add(TimeUtils.longToTimeString(lastSchedTime));
        result.add(TimeUtils.longToTimeString(lastVisitedTime));
        result.add(TimeUtils.longToTimeString(finishedTime));
        result.add(copyTimeMs > 0 ? String.valueOf((double) copySize / copyTimeMs / 1000.0) : FeConstants.NULL_STRING);
        result.add(String.valueOf(failedSchedCounter));
        result.add(String.valueOf(failedRunningCounter));
        result.add(TimeUtils.longToTimeString(lastAdjustPrioTime));
        result.add(String.valueOf(visibleVersion));
        result.add(String.valueOf(0));
        result.add(String.valueOf(committedVersion));
        result.add(String.valueOf(0));
        result.add(Strings.nullToEmpty(errMsg));
        return result;
    }

    public TTabletSchedule toTabletScheduleThrift() {
        TTabletSchedule result = new TTabletSchedule();
        result.setTablet_id(tblId);
        result.setPartition_id(partitionId);
        result.setTablet_id(tabletId);
        result.setType(type.name());
        result.setPriority(dynamicPriority != null ? dynamicPriority.name() : "");
        result.setState(state != null ? state.name() : "");
        result.setTablet_status(tabletHealthStatus != null ? tabletHealthStatus.name() : "");
        result.setCreate_time(createTime / 1000.0);
        result.setSchedule_time(lastSchedTime / 1000.0);
        result.setFinish_time(finishedTime / 1000.0);
        result.setClone_src(srcReplica == null ? -1 : srcReplica.getBackendId());
        result.setClone_dest(destBackendId);
        result.setClone_bytes(copySize);
        result.setClone_duration(copyTimeMs / 1000.0);
        result.setError_msg(errMsg);
        return result;
    }

    /*
     * First compared by dynamic priority. higher priority rank ahead.
     * If priority is equals, compared by last visit time, earlier visit time rank ahead.
     */
    @Override
    public int compareTo(TabletSchedCtx o) {
        if (dynamicPriority.ordinal() < o.dynamicPriority.ordinal()) {
            return 1;
        } else if (dynamicPriority.ordinal() > o.dynamicPriority.ordinal()) {
            return -1;
        } else {
            return Long.compare(lastVisitedTime, o.lastVisitedTime);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", status: ").append(tabletHealthStatus.name());
        sb.append(", state: ").append(state.name()).append(", type: ").append(type.name());
        if (srcReplica != null) {
            sb.append(". from backend: ").append(srcReplica.getBackendId());
            sb.append(", src path hash: ").append(srcPathHash);
        }
        if (destPathHash != -1) {
            sb.append(". to backend: ").append(destBackendId);
            sb.append(", dest path hash: ").append(destPathHash);
        }
        if (errMsg != null) {
            sb.append(". err: ").append(errMsg);
        }
        return sb.toString();
    }
}
