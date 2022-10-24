// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletSchedCtx.Priority;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents the local olap tablet related metadata.
 * LocalTablet is based on local disk storage and replicas are managed by StarRocks.
 */
public class LocalTablet extends Tablet {
    private static final Logger LOG = LogManager.getLogger(LocalTablet.class);

    public enum TabletStatus {
        HEALTHY,
        REPLICA_MISSING, // not enough alive replica num.
        VERSION_INCOMPLETE, // alive replica num is enough, but version is missing.
        REPLICA_RELOCATING, // replica is healthy, but is under relocating (eg. BE is decommission).
        REDUNDANT, // too much replicas.
        REPLICA_MISSING_IN_CLUSTER, // not enough healthy replicas in correct cluster.
        FORCE_REDUNDANT, // some replica is missing or bad, but there is no other backends for repair,
        // at least one replica has to be deleted first to make room for new replica.
        COLOCATE_MISMATCH, // replicas do not all locate in right colocate backends set.
        COLOCATE_REDUNDANT, // replicas match the colocate backends set, but redundant.
        NEED_FURTHER_REPAIR, // one of replicas need a definite repair.
    }

    @SerializedName(value = "replicas")
    private List<Replica> replicas;
    @SerializedName(value = "checkedVersion")
    private long checkedVersion;
    @SerializedName(value = "isConsistent")
    private boolean isConsistent;

    // last time that the tablet checker checks this tablet.
    // no need to persist
    private long lastStatusCheckTime = -1;

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

        return delete || !hasBackend;
    }

    public void addReplica(Replica replica, boolean updateInvertedIndex) {
        if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
            replicas.add(replica);
            if (updateInvertedIndex) {
                Catalog.getCurrentInvertedIndex().addReplica(id, replica);
            }
        }
    }

    public void addReplica(Replica replica) {
        addReplica(replica, true);
    }

    public List<Replica> getReplicas() {
        return this.replicas;
    }

    public Set<Long> getBackendIds() {
        Set<Long> beIds = Sets.newHashSet();
        for (Replica replica : replicas) {
            beIds.add(replica.getBackendId());
        }
        return beIds;
    }

    public List<String> getBackends() {
        List<String> backends = new ArrayList<String>(); 
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
            backends.add(backend.getHost()); 
        }
        return backends;
    }

    // for loading data
    public List<Long> getNormalReplicaBackendIds() {
        List<Long> beIds = Lists.newArrayList();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (infoService.checkBackendAlive(replica.getBackendId()) && state.canLoad()) {
                beIds.add(replica.getBackendId());
            }
        }
        return beIds;
    }

    // return map of (BE id -> path hash) of normal replicas
    public Multimap<Long, Long> getNormalReplicaBackendPathMap(int clusterId) {
        Multimap<Long, Long> map = HashMultimap.create();
        SystemInfoService infoService = Catalog.getCurrentCatalog().getOrCreateSystemInfo(clusterId);
        for (Replica replica : replicas) {
            if (replica.isBad()) {
                continue;
            }

            ReplicaState state = replica.getState();
            if (infoService.checkBackendAlive(replica.getBackendId())
                    && (state == ReplicaState.NORMAL || state == ReplicaState.ALTER)) {
                map.put(replica.getBackendId(), replica.getPathHash());
            }
        }
        return map;
    }

    // for query
    public void getQueryableReplicas(List<Replica> allQuerableReplica, List<Replica> localReplicas,
                                     long visibleVersion, long localBeId, int schemaHash) {
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
                        && (replica.getSchemaHash() == -1 || replica.getSchemaHash() == schemaHash)) {
                    allQuerableReplica.add(replica);
                    if (localBeId != -1 && replica.getBackendId() == localBeId) {
                        localReplicas.add(replica);
                    }
                }
            }
        }
    }

    public int getQueryableReplicasSize(long visibleVersion, int schemaHash) {
        int size = 0;
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
                        && (replica.getSchemaHash() == -1 || replica.getSchemaHash() == schemaHash)) {
                    size++;
                }
            }
        }
        return size;
    }

    public Replica getReplicaById(long replicaId) {
        for (Replica replica : replicas) {
            if (replica.getId() == replicaId) {
                return replica;
            }
        }
        return null;
    }

    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) {
            if (replica.getBackendId() == backendId) {
                return replica;
            }
        }
        return null;
    }

    public boolean deleteReplica(Replica replica) {
        if (replicas.contains(replica)) {
            replicas.remove(replica);
            Catalog.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendId());
            return true;
        }
        return false;
    }

    public boolean deleteReplicaByBackendId(long backendId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendId() == backendId) {
                iterator.remove();
                Catalog.getCurrentInvertedIndex().deleteReplica(id, backendId);
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public Replica deleteReplicaById(long replicaId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getId() == replicaId) {
                LOG.info("delete replica[" + replica.getId() + "]");
                iterator.remove();
                return replica;
            }
        }
        return null;
    }

    // for test,
    // and for some replay cases
    public void clearReplica() {
        this.replicas.clear();
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
                replicas.add(replica);
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= 6) {
            checkedVersion = in.readLong();
            in.readLong(); // read a version_hash for compatibility
            isConsistent = in.readBoolean();
        }
    }

    public static LocalTablet read(DataInput in) throws IOException {
        LocalTablet tablet = new LocalTablet();
        tablet.readFields(in);
        return tablet;
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

    @Override
    public long getDataSize() {
        long dataSize = 0;
        for (Replica replica : getReplicas()) {
            if (replica.getState() == ReplicaState.NORMAL
                    || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                dataSize += replica.getDataSize();
            }
        }
        return dataSize;
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
    public Pair<TabletStatus, TabletSchedCtx.Priority> getHealthStatusWithPriority(
            SystemInfoService systemInfoService, String clusterName,
            long visibleVersion, int replicationNum,
            List<Long> aliveBeIdsInCluster) {

        int alive = 0;
        int aliveAndVersionComplete = 0;
        int stable = 0;
        int availableInCluster = 0;

        Replica needFurtherRepairReplica = null;
        Set<String> hosts = Sets.newHashSet();
        for (Replica replica : replicas) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (backend == null || !backend.isAlive() || replica.getState() == ReplicaState.CLONE
                    || replica.getState() == ReplicaState.DECOMMISSION
                    || replica.isBad() || !hosts.add(backend.getHost())) {
                // this replica is not alive,
                // or if this replica is on same host with another replica, we also treat it as 'dead',
                // so that Tablet Scheduler will create a new replica on different host.
                // ATTN: Replicas on same host is a bug of previous StarRocks version, so we fix it by this way.
                continue;
            }
            alive++;

            if (replica.getLastFailedVersion() > 0 || replica.getVersion() < visibleVersion) {
                // this replica is alive but version incomplete
                continue;
            }
            aliveAndVersionComplete++;

            if (!backend.isAvailable()) {
                // this replica is alive, version complete, but backend is not available
                continue;
            }
            stable++;

            if (!backend.getOwnerClusterName().equals(clusterName)) {
                // this replica is available, version complete, but not in right cluster
                continue;
            }
            availableInCluster++;

            if (replica.needFurtherRepair() && needFurtherRepairReplica == null) {
                needFurtherRepairReplica = replica;
            }
        }

        // 1. alive replicas are not enough
        int aliveBackendsNum = aliveBeIdsInCluster.size();
        if (alive < replicationNum && replicas.size() >= aliveBackendsNum
                && aliveBackendsNum >= replicationNum && replicationNum > 1) {
            // there is no enough backend for us to create a new replica, so we have to delete an existing replica,
            // so there can be available backend for us to create a new replica.
            // And if there is only one replica, we will not handle it(maybe need human interference)
            // condition explain:
            // 1. alive < replicationNum: replica is missing or bad
            // 2. replicas.size() >= aliveBackendsNum: the existing replicas occupies all available backends
            // 3. aliveBackendsNum >= replicationNum: make sure after deleting, there will be at least one backend for new replica.
            // 4. replicationNum > 1: if replication num is set to 1, do not delete any replica, for safety reason
            return Pair.create(TabletStatus.FORCE_REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        } else if (alive < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.HIGH);
        } else if (alive < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING, TabletSchedCtx.Priority.NORMAL);
        }

        // 2. version complete replicas are not enough
        if (aliveAndVersionComplete < (replicationNum / 2) + 1) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.HIGH);
        } else if (aliveAndVersionComplete < replicationNum) {
            return Pair.create(TabletStatus.VERSION_INCOMPLETE, TabletSchedCtx.Priority.NORMAL);
        } else if (aliveAndVersionComplete > replicationNum) {
            if (needFurtherRepairReplica != null) {
                return Pair.create(TabletStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
            }
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return Pair.create(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
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
                    && replicationNum > 1) { // No BE can be choose to create a new replica
                return Pair.create(TabletStatus.FORCE_REDUNDANT,
                        stable < (replicationNum / 2) + 1 ? TabletSchedCtx.Priority.NORMAL :
                                TabletSchedCtx.Priority.LOW);
            }
            if (stable < (replicationNum / 2) + 1) {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, TabletSchedCtx.Priority.NORMAL);
            } else {
                return Pair.create(TabletStatus.REPLICA_RELOCATING, Priority.LOW);
            }
        }

        // 4. healthy replicas in cluster are not enough
        if (availableInCluster < replicationNum) {
            return Pair.create(TabletStatus.REPLICA_MISSING_IN_CLUSTER, TabletSchedCtx.Priority.LOW);
        } else if (replicas.size() > replicationNum) {
            if (needFurtherRepairReplica != null) {
                return Pair.create(TabletStatus.NEED_FURTHER_REPAIR, TabletSchedCtx.Priority.HIGH);
            }
            // we set REDUNDANT as VERY_HIGH, because delete redundant replicas can free the space quickly.
            return Pair.create(TabletStatus.REDUNDANT, TabletSchedCtx.Priority.VERY_HIGH);
        }

        // 5. healthy
        return Pair.create(TabletStatus.HEALTHY, TabletSchedCtx.Priority.NORMAL);
    }

    /**
     * Check colocate table's tablet health
     * 1. Mismatch:
     * backends set:       1,2,3
     * tablet replicas:    1,2,5
     * <p>
     * backends set:       1,2,3
     * tablet replicas:    1,2
     * <p>
     * backends set:       1,2,3
     * tablet replicas:    1,2,4,5
     * <p>
     * 2. Version incomplete:
     * backend matched, but some replica's version is incomplete
     * <p>
     * 3. Redundant:
     * backends set:       1,2,3
     * tablet replicas:    1,2,3,4
     * <p>
     * No need to check if backend is available. We consider all backends in 'backendsSet' are available,
     * If not, unavailable backends will be relocated by CalocateTableBalancer first.
     */
    public TabletStatus getColocateHealthStatus(long visibleVersion,
                                                int replicationNum, Set<Long> backendsSet) {

        // 1. check if replicas' backends are mismatch
        Set<Long> replicaBackendIds = getBackendIds();
        for (Long backendId : backendsSet) {
            if (!replicaBackendIds.contains(backendId)) {
                return TabletStatus.COLOCATE_MISMATCH;
            }
        }

        // 2. check version completeness
        for (Replica replica : replicas) {
            // do not check the replica that is not in the colocate backend set,
            // this kind of replica should be drooped.
            if (!backendsSet.contains(replica.getBackendId())) {
                continue;
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

    /**
     * check if this tablet is ready to be repaired, based on priority.
     * VERY_HIGH: repair immediately
     * HIGH:    delay Config.tablet_repair_delay_factor_second * 1;
     * NORNAL:  delay Config.tablet_repair_delay_factor_second * 2;
     * LOW:     delay Config.tablet_repair_delay_factor_second * 3;
     */
    public boolean readyToBeRepaired(TabletSchedCtx.Priority priority) {
        if (priority == Priority.VERY_HIGH) {
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
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000;
                break;
            case NORMAL:
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000 * 2;
                break;
            case LOW:
                ready = currentTime - lastStatusCheckTime > Config.tablet_repair_delay_factor_second * 1000 * 3;
                break;
            default:
                break;
        }

        return ready;
    }

    public void setLastStatusCheckTime(long lastStatusCheckTime) {
        this.lastStatusCheckTime = lastStatusCheckTime;
    }

    public String getReplicaInfos() {
        StringBuilder sb = new StringBuilder();
        for (Replica replica : replicas) {
            sb.append(String.format("%d:%d/%d/%d,", replica.getBackendId(), replica.getVersion(),
                    replica.getLastFailedVersion(), replica.getLastSuccessVersion()));
        }
        return sb.toString();
    }
}
