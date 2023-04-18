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

package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class Warehouse implements Writable {

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "id")
    private long id;

    public enum WarehouseState {
        INITIALIZING,
        RUNNING,
        SUSPENDED,
        SCALING
    }

    @SerializedName(value = "state")
    protected WarehouseState state = WarehouseState.INITIALIZING;

    private volatile boolean exist = true;

    public Warehouse(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public String getFullName() {
        return name;
    }

    public void setState(WarehouseState state) {
        this.state = state;
    }

    public WarehouseState getState() {
        return state;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public abstract void getProcNodeData(BaseProcResult result);

    public abstract Map<Long, Cluster> getClusters() throws DdlException;

<<<<<<< HEAD
    public void resumeSelf() throws DdlException {
        writeLock();
        try {
            clusters = applyComputeNodes();
            this.state = WarehouseState.RUNNING;
        } finally {
            writeUnLock();
        }
    }

    public void dropSelf() throws DdlException {
        readLock();
        try {
            releaseComputeNodes();
        } finally {
            readUnlock();
        }
    }

    private void releaseComputeNodes() throws DdlException {
        for (Cluster cluster : clusters.values()) {
            long workerGroupId = cluster.getWorkerGroupId();
            GlobalStateMgr.getCurrentStarOSAgent().deleteWorkerGroup(workerGroupId);
        }
    }

    private Map<Long, Cluster> applyComputeNodes() throws DdlException {
        Map<Long, Cluster> newClusters = new HashMap<>();
        for (int i = 0; i < minCluster; i++) {
            long groupId = GlobalStateMgr.getCurrentStarOSAgent().createWorkerGroup(this.size);
            Cluster cluster = new Cluster(GlobalStateMgr.getCurrentState().getNextId(), groupId);
            newClusters.put(cluster.getId(), cluster);
        }
        return newClusters;
    }

    public void modifyCluterSize() throws DdlException {
        readLock();
        try {
            for (Cluster cluster : clusters.values()) {
                long workerGroupId = cluster.getWorkerGroupId();
                GlobalStateMgr.getCurrentStarOSAgent().modifyWorkerGroup(workerGroupId, size);

                this.state = WarehouseState.SCALING;
            }
        } finally {
            readUnlock();
        }
    }

    public void addCluster() throws DdlException {
        writeLock();
        try {
            if (clusters.size() < maxCluster) {
                long workerGroupId = GlobalStateMgr.getCurrentStarOSAgent().createWorkerGroup(this.size);
                // for debug
                LOG.info("add cluster, worker group id is {}", workerGroupId);

                long clusterId = GlobalStateMgr.getCurrentState().getNextId();
                Cluster cluster = new Cluster(clusterId, workerGroupId);
                clusters.put(clusterId, cluster);
                AlterWhClusterOplog log = new AlterWhClusterOplog(this.getFullName(), cluster);
                GlobalStateMgr.getCurrentState().getEditLog().logAddCluster(log);

                this.state = WarehouseState.RUNNING;

                // for debug
                LOG.info("add cluster {} for warehouse {} ", clusterId, name);
            } else {
                throw new DdlException("cluster num of " + name + "is " +
                        clusters.size() + " exceed max cluster limit " + maxCluster);
            }
        } finally {
            writeUnLock();
        }
    }

    public void removeCluster() throws DdlException {
        writeLock();
        try {
            if (clusters.size() > minCluster) {
                Long clusterId = clusters.keySet().stream().findFirst().orElse(-1L);
                Cluster cluster = clusters.get(clusterId);
                GlobalStateMgr.getCurrentStarOSAgent().deleteWorkerGroup(cluster.getWorkerGroupId());
                clusters.remove(clusterId);
                AlterWhClusterOplog log = new AlterWhClusterOplog(this.getFullName(), cluster);
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveCluster(log);

                // for debug
                LOG.info("remove cluster {} for warehouse {} ", clusterId, name);
            } else {
                throw new DdlException("cluster num of " + name + " exceed min cluster limit " + minCluster);
            }
        } finally {
            writeUnLock();
        }
    }

    public void replayAddCluster(AlterWhClusterOplog log) {
        writeLock();
        try {
            Cluster cluster = log.getCluster();
            clusters.put(cluster.getId(), cluster);
            this.state = WarehouseState.RUNNING;
            // for debug
            LOG.info("replay add cluster {} for warehouse {} ", cluster.getId(), name);
        } finally {
            writeUnLock();
        }
    }

    public void replayRemoveCluster(AlterWhClusterOplog log)  {
        writeLock();
        try {
            Cluster cluster = log.getCluster();
            clusters.remove(cluster.getId());
            // for debug
            LOG.info("replay remove cluster {} for warehouse {} ", cluster.getId(), name);
        } finally {
            writeUnLock();
        }
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(this.getFullName(),
                this.getState().toString(),
                this.getSize(),
                String.valueOf(this.getMinCluster()),
                String.valueOf(this.getMaxCluster()),
                String.valueOf(this.clusters.size()),
                String.valueOf(this.getTotalPendingSqls()),
                String.valueOf(this.getTotalRunningSqls())));
    }
=======
    public abstract void setClusters(Map<Long, Cluster> clusters) throws DdlException;
>>>>>>> 7247435f6 ([Refactor]Refactor warehouse (#21629))

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Warehouse read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Warehouse.class);
    }
}
