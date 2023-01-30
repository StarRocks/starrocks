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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.DbsProcDir;
import com.starrocks.common.proc.ExternalDbsProcDir;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.QueryableReentrantReadWriteLock;
import com.starrocks.persist.AlterWhClusterOplog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Warehouse implements Writable {
    private static final Logger LOG = LogManager.getLogger(Warehouse.class);

    public static final ImmutableList<String> CLUSTER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ClusterId")
            .add("Pending")
            .add("Running")
            .build();

    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;

    private QueryableReentrantReadWriteLock rwLock;

    public enum WarehouseState {
        INITIALIZING,
        RUNNING,
        SUSPENDED,
        SCALING
    }

    @SerializedName(value = "state")
    private WarehouseState state = WarehouseState.INITIALIZING;

    // cluster id -> cluster obj
    @SerializedName(value = "clusters")
    private Map<Long, Cluster> clusters = new HashMap<>();

    // properties
    @SerializedName(value = "size")
    private String size = "S";
    @SerializedName(value = "minCluster")
    private int minCluster = 1;
    @SerializedName(value = "maxCluster")
    private int maxCluster = 5;
    // and others ...

    private volatile boolean exist = true;

    private AtomicInteger numPendingSqls;

    private final ClusterProcNode procNode = new ClusterProcNode();

    private void readLock() {
        this.rwLock.readLock().lock();
    }
    private void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    private void writeLock() {
        this.rwLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.rwLock.writeLock().unlock();
    }

    public Warehouse(long id, String name) {
        this.id = id;
        this.name = name;
        this.rwLock = new QueryableReentrantReadWriteLock(true);
    }

    public Warehouse(long id, String name, Map<String, String> properties) {
        this(id, name);
        if (properties != null) {
            if (properties.get("size") != null) {
                size = properties.get("size");
            }

            if (properties.get("min_cluster") != null) {
                minCluster = Integer.valueOf(properties.get("min_cluster"));
            }

            if (properties.get("max_cluster") != null) {
                maxCluster = Integer.valueOf(properties.get("max_cluster"));
            }
        }
    }

    public Warehouse() {
        this(0, null);
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
    // property setters and getters
    public void setSize(String size) {
        this.size = size;
    }
    public String getSize() {
        return size;
    }

    public void setMinCluster(int minCluster) {
        this.minCluster = minCluster;
    }

    public int getMinCluster() {
        return minCluster;
    }

    public void setMaxCluster(int maxCluster) {
        this.maxCluster = maxCluster;
    }

    public int getMaxCluster() {
        return maxCluster;
    }

    // and others...

    public long getTotalRunningSqls() {
        return -1L;
    }

    public int getTotalPendingSqls() {
        return -1;
    }

    public void setPendingSqls(int val) {
    }

    public int addAndGetPendingSqls(int delta) {
        return 1;
    }

    public void setClusters(Map<Long, Cluster> clusters) {
        this.clusters = clusters;
    }

    public Map<Long, Cluster> getClusters() {
        return clusters;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public List<List<String>> getClustersInfo() {
        return procNode.fetchResult().getRows();
    }

    public void suspendSelf(boolean isReplay) throws DdlException {
        writeLock();
        try {
            this.state = WarehouseState.SUSPENDED;
            if (!isReplay) {
                releaseComputeNodes();
            }
            clusters.clear();
        } finally {
            writeUnLock();
        }
    }

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
            // for debug
            LOG.info("release worker group {}", workerGroupId);
        }
    }

    private Map<Long, Cluster> applyComputeNodes() throws DdlException {
        Map<Long, Cluster> newClusters = new HashMap<>();
        for (int i = 0; i < minCluster; i++) {
            long groupId = GlobalStateMgr.getCurrentStarOSAgent().createWorkerGroup(this.size);
            Cluster cluster = new Cluster(GlobalStateMgr.getCurrentState().getNextId(), groupId);
            newClusters.put(cluster.getId(), cluster);
            // for debug
            LOG.info("apply new worker group {}", groupId);
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
                // for debug
                LOG.info("modify cluster {} size", cluster.getId());
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

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Warehouse read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Warehouse.class);
    }

    public class ClusterProcNode implements ProcDirInterface {

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String catalogName) throws AnalysisException {
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return new DbsProcDir(GlobalStateMgr.getCurrentState());
            }
            return new ExternalDbsProcDir(catalogName);
        }

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(CLUSTER_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (Map.Entry<Long, Cluster> entry : clusters.entrySet()) {
                    Cluster cluster = entry.getValue();
                    if (cluster == null) {
                        continue;
                    }
                    cluster.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }

}
