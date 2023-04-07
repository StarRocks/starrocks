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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.DbsProcDir;
import com.starrocks.common.proc.ExternalDbsProcDir;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.persist.AlterWhClusterOplog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// for k8s or pure-saas
public class ElasticWarehouse extends Warehouse {
    private static final Logger LOG = LogManager.getLogger(ElasticWarehouse.class);

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

    private final ClusterProcNode procNode = new ClusterProcNode();

    public ElasticWarehouse() {
        super(0, null);
    }


    public ElasticWarehouse(long id, String name, Map<String, String> properties) {
        super(id, name);
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

    @Override
    public void setClusters(Map<Long, Cluster> clusters) throws DdlException {
        this.clusters = clusters;
    }

    @Override
    public Map<Long, Cluster> getClusters() throws DdlException {
        return clusters;
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

    @Override
    public Cluster getAnyAvailableCluster() {
        return clusters.values().stream().findFirst().get();
    }

    @Override
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

    @Override
    public void resumeSelf() throws DdlException {
        writeLock();
        try {
            clusters = applyComputeNodes();
            this.state = WarehouseState.RUNNING;
        } finally {
            writeUnLock();
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

    public void dropSelf() throws DdlException {
        readLock();
        try {
            releaseComputeNodes();
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
                // TODO: deployment form is on-premise, create local cluster
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

    public List<List<String>> getClustersInfo() {
        return procNode.fetchResult().getRows();
    }

    @Override
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
