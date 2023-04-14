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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.util.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// on-premise
public class LocalWarehouse extends Warehouse {
    private static final Logger LOG = LogManager.getLogger(LocalWarehouse.class);

    @SerializedName(value = "cluster")
    Cluster cluster;

    private QueryableReentrantReadWriteLock rwLock;

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

    public LocalWarehouse(long id, String name) {
        super(id, name);
        this.rwLock = new QueryableReentrantReadWriteLock(true);
        long clusterId = GlobalStateMgr.getCurrentState().getNextId();
        cluster = new Cluster(clusterId);
    }

    @Override
    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(this.getFullName(),
                this.getState().toString(),
                String.valueOf(1L)));
    }

    @Override
    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    @Override
    public void suspendSelf(boolean isReplay) throws DdlException {
        writeLock();
        try {
            this.state = WarehouseState.SUSPENDED;
        } finally {
            writeUnLock();
        }
    }

    @Override
    public void resumeSelf() throws DdlException {
        writeLock();
        try {
            this.state = WarehouseState.RUNNING;
        } finally {
            writeUnLock();
        }
    }

    @Override
    public void dropSelf() throws DdlException {
        readLock();
        try {
            cluster.clearComputeNodes();
        } finally {
            readUnlock();
        }
    }

    @Override
    public Map<Long, Cluster> getClusters() throws DdlException {
        return ImmutableMap.of(cluster.getId(), cluster);
    }

    @Override
    public void setClusters(Map<Long, Cluster> clusters) throws DdlException {
        throw new SemanticException("not implemented");
    }
}
