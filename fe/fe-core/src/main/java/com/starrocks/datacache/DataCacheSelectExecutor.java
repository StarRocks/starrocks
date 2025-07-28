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

package com.starrocks.datacache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DataCacheSelectExecutor {
    private static final Logger LOG = LogManager.getLogger(DataCacheSelectExecutor.class);

    public static DataCacheSelectMetrics cacheSelect(DataCacheSelectStatement statement,
                                                             ConnectContext connectContext) throws Exception {
        InsertStmt insertStmt = statement.getInsertStmt();

        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        final Warehouse wh = warehouseManager.getWarehouse(connectContext.getCurrentWarehouseName());
        List<StmtExecutor> subStmtExecutors = Lists.newArrayList();
        for (long workerGroupId : wh.getWorkerGroupIds()) {
            ConnectContext subContext = buildCacheSelectConnectContext(statement, connectContext);
            ComputeResource computeResource = warehouseManager.getComputeResourceProvider().ofComputeResource(
                    wh.getId(), workerGroupId);
            subContext.setCurrentComputeResource(computeResource);
            StmtExecutor subStmtExecutor = StmtExecutor.newInternalExecutor(subContext, insertStmt);
            // Register new StmtExecutor into current ConnectContext's StmtExecutor, so we can handle ctrl+c command
            // If DataCacheSelect is forward to leader, connectContext's Executor is null
            if (connectContext.getExecutor() != null) {
                connectContext.getExecutor().registerSubStmtExecutor(subStmtExecutor);
            }
            subStmtExecutor.addRunningQueryDetail(insertStmt);
            try {
                subStmtExecutor.execute();
            } finally {
                subStmtExecutor.addFinishedQueryDetail();
            }

            if (subContext.getState().isError()) {
                // throw exception if StmtExecutor execute failed
                throw new StarRocksException(subContext.getState().getErrorMessage());
            }
            subStmtExecutors.add(subStmtExecutor);
        }

        DataCacheSelectMetrics metrics = null;
        for (StmtExecutor subStmtExecutor : subStmtExecutors) {
            Coordinator coordinator = subStmtExecutor.getCoordinator();
            Preconditions.checkNotNull(coordinator, "Coordinator can't be null");
            coordinator.join(subStmtExecutor.getExecTimeout());
            if (coordinator.isDone() && metrics == null) {
                metrics = subStmtExecutor.getCoordinator().getDataCacheSelectMetrics();
            }

            Preconditions.checkNotNull(metrics, "Failed to retrieve cache select metrics");
            // Don't update datacache metrics after cache select, because of datacache instance still not unified.
            // Here update will display wrong metrics in show backends/compute nodes
            // update backend's datacache metrics after cache select
            // updateBackendDataCacheMetrics(metrics);
        }
        return metrics;
    }

    // update BE's datacache metrics after cache select
    public static void updateBackendDataCacheMetrics(DataCacheSelectMetrics metrics) {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (Map.Entry<Long, LoadDataCacheMetrics> metric : metrics.getBeMetrics().entrySet()) {
            ComputeNode computeNode = clusterInfoService.getBackendOrComputeNode(metric.getKey());
            if (computeNode == null) {
                continue;
            }
            computeNode.updateDataCacheMetrics(metric.getValue().getLastDataCacheMetrics());
        }
    }

    public static ConnectContext buildCacheSelectConnectContext(DataCacheSelectStatement statement,
                                                                ConnectContext connectContext) {
        // Create a new ConnectContext for the sub task of cache select.
        final ConnectContext context = new ConnectContext(null);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setDatabase(connectContext.getDatabase());
        context.setQualifiedUser(connectContext.getQualifiedUser());
        context.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        context.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        context.setAuditEventBuilder(connectContext.getAuditEventBuilder());
        context.setSessionId(connectContext.getSessionId());
        context.setRemoteIP(connectContext.getRemoteIP());
        context.setQueryId(connectContext.getQueryId());
        context.getState().reset();

        // Generate one different execution_id here for different compute resources.
        // We make the high part of query id unchanged to facilitate tracing problem by log.
        TUniqueId queryId = UUIDUtil.toTUniqueId(connectContext.getQueryId());
        UUID uuid = UUIDUtil.genUUID();
        TUniqueId executionId = new TUniqueId(queryId.hi, uuid.getLeastSignificantBits());
        context.setExecutionId(executionId);
        LOG.debug("generate a new execution id {} for query {}", DebugUtil.printId(UUIDUtil.fromTUniqueid(executionId)),
                DebugUtil.printId(connectContext.getQueryId()));

        // NOTE: Ensure the thread local connect context is always the same with the newest ConnectContext.
        // NOTE: Ensure this thread local is removed after this method to avoid memory leak in JVM.
        context.setThreadLocalInfo();

        SessionVariable sessionVariable = (SessionVariable) context.getSessionVariable();
        // force enable datacache and populate
        sessionVariable.setEnableScanDataCache(true);
        sessionVariable.setEnablePopulateDataCache(true);
        sessionVariable.setDataCachePopulateMode(DataCachePopulateMode.ALWAYS.modeName());
        // make sure all accessed data must be cached
        sessionVariable.setEnableDataCacheAsyncPopulateMode(false);
        sessionVariable.setEnableDataCacheIOAdaptor(false);
        sessionVariable.setDataCacheEvictProbability(100);
        sessionVariable.setDataCachePriority(statement.getPriority());
        sessionVariable.setDatacacheTTLSeconds(statement.getTTLSeconds());
        sessionVariable.setEnableCacheSelect(true);

        return context;
    }
}
