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
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class DataCacheSelectExecutor {
    private static final Logger LOG = LogManager.getLogger(DataCacheSelectExecutor.class);

    public static DataCacheSelectMetrics cacheSelect(DataCacheSelectStatement statement,
                                                             ConnectContext connectContext) throws Exception {
        // backup original session variable
        SessionVariable sessionVariableBackup = connectContext.getSessionVariable();
        // clone an new session variable
        SessionVariable tmpSessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        // overwrite catalog
        tmpSessionVariable.setCatalog(statement.getCatalog());
        // force enable datacache and populate
        tmpSessionVariable.setEnableScanDataCache(true);
        tmpSessionVariable.setEnablePopulateDataCache(true);
        tmpSessionVariable.setDataCachePopulateMode(DataCachePopulateMode.ALWAYS.modeName());
        // make sure all accessed data must be cached
        tmpSessionVariable.setEnableDataCacheAsyncPopulateMode(false);
        tmpSessionVariable.setEnableDataCacheIOAdaptor(false);
        tmpSessionVariable.setDataCacheEvictProbability(100);
        tmpSessionVariable.setDataCachePriority(statement.getPriority());
        tmpSessionVariable.setDatacacheTTLSeconds(statement.getTTLSeconds());
        tmpSessionVariable.setEnableCacheSelect(true);
        connectContext.setSessionVariable(tmpSessionVariable);

        InsertStmt insertStmt = statement.getInsertStmt();
        StmtExecutor stmtExecutor = StmtExecutor.newInternalExecutor(connectContext, insertStmt);
        // Register new StmtExecutor into current ConnectContext's StmtExecutor, so we can handle ctrl+c command
        // If DataCacheSelect is forward to leader, connectContext's Executor is null
        if (connectContext.getExecutor() != null) {
            connectContext.getExecutor().registerSubStmtExecutor(stmtExecutor);
        }
        stmtExecutor.addRunningQueryDetail(insertStmt);
        try {
            stmtExecutor.execute();
        } finally {
            stmtExecutor.addFinishedQueryDetail();
        }

        if (connectContext.getState().isError()) {
            // throw exception if StmtExecutor execute failed
            throw new StarRocksException(connectContext.getState().getErrorMessage());
        }

        DataCacheSelectMetrics metrics = null;
        Coordinator coordinator = stmtExecutor.getCoordinator();
        Preconditions.checkNotNull(coordinator, "Coordinator can't be null");
        coordinator.join(stmtExecutor.getExecTimeout());
        if (coordinator.isDone()) {
            metrics = stmtExecutor.getCoordinator().getDataCacheSelectMetrics();
        }
        // set original session variable
        connectContext.setSessionVariable(sessionVariableBackup);

        Preconditions.checkNotNull(metrics, "Failed to retrieve cache select metrics");
        // Don't update datacache metrics after cache select, because of datacache instance still not unified.
        // Here update will display wrong metrics in show backends/compute nodes
        // update backend's datacache metrics after cache select
        // updateBackendDataCacheMetrics(metrics);
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
}
