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
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

public class DataCacheExecutor {
    private static final Logger LOG = LogManager.getLogger(DataCacheExecutor.class);

    public static Optional<DataCacheSelectMetrics> cacheSelect(DataCacheSelectStatement statement,
                                                             ConnectContext connectContext) throws Exception {
        // backup original session variable
        SessionVariable sessionVariableBackup = connectContext.getSessionVariable();
        // clone an new session variable
        SessionVariable tmpSessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        connectContext.setSessionVariable(tmpSessionVariable);

        // force enable populate datacache
        tmpSessionVariable.setEnablePopulateDataCache(true);

        InsertStmt insertStmt = statement.getInsertStmt();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, insertStmt);
        // register new StmtExecutor into current ConnectContext
        connectContext.getExecutor().registerSubStmtExecutor(stmtExecutor);
        stmtExecutor.execute();

        if (connectContext.getState().isError()) {
            throw new UserException(connectContext.getState().getErrorMessage());
        }

        DataCacheSelectMetrics metrics = null;
        Coordinator coordinator = stmtExecutor.getCoordinator();
        Preconditions.checkNotNull(coordinator, "Coordinator can't be null");
        coordinator.join(connectContext.getSessionVariable().getQueryTimeoutS());
        if (coordinator.isDone()) {
            metrics = stmtExecutor.getCoordinator().getDataCacheSelectMetrics();
            if (metrics != null) {
                updateBackendEndDataCacheMetrics(metrics);
            }
        }
        connectContext.setSessionVariable(sessionVariableBackup);
        return Optional.ofNullable(metrics);
    }

    // update BE's datacache metrics after cache select
    private static void updateBackendEndDataCacheMetrics(DataCacheSelectMetrics metrics) {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (Map.Entry<Long, LoadDataCacheMetrics> metric : metrics.getBeMetrics().entrySet()) {
            Backend backend = clusterInfoService.getBackend(metric.getKey());
            if (backend == null) {
                continue;
            }
            backend.updateDataCacheMetrics(metric.getValue().getLastDataCacheMetrics());
        }
    }
}
