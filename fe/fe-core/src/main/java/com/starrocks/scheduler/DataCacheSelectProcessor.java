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

package com.starrocks.scheduler;

import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.datacache.DataCacheSelectMetrics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class DataCacheSelectProcessor extends BaseTaskRunProcessor {
    private static final Logger LOG = LogManager.getLogger(DataCacheSelectProcessor.class);

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        StmtExecutor executor = null;
        try {
            ConnectContext ctx = context.getCtx();

            // We need to reset catalog name in ConnectContext, because ctx's session variable was reset in TaskRun::executeTaskRun()
            String catalogName = context.taskRun.getTask().getCatalogName();
            ctx.setCurrentCatalog(catalogName);

            ctx.getAuditEventBuilder().reset();
            ctx.getAuditEventBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp(context.getRemoteIp())
                    .setUser(ctx.getQualifiedUser())
                    .setCatalog(ctx.getCurrentCatalog())
                    .setDb(ctx.getDatabase());

            Tracers.register(ctx);
            Tracers.init(ctx, Tracers.Mode.TIMER, null);
            executor = ctx.executeSql(context.getDefinition());

            if (ctx.getState().isError()) {
                // throw exception if StmtExecutor execute failed
                throw new StarRocksException(ctx.getState().getErrorMessage());
            }

            // Cache select's metrics is held by sub StmtExecutor
            DataCacheSelectMetrics metrics = getDataCacheSelectMetrics(executor);
            // update compute node or backend's metrics
            DataCacheSelectExecutor.updateBackendDataCacheMetrics(metrics);
            DataCacheSelectStatement dataCacheSelectStatement = (DataCacheSelectStatement) executor.getParsedStmt();
            boolean isVerbose = dataCacheSelectStatement.isVerbose();
            context.getStatus().setExtraMessage(metrics.debugString(isVerbose));
        } finally {
            Tracers.close();
            if (executor != null) {
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            } else {
                // executor can be null if we encounter analysis error.
                auditAfterExec(context, null, null);
            }
        }
    }

    @NotNull
    private static DataCacheSelectMetrics getDataCacheSelectMetrics(StmtExecutor executor) throws StarRocksException {
        List<StmtExecutor> subExecutors = executor.getSubStmtExecutors();

        if (subExecutors.size() != 1) {
            throw new StarRocksException("No sub executor in DataCache Select");
        }

        DataCacheSelectMetrics metrics = subExecutors.get(0).getCoordinator().getDataCacheSelectMetrics();

        if (metrics == null) {
            throw new StarRocksException("Can't retrieve DataCache select metrics");
        }
        return metrics;
    }
}
