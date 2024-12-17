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

package com.starrocks.connector.metadata;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataExecutor {
    private static final Logger LOG = LogManager.getLogger(MetadataExecutor.class);
    private StmtExecutor executor;

    public void asyncExecuteSQL(MetadataCollectJob job) {
        ConnectContext context = job.getContext();
        context.setThreadLocalInfo();
        String sql = job.getSql();
        ExecPlan execPlan;
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            execPlan = StatementPlanner.plan(parsedStmt, context, job.getSinkType());
        } catch (Exception e) {
            LOG.error("Failed to execute metadata collect job", e);
            context.getState().setError(e.getMessage());
            return;
        }

        this.executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);

        LOG.info("Start to execute metadata collect job on {}.{}.{}", job.getCatalogName(), job.getDbName(), job.getTableName());
        executor.executeStmtWithResultQueue(context, execPlan, job.getResultQueue());
    }

    public Coordinator getCoordinator() {
        if (executor != null) {
            return executor.getCoordinator();
        } else {
            return null;
        }
    }

}
