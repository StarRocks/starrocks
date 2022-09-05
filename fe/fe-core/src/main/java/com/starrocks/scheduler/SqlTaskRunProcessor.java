// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.parser.SqlParser;

// Execute a basic task of SQL\
public class SqlTaskRunProcessor extends BaseTaskRunProcessor {

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        StmtExecutor executor = null;
        try {
            ConnectContext ctx = context.getCtx();
            ctx.getAuditEventBuilder().reset();
            ctx.getAuditEventBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp(context.getRemoteIp())
                    .setUser(ctx.getQualifiedUser())
                    .setDb(ctx.getDatabase())
                    .setCatalog(ctx.getCurrentCatalog());
            ctx.getPlannerProfile().reset();
            String definition = context.getDefinition();
            StatementBase sqlStmt = SqlParser.parse(definition, ctx.getSessionVariable()).get(0);
            sqlStmt.setOrigStmt(new OriginStatement(definition, 0));
            executor = new StmtExecutor(ctx, sqlStmt);
            ctx.setExecutor(executor);
            ctx.setThreadLocalInfo();
            executor.execute();
        } finally {
            if (executor != null) {
                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            } else {
                // executor can be null if we encounter analysis error.
                auditAfterExec(context, null, null);
            }
        }
    }
}
