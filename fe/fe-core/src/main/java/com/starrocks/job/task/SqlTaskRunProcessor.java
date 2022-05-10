// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.job.task;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.parser.SqlParser;

// Execute a basic task of SQL
public class SqlTaskRunProcessor implements TaskRunProcessor {

    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        ConnectContext ctx = context.getCtx();
        StatementBase sqlStmt = SqlParser.parse(context.getDefinition(), ctx.getSessionVariable().getSqlMode()).get(0);
        StmtExecutor executor = new StmtExecutor(ctx, sqlStmt);
        ctx.setExecutor(executor);
        ctx.setThreadLocalInfo();
        executor.execute();
    }

}
