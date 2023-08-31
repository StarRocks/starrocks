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

import com.starrocks.common.NotImplementedException;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

public abstract class BaseTaskRunProcessor implements TaskRunProcessor {
    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        throw new NotImplementedException("Method processTaskRun need to implement");
    }

    @Override
    public void postTaskRun(TaskRunContext context) throws Exception {
        if (StringUtils.isNotEmpty(context.getPostRun())) {
            ConnectContext ctx = context.getCtx();
            executeSql(ctx, context.getPostRun());
        }
    }

    protected StmtExecutor executeSql(ConnectContext ctx, String sql) throws Exception {
        StatementBase sqlStmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        sqlStmt.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = new StmtExecutor(ctx, sqlStmt);
        ctx.setExecutor(executor);
        ctx.setThreadLocalInfo();
        executor.execute();
        return executor;
    }

    protected void auditAfterExec(TaskRunContext context, StatementBase parsedStmt, PQueryStatistics statistics) {
        String origStmt = context.getDefinition();
        ConnectContext ctx = context.getCtx();
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.auditAfterExec(origStmt, parsedStmt, statistics);
    }
}
