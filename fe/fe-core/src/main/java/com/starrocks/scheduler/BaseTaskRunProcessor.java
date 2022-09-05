// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.NotImplementedException;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;

public abstract class BaseTaskRunProcessor implements TaskRunProcessor {
    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        throw new NotImplementedException("Method processTaskRun need to implement");
    }

    protected void auditAfterExec(TaskRunContext context, StatementBase parsedStmt, PQueryStatistics statistics) {
        String origStmt = context.getDefinition();
        ConnectContext ctx = context.getCtx();
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.auditAfterExec(origStmt, parsedStmt, statistics);
    }
}
