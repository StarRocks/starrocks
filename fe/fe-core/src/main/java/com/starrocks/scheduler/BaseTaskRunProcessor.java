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

import com.starrocks.common.exception.NotImplementedException;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.sql.ast.StatementBase;
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
            ctx.executeSql(context.getPostRun());
        }
    }

    protected void auditAfterExec(TaskRunContext context, StatementBase parsedStmt, PQueryStatistics statistics) {
        String origStmt = context.getDefinition();
        ConnectContext ctx = context.getCtx();
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.auditAfterExec(origStmt, parsedStmt, statistics);
    }
}
