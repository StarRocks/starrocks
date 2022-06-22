// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DdlExecutor;
import com.starrocks.qe.ShowResultSet;

public class DataDefinitionExecutorFactory {
    private static final ImmutableMap<Class<? extends StatementBase>, DataDefinitionExecutor> executorMap =
            new ImmutableMap.Builder<Class<? extends StatementBase>, DataDefinitionExecutor>()
                    .put(CreateDbStmt.class, new CreateDbExecutor())
                    .put(DropDbStmt.class, new DropDbExecutor())
                    .build();

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        DataDefinitionExecutor executor = executorMap.get(stmt.getClass());
        if (executor != null) {
            return executor.execute(stmt, context);
        } else {
            return DdlExecutor.execute(context.getGlobalStateMgr(), (DdlStmt) stmt);
        }
    }
}
