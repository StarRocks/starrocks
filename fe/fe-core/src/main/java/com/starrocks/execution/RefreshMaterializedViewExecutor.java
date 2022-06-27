// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;

import static com.starrocks.scheduler.Constants.MANUAL_TASK_RUN_PRIORITY;

public class RefreshMaterializedViewExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        final RefreshMaterializedViewStatement refreshMaterializedViewStatement = (RefreshMaterializedViewStatement) stmt;
        context.getGlobalStateMgr().getLocalMetastore()
                .refreshMaterializedView(refreshMaterializedViewStatement.getMvName().getDb(),
                        refreshMaterializedViewStatement.getMvName().getTbl(), MANUAL_TASK_RUN_PRIORITY);
        return null;
    }
}
