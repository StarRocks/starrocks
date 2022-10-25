// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;

public class RefreshMaterializedViewExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        final RefreshMaterializedViewStatement refreshMaterializedViewStatement = (RefreshMaterializedViewStatement) stmt;
        context.getGlobalStateMgr().getLocalMetastore()
                .refreshMaterializedView(refreshMaterializedViewStatement, Constants.TaskRunPriority.NORMAL.value());
        return null;
    }
}
