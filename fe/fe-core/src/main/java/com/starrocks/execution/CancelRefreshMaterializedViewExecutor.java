// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;

public class CancelRefreshMaterializedViewExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        final CancelRefreshMaterializedViewStmt cancelRefresh = (CancelRefreshMaterializedViewStmt) stmt;
        context.getGlobalStateMgr().getLocalMetastore()
                .cancelRefreshMaterializedView(cancelRefresh.getMvName().getDb(), cancelRefresh.getMvName().getTbl());
        return null;
    }
}
