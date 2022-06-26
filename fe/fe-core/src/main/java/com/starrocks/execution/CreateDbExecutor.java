// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class CreateDbExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        CreateDbStmt createDbStmt = (CreateDbStmt) stmt;
        String clusterName = createDbStmt.getClusterName();
        String fullDbName = createDbStmt.getFullDbName();
        boolean isSetIfNotExists = createDbStmt.isSetIfNotExists();
        context.getGlobalStateMgr().getLocalMetastore().createDb(clusterName, fullDbName, isSetIfNotExists);
        return null;
    }
}
