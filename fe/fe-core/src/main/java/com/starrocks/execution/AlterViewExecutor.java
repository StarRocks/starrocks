// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class AlterViewExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws UserException {
        context.getGlobalStateMgr().alterView((AlterViewStmt) stmt);
        return null;
    }
}
