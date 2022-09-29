// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.starrocks.analysis.AlterLoadStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class AlterLoadExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws UserException {
        context.getGlobalStateMgr().getLoadManager().alterLoadJob((AlterLoadStmt) stmt);
        return null;
    }
}
