// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class CreateViewExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        context.getGlobalStateMgr().createView((CreateViewStmt) stmt);
        return null;
    }
}
