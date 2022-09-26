// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.RevokePrivilegeStmt;

public class RevokeExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        if (context.getGlobalStateMgr().isUsingNewPrivilege()) {
            context.getGlobalStateMgr().getPrivilegeManager().revoke((RevokePrivilegeStmt) stmt);
        } else {
            context.getGlobalStateMgr().getAuth().revoke((RevokePrivilegeStmt) stmt);
        }
        return null;
    }
}
