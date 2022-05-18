// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;


import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;

import java.util.ArrayList;
import java.util.List;

public class GrantRevokeImpersonateAnalyzer {

    public static void analyze(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
        // authorized user & secured user
        // share the same logic
        List<UserIdentity> userList = new ArrayList<>();
        userList.add(stmt.getAuthorizedUser());
        userList.add(stmt.getSecuredUser());

        for (UserIdentity userIdent : userList) {
            // 1. validate user
            try {
                userIdent.analyze(session.getClusterName());
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }


            // 2. check if user exists
            if (!session.getGlobalStateMgr().getAuth().getUserPrivTable().doesUserExist(userIdent)) {
                throw new SemanticException("user " + userIdent + " not exist!");
            }
        }
    }
}
