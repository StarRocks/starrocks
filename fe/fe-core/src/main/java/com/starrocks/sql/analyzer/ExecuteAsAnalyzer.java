// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;


import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ExecuteAsStmt;


public class ExecuteAsAnalyzer {

    public static void analyze(ExecuteAsStmt stmt, ConnectContext session) {
        if (stmt.isAllowRevert()) {
            throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
        }

        UserIdentity userIdent = stmt.getToUser();
        // validate user
        try {
            userIdent.analyze(session.getClusterName());
        } catch (AnalysisException e) {
            // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
            // that is permitted to throw during analyzing phrase under the new framework for compatibility.
            // Remove it after all old methods migrate to the new framework
            throw new SemanticException(e.getMessage());
        }

        // check if user exists
        if (!session.getGlobalStateMgr().getAuth().getUserPrivTable().doesUserExist(userIdent)) {
            throw new SemanticException("user " + userIdent + " not exist!");
        }
    }
}
