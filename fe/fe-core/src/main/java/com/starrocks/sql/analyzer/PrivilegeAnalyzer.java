// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;


public class PrivilegeAnalyzer {

    /**
     * analyse user identity + check if user exists in UserPrivTable
     */
    private static void analyseUserAndCheckExist(UserIdentity userIdent, ConnectContext session) {
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

    /**
     * check if role name valid and get full role name
     */
    private static String analyseRoleName(String roleName, ConnectContext session) {
        try {
            FeNameFormat.checkRoleName(roleName, true /* can be admin */, "Can not granted/revoke role to user");
        } catch (AnalysisException e) {
            // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
            // that is permitted to throw during analyzing phrase under the new framework for compatibility.
            // Remove it after all old methods migrate to the new framework
            throw new SemanticException(e.getMessage());
        }
        String qualifiedRole = ClusterNamespace.getFullName(session.getClusterName(), roleName);
        if (!session.getGlobalStateMgr().getAuth().doesRoleExist(qualifiedRole)) {
            throw new SemanticException("role " + qualifiedRole + " not exist!");
        }
        return qualifiedRole;
    }

    /**
     * analyse EXECUTE AS statment
     */
    public static void analyze(ExecuteAsStmt stmt, ConnectContext session) {
        if (stmt.isAllowRevert()) {
            throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
        }
        analyseUserAndCheckExist(stmt.getToUser(), session);
    }

    /**
     * GRANT IMPERSONATE ON XX TO XX
     * REVOKE IMPERSONATE ON XX TO XX
     */
    public static void analyze(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
        analyseUserAndCheckExist(stmt.getAuthorizedUser(), session);
        analyseUserAndCheckExist(stmt.getSecuredUser(), session);
    }

    /**
     * GRANT rolexx to userxx
     * REVOKE rolexx from userxx
     */
    public static void analyze(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
        analyseUserAndCheckExist(stmt.getUserIdent(), session);
        stmt.setQualifiedRole(analyseRoleName(stmt.getRole(), session));
    }
}
