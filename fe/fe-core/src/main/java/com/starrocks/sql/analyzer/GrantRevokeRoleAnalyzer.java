// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;


import com.starrocks.analysis.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;

public class GrantRevokeRoleAnalyzer {

    public static void analyze(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
        Auth auth = session.getGlobalStateMgr().getAuth();

        // validate user
        UserIdentity userIdent = stmt.getUserIdent();
        try {
            userIdent.analyze(session.getClusterName());
        } catch (AnalysisException e) {
            // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
            // that is permitted to throw during analyzing phrase under the new framework for compatibility.
            // Remove it after all old methods migrate to the new framework
            throw new SemanticException(e.getMessage());
        }
        if (!auth.getUserPrivTable().doesUserExist(userIdent)) {
            throw new SemanticException("user " + userIdent + " not exist!");
        }

        // validate role
        // notice that this method is shared with REVOKE ROLE statement
        try {
            FeNameFormat.checkRoleName(stmt.getRole(), true /* can be admin */, "Can not granted/revoke role to user");
        } catch (AnalysisException e) {
            // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
            // that is permitted to throw during analyzing phrase under the new framework for compatibility.
            // Remove it after all old methods migrate to the new framework
            throw new SemanticException(e.getMessage());
        }
        String qualifiedRole = ClusterNamespace.getFullName(session.getClusterName(), stmt.getRole());
        if (!auth.doesRoleExist(qualifiedRole)) {
            throw new SemanticException("role " + qualifiedRole + " not exist!");
        }
        stmt.setQualifiedRole(qualifiedRole);


    }
}
