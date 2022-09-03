// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;


public class PrivilegeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        /**
         * analyse user identity + check if user exists in UserPrivTable
         */
        private void analyseUserAndCheckExist(UserIdentity userIdent, ConnectContext session) {
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
        private String analyseRoleName(String roleName, ConnectContext session) {
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
         * GRANT rolexx to userxx
         * REVOKE rolexx from userxx
         */
        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            analyseUserAndCheckExist(stmt.getUserIdent(), session);
            stmt.setQualifiedRole(analyseRoleName(stmt.getRole(), session));
            return null;
        }

        /**
         * GRANT IMPERSONATE ON XX TO XX
         * GRANT IMPERSONATE ON XX TO ROLE XX
         * REVOKE IMPERSONATE ON XX FROM XX
         * REVOKE IMPERSONATE ON XX FROM ROLE XX
         */
        @Override
        public Void visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
            analyseUserAndCheckExist(stmt.getSecuredUser(), session);
            if (stmt.getAuthorizedUser() != null) {
                analyseUserAndCheckExist(stmt.getAuthorizedUser(), session);
            } else {
                String qulifiedRole = analyseRoleName(stmt.getAuthorizedRoleName(), session);
                stmt.setAuthorizedRoleName(qulifiedRole);
            }
            return null;
        }

        /**
         * analyse EXECUTE AS statment
         */
        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            if (stmt.isAllowRevert()) {
                throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
            }
            analyseUserAndCheckExist(stmt.getToUser(), session);
            return null;
        }

    }
}
