// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;

public class PrivilegeStmtAnalyzerV2 {
    private PrivilegeStmtAnalyzerV2() {}
    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private AuthenticationManager authenticationManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authenticationManager = session.getGlobalStateMgr().getAuthenticationManager();
            visit(statement, session);
        }

        /**
         * analyse user identity + check if user exists in UserPrivTable
         */
        private void analyseUser(UserIdentity userIdent, boolean checkExist) {
            // analyse user identity
            try {
                userIdent.analyze();
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }

            // check if user exists
            if (checkExist && !authenticationManager.doesUserExist(userIdent)) {
                throw new SemanticException("user " + userIdent + " not exist!");
            }
        }

        /**
         * check if role name valid and get full role name
         */
        private String validRoleName(String roleName, boolean canBeAdmin, String errMsg) {
            try {
                FeNameFormat.checkRoleName(roleName, canBeAdmin, errMsg);
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
            return roleName;
        }

        @Override
        public Void visitCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), stmt instanceof AlterUserStmt);

            if (stmt.getAuthPlugin() == null) {
                stmt.setAuthPlugin(authenticationManager.getDefaultPlugin());
            }
            try {
                String pluginName = stmt.getAuthPlugin();
                AuthenticationProvider provider = AuthenticationProviderFactory.create(pluginName);
                UserIdentity userIdentity = stmt.getUserIdent();
                UserAuthenticationInfo info = provider.validAuthenticationInfo(
                        userIdentity, stmt.getOriginalPassword(), stmt.getAuthString());
                info.setAuthPlugin(pluginName);
                info.setOrigUserHost(userIdentity.getQualifiedUser(), userIdentity.getHost());
                stmt.setAuthenticationInfo(info);
            } catch (AuthenticationException e) {
                SemanticException exception = new SemanticException("invalidate authentication: " + e.getMessage());
                exception.initCause(e);
                throw exception;
            }

            if (stmt.hasRole()) {
                throw new SemanticException("role not supported!");
            }
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            String roleName = validRoleName(stmt.getQualifiedRole(), false, "Can not create role");
            if (session.getGlobalStateMgr().getPrivilegeManager().checkRoleExists(roleName)) {
                throw new SemanticException("Can not create role %s: already exists!", roleName);
            }
            stmt.setQualifiedRole(roleName);
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            String roleName = validRoleName(stmt.getQualifiedRole(), false, "Can not drop role");
            PrivilegeManager privilegeManager = session.getGlobalStateMgr().getPrivilegeManager();
            if (!privilegeManager.checkRoleExists(roleName)) {
                throw new SemanticException("Can not drop role %s: cannot find role!", roleName);
            }
            stmt.setQualifiedRole(roleName);
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), false);
            if (stmt.getUserIdent().equals(UserIdentity.ROOT)) {
                throw new SemanticException("cannot drop root!");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            // validate user/role
            if (stmt.getUserIdentity() != null) {
                analyseUser(stmt.getUserIdentity(), true);
            } else {
                // TODO
                throw new SemanticException("not supported");
            }
            try {
                PrivilegeManager privilegeManager = session.getGlobalStateMgr().getPrivilegeManager();
                stmt.setTypeId(privilegeManager.analyzeType(stmt.getPrivType()));
                stmt.setActionList(privilegeManager.analyzeActionSet(stmt.getPrivType(), stmt.getTypeId(), stmt.getPrivList()));
                stmt.setObject(privilegeManager.analyzeObject(stmt.getPrivType(), stmt.getPrivilegeObjectNameTokenList()));
                privilegeManager.validateGrant(stmt.getTypeId(), stmt.getActionList(), stmt.getObject());
            } catch (PrivilegeException e) {
                SemanticException exception = new SemanticException(e.getMessage());
                exception.initCause(e);
                throw exception;
            }
            return null;
        }

    }
}
