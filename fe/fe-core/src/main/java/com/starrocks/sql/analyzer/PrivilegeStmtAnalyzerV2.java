// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.ArrayList;
import java.util.List;

public class PrivilegeStmtAnalyzerV2 {
    private PrivilegeStmtAnalyzerV2() {
    }

    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitor().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private AuthenticationManager authenticationManager = null;
        private PrivilegeManager privilegeManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authenticationManager = session.getGlobalStateMgr().getAuthenticationManager();
            privilegeManager = session.getGlobalStateMgr().getPrivilegeManager();
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
                throw new SemanticException("cannot find user " + userIdent + "!");
            }
        }

        /**
         * check if role name valid and get full role name
         */
        private void validRoleName(String roleName, String errMsg, boolean checkExist) {
            try {
                // always set to true, we can validate if it's allowed to operation on admin later
                FeNameFormat.checkRoleName(roleName, true, errMsg);
            } catch (AnalysisException e) {
                // TODO AnalysisException used to raise in all old methods is captured and translated to SemanticException
                // that is permitted to throw during analyzing phrase under the new framework for compatibility.
                // Remove it after all old methods migrate to the new framework
                throw new SemanticException(e.getMessage());
            }
            // check if role exists
            if (checkExist && !privilegeManager.checkRoleExists(roleName)) {
                throw new SemanticException(errMsg + ": cannot find role " + roleName + "!");
            }
        }

        @Override
        public Void visitCreateAlterUserStatement(BaseCreateAlterUserStmt stmt, ConnectContext session) {
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
            validRoleName(stmt.getQualifiedRole(), "Can not create role", false);
            if (session.getGlobalStateMgr().getPrivilegeManager().checkRoleExists(stmt.getQualifiedRole())) {
                throw new SemanticException("Can not create role %s: already exists!", stmt.getQualifiedRole());
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            validRoleName(stmt.getQualifiedRole(), "Can not drop role", true);
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
                validRoleName(stmt.getRole(), "Can not grant/revoke to role", true);
            }

            try {
                if (stmt.hasPrivilegeObject()) {
                    List<PEntryObject> objectList = new ArrayList<>();
                    if (stmt.getUserPrivilegeObjectList() != null) {
                        // objects are user
                        stmt.setTypeId(privilegeManager.analyzeType(stmt.getPrivType()));
                        for (UserIdentity userIdentity : stmt.getUserPrivilegeObjectList()) {
                            analyseUser(userIdentity, true);
                            objectList.add(privilegeManager.analyzeUserObject(stmt.getPrivType(), userIdentity));
                        }
                    } else if (stmt.getPrivilegeObjectNameTokensList() != null) {
                        // normal objects
                        stmt.setTypeId(privilegeManager.analyzeType(stmt.getPrivType()));
                        for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                            objectList.add(privilegeManager.analyzeObject(stmt.getPrivType(), tokens));
                        }
                    } else {
                        // all statement
                        // TABLES -> TABLE
                        stmt.setPrivType(privilegeManager.analyzeTypeInPlural(stmt.getPrivType()));
                        // TABLE -> 0/1
                        stmt.setTypeId(privilegeManager.analyzeType(stmt.getPrivType()));
                        objectList.add(privilegeManager.analyzeObject(
                                stmt.getPrivType(), stmt.getAllTypeList(), stmt.getRestrictType(),
                                stmt.getRestrictName()));
                    }
                    stmt.setObjectList(objectList);
                } else {
                    stmt.setTypeId(privilegeManager.analyzeType(stmt.getPrivType()));
                    stmt.setObjectList(null);
                }
                privilegeManager.validateGrant(stmt.getPrivType(), stmt.getPrivList(), stmt.getObjectList());
                stmt.setActionList(privilegeManager.analyzeActionSet(stmt.getPrivType(), stmt.getTypeId(), stmt.getPrivList()));
            } catch (PrivilegeException e) {
                SemanticException exception = new SemanticException(e.getMessage());
                exception.initCause(e);
                throw exception;
            }
            return null;
        }

        @Override
        public Void visitSetRoleStatement(SetRoleStmt stmt, ConnectContext session) {
            for (String roleName : stmt.getRoles()) {
                validRoleName(roleName, "Cannot set role", true);
            }
            return null;
        }

        /**
         * GRANT rolexx to userxx
         * GRANT role1 to role role2
         * REVOKE rolexx from userxx
         * REVOKE role1 from role role2
         */
        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            if (stmt.getUserIdent() != null) {
                analyseUser(stmt.getUserIdent(), true);
                validRoleName(stmt.getGranteeRole(), "Can not granted/revoke role to user", true);
            } else {
                validRoleName(stmt.getGranteeRole(), "Can not granted/revoke role to role", true);
                validRoleName(stmt.getRole(), "Can not granted/revoke role to role", true);
            }
            return null;
        }
    }
}
