// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.LDAPAuthenticationProvider;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.privilege.FunctionPEntryObject;
import com.starrocks.privilege.GlobalFunctionPEntryObject;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import org.apache.commons.lang3.EnumUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

        /**
         * Get scrambled password from plain password
         */
        private byte[] analysePassword(String originalPassword, boolean isPasswordPlain) {
            if (Strings.isNullOrEmpty(originalPassword)) {
                return MysqlPassword.EMPTY_PASSWORD;
            }
            try {
                if (isPasswordPlain) {
                    return MysqlPassword.makeScrambledPassword(originalPassword);
                } else {
                    return MysqlPassword.checkPassword(originalPassword);
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public Void visitCreateAlterUserStatement(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            analyseUser(stmt.getUserIdent(), stmt instanceof AlterUserStmt);

            String authPluginUsing = null;
            if (stmt.getAuthPlugin() == null) {
                authPluginUsing = authenticationManager.getDefaultPlugin();
                stmt.setScramblePassword(
                        analysePassword(stmt.getOriginalPassword(), stmt.isPasswordPlain()));
            } else {
                authPluginUsing = stmt.getAuthPlugin();
                if (authPluginUsing.equals(PlainPasswordAuthenticationProvider.PLUGIN_NAME)) {
                    // In this case, authString is the password
                    stmt.setScramblePassword(
                            analysePassword(stmt.getAuthString(), stmt.isPasswordPlain()));
                } else {
                    stmt.setScramblePassword(new byte[0]);
                }

                if (authPluginUsing.equals(LDAPAuthenticationProvider.PLUGIN_NAME)) {
                    stmt.setUserForAuthPlugin(stmt.getAuthString());
                }
            }

            try {
                AuthenticationProvider provider = AuthenticationProviderFactory.create(authPluginUsing);
                UserIdentity userIdentity = stmt.getUserIdent();
                UserAuthenticationInfo info = provider.validAuthenticationInfo(
                        userIdentity, stmt.getOriginalPassword(), stmt.getAuthString());
                info.setAuthPlugin(authPluginUsing);
                info.setOrigUserHost(userIdentity.getQualifiedUser(), userIdentity.getHost());
                stmt.setAuthenticationInfo(info);
                if (stmt instanceof AlterUserStmt) {
                    session.getGlobalStateMgr().getAuthenticationManager().checkPasswordReuse(
                            userIdentity, stmt.getOriginalPassword());
                }
            } catch (AuthenticationException | DdlException e) {
                SemanticException exception = new SemanticException("invalidate authentication: " + e.getMessage());
                exception.initCause(e);
                throw exception;
            }

            if (stmt.hasRole()) {
                validRoleName(stmt.getQualifiedRole(), "Valid role name fail", true);
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

        private FunctionName parseFunctionName(BaseGrantRevokePrivilegeStmt stmt)
                throws PrivilegeException, AnalysisException {
            stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));
            String[] name = stmt.getFunctionName().split("\\.");
            FunctionName functionName;
            if (stmt.getObjectType() == ObjectType.GLOBAL_FUNCTION) {
                if (name.length != 1) {
                    throw new AnalysisException("global function has no database");
                }
                functionName = new FunctionName(name[0]);
                functionName.setAsGlobalFunction();
            } else {
                if (name.length == 2) {
                    functionName = new FunctionName(name[0], name[1]);
                } else {
                    String dbName = ConnectContext.get().getDatabase();
                    if (dbName.equals("")) {
                        throw new AnalysisException("database not selected");
                    }
                    functionName = new FunctionName(dbName, name[0]);
                }
            }
            return functionName;
        }

        private PEntryObject parseFunctionObject(BaseGrantRevokePrivilegeStmt stmt, FunctionSearchDesc searchDesc)
                throws PrivilegeException {
            FunctionName name = searchDesc.getName();
            if (name.isGlobalFunction()) {
                Function function = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr()
                        .getFunction(searchDesc);
                if (function == null) {
                    return privilegeManager.analyzeObject(
                            analyzeObjectType(stmt.getObjectTypeUnResolved()),
                            Collections.singletonList(GlobalFunctionPEntryObject.FUNC_NOT_FOUND));
                } else {
                    return privilegeManager.analyzeObject(
                            analyzeObjectType(stmt.getObjectTypeUnResolved()),
                            Collections.singletonList(function.signatureString())
                    );
                }
            }

            Database db = GlobalStateMgr.getCurrentState().getDb(name.getDb());
            Function function = db.getFunction(searchDesc);
            if (null == function) {
                return privilegeManager.analyzeObject(
                        analyzeObjectType(stmt.getObjectTypeUnResolved()),
                        Arrays.asList(db.getFullName(), FunctionPEntryObject.FUNC_NOT_FOUND)
                );
            } else {
                return privilegeManager.analyzeObject(
                        analyzeObjectType(stmt.getObjectTypeUnResolved()),
                        Arrays.asList(function.dbName(), function.signatureString())
                );
            }
        }

        private ObjectType analyzeObjectType(String objectTypeUnResolved) {
            if (!EnumUtils.isValidEnumIgnoreCase(ObjectType.class, objectTypeUnResolved)) {
                throw new SemanticException("cannot find privilege object type " + objectTypeUnResolved);
            }
            return ObjectType.valueOf(objectTypeUnResolved);
        }

        private PrivilegeType analyzePrivType(ObjectType objectType, String privTypeString) {
            if (!EnumUtils.isValidEnumIgnoreCase(PrivilegeType.class, privTypeString)) {
                throw new SemanticException("cannot find privilege type " + privTypeString);
            }

            PrivilegeType privilegeType = PrivilegeType.valueOf(privTypeString);
            if (!privilegeManager.isAvailablePirvType(objectType, privilegeType)) {
                throw new SemanticException("Cant grant " + privTypeString + " to object " + objectType);
            }
            return privilegeType;
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
                        stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));
                        for (UserIdentity userIdentity : stmt.getUserPrivilegeObjectList()) {
                            analyseUser(userIdentity, true);
                            objectList.add(privilegeManager.analyzeUserObject(stmt.getObjectType(), userIdentity));
                        }
                    } else if (stmt.getPrivilegeObjectNameTokensList() != null) {
                        // normal objects
                        stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));
                        for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                            if (tokens.size() < 1 || tokens.size() > 2) {
                                throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
                            }

                            if (tokens.size() == 1 && (stmt.getObjectType().equals(ObjectType.TABLE) ||
                                    stmt.getObjectType().equals(ObjectType.VIEW) ||
                                    stmt.getObjectType().equals(ObjectType.MATERIALIZED_VIEW))) {
                                List<String> tokensWithDatabase = new ArrayList<>();
                                if (Strings.isNullOrEmpty(session.getDatabase())) {
                                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                                } else {
                                    tokensWithDatabase.add(session.getDatabase());
                                }

                                tokensWithDatabase.add(tokens.get(0));
                                objectList.add(privilegeManager.analyzeObject(stmt.getObjectType(),
                                        tokensWithDatabase));
                            } else {
                                objectList.add(privilegeManager.analyzeObject(stmt.getObjectType(), tokens));
                            }
                        }
                    } else if (stmt.getFunctionArgsDef() != null) {
                        FunctionName functionName = parseFunctionName(stmt);
                        FunctionArgsDef argsDef = stmt.getFunctionArgsDef();
                        argsDef.analyze();
                        FunctionSearchDesc searchDesc = new FunctionSearchDesc(functionName,
                                argsDef.getArgTypes(),
                                argsDef.isVariadic());
                        PEntryObject object = parseFunctionObject(stmt, searchDesc);
                        objectList.add(object);
                    } else {
                        // all statement
                        // TABLES -> TABLE
                        ObjectType objectType = privilegeManager.getObjectByPlural(stmt.getObjectTypeUnResolved());

                        // TABLE -> 0/1
                        stmt.setObjectType(objectType);

                        if (stmt.getTokens().size() == 1 && (stmt.getObjectType().equals(ObjectType.TABLE) ||
                                stmt.getObjectType().equals(ObjectType.VIEW) ||
                                stmt.getObjectType().equals(ObjectType.MATERIALIZED_VIEW))) {
                            throw new SemanticException("ALL " + stmt.getObjectTypeUnResolved() +
                                    " must be restricted with database");
                        }

                        if (stmt.getObjectType().equals(ObjectType.USER)) {
                            if (stmt.getTokens().size() != 1) {
                                throw new SemanticException("invalid ALL statement for user! only support ON ALL USERS");
                            } else {
                                objectList.add(privilegeManager.analyzeUserObject(objectType, null));
                            }
                        } else {
                            objectList.add(privilegeManager.analyzeObject(objectType, stmt.getTokens()));
                        }
                    }
                    stmt.setObjectList(objectList);
                } else {
                    stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));
                    stmt.setObjectList(null);
                }

                List<PrivilegeType> privilegeTypes = new ArrayList<>();
                for (String privTypeUnResolved : stmt.getPrivilegeTypeUnResolved()) {
                    if (privTypeUnResolved.equals("ALL")) {
                        privilegeTypes.addAll(privilegeManager.getAvailablePrivType(stmt.getObjectType()));
                    } else {
                        privilegeTypes.add(analyzePrivType(stmt.getObjectType(), privTypeUnResolved));
                    }
                }

                stmt.setPrivilegeTypes(privilegeTypes);

                privilegeManager.validateGrant(stmt.getObjectType(), stmt.getPrivilegeTypes(), stmt.getObjectList());
            } catch (PrivilegeException | AnalysisException e) {
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

        @Override
        public Void visitSetDefaultRoleStatement(SetDefaultRoleStmt stmt, ConnectContext session) {
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
                stmt.getGranteeRole().forEach(role ->
                        validRoleName(role, "Can not granted/revoke role to user", true));
            } else {
                validRoleName(stmt.getRole(), "Can not granted/revoke role to role", true);
                stmt.getGranteeRole().forEach(role ->
                        validRoleName(role, "Can not granted/revoke role to user", true));
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            if (stmt.isAllowRevert()) {
                throw new SemanticException("`EXECUTE AS` must use with `WITH NO REVERT` for now!");
            }
            analyseUser(stmt.getToUser(), true);
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt stmt, ConnectContext session) {
            if (stmt.getUserIdent() != null) {
                analyseUser(stmt.getUserIdent(), true);
            } else if (stmt.getRole() != null) {
                validRoleName(stmt.getRole(), "There is no such grant defined for role " + stmt.getRole(), true);
            } else {
                stmt.setUserIdent(session.getCurrentUserIdentity());
            }

            return null;
        }
    }
}
