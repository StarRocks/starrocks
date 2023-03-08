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
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.AuthenticationProviderFactory;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
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
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang3.EnumUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
            userIdent.analyze();

            // check if user exists
            if (checkExist && !authenticationManager.doesUserExist(userIdent)) {
                throw new SemanticException("cannot find user " + userIdent + "!");
            }
        }

        /**
         * check if role name valid and get full role name
         */
        private void validRoleName(String roleName, String errMsg, boolean checkExist) {
            // always set to true, we can validate if it's allowed to operation on admin later
            FeNameFormat.checkRoleName(roleName, true, errMsg);
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
        public Void visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            stmt.getUserIdentity().analyze();
            if (stmt instanceof CreateUserStmt) {
                CreateUserStmt createUserStmt = (CreateUserStmt) stmt;
                if (authenticationManager.doesUserExist(createUserStmt.getUserIdentity()) && !createUserStmt.isIfNotExists()) {
                    throw new SemanticException("Operation CREATE USER failed for " + createUserStmt.getUserIdentity()
                            + " : user already exists");
                }
            } else {
                AlterUserStmt alterUserStmt = (AlterUserStmt) stmt;
                if (!authenticationManager.doesUserExist(stmt.getUserIdentity()) && !alterUserStmt.isIfExists()) {
                    throw new SemanticException("Operation ALTER USER failed for " + alterUserStmt.getUserIdentity()
                            + " : user not exists");
                }
            }

            byte[] password = MysqlPassword.EMPTY_PASSWORD;
            String authPluginUsing;
            if (stmt.getAuthPluginName() == null) {
                authPluginUsing = authenticationManager.getDefaultPlugin();
                password = analysePassword(stmt.getOriginalPassword(), stmt.isPasswordPlain());
            } else {
                authPluginUsing = stmt.getAuthPluginName();
                if (authPluginUsing.equals(PlainPasswordAuthenticationProvider.PLUGIN_NAME)) {
                    // In this case, authString is the password
                    password = analysePassword(stmt.getAuthStringUnResolved(), stmt.isPasswordPlain());
                }
            }

            try {
                AuthenticationProvider provider = AuthenticationProviderFactory.create(authPluginUsing);
                UserIdentity userIdentity = stmt.getUserIdentity();
                UserAuthenticationInfo info = provider.validAuthenticationInfo(
                        userIdentity, new String(password, StandardCharsets.UTF_8), stmt.getAuthStringUnResolved());
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

            if (!stmt.getDefaultRoles().isEmpty()) {
                stmt.getDefaultRoles().forEach(r -> validRoleName(r, "Valid role name fail", true));
            }
            return null;
        }

        private boolean needProtectAdminUser(UserIdentity userIdentity, ConnectContext context) {
            return Config.authorization_enable_admin_user_protection &&
                    userIdentity.getQualifiedUser().equalsIgnoreCase("admin") &&
                    !context.getCurrentUserIdentity().equals(UserIdentity.ROOT);
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            UserIdentity userIdentity = stmt.getUserIdentity();
            userIdentity.analyze();

            if (needProtectAdminUser(userIdentity, session)) {
                throw new SemanticException("'admin' user cannot be dropped because of " +
                        "'authorization_enable_admin_user_protection' configuration is enabled");
            }

            if (!authenticationManager.doesUserExist(userIdentity) && !stmt.isIfExists()) {
                throw new SemanticException("Operation DROP USER failed for " + userIdentity + " : user not exists");
            }

            if (stmt.getUserIdentity().equals(UserIdentity.ROOT)) {
                throw new SemanticException("Operation DROP USER failed for " + UserIdentity.ROOT +
                        " : cannot drop user " + UserIdentity.ROOT);
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null) {
                analyseUser(user, true);
            } else if (!statement.isAll()) {
                statement.setUserIdent(context.getCurrentUserIdentity());
            }
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            for (String roleName : stmt.getRoles()) {
                FeNameFormat.checkRoleName(roleName, true, "Can not create role");
                if (privilegeManager.checkRoleExists(roleName) && !stmt.isIfNotExists()) {
                    throw new SemanticException("Operation CREATE ROLE failed for " + roleName + " : role already exists");
                }
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            for (String roleName : stmt.getRoles()) {
                FeNameFormat.checkRoleName(roleName, true, "Can not create role");
                if (!privilegeManager.checkRoleExists(roleName) && !stmt.isIfExists()) {
                    throw new SemanticException("Operation DROP ROLE failed for " + roleName + " : role not exists");
                }
            }
            return null;
        }

        private PEntryObject parseFunctionObject(BaseGrantRevokePrivilegeStmt stmt, FunctionSearchDesc searchDesc)
                throws PrivilegeException {
            FunctionName name = searchDesc.getName();
            if (name.isGlobalFunction()) {
                Function function = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr()
                        .getFunction(searchDesc);
                if (function == null) {
                    return privilegeManager.generateObject(
                            analyzeObjectType(stmt.getObjectTypeUnResolved()),
                            Collections.singletonList(GlobalFunctionPEntryObject.FUNC_NOT_FOUND));
                } else {
                    return privilegeManager.generateObject(
                            analyzeObjectType(stmt.getObjectTypeUnResolved()),
                            Collections.singletonList(function.signatureString())
                    );
                }
            }

            Database db = GlobalStateMgr.getCurrentState().getDb(name.getDb());
            Function function = db.getFunction(searchDesc);
            if (null == function) {
                return privilegeManager.generateObject(
                        analyzeObjectType(stmt.getObjectTypeUnResolved()),
                        Arrays.asList(db.getFullName(), FunctionPEntryObject.FUNC_NOT_FOUND)
                );
            } else {
                return privilegeManager.generateObject(
                        analyzeObjectType(stmt.getObjectTypeUnResolved()),
                        Arrays.asList(function.dbName(), function.signatureString())
                );
            }
        }

        private ObjectType analyzeObjectType(String objectTypeUnResolved) {
            String o = objectTypeUnResolved.replace(" ", "_");

            if (!EnumUtils.isValidEnumIgnoreCase(ObjectType.class, o)) {
                throw new SemanticException("cannot find privilege object type " + objectTypeUnResolved);
            }
            return ObjectType.valueOf(o);
        }

        private PrivilegeType analyzePrivType(ObjectType objectType, String privTypeString) {
            String p = privTypeString.replace(" ", "_");

            if (!EnumUtils.isValidEnumIgnoreCase(PrivilegeType.class, p)) {
                throw new SemanticException("cannot find privilege type " + privTypeString);
            }

            PrivilegeType privilegeType = PrivilegeType.valueOf(p);
            if (!privilegeManager.isAvailablePirvType(objectType, privilegeType)) {
                throw new SemanticException("Cannot grant or revoke " + privTypeString + " on '"
                        + objectType + "' type object");
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
                            objectList.add(privilegeManager.generateUserObject(stmt.getObjectType(), userIdentity));
                        }
                    } else if (stmt.getPrivilegeObjectNameTokensList() != null) {
                        // normal objects
                        ObjectType objectType = analyzeObjectType(stmt.getObjectTypeUnResolved());
                        stmt.setObjectType(objectType);
                        for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                            if (objectType.equals(ObjectType.TABLE) && tokens.size() == 3) {
                                objectList.add(privilegeManager.generateObject(objectType, tokens));
                                continue;
                            } else if (objectType.equals(ObjectType.DATABASE) && tokens.size() == 2) {
                                objectList.add(privilegeManager.generateObject(objectType, tokens));
                                continue;
                            }
                            if (tokens.size() < 1 || tokens.size() > 2) {
                                throw new PrivilegeException(
                                        "invalid object tokens, should have one or two, current: " + tokens);
                            }

                            List<String> fullTokens = new LinkedList<>();
                            if (tokens.size() == 1 && (objectType.equals(ObjectType.TABLE) ||
                                    objectType.equals(ObjectType.VIEW) ||
                                    objectType.equals(ObjectType.MATERIALIZED_VIEW))) {
                                if (Strings.isNullOrEmpty(session.getDatabase())) {
                                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                                } else {
                                    fullTokens.add(session.getDatabase());
                                }
                                fullTokens.add(tokens.get(0));
                            } else {
                                fullTokens.addAll(tokens);
                            }

                            if (objectType.equals(ObjectType.TABLE) || objectType.equals(ObjectType.DATABASE)) {
                                if (Strings.isNullOrEmpty(session.getCurrentCatalog())) {
                                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_CATALOG_ERROR);
                                } else {
                                    fullTokens.add(0, session.getCurrentCatalog());
                                }
                            }
                            objectList.add(privilegeManager.generateObject(objectType, fullTokens));
                        }
                    } else if (stmt.getFunctions() != null) {
                        stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));

                        for (Pair<FunctionName, FunctionArgsDef> f : stmt.getFunctions()) {
                            FunctionName functionName = f.first;
                            if (functionName.getDb() == null) {
                                String dbName = ConnectContext.get().getDatabase();
                                if (dbName.equals("")) {
                                    throw new SemanticException("database not selected");
                                }
                                functionName.setDb(dbName);
                            }

                            FunctionArgsDef argsDef = f.second;
                            argsDef.analyze();
                            FunctionSearchDesc searchDesc = new FunctionSearchDesc(functionName,
                                    argsDef.getArgTypes(),
                                    argsDef.isVariadic());
                            PEntryObject object = parseFunctionObject(stmt, searchDesc);
                            objectList.add(object);
                        }

                    } else {
                        // grant on all object
                        ObjectType objectType = ObjectType.OBJECT_TO_PLURAL.get(stmt.getObjectTypeUnResolved());
                        if (objectType == null) {
                            throw new SemanticException("invalid plural privilege type " +
                                    stmt.getObjectTypeUnResolved());
                        }
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
                                objectList.add(privilegeManager.generateUserObject(objectType, null));
                            }
                        } else {
                            List<String> fullTokens = new LinkedList<>(stmt.getTokens());
                            if (objectType.equals(ObjectType.TABLE) || objectType.equals(ObjectType.DATABASE)) {
                                if (Strings.isNullOrEmpty(session.getCurrentCatalog())) {
                                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_CATALOG_ERROR);
                                } else {
                                    fullTokens.add(0, session.getCurrentCatalog());
                                }
                            }
                            objectList.add(privilegeManager.generateObject(objectType, fullTokens));
                        }
                    }
                    stmt.setObjectList(objectList);
                } else {
                    stmt.setObjectType(analyzeObjectType(stmt.getObjectTypeUnResolved()));
                    stmt.setObjectList(Arrays.asList(new PEntryObject[] {null}));
                }

                List<PrivilegeType> privilegeTypes = new ArrayList<>();
                for (String privTypeUnResolved : stmt.getPrivilegeTypeUnResolved()) {
                    if (privTypeUnResolved.equalsIgnoreCase("all")
                            || privTypeUnResolved.equalsIgnoreCase("all privileges")) {
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
            analyseUser(stmt.getUserIdentity(), true);
            try {
                for (String roleName : stmt.getRoles()) {
                    validRoleName(roleName, "Cannot set role", true);

                    Long roleId = privilegeManager.getRoleIdByNameAllowNull(roleName);
                    Set<Long> roleIdsForUser = privilegeManager.getRoleIdsByUser(stmt.getUserIdentity());
                    if (roleId == null || !roleIdsForUser.contains(roleId)) {
                        throw new SemanticException("Role " + roleName + " is not granted to " +
                                stmt.getUserIdentity().toString());
                    }
                }
            } catch (PrivilegeException e) {
                throw new SemanticException(e.getMessage());
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
            if (stmt.getUserIdentity() != null) {
                analyseUser(stmt.getUserIdentity(), true);
                if (needProtectAdminUser(stmt.getUserIdentity(), session)) {
                    throw new SemanticException("roles of 'admin' user cannot be changed because of " +
                            "'authorization_enable_admin_user_protection' configuration is enabled");
                }
                stmt.getGranteeRole().forEach(role ->
                        validRoleName(role, "Can not granted/revoke role to/from user", true));
            } else {
                validRoleName(stmt.getRole(), "Can not granted/revoke role to/from role", true);
                stmt.getGranteeRole().forEach(role ->
                        validRoleName(role, "Can not granted/revoke role to/from user", true));
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
