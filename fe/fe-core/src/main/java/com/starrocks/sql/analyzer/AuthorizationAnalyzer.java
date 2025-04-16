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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class AuthorizationAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AuthorizationAnalyzerVisitor().analyze(statement, session);
    }

    public static class AuthorizationAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        private AuthenticationMgr authenticationManager = null;
        private AuthorizationMgr authorizationManager = null;

        public void analyze(StatementBase statement, ConnectContext session) {
            authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
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
            if (checkExist && !authorizationManager.checkRoleExists(roleName)) {
                throw new SemanticException(errMsg + ": cannot find role " + roleName + "!");
            }
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            for (String roleName : stmt.getRoles()) {
                FeNameFormat.checkRoleName(roleName, true, "Can not create role");
                if (authorizationManager.checkRoleExists(roleName) && !stmt.isIfNotExists()) {
                    throw new SemanticException(
                            "Operation CREATE ROLE failed for " + roleName + " : role already exists");
                }
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            for (String roleName : stmt.getRoles()) {
                FeNameFormat.checkRoleName(roleName, true, "Can not create role");
                if (!authorizationManager.checkRoleExists(roleName) && !stmt.isIfExists()) {
                    throw new SemanticException("Operation DROP ROLE failed for " + roleName + " : role not exists");
                }
            }
            return null;
        }

        private ObjectType analyzeObjectType(String objectTypeUnResolved) {
            return authorizationManager.getObjectType(objectTypeUnResolved);
        }

        private PrivilegeType analyzePrivType(ObjectType objectType, String privTypeString) {
            PrivilegeType privilegeType = authorizationManager.getPrivilegeType(privTypeString);
            if (privilegeType == null) {
                throw new SemanticException("cannot find privilege type " + privTypeString);
            }

            if (!authorizationManager.isAvailablePrivType(objectType, privilegeType)) {
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
                ObjectType objectType = analyzeObjectType(stmt.getObjectTypeUnResolved());
                stmt.setObjectType(objectType);

                List<PEntryObject> objectList = new ArrayList<>();
                if (objectType.equals(ObjectType.USER)) {
                    List<UserIdentity> userIdentities = analyzeUserPrivToken(stmt);
                    for (UserIdentity userIdentity : userIdentities) {
                        objectList.add(authorizationManager.generateUserObject(ObjectType.USER, userIdentity));
                    }
                } else if (objectType.equals(ObjectType.FUNCTION) || objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
                    List<Pair<Long, Long>> funcPrivTokenList = analyzeFuncPrivToken(stmt, objectType);
                    for (Pair<Long, Long> funcPrivToken : funcPrivTokenList) {
                        objectList.add(authorizationManager.generateFunctionObject(objectType,
                                funcPrivToken.first, funcPrivToken.second));
                    }
                } else if (objectType.equals(ObjectType.SYSTEM)) {
                    objectList.addAll(Arrays.asList(new PEntryObject[] {null}));
                } else {
                    List<List<String>> tokens = analyzeTokens(stmt, objectType, session);
                    for (List<String> token : tokens) {
                        objectList.add(authorizationManager.generateObject(objectType, token));
                    }
                }

                stmt.setObjectList(objectList);

                List<PrivilegeType> privilegeTypes = new ArrayList<>();
                for (String privTypeUnResolved : stmt.getPrivilegeTypeUnResolved()) {
                    if (privTypeUnResolved.equalsIgnoreCase("all")
                            || privTypeUnResolved.equalsIgnoreCase("all privileges")) {
                        privilegeTypes.addAll(authorizationManager.getAvailablePrivType(stmt.getObjectType()));
                    } else {
                        privilegeTypes.add(analyzePrivType(stmt.getObjectType(), privTypeUnResolved));
                    }
                }

                stmt.setPrivilegeTypes(privilegeTypes);

                authorizationManager.validateGrant(stmt.getObjectType(), stmt.getPrivilegeTypes(),
                        stmt.getObjectList());
            } catch (PrivilegeException | AnalysisException e) {
                SemanticException exception = new SemanticException(e.getMessage());
                exception.initCause(e);
                throw exception;
            }
            return null;
        }

        public List<UserIdentity> analyzeUserPrivToken(BaseGrantRevokePrivilegeStmt stmt) {
            List<UserIdentity> userIdentities = new ArrayList<>();
            if (stmt.isGrantOnALL()) {
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList().size() == 1);

                List<String> tokens = stmt.getPrivilegeObjectNameTokensList().get(0);
                if (tokens.size() != 1) {
                    throw new SemanticException(
                            "Invalid grant statement with error privilege object " + tokens);
                }
                userIdentities.add(null);
            } else {
                for (UserIdentity userIdentity : stmt.getUserPrivilegeObjectList()) {
                    analyseUser(userIdentity, true);
                    userIdentities.add(userIdentity);
                }
            }
            return userIdentities;
        }

        public List<Pair<Long, Long>> analyzeFuncPrivToken(BaseGrantRevokePrivilegeStmt stmt, ObjectType objectType)
                throws AnalysisException {
            List<Pair<Long, Long>> funcPrivTokenList = new ArrayList<>();

            if (stmt.isGrantOnALL()) {
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList().size() == 1);

                List<String> tokens = stmt.getPrivilegeObjectNameTokensList().get(0);
                if (ObjectType.FUNCTION.equals(objectType)) {
                    if (tokens.size() != 2) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }

                    if (tokens.get(0).equals("*")) {
                        funcPrivTokenList.add(new Pair<>(PrivilegeBuiltinConstants.ALL_DATABASE_ID,
                                PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID));
                    } else {
                        Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(tokens.get(0));
                        if (database == null) {
                            throw new SemanticException("Database %s is not found", tokens.get(0));
                        }

                        funcPrivTokenList.add(new Pair<>(database.getId(), PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID));
                    }
                } else if (ObjectType.GLOBAL_FUNCTION.equals(objectType)) {
                    if (tokens.size() != 1) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }

                    funcPrivTokenList.add(new Pair<>(PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID,
                            PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID));
                }
            } else {
                if (ObjectType.FUNCTION.equals(objectType)) {
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
                                argsDef.getArgTypes(), argsDef.isVariadic());

                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(functionName.getDb());
                        long databaseID = db.getId();
                        Function function = db.getFunction(searchDesc);

                        if (function == null) {
                            throw new SemanticException("cannot find function " + functionName + "!");
                        } else {
                            funcPrivTokenList.add(new Pair<>(databaseID, function.getFunctionId()));
                        }
                    }
                } else if (ObjectType.GLOBAL_FUNCTION.equals(objectType)) {
                    for (Pair<FunctionName, FunctionArgsDef> f : stmt.getFunctions()) {
                        FunctionName functionName = f.first;
                        FunctionArgsDef argsDef = f.second;
                        argsDef.analyze();
                        FunctionSearchDesc searchDesc = new FunctionSearchDesc(functionName,
                                argsDef.getArgTypes(), argsDef.isVariadic());

                        Function function = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr()
                                .getFunction(searchDesc);

                        if (function == null) {
                            throw new SemanticException("cannot find function " + functionName + "!");
                        } else {
                            funcPrivTokenList.add(new Pair<>(PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID,
                                    function.getFunctionId()));
                        }
                    }
                }
            }

            return funcPrivTokenList;
        }

        public List<List<String>> analyzeTokens(BaseGrantRevokePrivilegeStmt stmt, ObjectType objectType,
                                                ConnectContext session) {
            List<List<String>> objectTokenList = new ArrayList<>();
            if (stmt.isGrantOnALL()) {
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList().size() == 1);

                List<String> tokens = stmt.getPrivilegeObjectNameTokensList().get(0);
                if (ObjectType.TABLE.equals(objectType)) {
                    if (tokens.size() != 2) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }
                    objectTokenList.add(Lists.newArrayList(session.getCurrentCatalog(), tokens.get(0), tokens.get(1)));
                } else if (ObjectType.VIEW.equals(objectType)
                        || ObjectType.MATERIALIZED_VIEW.equals(objectType)
                        || ObjectType.PIPE.equals(objectType)) {
                    if (tokens.size() != 2) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }
                    objectTokenList.add(tokens);
                } else if (ObjectType.DATABASE.equals(objectType)) {
                    if (tokens.size() != 1) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }
                    objectTokenList.add(Lists.newArrayList(session.getCurrentCatalog(), tokens.get(0)));
                } else if (ObjectType.RESOURCE.equals(objectType)
                        || ObjectType.CATALOG.equals(objectType)
                        || ObjectType.RESOURCE_GROUP.equals(objectType)
                        || ObjectType.STORAGE_VOLUME.equals(objectType)
                        || ObjectType.WAREHOUSE.equals(objectType)) {
                    if (tokens.size() != 1) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }
                    objectTokenList.add(tokens);
                } else {
                    throw new SemanticException("Grant/Revoke unsupported object type " + objectType.name());
                }
            } else {
                if (ObjectType.TABLE.equals(objectType)) {
                    Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);

                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        TableName tableName;
                        if (tokens.size() == 3) {
                            tableName = new TableName(tokens.get(0), tokens.get(1), tokens.get(2));
                        } else if (tokens.size() == 2) {
                            tableName = new TableName(tokens.get(0), tokens.get(1));
                            tableName.normalization(session);
                        } else if (tokens.size() == 1) {
                            tableName = new TableName("", tokens.get(0));
                            tableName.normalization(session);
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }

                        objectTokenList.add(Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
                    }
                } else if (ObjectType.VIEW.equals(objectType) || ObjectType.MATERIALIZED_VIEW.equals(objectType)) {
                    Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);

                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        TableName tableName;
                        if (tokens.size() == 2) {
                            tableName = new TableName(tokens.get(0), tokens.get(1));
                        } else if (tokens.size() == 1) {
                            tableName = new TableName("", tokens.get(0));
                            tableName.normalization(session);
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }

                        objectTokenList.add(Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
                    }
                } else if (ObjectType.PIPE.equals(objectType)) {
                    Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);
                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        PipeName pipeName;
                        if (tokens.size() == 2) {
                            pipeName = new PipeName(tokens.get(0), tokens.get(1));
                        } else if (tokens.size() == 1) {
                            pipeName = new PipeName("", tokens.get(0));
                            PipeAnalyzer.analyzePipeName(pipeName, session);
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }

                        objectTokenList.add(Lists.newArrayList(pipeName.getDbName(), pipeName.getPipeName()));
                    }
                } else if (ObjectType.DATABASE.equals(objectType)) {
                    Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);

                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        if (tokens.size() == 2) {
                            objectTokenList.add(Lists.newArrayList(tokens.get(0), tokens.get(1)));
                        } else if (tokens.size() == 1) {
                            objectTokenList.add(Lists.newArrayList(session.getCurrentCatalog(), tokens.get(0)));
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }
                    }
                } else if (ObjectType.RESOURCE.equals(objectType)
                        || ObjectType.CATALOG.equals(objectType)
                        || ObjectType.RESOURCE_GROUP.equals(objectType)
                        || ObjectType.STORAGE_VOLUME.equals(objectType)
                        || ObjectType.WAREHOUSE.equals(objectType)) {
                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        if (tokens.size() != 1) {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }
                        objectTokenList.add(tokens);
                    }
                } else {
                    throw new SemanticException("Grant/Revoke unsupported object type " + objectType.name());
                }
            }

            return objectTokenList;
        }

        @Override
        public Void visitSetRoleStatement(SetRoleStmt stmt, ConnectContext session) {
            UserIdentity currentUser = session.getCurrentUserIdentity();
            if (currentUser != null && currentUser.isEphemeral()) {
                throw new SemanticException(
                        "set role statement is not supported for ephemeral user " + currentUser);
            }

            for (String roleName : stmt.getRoles()) {
                validRoleName(roleName, "Cannot set role", true);
            }
            return null;
        }

        @Override
        public Void visitSetDefaultRoleStatement(SetDefaultRoleStmt stmt, ConnectContext session) {
            UserIdentity currentUser = session.getCurrentUserIdentity();
            if (currentUser != null && currentUser.isEphemeral()) {
                throw new SemanticException(
                        "set default role statement is not supported for ephemeral user " + currentUser);
            }

            analyseUser(stmt.getUserIdentity(), true);
            try {
                for (String roleName : stmt.getRoles()) {
                    validRoleName(roleName, "Cannot set role", true);

                    Long roleId = authorizationManager.getRoleIdByNameAllowNull(roleName);
                    Set<Long> roleIdsForUser = authorizationManager.getRoleIdsByUser(stmt.getUserIdentity());
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

        private boolean needProtectAdminUser(UserIdentity userIdentity, ConnectContext context) {
            return Config.authorization_enable_admin_user_protection &&
                    userIdentity.getUser().equalsIgnoreCase("admin") &&
                    !context.getCurrentUserIdentity().equals(UserIdentity.ROOT);
        }
    }
}