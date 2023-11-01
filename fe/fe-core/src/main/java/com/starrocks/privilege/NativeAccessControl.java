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

package com.starrocks.privilege;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NativeAccessControl implements AccessControl {
    private static final Logger LOG = LogManager.getLogger(NativeAccessControl.class);

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.SYSTEM, null)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.SYSTEM, null);
        }
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        if (!privilegeType.equals(PrivilegeType.IMPERSONATE)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.USER, impersonateUser.getUser());
        }

        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        if (!authorizationManager.canExecuteAs(currentUser, roleIds, impersonateUser)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.USER, impersonateUser.getUser());
        }
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.CATALOG,
                Collections.singletonList(catalogName))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.CATALOG, Collections.singletonList(catalogName))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.DATABASE, Arrays.asList(catalog, db))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.DATABASE, Arrays.asList(catalogName, db))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        Preconditions.checkNotNull(tableName.getDb());

        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.TABLE,
                Arrays.asList(catalog, tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        Preconditions.checkNotNull(tableName.getDb());

        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db) {
        checkAnyActionOnTable(currentUser, roleIds, new TableName(catalog, db, "*"));
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.VIEW,
                Arrays.asList(tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        checkAnyActionOnView(currentUser, roleIds, new TableName(db, "*"));
    }

    @Override
    public void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                            PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.MATERIALIZED_VIEW,
                Arrays.asList(tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.MATERIALIZED_VIEW,
                Arrays.asList(tableName.getDb(), tableName.getTbl()))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        checkAnyActionOnMaterializedView(currentUser, roleIds, new TableName(db, "*"));
    }

    @Override
    public void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                    PrivilegeType privilegeType) {
        if (!checkFunctionAction(currentUser, roleIds, ObjectType.FUNCTION, database.getId(), function, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function) {
        if (!checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.FUNCTION, database, function)) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String database) {
        if (!checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.FUNCTION, database, null)) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.FUNCTION, "ANY");
        }
    }

    @Override
    public void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                          PrivilegeType privilegeType) {
        if (!checkFunctionAction(currentUser, roleIds, ObjectType.GLOBAL_FUNCTION,
                PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID, function, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.GLOBAL_FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function) {
        if (!checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.GLOBAL_FUNCTION,
                null, function)) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.GLOBAL_FUNCTION,
                    function.getSignature());
        }
    }

    @Override
    public void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType) {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            // 1. check for specified action on any table in this db
            if (manager.provider.isAvailablePrivType(ObjectType.TABLE, privilegeType)) {
                PEntryObject allTableInDbObject = manager.provider.generateObject(
                        ObjectType.TABLE,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(ObjectType.TABLE, allTableInDbObject, collection, privilegeType)) {
                    return;
                }
            }

            // 2. check for specified action on any view in this db
            if (manager.provider.isAvailablePrivType(ObjectType.VIEW, privilegeType)) {
                PEntryObject allViewInDbObject = manager.provider.generateObject(
                        ObjectType.VIEW,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(ObjectType.VIEW, allViewInDbObject, collection, privilegeType)) {
                    return;
                }
            }

            // 3. check for specified action on any mv in this db
            if (manager.provider.isAvailablePrivType(ObjectType.MATERIALIZED_VIEW, privilegeType)) {
                PEntryObject allMvInDbObject = manager.provider.generateObject(
                        ObjectType.MATERIALIZED_VIEW,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(
                        ObjectType.MATERIALIZED_VIEW, allMvInDbObject, collection, privilegeType)) {
                    return;
                }
            }
            throw new AccessDeniedException(ErrorReport.reportCommon(null, ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    privilegeType.name() + " IN DATABASE"));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action {} in database {}, message: {}",
                    privilegeType, db, e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action {} in db {}", privilegeType, db, e);
            throw new AccessDeniedException(ErrorReport.reportCommon(null, ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    privilegeType.name() + " IN DATABASE"));
        }
    }

    @Override
    public void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds,
                privilegeType, ObjectType.RESOURCE, Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.RESOURCE, Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds,
                privilegeType, ObjectType.RESOURCE_GROUP, Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE_GROUP, name);
        }
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds,
                privilegeType, ObjectType.STORAGE_VOLUME, Collections.singletonList(storageVolume))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectType.STORAGE_VOLUME, Collections.singletonList(storageVolume))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public void withGrantOption(UserIdentity currentUser, Set<Long> roleIds, ObjectType type, List<PrivilegeType> wants,
                                List<PEntryObject> objects) throws AccessDeniedException {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        if (!authorizationManager.allowGrant(currentUser, roleIds, type, wants, objects)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.GRANT.name(), ObjectType.SYSTEM, null);
        }
    }

    /**
     * Check whether current user has any privilege action on Function
     */

    private boolean checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, ObjectType objectType,
                                        long databaseId, Function function, PrivilegeType privilegeType) {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject object = manager.provider.generateFunctionObject(objectType, databaseId, function.getFunctionId(),
                    GlobalStateMgr.getCurrentState());
            return manager.provider.check(objectType, privilegeType, object, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    function.getSignature(), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    function.getSignature(), e);
            return false;
        }
    }

    protected static boolean checkObjectTypeAction(UserIdentity userIdentity, Set<Long> roleIds,
                                                   PrivilegeType privilegeType,
                                                   ObjectType objectType, List<String> objectTokens) {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            return manager.checkAction(collection, objectType, privilegeType, objectTokens);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    getFullyQualifiedNameFromListAllowNull(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    getFullyQualifiedNameFromListAllowNull(objectTokens), e);
            return false;
        }
    }

    private static String getFullyQualifiedNameFromListAllowNull(List<String> objectTokens) {
        return objectTokens.stream()
                .map(e -> e == null ? "null" : e)
                .collect(Collectors.joining("."));
    }

    protected static boolean checkAnyActionOnObject(UserIdentity currentUser, Set<Long> roleIds, ObjectType objectType,
                                                    List<String> objectTokens) {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject pEntryObject = manager.provider.generateObject(
                    objectType, objectTokens, GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(objectType, pEntryObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), getFullyQualifiedNameFromListAllowNull(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), getFullyQualifiedNameFromListAllowNull(objectTokens), e);
            return false;
        }
    }

    private static boolean checkAnyActionOnFunctionObject(UserIdentity currentUser, Set<Long> roleIds,
                                                          ObjectType objectType,
                                                          String dbName, Function function) {
        // database == null means global function
        long databaseId;
        if (dbName == null) {
            databaseId = PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID;
        } else {
            Database database = MetaUtils.getDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName);
            databaseId = database.getId();
        }

        long functionId;
        if (function == null) {
            functionId = PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID;
        } else {
            functionId = function.getFunctionId();
        }

        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject pEntryObject = manager.provider.generateFunctionObject(
                    objectType, databaseId, functionId, GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(objectType, pEntryObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), functionId, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), functionId, e);
            return false;
        }
    }
}
