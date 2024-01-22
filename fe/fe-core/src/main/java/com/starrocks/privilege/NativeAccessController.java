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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NativeAccessController implements AccessController {
    private static final Logger LOG = LogManager.getLogger(NativeAccessController.class);

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.SYSTEM, null);
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        if (!privilegeType.equals(PrivilegeType.IMPERSONATE)) {
            throw new AccessDeniedException();
        }

        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        if (!authorizationManager.canExecuteAs(currentUser, roleIds, impersonateUser)) {
            throw new AccessDeniedException();
        }
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.CATALOG, Collections.singletonList(catalogName));
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName)
            throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.CATALOG, Collections.singletonList(catalogName));
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.DATABASE, Arrays.asList(catalog, db));
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.DATABASE, Arrays.asList(catalogName, db));
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        Preconditions.checkNotNull(tableName.getDb());

        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.TABLE,
                Arrays.asList(catalog, tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        Preconditions.checkNotNull(tableName.getDb());

        checkAnyActionOnObject(currentUser, roleIds, ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db)
            throws AccessDeniedException {
        checkAnyActionOnTable(currentUser, roleIds, new TableName(catalog, db, "*"));
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.VIEW,
                Arrays.asList(catalog, tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.VIEW,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db)
            throws AccessDeniedException {
        checkAnyActionOnView(currentUser, roleIds, new TableName(catalog, db, "*"));
    }

    @Override
    public void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.MATERIALIZED_VIEW,
                Arrays.asList(tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.MATERIALIZED_VIEW,
                Arrays.asList(tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db)
            throws AccessDeniedException {
        checkAnyActionOnMaterializedView(currentUser, roleIds, new TableName(db, "*"));
    }

    @Override
    public void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        checkFunctionAction(currentUser, roleIds, ObjectType.FUNCTION, database.getId(), function, privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function)
            throws AccessDeniedException {
        checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.FUNCTION, database, function);
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String database)
            throws AccessDeniedException {
        checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.FUNCTION, database, null);
    }

    @Override
    public void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        checkFunctionAction(currentUser, roleIds, ObjectType.GLOBAL_FUNCTION,
                PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID, function, privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function)
            throws AccessDeniedException {
        checkAnyActionOnFunctionObject(currentUser, roleIds, ObjectType.GLOBAL_FUNCTION, null, function);
    }

    @Override
    public void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
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
            throw new AccessDeniedException();
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action {} in database {}, message: {}",
                    privilegeType, db, e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action {} in db {}", privilegeType, db, e);
            throw new AccessDeniedException();
        }
    }

    @Override
    public void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.RESOURCE, Collections.singletonList(name));
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.RESOURCE, Collections.singletonList(name));
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.RESOURCE_GROUP, Collections.singletonList(name));
    }

    @Override
    public void checkPipeAction(UserIdentity currentUser, Set<Long> roleIds, PipeName name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectType.PIPE,
                Lists.newArrayList(name.getDbName(), name.getPipeName()));
    }

    @Override
    public void checkAnyActionOnPipe(UserIdentity currentUser, Set<Long> roleIds, PipeName name)
            throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.PIPE,
                Lists.newArrayList(name.getDbName(), name.getPipeName()));
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        checkObjectTypeAction(currentUser, roleIds,
                privilegeType, ObjectType.STORAGE_VOLUME, Collections.singletonList(storageVolume));
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume)
            throws AccessDeniedException {
        checkAnyActionOnObject(currentUser, roleIds, ObjectType.STORAGE_VOLUME, Collections.singletonList(storageVolume));
    }

    @Override
    public void withGrantOption(UserIdentity currentUser, Set<Long> roleIds, ObjectType type, List<PrivilegeType> wants,
                                List<PEntryObject> objects) throws AccessDeniedException {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        if (!authorizationManager.allowGrant(currentUser, roleIds, type, wants, objects)) {
            throw new AccessDeniedException();
        }
    }

    /**
     * Check whether current user has any privilege action on Function
     */

    private void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, ObjectType objectType, long databaseId,
                                     Function function, PrivilegeType privilegeType) throws AccessDeniedException {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject object = manager.provider.generateFunctionObject(objectType, databaseId, function.getFunctionId(),
                    GlobalStateMgr.getCurrentState());
            boolean checkResult = manager.provider.check(objectType, privilegeType, object, collection);
            if (!checkResult) {
                throw new AccessDeniedException();
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    function.getSignature(), e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    function.getSignature(), e);
            throw new AccessDeniedException();
        }
    }

    protected static void checkObjectTypeAction(UserIdentity userIdentity, Set<Long> roleIds, PrivilegeType privilegeType,
                                                ObjectType objectType, List<String> objectTokens) throws AccessDeniedException {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            boolean checkResult = manager.checkAction(collection, objectType, privilegeType, objectTokens);
            if (!checkResult) {
                throw new AccessDeniedException();
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    getFullyQualifiedNameFromListAllowNull(objectTokens), e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    getFullyQualifiedNameFromListAllowNull(objectTokens), e);
            throw new AccessDeniedException();
        }
    }

    private static String getFullyQualifiedNameFromListAllowNull(List<String> objectTokens) {
        return objectTokens.stream()
                .map(e -> e == null ? "null" : e)
                .collect(Collectors.joining("."));
    }

    protected static void checkAnyActionOnObject(UserIdentity currentUser, Set<Long> roleIds, ObjectType objectType,
                                                 List<String> objectTokens) throws AccessDeniedException {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            PrivilegeCollectionV2 collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject pEntryObject = manager.provider.generateObject(
                    objectType, objectTokens, GlobalStateMgr.getCurrentState());
            boolean checkResult = manager.provider.searchAnyActionOnObject(objectType, pEntryObject, collection);
            if (!checkResult) {
                throw new AccessDeniedException();
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), getFullyQualifiedNameFromListAllowNull(objectTokens), e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), getFullyQualifiedNameFromListAllowNull(objectTokens), e);
            throw new AccessDeniedException();
        }
    }

    private static void checkAnyActionOnFunctionObject(UserIdentity currentUser, Set<Long> roleIds,
                                                       ObjectType objectType,
                                                       String dbName, Function function) throws AccessDeniedException {
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
            boolean checkResult = manager.provider.searchAnyActionOnObject(objectType, pEntryObject, collection);
            if (!checkResult) {
                throw new AccessDeniedException();
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), functionId, e.getMessage());
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), functionId, e);
            throw new AccessDeniedException();
        }
    }
}
