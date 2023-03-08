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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class PrivilegeActions {
    private static final Logger LOG = LogManager.getLogger(PrivilegeActions.class);

    private static boolean checkObjectTypeAction(UserIdentity userIdentity, Set<Long> roleIds,
                                                 PrivilegeType privilegeType,
                                                 ObjectType objectType, List<String> objectTokens) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            return manager.checkAction(collection, objectType, privilegeType, objectTokens);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on {} {}, message: {}",
                    privilegeType, objectType.name().replace("_", " "),
                    Joiner.on(".").join(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on {} {}",
                    privilegeType, objectType.name().replace("_", " "),
                    Joiner.on(".").join(objectTokens), e);
            return false;
        }
    }

    public static boolean checkTableAction(ConnectContext connectContext, String db, String table,
                                           PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.TABLE, Arrays.asList(db, table));
    }

    public static boolean checkTableAction(ConnectContext connectContext, String catalogName, String db, String table,
                                           PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.TABLE, Arrays.asList(catalogName, db, table));
    }

    public static boolean checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, String db, String table,
                                           PrivilegeType privilegeType) {
        return checkObjectTypeAction(userIdentity, roleIds, privilegeType, ObjectType.TABLE, Arrays.asList(db, table));
    }

    public static boolean checkDbAction(ConnectContext connectContext, String db, PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.DATABASE, Collections.singletonList(db));
    }

    public static boolean checkDbAction(ConnectContext connectContext,
                                        String catalogName, String db, PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.DATABASE, Arrays.asList(catalogName, db));
    }

    public static boolean checkSystemAction(ConnectContext connectContext, PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.SYSTEM, null);
    }

    public static boolean checkSystemAction(UserIdentity userIdentity, Set<Long> roleIds, PrivilegeType privilegeType) {
        return checkObjectTypeAction(userIdentity, roleIds, privilegeType, ObjectType.SYSTEM, null);
    }

    public static boolean checkResourceAction(ConnectContext connectContext, String name, PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.RESOURCE, Collections.singletonList(name));
    }

    public static boolean checkViewAction(ConnectContext connectContext, String db, String view,
                                          PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.VIEW, Arrays.asList(db, view));
    }

    public static boolean checkCatalogAction(ConnectContext connectContext, String name, PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.CATALOG, Collections.singletonList(name));
    }

    public static boolean checkMaterializedViewAction(ConnectContext connectContext, String db, String materializedView,
                                                      PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.MATERIALIZED_VIEW, Arrays.asList(db, materializedView));
    }

    public static boolean checkFunctionAction(ConnectContext connectContext, String db, String functionSig,
                                              PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.FUNCTION, Arrays.asList(db, functionSig));
    }

    public static boolean checkResourceGroupAction(ConnectContext connectContext, String name,
                                                   PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.RESOURCE_GROUP, Collections.singletonList(name));
    }

    public static boolean checkGlobalFunctionAction(ConnectContext connectContext, String name,
                                                    PrivilegeType privilegeType) {
        return checkObjectTypeAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                privilegeType, ObjectType.GLOBAL_FUNCTION, Collections.singletonList(name));
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    public static boolean checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            // 1. check for specified action on any table in this db

            if (manager.provider.isAvailablePrivType(ObjectType.TABLE, privilegeType)) {
                PEntryObject allTableInDbObject = manager.provider.generateObject(
                        ObjectType.TABLE,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(ObjectType.TABLE, allTableInDbObject, collection,
                        privilegeType)) {
                    return true;
                }
            }

            // 2. check for specified action on any view in this db
            if (manager.provider.isAvailablePrivType(ObjectType.VIEW, privilegeType)) {
                PEntryObject allViewInDbObject = manager.provider.generateObject(
                        ObjectType.VIEW,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(ObjectType.VIEW, allViewInDbObject, collection,
                        privilegeType)) {
                    return true;
                }
            }

            // 3. check for specified action on any mv in this db
            if (manager.provider.isAvailablePrivType(ObjectType.MATERIALIZED_VIEW, privilegeType)) {
                PEntryObject allMvInDbObject = manager.provider.generateObject(
                        ObjectType.MATERIALIZED_VIEW,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                return manager.provider.searchActionOnObject(
                        ObjectType.MATERIALIZED_VIEW, allMvInDbObject, collection, privilegeType);
            }
            return false;
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action {} in database {}, message: {}",
                    privilegeType, db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action {} in db {}", privilegeType, db, e);
            return false;
        }
    }

    private static boolean checkAnyActionOnObject(UserIdentity currentUser, Set<Long> roleIds, ObjectType objectType,
                                                  List<String> objectTokens) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject pEntryObject = manager.provider.generateObject(
                    objectType, objectTokens, GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(objectType, pEntryObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on {} {}, message: {}",
                    objectType.name(), Joiner.on(".").join(objectTokens), e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on {} {}",
                    objectType.name(), Joiner.on(".").join(objectTokens), e);
            return false;
        }
    }

    public static boolean checkAnyActionOnTable(ConnectContext context, String db, String table) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), ObjectType.TABLE,
                Lists.newArrayList(db, table));
    }

    public static boolean checkAnyActionOnTable(ConnectContext context, String catalogName, String db, String table) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), ObjectType.TABLE,
                Lists.newArrayList(catalogName, db, table));
    }

    public static boolean checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, String db, String table) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.TABLE, Lists.newArrayList(db, table));
    }

    public static boolean checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds,
                                                String catalogName, String db, String table) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.TABLE,
                Lists.newArrayList(catalogName, db, table));
    }

    /**
     * show databases; use database
     */
    public static boolean checkAnyActionOnDb(ConnectContext context, String db) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.DATABASE, Collections.singletonList(db));
    }

    public static boolean checkAnyActionOnDb(ConnectContext context, String catalogName, String db) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.DATABASE, Arrays.asList(catalogName, db));
    }

    public static boolean checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String db) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.DATABASE, Collections.singletonList(db));
    }

    public static boolean checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds,
                                             String catalogName, String db) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.DATABASE, Arrays.asList(catalogName, db));
    }

    public static boolean checkAnyActionOnResource(ConnectContext context, String name) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.RESOURCE, Collections.singletonList(name));
    }

    public static boolean checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, String db, String view) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.VIEW, Arrays.asList(db, view));
    }

    public static boolean checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), ObjectType.CATALOG,
                Collections.singletonList(catalogName));
    }

    public static boolean checkAnyActionOnCatalog(UserIdentity userIdentity, Set<Long> roleIds, String catalogName) {
        return checkAnyActionOnObject(userIdentity, roleIds, ObjectType.CATALOG, Collections.singletonList(catalogName));
    }

    public static boolean checkAnyActionOnOrInCatalog(ConnectContext connectContext, String catalogName) {
        return checkAnyActionOnOrInCatalog(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                catalogName);
    }

    public static boolean checkAnyActionOnOrInCatalog(UserIdentity userIdentity, Set<Long> roleIds,
                                                      String catalogName) {
        // check for any action on catalog or on db or on table/view/mv/function
        return checkAnyActionOnCatalog(userIdentity, roleIds, catalogName)
                || checkAnyActionOnDb(userIdentity, roleIds, catalogName, "*")
                || checkAnyActionOnTable(userIdentity, roleIds, catalogName, "*", "*")
                || (CatalogMgr.isInternalCatalog(catalogName) &&
                (checkAnyActionOnView(userIdentity, roleIds, "*", "*")
                        || checkAnyActionOnMaterializedView(userIdentity, roleIds, "*", "*")
                        || checkAnyActionOnFunction(userIdentity, roleIds, "*", "*")));
    }

    public static boolean checkAnyActionOnMaterializedView(ConnectContext context, String db, String materializedView) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.MATERIALIZED_VIEW, Arrays.asList(db, materializedView));
    }

    public static boolean checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db,
                                                           String materializedView) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.MATERIALIZED_VIEW,
                Arrays.asList(db, materializedView));
    }

    public static boolean checkAnyActionOnFunction(ConnectContext context, String db, String functionSig) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.FUNCTION, Arrays.asList(db, functionSig));
    }

    public static boolean checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String db,
                                                   String functionSig) {
        return checkAnyActionOnObject(currentUser, roleIds, ObjectType.FUNCTION, Arrays.asList(db, functionSig));
    }

    public static boolean checkAnyActionOnGlobalFunction(ConnectContext context, String functionSig) {
        return checkAnyActionOnObject(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                ObjectType.GLOBAL_FUNCTION, Collections.singletonList(functionSig));
    }

    public static boolean checkAnyActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds,
                                                          String dbName, Table tbl) {
        Table.TableType type = tbl.getType();
        switch (type) {
            case OLAP:
            case LAKE:
                return checkAnyActionOnTable(currentUser, roleIds, dbName, tbl.getName());
            case MATERIALIZED_VIEW:
            case LAKE_MATERIALIZED_VIEW:
                return checkAnyActionOnMaterializedView(currentUser, roleIds, dbName, tbl.getName());
            case VIEW:
                return checkAnyActionOnView(currentUser, roleIds, dbName, tbl.getName());
            default:
                return false;
        }
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static boolean checkAnyActionOnOrInDb(ConnectContext connectContext, String db) {
        return checkAnyActionOnOrInDb(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(), db);
    }

    public static boolean checkAnyActionOnOrInDb(ConnectContext connectContext, String catalogName, String db) {
        return checkAnyActionOnOrInDb(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                catalogName, db);
    }

    public static boolean checkAnyActionOnOrInDb(UserIdentity userIdentity, Set<Long> roleIds, String db) {
        return checkAnyActionOnOrInDb(userIdentity, roleIds, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
    }

    public static boolean checkAnyActionOnOrInDb(UserIdentity userIdentity, Set<Long> roleIds,
                                                 String catalogName, String db) {
        // check for any action on db or table/view/mv/function
        return checkAnyActionOnDb(userIdentity, roleIds, catalogName, db)
                || checkAnyActionOnTable(userIdentity, roleIds, catalogName, db, "*")
                || (CatalogMgr.isInternalCatalog(catalogName) &&
                (checkAnyActionOnView(userIdentity, roleIds, db, "*")
                        || checkAnyActionOnMaterializedView(userIdentity, roleIds, db, "*")
                        || checkAnyActionOnFunction(userIdentity, roleIds, db, "*")));
    }
}
