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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class PrivilegeActions {
    private static final Logger LOG = LogManager.getLogger(PrivilegeActions.class);

    public static boolean checkTableAction(ConnectContext connectContext, String db, String table, PrivilegeType action) {
        return checkTableAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(), db, table, action);
    }

    public static boolean checkTableAction(
            UserIdentity userIdentity, Set<Long> roleIds, String db, String table, PrivilegeType privilegeType) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            return manager.checkAction(collection, ObjectType.TABLE, privilegeType, Arrays.asList(db, table));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on table {}.{}, message: {}",
                    privilegeType, db, table, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on table {}.{}", privilegeType, db, table, e);
            return false;
        }
    }

    public static boolean checkDbAction(ConnectContext context, String db, PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.DATABASE, action, Collections.singletonList(db));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on database {}, message: {}",
                    action, db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on db {}", action, db, e);
            return false;
        }
    }

    public static boolean checkSystemAction(ConnectContext connectContext, PrivilegeType action) {
        return checkSystemAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(), action);
    }

    public static boolean checkSystemAction(
            UserIdentity userIdentity, Set<Long> roleIds, PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(userIdentity, roleIds);
            return manager.checkAction(collection, ObjectType.SYSTEM, action, null);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on system", action, e);
            return false;
        }
    }

    public static boolean checkResourceAction(ConnectContext context, String name,
                                              PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.RESOURCE, action, Collections.singletonList(name));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on resource {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on resource {}", action, name, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnResource(ConnectContext context, String name) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            // 1. check for any action on resource
            PEntryObject resourceObject = manager.provider.generateObject(
                    ObjectType.RESOURCE, Collections.singletonList(name), GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.RESOURCE, resourceObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on resource {}, message: {}",
                    name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on resource {}", name, e);
            return false;
        }
    }

    public static boolean checkResourceGroupAction(ConnectContext context, String name,
                                                   PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.RESOURCE_GROUP, action, Collections.singletonList(name));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on resource group {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on resource group {}", action, name, e);
            return false;
        }
    }

    public static boolean checkGlobalFunctionAction(ConnectContext context, String name,
                                                    PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.GLOBAL_FUNCTION, action, Collections.singletonList(name));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on global function {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on global function {}", action, name, e);
            return false;
        }
    }

    public static boolean checkCatalogAction(ConnectContext context, String name,
                                             PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.CATALOG, action, Collections.singletonList(name));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on catalog {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on catalog {}", action, name, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            // 1. check for any action on catalog
            PEntryObject catalogObject = manager.provider.generateObject(
                    ObjectType.CATALOG, Collections.singletonList(catalogName), GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.CATALOG, catalogObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on catalog {}, message: {}",
                    catalogName, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on catalog {}", catalogName, e);
            return false;
        }
    }

    public static boolean checkViewAction(
            ConnectContext context, String db, String view, PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.VIEW, action, Arrays.asList(db, view));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on view {}.{}, message: {}",
                    action, db, view, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on view {}.{}", action, db, view, e);
            return false;
        }
    }

    public static boolean checkMaterializedViewAction(
            ConnectContext context, String db, String materializedView,
            PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.MATERIALIZED_VIEW, action, Arrays.asList(db, materializedView));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on materialized view {}.{}, message: {}",
                    action, db, materializedView, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on materialized view {}.{}",
                    action, db, materializedView, e);
            return false;
        }
    }

    public static boolean checkFunctionAction(
            ConnectContext context, String db, String functionSig,
            PrivilegeType action) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            return manager.checkAction(collection, ObjectType.FUNCTION, action, Arrays.asList(db, functionSig));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on function {}.{}, message: {}",
                    action, db, functionSig, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on function {}.{}",
                    action, db, functionSig, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnMaterializedView(ConnectContext context, String db, String materializedView) {
        return checkAnyActionOnMaterializedView(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                db, materializedView);
    }

    public static boolean checkAnyActionOnMaterializedView(
            UserIdentity currentUser, Set<Long> roleIds, String db, String materializedView) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject materializedViewObject = manager.provider.generateObject(
                    ObjectType.MATERIALIZED_VIEW, Arrays.asList(db, materializedView),
                    GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.MATERIALIZED_VIEW, materializedViewObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on materialized view {}.{}, message: {}",
                    db, materializedView, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on materialized view {}.{}",
                    db, materializedView, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnView(ConnectContext context, String db, String view) {
        return checkAnyActionOnView(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), db, view);
    }

    public static boolean checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, String db, String view) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject viewObject = manager.provider.generateObject(
                    ObjectType.VIEW, Arrays.asList(db, view), GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.VIEW, viewObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on view {}.{}, message: {}",
                    db, view, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on view {}.{}", db, view, e);
            return false;
        }
    }

    /**
     * show databases; use database
     */
    public static boolean checkAnyActionOnDb(ConnectContext context, String db) {
        return checkAnyActionOnDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), db);
    }

    public static boolean checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String db) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            // 1. check for any action on db
            PEntryObject dbObject = manager.provider.generateObject(
                    ObjectType.DATABASE, Collections.singletonList(db), GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.DATABASE, dbObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on database {}, message: {}",
                    db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on db {}", db, e);
            return false;
        }
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */

    public static boolean checkAnyActionOnOrInDb(ConnectContext context, String db) {
        return checkAnyActionOnOrInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), db);
    }

    public static boolean checkAnyActionOnOrInDb(UserIdentity currentUser, Set<Long> roleIds, String db) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            // 1. check for any action on db
            if (checkAnyActionOnDb(currentUser, roleIds, db)) {
                return true;
            }
            // 2. check for any action on any table in this db
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject allTableInDbObject = manager.provider.generateObject(
                    ObjectType.TABLE,
                    Lists.newArrayList(db, "*"),
                    GlobalStateMgr.getCurrentState());
            if (manager.provider.searchAnyActionOnObject(ObjectType.TABLE, allTableInDbObject, collection)) {
                return true;
            }
            // 3. check for any action on any view in this db
            PEntryObject allViewInDbObject = manager.provider.generateObject(
                    ObjectType.VIEW,
                    Lists.newArrayList(db, "*"),
                    GlobalStateMgr.getCurrentState());
            if (manager.provider.searchAnyActionOnObject(ObjectType.VIEW, allViewInDbObject, collection)) {
                return true;
            }
            // 4. check for any action on any mv in this db
            PEntryObject allMvInDbObject = manager.provider.generateObject(
                    ObjectType.MATERIALIZED_VIEW,
                    Lists.newArrayList(db, "*"),
                    GlobalStateMgr.getCurrentState());
            if (manager.provider.searchAnyActionOnObject(ObjectType.MATERIALIZED_VIEW, allMvInDbObject, collection)) {
                return true;
            }

            // 5. check for any action on any function in this db
            PEntryObject allFunctionsInDbObject = manager.provider.generateObject(
                    ObjectType.FUNCTION,
                    Lists.newArrayList(db, "*"),
                    GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.FUNCTION, allFunctionsInDbObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on or in database {}, message: {}",
                    db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on or in db {}", db, e);
            return false;
        }
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
                if (manager.provider.searchActionOnObject(ObjectType.TABLE, allTableInDbObject, collection, privilegeType)) {
                    return true;
                }
            }

            // 2. check for specified action on any view in this db
            if (manager.provider.isAvailablePrivType(ObjectType.VIEW, privilegeType)) {
                PEntryObject allViewInDbObject = manager.provider.generateObject(
                        ObjectType.VIEW,
                        Lists.newArrayList(db, "*"),
                        GlobalStateMgr.getCurrentState());
                if (manager.provider.searchActionOnObject(ObjectType.VIEW, allViewInDbObject, collection, privilegeType)) {
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

    public static boolean checkAnyActionOnTable(ConnectContext context, String db, String table) {
        return checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), db, table);
    }

    public static boolean checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, String db, String table) {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(currentUser, roleIds);
            PEntryObject tableObject = manager.provider.generateObject(
                    ObjectType.TABLE, Arrays.asList(db, table), GlobalStateMgr.getCurrentState());
            return manager.provider.searchAnyActionOnObject(ObjectType.TABLE, tableObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on table {}.{}, message: {}",
                    db, table, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on table {}.{}", db, table, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, String dbName, Table tbl) {
        Table.TableType type = tbl.getType();
        switch (type) {
            case OLAP:
                return checkAnyActionOnTable(currentUser, roleIds, dbName, tbl.getName());
            case MATERIALIZED_VIEW:
                return checkAnyActionOnMaterializedView(currentUser, roleIds, dbName, tbl.getName());
            case VIEW:
                return checkAnyActionOnView(currentUser, roleIds, dbName, tbl.getName());
            default:
                return false;
        }
    }
}
