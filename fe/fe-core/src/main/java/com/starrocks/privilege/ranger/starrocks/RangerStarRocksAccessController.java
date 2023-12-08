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

package com.starrocks.privilege.ranger.starrocks;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.RangerAccessController;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerStarRocksAccessController extends RangerAccessController {
    public RangerStarRocksAccessController() {
        super("starrocks", null);
    }

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.SYSTEM, null);
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.USER,
                Lists.newArrayList(impersonateUser.getUser()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.CATALOG, Lists.newArrayList(catalogName));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.CATALOG, Lists.newArrayList(catalogName));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.DATABASE, Lists.newArrayList(catalogName, db));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.DATABASE,
                Lists.newArrayList(catalogName, db));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalog, db);
        for (Table table : database.getTables()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                    Lists.newArrayList(catalog, database.getFullName(), table.getName()));
            try {
                hasPermission(resource, currentUser, PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getViews()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                    Lists.newArrayList(database.getFullName(), table.getName()));
            try {
                hasPermission(resource, currentUser, PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.MATERIALIZED_VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.MATERIALIZED_VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getMaterializedViews()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                    Lists.newArrayList(database.getFullName(), table.getName()));
            try {
                hasPermission(resource, currentUser, PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                Lists.newArrayList(database.getFullName(), function.getSignature()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                Lists.newArrayList(database, function.getSignature()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Function function : database.getFunctions()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                    Lists.newArrayList(database.getFullName(), function.getSignature()));
            try {
                hasPermission(resource, currentUser, PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.GLOBAL_FUNCTION,
                Lists.newArrayList(function.getSignature()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.GLOBAL_FUNCTION,
                Lists.newArrayList(function.getSignature()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    @Override
    public void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getDb(db);
        for (Table table : database.getTables()) {
            if (table.isView()) {
                checkViewAction(userIdentity, roleIds, new TableName(database.getFullName(), table.getName()), privilegeType);
            } else if (table.isMaterializedView()) {
                checkMaterializedViewAction(userIdentity, roleIds,
                        new TableName(database.getFullName(), table.getName()), privilegeType);
            } else {
                checkTableAction(userIdentity, roleIds, new TableName(database.getFullName(), table.getName()), privilegeType);
            }
        }
    }

    @Override
    public void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE, Lists.newArrayList(name));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE, Lists.newArrayList(name));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE_GROUP, Lists.newArrayList(name));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkPipeAction(UserIdentity currentUser, Set<Long> roleIds, PipeName name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.PIPE,
                Lists.newArrayList(name.getDbName(), name.getPipeName()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnPipe(UserIdentity currentUser, Set<Long> roleIds, PipeName pipe)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.PIPE,
                Lists.newArrayList(pipe.getDbName(), pipe.getPipeName()));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.STORAGE_VOLUME,
                Lists.newArrayList(storageVolume));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.STORAGE_VOLUME,
                Lists.newArrayList(storageVolume));
        hasPermission(resource, currentUser, PrivilegeType.ANY);
    }

    private void hasPermission(RangerStarRocksResource resource, UserIdentity user, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = privilegeType.name().toLowerCase(ENGLISH);
        }

        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(resource, user, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);

        if (result == null || !result.getIsAllowed()) {
            throw new AccessDeniedException();
        }
    }
}