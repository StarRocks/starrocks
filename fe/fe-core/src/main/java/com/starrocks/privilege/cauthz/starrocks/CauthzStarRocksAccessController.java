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

package com.starrocks.privilege.cauthz.starrocks;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.cauthz.CauthzAccessController;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;

import java.util.Set;

import static java.util.Locale.ENGLISH;

/*
 * CauthzStarRocksAccessController is used to check whether the current user
 * has the specified privilege to act on a resource. The check methods in this class exist
 * to obtain authorization decisions on differing chains of object types 
 * such as catalog, database, table, view, function, etc.
 */
public class CauthzStarRocksAccessController extends CauthzAccessController {
    public CauthzStarRocksAccessController() {
        super();
    }

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setSystem().build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setUser(impersonateUser.getUser()).build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalog, db);
        for (Table table : database.getTables()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(catalog)
                                .setDatabase(database.getFullName())
                                .setTable(table.getName())
                                .build(),
                        currentUser,
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkColumnAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(CauthzStarRocksResource.builder()
                        .setCatalog(tableName.getCatalog())
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                currentUser, privilegeType);
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getViews()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setView(table.getName())
                                .build(),
                        currentUser,
                        PrivilegeType.ANY);
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
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getMaterializedViews()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setMaterializedView(table.getName())
                                .build(),
                        currentUser,
                        PrivilegeType.ANY);
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
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database.getFullName())
                        .setFunction(function.getSignature())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database)
                        .setFunction(function.getSignature())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Function function : database.getFunctions()) {
            try {
                hasPermission(
                        CauthzStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setFunction(function.getSignature())
                                .build(),
                        currentUser,
                        PrivilegeType.ANY);
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
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    @Override
    public void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getDb(db);
        for (Table table : database.getTables()) {
            if (table.isOlapView()) {
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
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setResourceGroup(name)
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkPipeAction(UserIdentity currentUser, Set<Long> roleIds, PipeName name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setDatabase(name.getDbName())
                        .setPipe(name.getPipeName())
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnPipe(UserIdentity currentUser, Set<Long> roleIds, PipeName pipeName)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setDatabase(pipeName.getDbName())
                        .setPipe(pipeName.getPipeName())
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume)
            throws AccessDeniedException {
        hasPermission(
                CauthzStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        return privilegeType.name().toLowerCase(ENGLISH);
    }
}

