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

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.RangerAccessController;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerStarRocksAccessController extends RangerAccessController {

    public RangerStarRocksAccessController() {
        super("starrocks", null);
    }

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder().setSystem().build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.SYSTEM, null);
        }
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        if (!hasPermission(
                RangerStarRocksResource.builder().setUser(impersonateUser.getUser()).build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.USER, impersonateUser.getUser());
        }
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        if (!hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
        if (!hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        if (!hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db) {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalog, db);
        for (Table table : database.getTables()) {
            if (hasPermission(
                    RangerStarRocksResource.builder()
                            .setCatalog(catalog)
                            .setDatabase(database.getFullName())
                            .setTable(table.getName())
                            .build(),
                    currentUser,
                    PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, db);
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getViews()) {
            if (hasPermission(
                    RangerStarRocksResource.builder()
                            .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                            .setDatabase(database.getFullName())
                            .setView(table.getName())
                            .build(),
                    currentUser,
                    PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.VIEW, db);
    }

    @Override
    public void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                            PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(),
                    ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getMaterializedViews()) {
            if (hasPermission(
                    RangerStarRocksResource.builder()
                            .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                            .setDatabase(database.getFullName())
                            .setMaterializedView(table.getName())
                            .build(),
                    currentUser,
                    PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.MATERIALIZED_VIEW, db);
    }

    @Override
    public void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                    PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database.getFullName())
                        .setFunction(function.getSignature())
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database)
                        .setFunction(function.getSignature())
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Function function : database.getFunctions()) {
            if (hasPermission(
                    RangerStarRocksResource.builder()
                            .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                            .setDatabase(database.getFullName())
                            .setFunction(function.getSignature())
                            .build(),
                    currentUser,
                    PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.FUNCTION, db);
    }

    @Override
    public void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                          PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.GLOBAL_FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function) {
        if (!currentUser.equals(UserIdentity.ROOT)) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectType.GLOBAL_FUNCTION, function.getSignature());
        }
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    @Override
    public void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType) {
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
    public void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setResourceGroup(name)
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE_GROUP, name);
        }
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume) {
        if (!hasPermission(
                RangerStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            Expr columnMaskingExpression = getColumnMaskingExpression(RangerStarRocksResource.builder()
                    .setCatalog(tableName.getCatalog())
                    .setDatabase(tableName.getDb())
                    .setTable(tableName.getTbl())
                    .setColumn(column.getName())
                    .build(), column, context);
            if (columnMaskingExpression != null) {
                maskingExprMap.put(column.getName(), columnMaskingExpression);
            }
        }

        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        return getRowAccessExpression(RangerStarRocksResource.builder()
                .setCatalog(tableName.getCatalog())
                .setDatabase(tableName.getDb())
                .setTable(tableName.getTbl())
                .build(), context);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        return privilegeType.name().toLowerCase(ENGLISH);
    }
}