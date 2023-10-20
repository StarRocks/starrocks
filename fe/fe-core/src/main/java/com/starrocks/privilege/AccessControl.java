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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AccessControl {
    default void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.SYSTEM, null);
    }

    default void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName,
                                    PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.CATALOG, catalogName);
    }

    default void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.CATALOG, catalogName);
    }

    default void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                               PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, catalogName);
    }

    default void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.DATABASE, catalogName);
    }

    default void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
    }

    default void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.TABLE, tableName.getTbl());
    }

    default void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.TABLE, "ANY");
    }

    default void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.VIEW, tableName.getTbl());
    }

    default void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.VIEW, tableName.getTbl());
    }

    default void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.VIEW, "ANY");
    }

    default void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                             PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
    }

    default void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
    }

    default void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.MATERIALIZED_VIEW, "ANY");
    }

    default void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                     PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.FUNCTION, function.getSignature());
    }

    default void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.FUNCTION, function.getSignature());
    }

    default void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String database) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.FUNCTION, "ANY");
    }

    default void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                           PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.GLOBAL_FUNCTION, function.getSignature());
    }

    default void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.GLOBAL_FUNCTION, function.getSignature());
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    default void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.DATABASE, db);
    }

    default void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE, name);
    }

    default void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.RESOURCE, name);
    }

    default void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE_GROUP, name);
    }

    default void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                          PrivilegeType privilegeType) {
        AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.STORAGE_VOLUME, storageVolume);
    }

    default void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume) {
        AccessDeniedException.reportAccessDenied("ANY", ObjectType.STORAGE_VOLUME, storageVolume);
    }

    default Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        return null;
    }

    default Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        return null;
    }
}