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
import com.starrocks.sql.ast.pipe.PipeName;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AccessController {
    default void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                 PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                               PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkColumnsAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                    Set<String> columns, PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkViewColumnsAction(UserIdentity currentUser, Set<Long> roleIds,
                                        TableName tableName, Set<String> columns,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkMaterializedViewColumnsAction(UserIdentity currentUser, Set<Long> roleIds,
                                                    TableName tableName, Set<String> columns,
                                                    PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                             PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                     PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String database)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                           PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    default void checkActionInDb(UserIdentity currentUser, Set<Long> roleIds, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkPipeAction(UserIdentity currentUser, Set<Long> roleIds, PipeName name,
                                 PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnPipe(UserIdentity currentUser, Set<Long> roleIds, PipeName name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void withGrantOption(UserIdentity currentUser, Set<Long> roleIds,
                                 ObjectType type, List<PrivilegeType> wants, List<PEntryObject> objects)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        return null;
    }

    default Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        return null;
    }
}