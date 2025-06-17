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

package com.starrocks.authorization;

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

public interface AccessController {
    default void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                 PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkCatalogAction(ConnectContext context, String catalogName,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnCatalog(ConnectContext context, String catalogName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkDbAction(ConnectContext context, String catalogName, String db,
                               PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkColumnAction(ConnectContext context, TableName tableName,
                                   String column, PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyView(ConnectContext context, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                             PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnMaterializedView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyMaterializedView(ConnectContext context, String db)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkFunctionAction(ConnectContext context, Database database, Function function,
                                     PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyFunction(ConnectContext context, String database)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkGlobalFunctionAction(ConnectContext context, Function function,
                                           PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnGlobalFunction(ConnectContext context, Function function)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    default void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkResourceAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnResource(ConnectContext context, String name) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkResourceGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkPipeAction(ConnectContext context, PipeName name,
                                 PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnPipe(ConnectContext context, PipeName name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkStorageVolumeAction(ConnectContext context, String storageVolume,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void withGrantOption(ConnectContext context,
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

    default void checkWarehouseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnWarehouse(ConnectContext context, String name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }
}