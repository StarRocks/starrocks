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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public interface SystemAccessControl {

    default void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType) {
    }

    default void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
    }

    default void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                               PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
    }

    default void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
    }

    default void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {

    }

    default void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
    }

    default void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                             PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
    }

    default void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                     PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, long databaseId, long functionSig) {
    }

    default void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                           PrivilegeType privilegeType) {

    }

    default void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Long functionId) {

    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    default void checkActionInDb(UserIdentity userIdentity, Set<Long> roleIds, String db, PrivilegeType privilegeType) {

    }

    default void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {

    }

    default void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {

    }

    default void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {

    }

    default void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                          PrivilegeType privilegeType) {

    }

    default void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String name) {

    }
}