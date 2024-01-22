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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessController;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.PipeName;
import org.apache.commons.collections4.ListUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Authorizer {
    private static final AccessControlProvider INSTANCE;

    static {
        if (Config.access_control.equals("ranger")) {
            INSTANCE = new AccessControlProvider(new AuthorizerStmtVisitor(), new RangerStarRocksAccessController());
        } else {
            INSTANCE = new AccessControlProvider(new AuthorizerStmtVisitor(), new NativeAccessController());
        }
    }

    public static AccessControlProvider getInstance() {
        return INSTANCE;
    }

    public static void check(StatementBase statement, ConnectContext context) {
        getInstance().getPrivilegeCheckerVisitor().check(statement, context);
    }

    public static void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkSystemAction(currentUser, roleIds, privilegeType);
    }

    public static void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkUserAction(currentUser, roleIds, impersonateUser, privilegeType);
    }

    public static void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkCatalogAction(currentUser, roleIds, catalogName, privilegeType);
    }

    public static void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName)
            throws AccessDeniedException {
        //Any user has an implicit usage permission on the internal catalog
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                    .checkAnyActionOnCatalog(currentUser, roleIds, catalogName);
        }
    }

    public static void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                                     PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(catalogName)
                .checkDbAction(currentUser, roleIds, catalogName, db, privilegeType);
    }

    public static void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnDb(currentUser, roleIds, catalogName, db);
    }

    public static void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, String db, String table,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, table);
        Optional<Table> tableObj = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName);
        if (tableObj.isPresent() && !tableObj.get().isTable() && privilegeType.equals(PrivilegeType.INSERT)) {
            return;
        }
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkTableAction(currentUser, roleIds,
                        new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, table), privilegeType);
    }

    public static void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db,
                                        String table, PrivilegeType privilegeType) throws AccessDeniedException {
        TableName tableName = new TableName(catalog, db, table);
        Optional<Table> tableObj = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName);
        if (tableObj.isPresent() && !tableObj.get().isTable() && privilegeType.equals(PrivilegeType.INSERT)) {
            return;
        }
        getInstance().getAccessControlOrDefault(catalog).checkTableAction(currentUser, roleIds,
                new TableName(catalog, db, table), privilegeType);
    }

    public static void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
        Optional<Table> table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName);
        if (table.isPresent() && !table.get().isTable() && privilegeType.equals(PrivilegeType.INSERT)) {
            return;
        }
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog)
                .checkTableAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog).checkAnyActionOnTable(currentUser, roleIds, tableName);
    }

    public static void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog)
                .checkViewAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnView(currentUser, roleIds, tableName);
    }

    public static void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                                   PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkMaterializedViewAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds,
                                                        TableName tableName) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnMaterializedView(currentUser, roleIds, tableName);
    }

    public static void checkActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                                    PrivilegeType privilegeType) throws AccessDeniedException {
        Optional<Table> table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName);
        if (table.isPresent()) {
            doCheckTableLikeObject(currentUser, roleIds, tableName.getDb(), table.get(), privilegeType);
        }
    }

    public static void checkAnyActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, String dbName,
                                                       Table tbl) throws AccessDeniedException {
        doCheckTableLikeObject(currentUser, roleIds, dbName, tbl, null);
    }

    private static void doCheckTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, String dbName,
                                               Table tbl, PrivilegeType privilegeType) throws AccessDeniedException {
        Table.TableType type = tbl.getType();
        switch (type) {
            case OLAP:
            case CLOUD_NATIVE:
            case MYSQL:
            case ELASTICSEARCH:
            case HIVE:
            case HIVE_VIEW:
            case ICEBERG:
            case HUDI:
            case JDBC:
            case DELTALAKE:
            case FILE:
            case SCHEMA:
            case PAIMON:
            case ODPS:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnTable(currentUser, roleIds, new TableName(tbl.getCatalogName(), dbName, tbl.getName()));
                } else {
                    checkTableAction(currentUser, roleIds, dbName, tbl.getName(), privilegeType);
                }
                break;
            case MATERIALIZED_VIEW:
            case CLOUD_NATIVE_MATERIALIZED_VIEW:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnMaterializedView(currentUser, roleIds, new TableName(dbName, tbl.getName()));
                } else {
                    checkMaterializedViewAction(currentUser, roleIds, new TableName(dbName, tbl.getName()),
                            privilegeType);
                }
                break;
            case VIEW:
                // `privilegeType == null` meaning we don't check specified action, just any action
                if (privilegeType == null) {
                    checkAnyActionOnView(currentUser, roleIds, new TableName(dbName, tbl.getName()));
                } else {
                    checkViewAction(currentUser, roleIds, new TableName(dbName, tbl.getName()), privilegeType);
                }
                break;
            default:
                throw new AccessDeniedException();
        }
    }

    public static void checkActionForAnalyzeStatement(UserIdentity userIdentity, Set<Long> currentRoleIds,
                                                      TableName tableName) {
        try {
            Authorizer.checkActionOnTableLikeObject(userIdentity, currentRoleIds,
                    tableName, PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    tableName.getCatalog(),
                    userIdentity, currentRoleIds,
                    PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName.getTbl());
        }
        Optional<Table> table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName);
        if (table.isPresent() && table.get().isTable()) {
            try {
                Authorizer.checkActionOnTableLikeObject(userIdentity, currentRoleIds,
                        tableName, PrivilegeType.INSERT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        tableName.getCatalog(),
                        userIdentity, currentRoleIds,
                        PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName.getTbl());
            }
        }
    }

    public static void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database,
                                           Function function, PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkFunctionAction(currentUser, roleIds, database, function, privilegeType);
    }

    public static void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnFunction(currentUser, roleIds, database, function);
    }

    public static void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                                 PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkGlobalFunctionAction(currentUser, roleIds, function, privilegeType);
    }

    public static void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnGlobalFunction(currentUser, roleIds, function);
    }

    public static void checkActionInDb(UserIdentity currentUser, Set<Long> roleIds, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkActionInDb(currentUser, roleIds, db, privilegeType);
    }

    /**
     * A lambda function that throws AccessDeniedException
     */
    @FunctionalInterface
    public interface AccessControlChecker {
        void check() throws AccessDeniedException;
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static void checkAnyActionOnOrInDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        Preconditions.checkNotNull(db, "db should not null");
        AccessController controller = getInstance().getAccessControlOrDefault(catalogName);

        List<AccessControlChecker> basicCheckers = ImmutableList.of(
                () -> controller.checkAnyActionOnDb(currentUser, roleIds, catalogName, db),
                () -> controller.checkAnyActionOnAnyTable(currentUser, roleIds, catalogName, db),
                () -> controller.checkAnyActionOnAnyView(currentUser, roleIds, catalogName, db)
        );
        List<AccessControlChecker> extraCheckers = ImmutableList.of(
                () -> controller.checkAnyActionOnAnyMaterializedView(currentUser, roleIds, db),
                () -> controller.checkAnyActionOnAnyFunction(currentUser, roleIds, db),
                () -> controller.checkAnyActionOnPipe(currentUser, roleIds, new PipeName("*", "*"))
        );
        List<AccessControlChecker> appliedCheckers = CatalogMgr.isInternalCatalog(catalogName) ?
                ListUtils.union(basicCheckers, extraCheckers) : basicCheckers;

        AccessDeniedException lastExcepton = null;
        for (AccessControlChecker checker : appliedCheckers) {
            try {
                checker.check();
                return;
            } catch (AccessDeniedException e) {
                lastExcepton = e;
            }
        }
        if (lastExcepton != null) {
            throw lastExcepton;
        }
    }

    public static void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                           PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnResource(currentUser, roleIds, name);
    }

    public static void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceGroupAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkPipeAction(UserIdentity currentUser, Set<Long> roleIds, PipeName name,
                                       PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkPipeAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkAnyActionOnPipe(UserIdentity currentUser, Set<Long> roleIds, PipeName name)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnPipe(currentUser, roleIds, name);
    }

    public static void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkStorageVolumeAction(currentUser, roleIds, storageVolume, privilegeType);
    }

    public static void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume)
            throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnStorageVolume(currentUser, roleIds, storageVolume);
    }

    public static void withGrantOption(UserIdentity currentUser, Set<Long> roleIds, ObjectType type, List<PrivilegeType> wants,
                                       List<PEntryObject> objects) throws AccessDeniedException {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME).
                withGrantOption(currentUser, roleIds, type, wants, objects);
    }

    public static Map<String, Expr> getColumnMaskingPolicy(ConnectContext currentUser, TableName tableName,
                                                           List<Column> columns) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        return getInstance().getAccessControlOrDefault(catalog)
                .getColumnMaskingPolicy(currentUser, tableName, columns);
    }

    public static Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        return getInstance().getAccessControlOrDefault(catalog).getRowAccessPolicy(currentUser, tableName);
    }
}
