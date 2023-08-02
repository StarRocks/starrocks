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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessControl;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public class Authorizer {
    private static final AccessControlProvider INSTANCE;

    static {
        INSTANCE = new AccessControlProvider(new AuthorizerStmtVisitor(), new NativeAccessControl());
    }

    public static AccessControlProvider getInstance() {
        return INSTANCE;
    }

    public static void check(StatementBase statement, ConnectContext context) {
        getInstance().getPrivilegeCheckerVisitor().check(statement, context);
    }

    public static void checkSystemAction(UserIdentity userIdentity, Set<Long> roleIds, PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkSystemAction(userIdentity, roleIds, privilegeType);
    }

    public static void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName,
                                          PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkCatalogAction(currentUser, roleIds, catalogName, privilegeType);
    }

    public static void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnCatalog(currentUser, roleIds, catalogName);
    }

    public static void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                                     PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(catalogName)
                .checkDbAction(currentUser, roleIds, catalogName, db, privilegeType);
    }

    public static void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnDb(currentUser, roleIds, catalogName, db);
    }

    public static void checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, String db, String table,
                                        PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkTableAction(userIdentity, roleIds,
                        new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, table), privilegeType);
    }

    public static void checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, String catalog, String db,
                                        String table, PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(catalog).checkTableAction(userIdentity, roleIds,
                new TableName(catalog, db, table), privilegeType);
    }

    public static void checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, TableName tableName,
                                        PrivilegeType privilegeType) {
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog).checkTableAction(userIdentity, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        String catalog = tableName.getCatalog();
        getInstance().getAccessControlOrDefault(catalog).checkAnyActionOnTable(currentUser, roleIds, tableName);
    }

    public static void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                       PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkViewAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnView(currentUser, roleIds, tableName);
    }

    public static void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                                   PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkMaterializedViewAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds,
                                                        TableName tableName) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnMaterializedView(currentUser, roleIds, tableName);
    }

    public static void checkAnyActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, String dbName,
                                                       Table tbl) {
        Table.TableType type = tbl.getType();
        switch (type) {
            case OLAP:
            case CLOUD_NATIVE:
            case MYSQL:
            case ELASTICSEARCH:
            case HIVE:
            case ICEBERG:
            case HUDI:
            case JDBC:
            case DELTALAKE:
            case FILE:
            case SCHEMA:
            case PAIMON:
                checkAnyActionOnTable(currentUser, roleIds, new TableName(dbName, tbl.getName()));
                break;
            case MATERIALIZED_VIEW:
            case CLOUD_NATIVE_MATERIALIZED_VIEW:
                checkAnyActionOnMaterializedView(currentUser, roleIds, new TableName(dbName, tbl.getName()));
                break;
            case VIEW:
                checkAnyActionOnView(currentUser, roleIds, new TableName(dbName, tbl.getName()));
                break;
            default:
                throw new AccessDeniedException(
                        ErrorReport.reportCommon(null, ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                "ANY ON TABLE/VIEW/MV OBJECT"));
        }
    }

    public static void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database,
                                           Function function,
                                           PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkFunctionAction(currentUser, roleIds, database, function, privilegeType);
    }

    public static void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database,
                                                Function function) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnFunction(currentUser, roleIds, database, function);
    }

    public static void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                                 PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkGlobalFunctionAction(currentUser, roleIds, function, privilegeType);
    }

    public static void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, Function function) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnGlobalFunction(currentUser, roleIds, function);
    }

    public static void checkActionInDb(UserIdentity currentUser, Set<Long> roleIds, String db,
                                       PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkActionInDb(currentUser, roleIds, db, privilegeType);
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static void checkAnyActionOnOrInDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName,
                                              String db) {
        Preconditions.checkNotNull(db, "db should not null");

        try {
            getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnDb(currentUser, roleIds, catalogName, db);
        } catch (AccessDeniedException e1) {
            try {
                getInstance().getAccessControlOrDefault(catalogName)
                        .checkAnyActionOnAnyTable(currentUser, roleIds, catalogName, db);
            } catch (AccessDeniedException e2) {
                if (CatalogMgr.isInternalCatalog(catalogName)) {
                    try {
                        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .checkAnyActionOnAnyView(currentUser, roleIds, db);
                    } catch (AccessDeniedException e3) {
                        try {
                            getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                    .checkAnyActionOnAnyMaterializedView(currentUser, roleIds, db);
                        } catch (AccessDeniedException e4) {
                            getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                    .checkAnyActionOnAnyFunction(currentUser, roleIds, db);
                        }
                    }
                } else {
                    throw new AccessDeniedException(ErrorReport.reportCommon(null,
                            ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ANY IN DATABASE " + db));
                }
            }
        }
    }

    public static void checkAnyActionOnOrInCatalog(UserIdentity userIdentity, Set<Long> roleIds, String catalogName) {
        try {
            getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnCatalog(userIdentity, roleIds, catalogName);
        } catch (AccessDeniedException e1) {
            try {
                getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnDb(userIdentity, roleIds, catalogName, "*");
            } catch (AccessDeniedException e2) {
                try {
                    getInstance().getAccessControlOrDefault(catalogName).checkAnyActionOnAnyTable(userIdentity, roleIds,
                            catalogName, "*");
                } catch (AccessDeniedException e3) {
                    if (CatalogMgr.isInternalCatalog(catalogName)) {
                        try {
                            getInstance().getAccessControlOrDefault(catalogName)
                                    .checkAnyActionOnAnyView(userIdentity, roleIds, "*");
                        } catch (AccessDeniedException e4) {
                            try {
                                getInstance().getAccessControlOrDefault(catalogName)
                                        .checkAnyActionOnAnyMaterializedView(userIdentity, roleIds, "*");
                            } catch (AccessDeniedException e5) {
                                getInstance().getAccessControlOrDefault(catalogName)
                                        .checkAnyActionOnAnyFunction(userIdentity, roleIds, null);
                            }
                        }
                    } else {
                        throw new AccessDeniedException(ErrorReport.reportCommon(null,
                                ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ANY IN CATALOG " + catalogName));
                    }
                }
            }
        }
    }

    public static void checkResourceAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                           PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnResource(currentUser, roleIds, name);
    }

    public static void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                                PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkResourceGroupAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                                PrivilegeType privilegeType) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkStorageVolumeAction(currentUser, roleIds, storageVolume, privilegeType);
    }

    public static void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String name) {
        getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                .checkAnyActionOnStorageVolume(currentUser, roleIds, name);
    }
}
