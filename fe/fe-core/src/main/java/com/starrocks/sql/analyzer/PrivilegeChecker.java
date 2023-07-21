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
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.SystemAccessControl;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public class PrivilegeChecker {
    private static final PrivilegeChecker INSTANCE = new PrivilegeChecker();

    public static PrivilegeChecker getInstance() {
        return INSTANCE;
    }

    private final PrivilegeCheckerVisitor privilegeCheckerVisitor;
    private final SystemAccessControl systemAccessControl;

    private PrivilegeChecker() {
        this.systemAccessControl = new PrivilegeActions();
        this.privilegeCheckerVisitor = new PrivilegeCheckerVisitor(this.systemAccessControl);
    }

    public static void check(StatementBase statement, ConnectContext context) {
        getInstance().privilegeCheckerVisitor.check(statement, context);
    }

    public static void checkSystemAction(UserIdentity userIdentity, Set<Long> roleIds, PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkSystemAction(userIdentity, roleIds, privilegeType);
    }

    public static void checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, String db, String table,
                                        PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkTableAction(userIdentity, roleIds,
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, table), privilegeType);
    }

    public static void checkTableAction(UserIdentity userIdentity, Set<Long> roleIds, String catalog, String db, String table,
                                        PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkTableAction(userIdentity, roleIds, new TableName(catalog, db, table),
                privilegeType);
    }

    public static void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                                   PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkMaterializedViewAction(currentUser, roleIds, tableName, privilegeType);
    }

    public static void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        getInstance().systemAccessControl.checkAnyActionOnTable(currentUser, roleIds, tableName);
    }

    public static void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        getInstance().systemAccessControl.checkAnyActionOnView(currentUser, roleIds, tableName);
    }

    public static void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        getInstance().systemAccessControl.checkAnyActionOnMaterializedView(currentUser, roleIds, tableName);
    }

    public static void checkAnyActionOnTableLikeObject(UserIdentity currentUser, Set<Long> roleIds, String dbName, Table tbl) {
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
                throw new AccessDeniedException(ErrorReport.reportCommon(null, ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ANY ON TABLE/VIEW/MV OBJECT"));
        }
    }

    public static void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                           PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkFunctionAction(currentUser, roleIds, database, function, privilegeType);
    }

    public static void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, long databaseId, long functionSig) {
        getInstance().systemAccessControl.checkAnyActionOnFunction(currentUser, roleIds, databaseId, functionSig);
    }

    public static void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                                 PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkGlobalFunctionAction(currentUser, roleIds, function, privilegeType);
    }

    public static void checkAnyActionOnGlobalFunction(UserIdentity currentUser, Set<Long> roleIds, long functionSig) {
        getInstance().systemAccessControl.checkAnyActionOnGlobalFunction(currentUser, roleIds, functionSig);
    }

    public static void checkActionInDb(UserIdentity currentUser, Set<Long> roleIds, String db, PrivilegeType privilegeType) {
        getInstance().systemAccessControl.checkActionInDb(currentUser, roleIds, db, privilegeType);
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static void checkAnyActionOnOrInDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        Preconditions.checkNotNull(db, "db should not null");

        try {
            getInstance().systemAccessControl.checkAnyActionOnDb(currentUser, roleIds, catalogName, db);
        } catch (AccessDeniedException e1) {
            try {
                getInstance().systemAccessControl.checkAnyActionOnTable(currentUser, roleIds,
                        new TableName(catalogName, db, "*"));
            } catch (AccessDeniedException e2) {
                if (CatalogMgr.isInternalCatalog(catalogName)) {
                    Database database = GlobalStateMgr.getCurrentState().getDb(db);
                    if (database == null) {
                        return;
                    }
                    try {
                        checkAnyActionOnView(currentUser, roleIds, new TableName(db, "*"));
                    } catch (AccessDeniedException e3) {
                        try {
                            checkAnyActionOnMaterializedView(currentUser, roleIds, new TableName(db, "*"));
                        } catch (AccessDeniedException e4) {
                            checkAnyActionOnFunction(currentUser, roleIds, database.getId(),
                                    PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID);
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
            getInstance().systemAccessControl.checkAnyActionOnCatalog(userIdentity, roleIds, catalogName);
        } catch (AccessDeniedException e1) {
            try {
                getInstance().systemAccessControl.checkAnyActionOnDb(userIdentity, roleIds, catalogName, "*");
            } catch (AccessDeniedException e2) {
                try {
                    getInstance().systemAccessControl.checkAnyActionOnTable(userIdentity, roleIds,
                            new TableName(catalogName, "*", "*"));
                } catch (AccessDeniedException e3) {
                    if (CatalogMgr.isInternalCatalog(catalogName)) {
                        try {
                            getInstance().systemAccessControl.checkAnyActionOnView(userIdentity, roleIds,
                                    new TableName("*", "*"));
                        } catch (AccessDeniedException e4) {
                            try {
                                getInstance().systemAccessControl.checkAnyActionOnMaterializedView(userIdentity, roleIds,
                                        new TableName("*", "*"));
                            } catch (AccessDeniedException e5) {
                                getInstance().systemAccessControl.checkAnyActionOnFunction(userIdentity, roleIds,
                                        PrivilegeBuiltinConstants.ALL_DATABASE_ID,
                                        PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID);
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

    public static void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        getInstance().systemAccessControl.checkAnyActionOnResource(currentUser, roleIds, name);
    }

    public static void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String name) {
        getInstance().systemAccessControl.checkAnyActionOnStorageVolume(currentUser, roleIds, name);
    }
}
