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

package com.starrocks.authorization.opa;

import com.google.common.collect.Maps;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpaAccessController extends ExternalAccessController implements AutoCloseable {
    private final OpaPolicyClient opaClient;
    private final boolean allowPermissionManagementOperations;

    public OpaAccessController() {
        this(new OpaHttpClient(), Config.opa_allow_permission_management_operations);
    }

    OpaAccessController(OpaPolicyClient opaClient, boolean allowPermissionManagementOperations) {
        this.opaClient = opaClient;
        this.allowPermissionManagementOperations = allowPermissionManagementOperations;
    }

    @Override
    public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
            throws AccessDeniedException {
        if (isPermissionManagementOperation(privilegeType)) {
            checkPermissionManagementOperation();
            return;
        }
        check(context, privilegeType, ObjectType.SYSTEM, OpaResource.system());
    }

    @Override
    public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.USER, OpaResource.user(impersonateUser.getUser()));
    }

    @Override
    public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.CATALOG, OpaResource.catalog(catalogName));
    }

    @Override
    public void checkAnyActionOnCatalog(ConnectContext context, String catalogName)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.CATALOG, OpaResource.catalog(catalogName));
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.DATABASE, OpaResource.database(catalogName, db));
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.DATABASE, OpaResource.database(catalogName, db));
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.TABLE, OpaResource.table(tableName));
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.TABLE, OpaResource.table(tableName));
    }

    @Override
    public void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.TABLE, OpaResource.table(catalog, db, "*"));
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.COLUMN, OpaResource.column(tableName, column));
    }

    @Override
    public void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.VIEW, OpaResource.view(defaultInternalCatalog(tableName)));
    }

    @Override
    public void checkAnyActionOnView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.VIEW, OpaResource.view(defaultInternalCatalog(tableName)));
    }

    @Override
    public void checkAnyActionOnAnyView(ConnectContext context, String db)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.VIEW,
                OpaResource.view(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, "*")));
    }

    @Override
    public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.MATERIALIZED_VIEW,
                OpaResource.materializedView(defaultInternalCatalog(tableName)));
    }

    @Override
    public void checkAnyActionOnMaterializedView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.MATERIALIZED_VIEW,
                OpaResource.materializedView(defaultInternalCatalog(tableName)));
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(ConnectContext context, String db)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.MATERIALIZED_VIEW,
                OpaResource.materializedView(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db, "*")));
    }

    @Override
    public void checkFunctionAction(ConnectContext context, Database database, Function function,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.FUNCTION, OpaResource.function(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database.getFullName(), function.getSignature()));
    }

    @Override
    public void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.FUNCTION,
                OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database, function.getSignature()));
    }

    @Override
    public void checkAnyActionOnAnyFunction(ConnectContext context, String database)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.FUNCTION,
                OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database, "*"));
    }

    @Override
    public void checkGlobalFunctionAction(ConnectContext context, Function function,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.GLOBAL_FUNCTION, OpaResource.globalFunction(function.getSignature()));
    }

    @Override
    public void checkAnyActionOnGlobalFunction(ConnectContext context, Function function)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.GLOBAL_FUNCTION, OpaResource.globalFunction(function.getSignature()));
    }

    @Override
    public void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, OpaRequest.OPERATION_CHECK_ACTION_IN_DB, privilegeType, ObjectType.DATABASE,
                OpaResource.database(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db));
    }

    @Override
    public void checkResourceAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.RESOURCE, OpaResource.resource(name));
    }

    @Override
    public void checkAnyActionOnResource(ConnectContext context, String name) throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.RESOURCE, OpaResource.resource(name));
    }

    @Override
    public void checkResourceGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.RESOURCE_GROUP, OpaResource.resourceGroup(name));
    }

    @Override
    public void checkPipeAction(ConnectContext context, PipeName name,
                                PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.PIPE, OpaResource.pipe(name.getDbName(), name.getPipeName()));
    }

    @Override
    public void checkAnyActionOnPipe(ConnectContext context, PipeName name)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.PIPE, OpaResource.pipe(name.getDbName(), name.getPipeName()));
    }

    @Override
    public void checkStorageVolumeAction(ConnectContext context, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        check(context, privilegeType, ObjectType.STORAGE_VOLUME, OpaResource.storageVolume(storageVolume));
    }

    @Override
    public void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume)
            throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.STORAGE_VOLUME, OpaResource.storageVolume(storageVolume));
    }

    @Override
    public void withGrantOption(ConnectContext context, ObjectType type, List<PrivilegeType> wants,
                                List<PEntryObject> objects) throws AccessDeniedException {
        checkPermissionManagementOperation();
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, String> columnToExpression = getColumnMaskingExpressions(context, tableName, columns);
        Map<String, Expr> columnToMask = Maps.newHashMap();
        for (Map.Entry<String, String> entry : columnToExpression.entrySet()) {
            columnToMask.put(entry.getKey(), parsePolicyExpression(entry.getValue(), context));
        }
        return columnToMask;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        List<String> filters = opaClient.getRowFilters(
                OpaRequest.create(context, OpaRequest.OPERATION_GET_ROW_FILTERS, PrivilegeType.SELECT,
                        ObjectType.TABLE, OpaResource.table(tableName))).stream()
                .filter(StringUtils::isNotBlank)
                .toList();
        if (filters.isEmpty()) {
            return null;
        }
        String filter = filters.stream()
                .map(expr -> "(" + expr + ")")
                .collect(Collectors.joining(" AND "));
        return parsePolicyExpression(filter, context);
    }

    @Override
    public void checkWarehouseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        check(context, privilegeType, ObjectType.WAREHOUSE, OpaResource.warehouse(name));
    }

    @Override
    public void checkAnyActionOnWarehouse(ConnectContext context, String name) throws AccessDeniedException {
        check(context, PrivilegeType.ANY, ObjectType.WAREHOUSE, OpaResource.warehouse(name));
    }

    @Override
    public void close() {
        opaClient.close();
    }

    private Map<String, String> getColumnMaskingExpressions(ConnectContext context, TableName tableName,
                                                            List<Column> columns) {
        List<String> columnNames = columns.stream().map(Column::getName).toList();
        if (opaClient.supportsBatchColumnMasks()) {
            List<OpaResource> resources = columnNames.stream()
                    .map(column -> OpaResource.column(tableName, column))
                    .toList();
            return opaClient.getBatchColumnMasks(OpaRequest.createBatchColumnMasks(context, tableName, resources),
                    columnNames);
        }

        Map<String, String> columnToExpression = Maps.newHashMap();
        for (Column column : columns) {
            Optional<String> expression = opaClient.getColumnMask(
                    OpaRequest.create(context, OpaRequest.OPERATION_GET_COLUMN_MASK, PrivilegeType.SELECT,
                            ObjectType.COLUMN, OpaResource.column(tableName, column.getName())));
            expression.ifPresent(mask -> columnToExpression.put(column.getName(), mask));
        }
        return columnToExpression;
    }

    private void check(ConnectContext context, PrivilegeType privilegeType, ObjectType objectType, OpaResource resource)
            throws AccessDeniedException {
        check(context, OpaRequest.OPERATION_CHECK, privilegeType, objectType, resource);
    }

    private void check(ConnectContext context, String operation, PrivilegeType privilegeType, ObjectType objectType,
                       OpaResource resource) throws AccessDeniedException {
        if (!opaClient.checkPermission(OpaRequest.create(context, operation, privilegeType, objectType, resource))) {
            throw new AccessDeniedException();
        }
    }

    private boolean isPermissionManagementOperation(PrivilegeType privilegeType) {
        return PrivilegeType.GRANT.equals(privilegeType);
    }

    private void checkPermissionManagementOperation() throws AccessDeniedException {
        if (!allowPermissionManagementOperations) {
            throw new AccessDeniedException();
        }
    }

    private Expr parsePolicyExpression(String expression, ConnectContext context) {
        try {
            return SqlParser.parseSqlToExpr(expression, context.getSessionVariable().getSqlMode());
        } catch (RuntimeException e) {
            throw new OpaQueryException("invalid OPA policy expression: " + expression, e);
        }
    }

    private TableName defaultInternalCatalog(TableName tableName) {
        if (tableName.getCatalog() != null) {
            return tableName;
        }
        return new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, tableName.getDb(), tableName.getTbl());
    }
}
