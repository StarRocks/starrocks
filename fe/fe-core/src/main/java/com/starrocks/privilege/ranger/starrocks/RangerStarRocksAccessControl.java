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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.AccessControl;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerStarRocksAccessControl implements AccessControl {
    private static RangerBasePlugin rangerPlugin = null;

    public RangerStarRocksAccessControl() {
        rangerPlugin = new RangerBasePlugin("starrocks", "starrocks");
        rangerPlugin.init(); // this will initialize policy engine and policy refresher
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkSystemAction(UserIdentity currentUser, Set<Long> roleIds, PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.SYSTEM, null);
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.SYSTEM, null);
        }
    }

    @Override
    public void checkUserAction(UserIdentity currentUser, Set<Long> roleIds, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.USER,
                Lists.newArrayList(impersonateUser.getUser()));
        hasPermission(resource, currentUser, privilegeType);
    }

    @Override
    public void checkCatalogAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.CATALOG, Lists.newArrayList(catalogName));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkAnyActionOnCatalog(UserIdentity currentUser, Set<Long> roleIds, String catalogName) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.CATALOG, Lists.newArrayList(catalogName));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.CATALOG, catalogName);
        }
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.DATABASE, Lists.newArrayList(catalogName, db));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.DATABASE,
                Lists.newArrayList(catalogName, db));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                Lists.newArrayList(catalog, tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyTable(UserIdentity currentUser, Set<Long> roleIds, String catalog, String db) {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalog, db);
        for (Table table : database.getTables()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.TABLE,
                    Lists.newArrayList(catalog, database.getFullName(), table.getName()));
            if (hasPermission(resource, currentUser, PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, db);
    }

    @Override
    public void checkViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getViews()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                    Lists.newArrayList(database.getFullName(), table.getName()));
            if (hasPermission(resource, currentUser, PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.VIEW, db);
    }

    @Override
    public void checkMaterializedViewAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                            PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.MATERIALIZED_VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnMaterializedView(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.MATERIALIZED_VIEW,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(),
                    ObjectType.MATERIALIZED_VIEW, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Table table : database.getMaterializedViews()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.VIEW,
                    Lists.newArrayList(database.getFullName(), table.getName()));
            if (hasPermission(resource, currentUser, PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.MATERIALIZED_VIEW, db);
    }

    @Override
    public void checkFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Database database, Function function,
                                    PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                Lists.newArrayList(database.getFullName(), function.getSignature()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnFunction(UserIdentity currentUser, Set<Long> roleIds, String database, Function function) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                Lists.newArrayList(database, function.getSignature()));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.FUNCTION, function.getSignature());
        }
    }

    @Override
    public void checkAnyActionOnAnyFunction(UserIdentity currentUser, Set<Long> roleIds, String db) {
        Database database = GlobalStateMgr.getServingState().getDb(db);
        for (Function function : database.getFunctions()) {
            RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.FUNCTION,
                    Lists.newArrayList(database.getFullName(), function.getSignature()));
            if (hasPermission(resource, currentUser, PrivilegeType.ANY)) {
                return;
            }
        }
        AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.FUNCTION, db);
    }

    @Override
    public void checkGlobalFunctionAction(UserIdentity currentUser, Set<Long> roleIds, Function function,
                                          PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.GLOBAL_FUNCTION,
                Lists.newArrayList(function.getSignature()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
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
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE, Lists.newArrayList(name));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkAnyActionOnResource(UserIdentity currentUser, Set<Long> roleIds, String name) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE, Lists.newArrayList(name));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.RESOURCE, name);
        }
    }

    @Override
    public void checkResourceGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.RESOURCE_GROUP, Lists.newArrayList(name));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.RESOURCE_GROUP, name);
        }
    }

    @Override
    public void checkStorageVolumeAction(UserIdentity currentUser, Set<Long> roleIds, String storageVolume,
                                         PrivilegeType privilegeType) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.STORAGE_VOLUME,
                Lists.newArrayList(storageVolume));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public void checkAnyActionOnStorageVolume(UserIdentity currentUser, Set<Long> roleIds, String storageVolume) {
        RangerStarRocksResource resource = new RangerStarRocksResource(ObjectType.STORAGE_VOLUME,
                Lists.newArrayList(storageVolume));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.STORAGE_VOLUME, storageVolume);
        }
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                    new RangerStarRocksResource(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), column.getName()),
                    context.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));

            RangerAccessResult result = rangerPlugin.evalDataMaskPolicies(request, null);
            if (result.isMaskEnabled()) {
                String maskType = result.getMaskType();
                RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
                String transformer = null;

                if (maskTypeDef != null) {
                    transformer = maskTypeDef.getTransformer();
                }

                if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
                    transformer = "NULL";
                } else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
                    String maskedValue = result.getMaskedValue();

                    if (maskedValue == null) {
                        transformer = "NULL";
                    } else {
                        transformer = maskedValue;
                    }
                }

                if (StringUtils.isNotEmpty(transformer)) {
                    transformer = transformer.replace("{col}", column.getName())
                            .replace("{type}", column.getType().toSql());
                }

                maskingExprMap.put(column.getName(),
                        SqlParser.parseSqlToExpr(transformer, context.getSessionVariable().getSqlMode()));
            }
        }
        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                new RangerStarRocksResource(ObjectType.TABLE,
                        Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                currentUser.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));
        RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
        if (result != null && result.isRowFilterEnabled()) {
            return SqlParser.parseSqlToExpr(result.getFilterExpr(), currentUser.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }

    private boolean hasPermission(RangerStarRocksResource resource, UserIdentity user, PrivilegeType privilegeType) {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = privilegeType.name().toLowerCase(ENGLISH);
        }

        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(resource, user, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        if (result != null && result.getIsAllowed()) {
            return true;
        } else {
            return false;
        }
    }
}