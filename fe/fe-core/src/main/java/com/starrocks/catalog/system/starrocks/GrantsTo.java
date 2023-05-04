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
package com.starrocks.catalog.system.starrocks;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.privilege.ActionSet;
import com.starrocks.privilege.AuthorizationManager;
import com.starrocks.privilege.CatalogPEntryObject;
import com.starrocks.privilege.DbPEntryObject;
import com.starrocks.privilege.FunctionPEntryObject;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeCollection;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ResourceGroupPEntryObject;
import com.starrocks.privilege.ResourcePEntryObject;
import com.starrocks.privilege.TablePEntryObject;
import com.starrocks.privilege.UserPEntryObject;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetGrantsToRolesOrUserItem;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGetGrantsToRolesOrUserResponse;
import com.starrocks.thrift.TGrantsToType;
import com.starrocks.thrift.TSchemaTableType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class GrantsTo {
    public static SystemTable createGrantsToRoles() {
        return new SystemTable(SystemId.GRANTS_TO_ROLES_ID, "grants_to_roles", Table.TableType.SCHEMA,
                builder()
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_GRANT_TO_ROLES);
    }

    public static SystemTable createGrantsToUsers() {
        return new SystemTable(SystemId.GRANTS_TO_USERS_ID, "grants_to_users", Table.TableType.SCHEMA,
                builder()
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_GRANT_TO_USERS);
    }

    public static TGetGrantsToRolesOrUserResponse getGrantsTo(TGetGrantsToRolesOrUserRequest request) {
        AuthorizationManager authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationManager();
        TGetGrantsToRolesOrUserResponse tGetGrantsToRolesOrUserResponse = new TGetGrantsToRolesOrUserResponse();
        if (request.getType().equals(TGrantsToType.USER)) {
            Set<UserIdentity> userIdentities = authorizationManager.getAllUserIdentities();
            for (UserIdentity userIdentity : userIdentities) {
                Map<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> privileges =
                        authorizationManager.getMergedTypeToPrivilegeEntryListByUser(userIdentity);
                Set<TGetGrantsToRolesOrUserItem> items =
                        getGrantItems(authorizationManager, userIdentity.toString(), privileges);
                items.forEach(tGetGrantsToRolesOrUserResponse::addToGrants_to);
            }
        } else {
            List<String> roles = authorizationManager.getAllRoles();
            for (String grantee : roles) {
                Map<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> privileges =
                        authorizationManager.getTypeToPrivilegeEntryListByRole(grantee);
                Set<TGetGrantsToRolesOrUserItem> items = getGrantItems(authorizationManager, grantee, privileges);
                items.forEach(tGetGrantsToRolesOrUserResponse::addToGrants_to);
            }
        }

        return tGetGrantsToRolesOrUserResponse;
    }

    private static Set<TGetGrantsToRolesOrUserItem> getGrantItems(
            AuthorizationManager authorizationManager, String grantee,
            Map<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> privileges) {

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Set<TGetGrantsToRolesOrUserItem> items = new HashSet<>();
        for (Map.Entry<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> privEntryMaps : privileges.entrySet()) {
            for (PrivilegeCollection.PrivilegeEntry privilegeEntry : privEntryMaps.getValue()) {
                Set<List<String>> objects = new HashSet<>();
                switch (privEntryMaps.getKey()) {
                    case CATALOG: {
                        CatalogPEntryObject catalogPEntryObject = (CatalogPEntryObject) privilegeEntry.getObject();
                        if (catalogPEntryObject.getId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                            objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, null, null));
                            Map<String, Catalog> catalogMap = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs();
                            for (Catalog catalog : catalogMap.values()) {
                                objects.add(Lists.newArrayList(catalog.getName(), null, null));
                            }
                        } else {
                            String catalogName = getCatalogName(catalogPEntryObject.getId());
                            if (catalogName == null) {
                                continue;
                            }
                            objects.add(Lists.newArrayList(catalogName, null, null));
                        }
                        break;
                    }

                    case DATABASE: {
                        DbPEntryObject dbPEntryObject = (DbPEntryObject) privilegeEntry.getObject();
                        if (dbPEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                            List<String> catalogs = new ArrayList<>(GlobalStateMgr.getCurrentState().getCatalogMgr()
                                    .getCatalogs().keySet());
                            catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

                            for (String catalogName : catalogs) {
                                for (String dbName : metadataMgr.listDbNames(catalogName)) {
                                    Database database = metadataMgr.getDb(catalogName, dbName);
                                    if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                        continue;
                                    }
                                    objects.add(Lists.newArrayList(catalogName, dbName, null));
                                }
                            }
                        } else {
                            String catalogName = getCatalogName(dbPEntryObject.getCatalogId());
                            if (catalogName == null) {
                                continue;
                            }

                            if (dbPEntryObject.getUUID().equalsIgnoreCase(PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                                List<String> dbNames = metadataMgr.listDbNames(catalogName);
                                for (String dbName : dbNames) {
                                    Database database = metadataMgr.getDb(catalogName, dbName);
                                    if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                        continue;
                                    }
                                    objects.add(Lists.newArrayList(catalogName, database.getFullName(), null));
                                }
                            } else {
                                Database database;
                                if (CatalogMgr.isInternalCatalog(catalogName)) {
                                    database = GlobalStateMgr.getCurrentState().getDb(Long.parseLong(dbPEntryObject.getUUID()));

                                } else {
                                    String dbName = ExternalCatalog.getDbNameFromUUID(dbPEntryObject.getUUID());
                                    database = metadataMgr.getDb(catalogName, dbName);
                                }
                                if (database == null) {
                                    continue;
                                }
                                if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                    continue;
                                }

                                objects.add(Lists.newArrayList(catalogName, database.getFullName(), null));
                            }
                        }
                        break;
                    }

                    case TABLE:
                    case VIEW:
                    case MATERIALIZED_VIEW: {
                        TablePEntryObject tablePEntryObject = (TablePEntryObject) privilegeEntry.getObject();
                        if (tablePEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                            List<String> catalogs = new ArrayList<>(GlobalStateMgr.getCurrentState().getCatalogMgr()
                                    .getCatalogs().keySet());
                            catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

                            for (String catalogName : catalogs) {
                                for (String dbName : metadataMgr.listDbNames(catalogName)) {
                                    Database database = metadataMgr.getDb(catalogName, dbName);
                                    if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                        continue;
                                    }
                                    for (Table table : database.getTables()) {
                                        if (privEntryMaps.getKey().equals(ObjectType.VIEW) && !(table instanceof View)) {
                                            continue;
                                        } else if (privEntryMaps.getKey().equals(ObjectType.MATERIALIZED_VIEW)
                                                && !table.isMaterializedView()) {
                                            continue;
                                        }

                                        objects.add(Lists.newArrayList(catalogName, dbName, table.getName()));
                                    }
                                }
                            }
                        } else {
                            String catalogName = getCatalogName(tablePEntryObject.getCatalogId());
                            if (catalogName == null) {
                                continue;
                            }

                            if (tablePEntryObject.getDatabaseUUID().equalsIgnoreCase(
                                    PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                                List<String> dbNames = metadataMgr.listDbNames(catalogName);
                                for (String dbName : dbNames) {
                                    Database database = metadataMgr.getDb(catalogName, dbName);
                                    if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                        continue;
                                    }
                                    for (Table table : database.getTables()) {
                                        if (privEntryMaps.getKey().equals(ObjectType.VIEW) && !(table instanceof View)) {
                                            continue;
                                        } else if (privEntryMaps.getKey().equals(ObjectType.MATERIALIZED_VIEW)
                                                && !table.isMaterializedView()) {
                                            continue;
                                        }

                                        objects.add(Lists.newArrayList(catalogName, database.getFullName(), table.getName()));
                                    }
                                }
                            } else {
                                Database database;
                                if (CatalogMgr.isInternalCatalog(tablePEntryObject.getCatalogId())) {
                                    database = GlobalStateMgr.getCurrentState()
                                            .getDb(Long.parseLong(tablePEntryObject.getDatabaseUUID()));
                                } else {
                                    String dbName = ExternalCatalog.getDbNameFromUUID(tablePEntryObject.getDatabaseUUID());
                                    database = metadataMgr.getDb(catalogName, dbName);
                                }
                                if (database == null) {
                                    continue;
                                }

                                if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                    continue;
                                }

                                if (tablePEntryObject.getTableUUID().equalsIgnoreCase(
                                        PrivilegeBuiltinConstants.ALL_TABLES_UUID)) {
                                    for (Table table : database.getTables()) {
                                        if (privEntryMaps.getKey().equals(ObjectType.VIEW) && !(table instanceof View)) {
                                            continue;
                                        } else if (privEntryMaps.getKey().equals(ObjectType.MATERIALIZED_VIEW)
                                                && !table.isMaterializedView()) {
                                            continue;
                                        }
                                        objects.add(Lists.newArrayList(catalogName, database.getFullName(), table.getName()));
                                    }
                                } else {
                                    Table table = database.getTable(Long.parseLong(tablePEntryObject.getTableUUID()));
                                    objects.add(Lists.newArrayList(catalogName, database.getFullName(), table.getName()));
                                }
                            }
                        }
                        break;
                    }
                    case USER: {
                        UserPEntryObject tablePEntryObject = (UserPEntryObject) privilegeEntry.getObject();
                        UserIdentity userIdentity = tablePEntryObject.getUserIdentity();
                        if (userIdentity == null) {
                            List<String> allUsers = authorizationManager.getAllUsers();
                            for (String user : allUsers) {
                                objects.add(Lists.newArrayList(null, null, user));
                            }
                        } else {
                            objects.add(Lists.newArrayList(null, null, userIdentity.toString()));
                        }
                        break;
                    }

                    case RESOURCE: {
                        ResourcePEntryObject resourcePEntryObject = (ResourcePEntryObject) privilegeEntry.getObject();
                        String resourceName = resourcePEntryObject.getName();
                        if (resourceName == null) {
                            Set<String> allResources = GlobalStateMgr.getCurrentState().getResourceMgr().getAllResourceName();
                            for (String resource : allResources) {
                                objects.add(Lists.newArrayList(null, null, resource));
                            }
                        } else {
                            objects.add(Lists.newArrayList(null, null, resourceName));
                        }
                        break;
                    }

                    case RESOURCE_GROUP: {
                        ResourceGroupPEntryObject resourceGroupPEntryObject =
                                (ResourceGroupPEntryObject) privilegeEntry.getObject();
                        long resourceGroupId = resourceGroupPEntryObject.getId();
                        if (resourceGroupId == PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID) {
                            Set<String> allResourceGroupNames = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
                                    .getAllResourceGroupNames();
                            for (String resource : allResourceGroupNames) {
                                objects.add(Lists.newArrayList(null, null, resource));
                            }
                        } else {
                            ResourceGroup resourceGroup =
                                    GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroupId);
                            objects.add(Lists.newArrayList(null, null, resourceGroup.getName()));
                        }
                        break;
                    }
                    case FUNCTION: {
                        FunctionPEntryObject functionPEntryObject = (FunctionPEntryObject) privilegeEntry.getObject();
                        long databaseId = functionPEntryObject.getDatabaseId();
                        if (databaseId == PrivilegeBuiltinConstants.ALL_DATABASE_ID) {
                            List<String> dbNames = metadataMgr.listDbNames(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                            for (String dbName : dbNames) {
                                Database database = GlobalStateMgr.getCurrentState().getDb(dbName);
                                if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                    continue;
                                }
                                List<Function> functions = database.getFunctions();
                                for (Function function : functions) {
                                    objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                            database.getFullName(), function.signatureString()));
                                }
                            }
                        } else {
                            Database database = GlobalStateMgr.getCurrentState().getDb(databaseId);
                            if (functionPEntryObject.getFunctionId().equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
                                List<Function> functions = database.getFunctions();
                                for (Function function : functions) {
                                    objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                            database.getFullName(), function.signatureString()));
                                }
                            } else {
                                for (Function f : database.getFunctions()) {
                                    if (f.getFunctionId() == functionPEntryObject.getFunctionId()) {
                                        objects.add(Lists.newArrayList(null, null, f.signatureString()));
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                    case GLOBAL_FUNCTION: {
                        FunctionPEntryObject globalFunctionPEntryObject =
                                (FunctionPEntryObject) privilegeEntry.getObject();
                        GlobalFunctionMgr globalFunctionMgr = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr();

                        if (globalFunctionPEntryObject.getFunctionId() ==
                                PrivilegeBuiltinConstants.GLOBAL_FUNCTION_DEFAULT_DATABASE_ID) {
                            for (Function function : globalFunctionMgr.getFunctions()) {
                                objects.add(Lists.newArrayList(null, null, function.signatureString()));
                            }
                        } else {
                            for (Function f : globalFunctionMgr.getFunctions()) {
                                if (f.getFunctionId() == globalFunctionPEntryObject.getFunctionId()) {
                                    objects.add(Lists.newArrayList(null, null, f.signatureString()));
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    case SYSTEM: {
                        objects.add(Lists.newArrayList(null, null, null));
                        break;
                    }
                }

                ActionSet actionSet = privilegeEntry.getActionSet();
                List<PrivilegeType> privilegeTypes = authorizationManager.analyzeActionSet(privEntryMaps.getKey(), actionSet);

                for (PrivilegeType privilegeType : privilegeTypes) {
                    for (List<String> object : objects) {
                        TGetGrantsToRolesOrUserItem tGetGrantsToRolesOrUserItem = new TGetGrantsToRolesOrUserItem();
                        tGetGrantsToRolesOrUserItem.setGrantee(grantee);
                        tGetGrantsToRolesOrUserItem.setObject_catalog(object.get(0));
                        tGetGrantsToRolesOrUserItem.setObject_database(object.get(1));
                        tGetGrantsToRolesOrUserItem.setObject_name(object.get(2));
                        tGetGrantsToRolesOrUserItem.setObject_type(privEntryMaps.getKey().name().replace("_", " "));
                        tGetGrantsToRolesOrUserItem.setPrivilege_type(privilegeType.name().replace("_", " "));
                        tGetGrantsToRolesOrUserItem.setIs_grantable(privilegeEntry.isWithGrantOption());

                        items.add(tGetGrantsToRolesOrUserItem);
                    }
                }
            }
        }
        return items;
    }

    private static String getCatalogName(Long catalogId) {
        String catalogName;
        if (catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        } else {
            Optional<Catalog> catalogOptional = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogById(catalogId);
            if (!catalogOptional.isPresent()) {
                return null;
            }
            Catalog catalog = catalogOptional.get();
            catalogName = catalog.getName();
        }
        return catalogName;
    }
}