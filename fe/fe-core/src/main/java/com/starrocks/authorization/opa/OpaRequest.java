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

import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

class OpaRequest {
    static final String OPERATION_CHECK = "check";
    static final String OPERATION_CHECK_ACTION_IN_DB = "check_action_in_db";
    static final String OPERATION_GET_ROW_FILTERS = "get_row_filters";
    static final String OPERATION_GET_COLUMN_MASK = "get_column_mask";
    static final String OPERATION_GET_COLUMN_MASKS = "get_column_masks";

    @SerializedName("context")
    private final OpaContext context;
    @SerializedName("action")
    private final OpaAction action;

    private OpaRequest(OpaContext context, OpaAction action) {
        this.context = context;
        this.action = action;
    }

    static OpaRequest createCheck(ConnectContext context, PrivilegeType privilegeType, ObjectType objectType,
                                  OpaResource resource) {
        return create(context, OPERATION_CHECK, privilegeType, objectType, resource, null);
    }

    static OpaRequest create(ConnectContext context, String operation, PrivilegeType privilegeType,
                             ObjectType objectType, OpaResource resource) {
        return create(context, operation, privilegeType, objectType, resource, null);
    }

    static OpaRequest createBatchColumnMasks(ConnectContext context, TableName tableName, List<OpaResource> columns) {
        OpaResource table = OpaResource.table(tableName);
        return create(context, OPERATION_GET_COLUMN_MASKS, PrivilegeType.SELECT, ObjectType.COLUMN, table, columns);
    }

    private static OpaRequest create(ConnectContext context, String operation, PrivilegeType privilegeType,
                                     ObjectType objectType, OpaResource resource, List<OpaResource> filterResources) {
        return new OpaRequest(OpaContext.create(context, resource),
                new OpaAction(operation, privilegeType.name(), objectType.name(), resource, filterResources));
    }

    OpaContext getContext() {
        return context;
    }

    OpaAction getAction() {
        return action;
    }
}

class OpaContext {
    @SerializedName("user")
    private final String user;
    @SerializedName("groups")
    private final List<String> groups;
    @SerializedName("host")
    private final String host;
    @SerializedName("queryId")
    private final String queryId;
    @SerializedName("catalog")
    private final String catalog;
    @SerializedName("database")
    private final String database;

    private OpaContext(String user, List<String> groups, String host, String queryId, String catalog, String database) {
        this.user = user;
        this.groups = groups;
        this.host = host;
        this.queryId = queryId;
        this.catalog = catalog;
        this.database = database;
    }

    static OpaContext create(ConnectContext context, OpaResource resource) {
        UserIdentity userIdentity = context.getCurrentUserIdentity();
        String user = userIdentity == null ? context.getQualifiedUser() : userIdentity.getUser();
        String host = userIdentity == null ? null : userIdentity.getHost();
        UUID queryId = context.getQueryId();
        Set<String> userGroups = context.getGroups();
        List<String> groups = userGroups == null ? List.of() :
                userGroups.stream().sorted(Comparator.naturalOrder()).toList();
        String catalog = resource == null || resource.getCatalog() == null ?
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : resource.getCatalog();
        String database = resource == null ? null : resource.getDatabase();
        return new OpaContext(user, groups, host, queryId == null ? null : queryId.toString(), catalog, database);
    }

    String getUser() {
        return user;
    }

    List<String> getGroups() {
        return groups;
    }
}

class OpaAction {
    @SerializedName("operation")
    private final String operation;
    @SerializedName("privilege")
    private final String privilege;
    @SerializedName("objectType")
    private final String objectType;
    @SerializedName("resource")
    private final OpaResource resource;
    @SerializedName("filterResources")
    private final List<OpaResource> filterResources;

    OpaAction(String operation, String privilege, String objectType, OpaResource resource,
              List<OpaResource> filterResources) {
        this.operation = operation;
        this.privilege = privilege;
        this.objectType = objectType;
        this.resource = resource;
        this.filterResources = filterResources;
    }

    String getOperation() {
        return operation;
    }

    String getPrivilege() {
        return privilege;
    }

    String getObjectType() {
        return objectType;
    }

    OpaResource getResource() {
        return resource;
    }

    List<OpaResource> getFilterResources() {
        return filterResources;
    }
}

class OpaResource {
    @SerializedName("system")
    private final String system;
    @SerializedName("catalog")
    private final String catalog;
    @SerializedName("database")
    private final String database;
    @SerializedName("table")
    private final String table;
    @SerializedName("column")
    private final String column;
    @SerializedName("view")
    private final String view;
    @SerializedName("materialized_view")
    private final String materializedView;
    @SerializedName("function")
    private final String function;
    @SerializedName("global_function")
    private final String globalFunction;
    @SerializedName("user")
    private final String user;
    @SerializedName("resource")
    private final String resource;
    @SerializedName("resource_group")
    private final String resourceGroup;
    @SerializedName("storage_volume")
    private final String storageVolume;
    @SerializedName("pipe")
    private final String pipe;
    @SerializedName("warehouse")
    private final String warehouse;

    private OpaResource(String system, String catalog, String database, String table, String column, String view,
                        String materializedView, String function, String globalFunction, String user, String resource,
                        String resourceGroup, String storageVolume, String pipe, String warehouse) {
        this.system = system;
        this.catalog = catalog;
        this.database = database;
        this.table = table;
        this.column = column;
        this.view = view;
        this.materializedView = materializedView;
        this.function = function;
        this.globalFunction = globalFunction;
        this.user = user;
        this.resource = resource;
        this.resourceGroup = resourceGroup;
        this.storageVolume = storageVolume;
        this.pipe = pipe;
        this.warehouse = warehouse;
    }

    static OpaResource system() {
        return new OpaResource("*", null, null, null, null, null, null, null, null, null, null, null, null, null,
                null);
    }

    static OpaResource user(String user) {
        return new OpaResource(null, null, null, null, null, null, null, null, null, user, null, null, null, null, null);
    }

    static OpaResource catalog(String catalog) {
        return new OpaResource(null, defaultCatalog(catalog), null, null, null, null, null, null, null, null, null, null,
                null, null, null);
    }

    static OpaResource database(String catalog, String database) {
        return new OpaResource(null, defaultCatalog(catalog), database, null, null, null, null, null, null, null, null,
                null, null, null, null);
    }

    static OpaResource table(TableName tableName) {
        return new OpaResource(null, defaultCatalog(tableName.getCatalog()), tableName.getDb(), tableName.getTbl(),
                null, null, null, null, null, null, null, null, null, null, null);
    }

    static OpaResource table(String catalog, String database, String table) {
        return new OpaResource(null, defaultCatalog(catalog), database, table, null, null, null, null, null, null, null,
                null, null, null, null);
    }

    static OpaResource column(TableName tableName, String column) {
        return new OpaResource(null, defaultCatalog(tableName.getCatalog()), tableName.getDb(), tableName.getTbl(),
                column, null, null, null, null, null, null, null, null, null, null);
    }

    static OpaResource view(TableName tableName) {
        return new OpaResource(null, defaultCatalog(tableName.getCatalog()), tableName.getDb(), null, null,
                tableName.getTbl(), null, null, null, null, null, null, null, null, null);
    }

    static OpaResource materializedView(TableName tableName) {
        return new OpaResource(null, defaultCatalog(tableName.getCatalog()), tableName.getDb(), null, null, null,
                tableName.getTbl(), null, null, null, null, null, null, null, null);
    }

    static OpaResource function(String catalog, String database, String function) {
        return new OpaResource(null, defaultCatalog(catalog), database, null, null, null, null, function, null, null,
                null, null, null, null, null);
    }

    static OpaResource globalFunction(String globalFunction) {
        return new OpaResource(null, null, null, null, null, null, null, null, globalFunction, null, null, null, null,
                null, null);
    }

    static OpaResource resource(String resource) {
        return new OpaResource(null, null, null, null, null, null, null, null, null, null, resource, null, null, null,
                null);
    }

    static OpaResource resourceGroup(String resourceGroup) {
        return new OpaResource(null, null, null, null, null, null, null, null, null, null, null, resourceGroup, null,
                null, null);
    }

    static OpaResource storageVolume(String storageVolume) {
        return new OpaResource(null, null, null, null, null, null, null, null, null, null, null, null, storageVolume,
                null, null);
    }

    static OpaResource pipe(String database, String pipe) {
        return new OpaResource(null, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database, null, null, null, null,
                null, null, null, null, null, null, pipe, null);
    }

    static OpaResource warehouse(String warehouse) {
        return new OpaResource(null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                warehouse);
    }

    private static String defaultCatalog(String catalog) {
        return catalog == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalog;
    }

    String getCatalog() {
        return catalog;
    }

    String getDatabase() {
        return database;
    }

    String getColumn() {
        return column;
    }

    String getTable() {
        return table;
    }
}
