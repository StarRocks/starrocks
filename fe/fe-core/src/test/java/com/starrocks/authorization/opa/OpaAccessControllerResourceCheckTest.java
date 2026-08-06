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

import com.google.gson.Gson;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OpaAccessControllerResourceCheckTest {
    private static final Gson GSON = new Gson();

    @Test
    public void testIdentityCatalogAndDatabaseChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();

        controller.checkUserAction(context, new UserIdentity("bob", "%"), PrivilegeType.IMPERSONATE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.IMPERSONATE,
                ObjectType.USER, OpaResource.user("bob")));
        controller.checkAnyActionOnCatalog(context, "analytics");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.CATALOG, OpaResource.catalog("analytics")));
        controller.checkDbAction(context, "analytics", "sales", PrivilegeType.CREATE_TABLE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.CREATE_TABLE,
                ObjectType.DATABASE, OpaResource.database("analytics", "sales")));
        controller.checkAnyActionOnDb(context, "analytics", "sales");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.DATABASE, OpaResource.database("analytics", "sales")));
        controller.checkActionInDb(context, "sales", PrivilegeType.SELECT);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK_ACTION_IN_DB, PrivilegeType.SELECT,
                ObjectType.DATABASE, OpaResource.database(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "sales")));
        controller.close();
    }

    @Test
    public void testTableViewAndMaterializedViewChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        TableName table = new TableName("analytics", "sales", "orders");
        TableName internalView = new TableName(null, "sales", "recent_orders");

        controller.checkAnyActionOnTable(context, table);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.TABLE, OpaResource.table(table)));
        controller.checkAnyActionOnAnyTable(context, "analytics", "sales");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.TABLE, OpaResource.table("analytics", "sales", "*")));
        controller.checkColumnAction(context, table, "order_id", PrivilegeType.SELECT);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.SELECT,
                ObjectType.COLUMN, OpaResource.column(table, "order_id")));
        controller.checkViewAction(context, internalView, PrivilegeType.SELECT);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.SELECT,
                ObjectType.VIEW, OpaResource.view(internalTable("sales", "recent_orders"))));
        controller.checkAnyActionOnView(context, internalView);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.VIEW, OpaResource.view(internalTable("sales", "recent_orders"))));
        controller.checkAnyActionOnAnyView(context, "sales");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.VIEW, OpaResource.view(internalTable("sales", "*"))));
        controller.checkMaterializedViewAction(context, internalView, PrivilegeType.REFRESH);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.REFRESH,
                ObjectType.MATERIALIZED_VIEW, OpaResource.materializedView(internalTable("sales", "recent_orders"))));
        controller.checkAnyActionOnMaterializedView(context, internalView);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.MATERIALIZED_VIEW, OpaResource.materializedView(internalTable("sales", "recent_orders"))));
        controller.checkAnyActionOnAnyMaterializedView(context, "sales");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.MATERIALIZED_VIEW, OpaResource.materializedView(internalTable("sales", "*"))));
        controller.close();
    }

    @Test
    public void testNamedResourceChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        PipeName pipe = new PipeName("sales", "load_orders");

        controller.checkResourceAction(context, "spark", PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.RESOURCE, OpaResource.resource("spark")));
        controller.checkAnyActionOnResource(context, "spark");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.RESOURCE, OpaResource.resource("spark")));
        controller.checkResourceGroupAction(context, "etl", PrivilegeType.ALTER);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ALTER,
                ObjectType.RESOURCE_GROUP, OpaResource.resourceGroup("etl")));
        controller.checkPipeAction(context, pipe, PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.PIPE, OpaResource.pipe("sales", "load_orders")));
        controller.checkAnyActionOnPipe(context, pipe);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.PIPE, OpaResource.pipe("sales", "load_orders")));
        controller.checkStorageVolumeAction(context, "s3", PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.STORAGE_VOLUME, OpaResource.storageVolume("s3")));
        controller.checkAnyActionOnStorageVolume(context, "s3");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.STORAGE_VOLUME, OpaResource.storageVolume("s3")));
        controller.checkWarehouseAction(context, "compute", PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.WAREHOUSE, OpaResource.warehouse("compute")));
        controller.checkAnyActionOnWarehouse(context, "compute");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.WAREHOUSE, OpaResource.warehouse("compute")));
        controller.close();
    }

    @Test
    public void testFunctionChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        Function function = new Function(new FunctionName("normalize"), new Type[] {IntegerType.INT},
                IntegerType.INT, false);

        controller.checkFunctionAction(context, new Database(1, "sales"), function, PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.FUNCTION, OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "sales",
                function.getSignature())));
        controller.checkAnyActionOnFunction(context, "sales", function);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.FUNCTION, OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "sales",
                function.getSignature())));
        controller.checkAnyActionOnAnyFunction(context, "sales");
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.FUNCTION, OpaResource.function(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "sales", "*")));
        controller.checkGlobalFunctionAction(context, function, PrivilegeType.USAGE);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.USAGE,
                ObjectType.GLOBAL_FUNCTION, OpaResource.globalFunction(function.getSignature())));
        controller.checkAnyActionOnGlobalFunction(context, function);
        assertLastAction(client, new ExpectedAction(OpaRequest.OPERATION_CHECK, PrivilegeType.ANY,
                ObjectType.GLOBAL_FUNCTION, OpaResource.globalFunction(function.getSignature())));
        controller.close();
    }

    private static void assertLastAction(RecordingOpaPolicyClient client, ExpectedAction expected) {
        OpaAction actual = client.requests.get(client.requests.size() - 1).getAction();
        Assertions.assertEquals(expected.operation(), actual.getOperation());
        Assertions.assertEquals(expected.privilege().name(), actual.getPrivilege());
        Assertions.assertEquals(expected.objectType().name(), actual.getObjectType());
        Assertions.assertEquals(GSON.toJsonTree(expected.resource()), GSON.toJsonTree(actual.getResource()));
    }

    private static TableName internalTable(String database, String table) {
        return new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, database, table);
    }

    private static ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity("alice", "%"));
        context.setQueryId(UUIDUtil.genUUID());
        return context;
    }

    private static class RecordingOpaPolicyClient implements OpaPolicyClient {
        private final List<OpaRequest> requests = new ArrayList<>();

        @Override
        public boolean checkPermission(OpaRequest request) {
            requests.add(request);
            return true;
        }

        @Override
        public List<String> getRowFilters(OpaRequest request) {
            return List.of();
        }

        @Override
        public Optional<String> getColumnMask(OpaRequest request) {
            return Optional.empty();
        }

        @Override
        public Map<String, String> getBatchColumnMasks(OpaRequest request, List<String> columnNames) {
            return Map.of();
        }

        @Override
        public boolean supportsBatchColumnMasks() {
            return false;
        }
    }

    private record ExpectedAction(String operation, PrivilegeType privilege, ObjectType objectType,
                                  OpaResource resource) {
    }
}
