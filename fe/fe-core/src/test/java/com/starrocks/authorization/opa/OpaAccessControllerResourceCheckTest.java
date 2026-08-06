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

import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.pipe.PipeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OpaAccessControllerResourceCheckTest {
    @Test
    public void testIdentityCatalogAndDatabaseChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();

        controller.checkUserAction(context, new UserIdentity("bob", "%"), PrivilegeType.IMPERSONATE);
        controller.checkAnyActionOnCatalog(context, "analytics");
        controller.checkDbAction(context, "analytics", "sales", PrivilegeType.CREATE_TABLE);
        controller.checkAnyActionOnDb(context, "analytics", "sales");
        controller.checkActionInDb(context, "sales", PrivilegeType.SELECT);

        Assertions.assertEquals(List.of(ObjectType.USER.name(), ObjectType.CATALOG.name(), ObjectType.DATABASE.name(),
                        ObjectType.DATABASE.name(), ObjectType.DATABASE.name()),
                objectTypes(client.requests));
        Assertions.assertEquals(OpaRequest.OPERATION_CHECK_ACTION_IN_DB,
                client.requests.get(4).getAction().getOperation());
        Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                client.requests.get(4).getAction().getResource().getCatalog());
    }

    @Test
    public void testTableViewAndMaterializedViewChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        TableName table = new TableName("analytics", "sales", "orders");
        TableName internalView = new TableName(null, "sales", "recent_orders");

        controller.checkAnyActionOnTable(context, table);
        controller.checkAnyActionOnAnyTable(context, "analytics", "sales");
        controller.checkColumnAction(context, table, "order_id", PrivilegeType.SELECT);
        controller.checkViewAction(context, internalView, PrivilegeType.SELECT);
        controller.checkAnyActionOnView(context, internalView);
        controller.checkAnyActionOnAnyView(context, "sales");
        controller.checkMaterializedViewAction(context, internalView, PrivilegeType.REFRESH);
        controller.checkAnyActionOnMaterializedView(context, internalView);
        controller.checkAnyActionOnAnyMaterializedView(context, "sales");

        Assertions.assertEquals(List.of(ObjectType.TABLE.name(), ObjectType.TABLE.name(), ObjectType.COLUMN.name(),
                        ObjectType.VIEW.name(), ObjectType.VIEW.name(), ObjectType.VIEW.name(),
                        ObjectType.MATERIALIZED_VIEW.name(), ObjectType.MATERIALIZED_VIEW.name(),
                        ObjectType.MATERIALIZED_VIEW.name()),
                objectTypes(client.requests));
        Assertions.assertEquals("*", client.requests.get(1).getAction().getResource().getTable());
        Assertions.assertEquals("order_id", client.requests.get(2).getAction().getResource().getColumn());
        Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                client.requests.get(3).getAction().getResource().getCatalog());
    }

    @Test
    public void testNamedResourceChecksBuildExpectedRequests() throws Exception {
        RecordingOpaPolicyClient client = new RecordingOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        PipeName pipe = new PipeName("sales", "load_orders");

        controller.checkResourceAction(context, "spark", PrivilegeType.USAGE);
        controller.checkAnyActionOnResource(context, "spark");
        controller.checkResourceGroupAction(context, "etl", PrivilegeType.ALTER);
        controller.checkPipeAction(context, pipe, PrivilegeType.USAGE);
        controller.checkAnyActionOnPipe(context, pipe);
        controller.checkStorageVolumeAction(context, "s3", PrivilegeType.USAGE);
        controller.checkAnyActionOnStorageVolume(context, "s3");
        controller.checkWarehouseAction(context, "compute", PrivilegeType.USAGE);
        controller.checkAnyActionOnWarehouse(context, "compute");

        Assertions.assertEquals(List.of(ObjectType.RESOURCE.name(), ObjectType.RESOURCE.name(),
                        ObjectType.RESOURCE_GROUP.name(), ObjectType.PIPE.name(), ObjectType.PIPE.name(),
                        ObjectType.STORAGE_VOLUME.name(), ObjectType.STORAGE_VOLUME.name(), ObjectType.WAREHOUSE.name(),
                        ObjectType.WAREHOUSE.name()),
                objectTypes(client.requests));
        Assertions.assertEquals("sales", client.requests.get(3).getAction().getResource().getDatabase());
        Assertions.assertEquals(PrivilegeType.ANY.name(), client.requests.get(8).getAction().getPrivilege());
    }

    private static List<String> objectTypes(List<OpaRequest> requests) {
        return requests.stream().map(request -> request.getAction().getObjectType()).toList();
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
}
