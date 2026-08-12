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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.rejected.RejectedRecordsTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class OpaAccessControllerTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db").useDatabase("db").withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withUser("alice");
    }

    @Test
    public void testCheckTableActionAllowsAndBuildsRequest() throws Exception {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);

        controller.checkTableAction(context(), new TableName("default_catalog", "db1", "tbl1"), PrivilegeType.SELECT);

        Assertions.assertEquals(1, client.accessRequests.size());
        OpaRequest request = client.accessRequests.get(0);
        Assertions.assertEquals("alice", request.getContext().getUser());
        Assertions.assertEquals(List.of("analytics", "finance"), request.getContext().getGroups());
        Assertions.assertEquals(OpaRequest.OPERATION_CHECK, request.getAction().getOperation());
        Assertions.assertEquals(PrivilegeType.SELECT.name(), request.getAction().getPrivilege());
        Assertions.assertEquals(ObjectType.TABLE.name(), request.getAction().getObjectType());
        Assertions.assertEquals("default_catalog", request.getAction().getResource().getCatalog());
        Assertions.assertEquals("db1", request.getAction().getResource().getDatabase());
        Assertions.assertEquals("tbl1", request.getAction().getResource().getTable());
    }

    @Test
    public void testDeniedThrows() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.allow = false;
        OpaAccessController controller = new OpaAccessController(client);

        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkCatalogAction(
                context(), "default_catalog", PrivilegeType.USAGE));
    }

    @Test
    public void testPermission() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);

        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(context(), PrivilegeType.OPERATE));

        client.allow = false;
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(context(), PrivilegeType.OPERATE));
    }

    @Test
    public void testRootUserDoesNotBypassOpa() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.allow = false;
        OpaAccessController controller = new OpaAccessController(client);
        ConnectContext context = context();
        context.setCurrentUserIdentity(UserIdentity.ROOT);

        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkSystemAction(
                context, PrivilegeType.OPERATE));
        Assertions.assertEquals(1, client.accessRequests.size());
    }

    @Test
    public void testPermissionManagementUsesNativeAccessController() throws Exception {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        AccessController nativeAccessController = Mockito.mock(AccessController.class);
        Mockito.doThrow(new AccessDeniedException()).when(nativeAccessController)
                .checkSystemAction(Mockito.any(), Mockito.eq(PrivilegeType.GRANT));
        OpaAccessController controller = new OpaAccessController(client, nativeAccessController);

        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkSystemAction(
                context(), PrivilegeType.GRANT));
        Assertions.assertTrue(client.accessRequests.isEmpty());
        Mockito.verify(nativeAccessController).checkSystemAction(Mockito.any(), Mockito.eq(PrivilegeType.GRANT));
    }

    @Test
    public void testMaskingNull() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);

        Map<String, Expr> masks = Assertions.assertDoesNotThrow(() -> controller.getColumnMaskingPolicy(context(),
                new TableName("db", "tbl"), Lists.newArrayList(new Column("v1", IntegerType.INT))));

        Assertions.assertTrue(masks.isEmpty());
    }

    @Test
    public void testMaskingExpr() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.columnMasks.put("v1", "NULL");
        OpaAccessController controller = new OpaAccessController(client);

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(context(), new TableName("db", "tbl"),
                Lists.newArrayList(new Column("v1", IntegerType.INT)));
        Assertions.assertTrue(masks.get("v1") instanceof NullLiteral);

        client.columnMasks.put("v1", "v1 + 1");
        masks = controller.getColumnMaskingPolicy(context(), new TableName("db", "tbl"),
                Lists.newArrayList(new Column("v1", IntegerType.INT)));
        Assertions.assertTrue(masks.get("v1") instanceof ArithmeticExpr);
    }

    @Test
    public void testWithGrantOptionUsesNativeAccessController() throws Exception {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        AccessController nativeAccessController = Mockito.mock(AccessController.class);
        OpaAccessController controller = new OpaAccessController(client, nativeAccessController);

        controller.withGrantOption(context(), ObjectType.TABLE, List.of(PrivilegeType.SELECT), List.<PEntryObject>of());

        Assertions.assertTrue(client.accessRequests.isEmpty());
        Mockito.verify(nativeAccessController).withGrantOption(Mockito.any(), Mockito.eq(ObjectType.TABLE),
                Mockito.eq(List.of(PrivilegeType.SELECT)), Mockito.eq(List.<PEntryObject>of()));
    }

    @Test
    public void testRowFiltersAreCombined() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.rowFilters = List.of("v1 = 1", "v2 = 2");
        OpaAccessController controller = new OpaAccessController(client);

        Expr rowFilter = controller.getRowAccessPolicy(context(), new TableName("default_catalog", "db1", "tbl1"));

        Assertions.assertTrue(rowFilter instanceof CompoundPredicate);
        Assertions.assertEquals(OpaRequest.OPERATION_GET_ROW_FILTERS, client.rowFilterRequests.get(0).getAction().getOperation());
    }

    @Test
    public void testRowAccessExpr() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.rowFilters = List.of("v1 = 1");
        OpaAccessController controller = new OpaAccessController(client);

        Expr rowFilter = controller.getRowAccessPolicy(context(), new TableName("db", "tbl"));

        Assertions.assertTrue(rowFilter instanceof BinaryPredicate);
    }

    @Test
    public void testRejectedRecordsRowAccessPolicyPrecedesOpa() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);

        Expr rowFilter = controller.getRowAccessPolicy(context(), new TableName(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                RejectedRecordsTable.DATABASE_NAME,
                RejectedRecordsTable.TABLE_NAME));

        Assertions.assertNotNull(rowFilter);
        Assertions.assertTrue(client.rowFilterRequests.isEmpty());
    }

    @Test
    public void testBatchColumnMasksArePreferred() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.supportBatch = true;
        client.batchMasks.put("v1", "NULL");
        OpaAccessController controller = new OpaAccessController(client);

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(context(),
                new TableName("default_catalog", "db1", "tbl1"),
                Lists.newArrayList(new Column("v1", IntegerType.INT), new Column("v2", IntegerType.INT)));

        Assertions.assertEquals(1, client.batchMaskRequests.size());
        Assertions.assertTrue(client.columnMaskRequests.isEmpty());
        Assertions.assertTrue(masks.get("v1") instanceof NullLiteral);
    }

    @Test
    public void testPerColumnMaskFallback() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.columnMasks.put("v1", "NULL");
        OpaAccessController controller = new OpaAccessController(client);

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(context(),
                new TableName("default_catalog", "db1", "tbl1"),
                Lists.newArrayList(new Column("v1", IntegerType.INT), new Column("v2", IntegerType.INT)));

        Assertions.assertEquals(2, client.columnMaskRequests.size());
        Assertions.assertTrue(masks.get("v1") instanceof NullLiteral);
    }

    @Test
    public void testInvalidPolicyExpressionFailsClosed() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        client.rowFilters = List.of("not a valid expression +");
        OpaAccessController controller = new OpaAccessController(client);

        Assertions.assertThrows(OpaQueryException.class, () -> controller.getRowAccessPolicy(
                context(), new TableName("default_catalog", "db1", "tbl1")));
    }

    @Test
    public void testAccessControlProvider() {
        AccessControlProvider accessControlProvider = new AccessControlProvider(null,
                new NativeAccessController());
        accessControlProvider.removeAccessControl("hive");

        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        accessControlProvider.setAccessControl("hive", new OpaAccessController(new FakeOpaPolicyClient()));
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive") instanceof OpaAccessController);
        accessControlProvider.removeAccessControl("hive");

        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive") instanceof NativeAccessController);

        accessControlProvider.setAccessControl("hive", new OpaAccessController(new FakeOpaPolicyClient()));
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive") instanceof OpaAccessController);
        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive") instanceof NativeAccessController);
    }

    @Test
    public void testAccessControlProviderClosesOpaController() {
        FakeOpaPolicyClient client = new FakeOpaPolicyClient();
        OpaAccessController controller = new OpaAccessController(client);
        AccessControlProvider provider = new AccessControlProvider(null, new NativeAccessController());

        provider.setAccessControl("hive", controller);
        provider.setAccessControl("hive", new NativeAccessController());

        Assertions.assertTrue(client.closed);
    }

    @Test
    public void testRewriteWithAlias() throws Exception {
        Expr mask = SqlParser.parseSqlToExpr("v4 + 1", new ConnectContext().getSessionVariable().getSqlMode());
        Map<String, Expr> columnMasks = Maps.newHashMap();
        columnMasks.put("v4", mask);

        try (MockedStatic<Authorizer> authorizerMockedStatic = Mockito.mockStatic(Authorizer.class)) {
            authorizerMockedStatic.when(() -> Authorizer.getColumnMaskingPolicy(Mockito.any(),
                    Mockito.any(), Mockito.any())).thenReturn(columnMasks);

            StatementBase stmt = SqlParser.parse("select * from t1 t",
                    connectContext.getSessionVariable().getSqlMode()).get(0);
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(stmt);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);

            QueryStatement queryStatement = (QueryStatement) stmt;
            Assertions.assertTrue(((SelectRelation) queryStatement.getQueryRelation()).getRelation()
                    instanceof SubqueryRelation);
        }
    }

    private static ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity("alice", "%"));
        context.setGroups(new HashSet<>(Set.of("finance", "analytics")));
        context.setQueryId(UUIDUtil.genUUID());
        return context;
    }

    private static class FakeOpaPolicyClient implements OpaPolicyClient {
        private boolean allow = true;
        private boolean supportBatch = false;
        private boolean closed = false;
        private List<String> rowFilters = List.of();
        private final Map<String, String> columnMasks = Maps.newHashMap();
        private final Map<String, String> batchMasks = Maps.newHashMap();
        private final List<OpaRequest> accessRequests = Lists.newArrayList();
        private final List<OpaRequest> rowFilterRequests = Lists.newArrayList();
        private final List<OpaRequest> columnMaskRequests = Lists.newArrayList();
        private final List<OpaRequest> batchMaskRequests = Lists.newArrayList();

        @Override
        public boolean checkPermission(OpaRequest request) {
            accessRequests.add(request);
            return allow;
        }

        @Override
        public List<String> getRowFilters(OpaRequest request) {
            rowFilterRequests.add(request);
            return rowFilters;
        }

        @Override
        public Optional<String> getColumnMask(OpaRequest request) {
            columnMaskRequests.add(request);
            return Optional.ofNullable(columnMasks.get(request.getAction().getResource().getColumn()));
        }

        @Override
        public Map<String, String> getBatchColumnMasks(OpaRequest request, List<String> columnNames) {
            batchMaskRequests.add(request);
            return batchMasks;
        }

        @Override
        public boolean supportsBatchColumnMasks() {
            return supportBatch;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
