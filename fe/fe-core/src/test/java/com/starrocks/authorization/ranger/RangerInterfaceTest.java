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
package com.starrocks.authorization.ranger;

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ColumnPrivilege;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.hive.RangerHiveAccessController;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.View;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerInterfaceTest {
    static ConnectContext connectContext;
    static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };

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

        // Create view and materialized view for ColumnPrivilege integration tests
        starRocksAssert.withView("create view db.test_view as select v4, v5 from db.t1");
        starRocksAssert.withMaterializedView("create materialized view db.test_mv " +
                "distributed by hash(v4) " +
                "refresh async START('9999-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\"replication_num\" = \"1\") " +
                "as select v4, v5 from db.t1;");

        // Create a security view used for checkViewPrivilege unit tests
        starRocksAssert.withView("create view db.test_security_view as select v4, v5 from db.t1");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
        Table secTbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_security_view");
        ((View) secTbl).setSecurity(true);

        // Create a security view referencing a materialized view, used for covering the
        // checkViewPrivilege branch where the underlying table is a materialized view.
        starRocksAssert.withView("create view db.test_security_view_mv as select v4, v5 from db.test_mv");
        Table secMvTbl = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_security_view_mv");
        ((View) secMvTbl).setSecurity(true);
    }

    @Test
    public void testMaskingNull() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", IntegerType.INT));

        RangerStarRocksAccessController connectScheduler = new RangerStarRocksAccessController();
        try {
            connectScheduler.getColumnMaskingPolicy(connectContext, tableName, columns);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testAccessControlProvider() {
        AccessControlProvider accessControlProvider = new AccessControlProvider(null,
                new NativeAccessController());
        accessControlProvider.removeAccessControl("hive");

        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.removeAccessControl("hive");

        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);

        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        Assertions.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);
    }

    @Test
    public void testMaskingExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_NULL);
                return result;
            }
        };

        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", IntegerType.INT));

        Map<String, Expr> e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assertions.assertTrue(new ArrayList<>(e.values()).get(0) instanceof NullLiteral);

        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_CUSTOM);
                result.setMaskedValue("v + 1");
                return result;
            }
        };

        e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assertions.assertTrue(new ArrayList<>(e.values()).get(0) instanceof ArithmeticExpr);
    }

    @Test
    public void testRowAccessExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setFilterExpr("v1 = 1");
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");

        Expr rowFilter = rangerStarRocksAccessController.getRowAccessPolicy(connectContext, tableName);
        Assertions.assertTrue(rowFilter instanceof BinaryPredicate);
    }

    @Test
    public void testPermission() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        try {
            rangerStarRocksAccessController.hasPermission(
                    RangerStarRocksResource.builder().setSystem().build(), UserIdentity.ROOT, Set.of(), PrivilegeType.OPERATE);
        } catch (Exception e) {
            Assertions.fail();
        }

        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        Assertions.assertThrows(AccessDeniedException.class, () -> rangerStarRocksAccessController.hasPermission(
                RangerStarRocksResource.builder().setSystem().build(),
                new UserIdentity("alice", "%"), Set.of(), PrivilegeType.OPERATE));
    }

    @Test
    public void testRootUserBypassesRanger() {
        // Mock Ranger to deny all requests
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        // root user must bypass Ranger even when Ranger denies everything
        Assertions.assertDoesNotThrow(() -> controller.hasPermission(
                RangerStarRocksResource.builder().setSystem().build(),
                UserIdentity.ROOT, Set.of(), PrivilegeType.OPERATE));
        Assertions.assertDoesNotThrow(() -> controller.hasPermission(
                RangerStarRocksResource.builder().setCatalog("hive_catalog").build(),
                UserIdentity.ROOT, Set.of(), PrivilegeType.USAGE));
        Assertions.assertDoesNotThrow(() -> controller.hasPermission(
                RangerStarRocksResource.builder().setCatalog("default_catalog").setDatabase("db1").build(),
                UserIdentity.ROOT, Set.of(), PrivilegeType.CREATE_TABLE));
        Assertions.assertDoesNotThrow(() -> controller.hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog("default_catalog").setDatabase("db1").setTable("t1").build(),
                UserIdentity.ROOT, Set.of(), PrivilegeType.SELECT));

        // non-root user must still go through Ranger and be denied
        UserIdentity normalUser = new UserIdentity("alice", "%");
        Assertions.assertThrows(AccessDeniedException.class, () -> controller.hasPermission(
                RangerStarRocksResource.builder().setSystem().build(),
                normalUser, Set.of(), PrivilegeType.OPERATE));
    }

    @Test
    public void testRewriteWithAlias() throws Exception {
        Expr e = SqlParser.parseSqlToExpr("v4+1", new ConnectContext().getSessionVariable().getSqlMode());
        HashMap<String, Expr> exprHashMap = new HashMap<>();
        exprHashMap.put("v4", e);

        try (MockedStatic<Authorizer> authorizerMockedStatic = Mockito.mockStatic(Authorizer.class)) {
            authorizerMockedStatic.when(() -> Authorizer.getColumnMaskingPolicy(Mockito.any(),
                    Mockito.any(), Mockito.any())).thenReturn(exprHashMap);

            StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse("select * from t1 t",
                    connectContext.getSessionVariable().getSqlMode()).get(0);
            //Build View SQL without Policy Rewrite
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(stmt);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);

            QueryStatement queryStatement = (QueryStatement) stmt;
            Assertions.assertTrue(((SelectRelation) queryStatement.getQueryRelation()).getRelation() instanceof SubqueryRelation);
        }
    }

    @Test
    public void testHiveConvertToAccessTypeCreate() {
        RangerHiveAccessController controller = new RangerHiveAccessController("hive-service");
        Assertions.assertEquals("select", controller.convertToAccessType(PrivilegeType.SELECT));
        Assertions.assertEquals("update", controller.convertToAccessType(PrivilegeType.INSERT));
        Assertions.assertEquals("create", controller.convertToAccessType(PrivilegeType.CREATE_TABLE));
        Assertions.assertEquals("create", controller.convertToAccessType(PrivilegeType.CREATE_VIEW));
        Assertions.assertEquals("create", controller.convertToAccessType(PrivilegeType.CREATE_DATABASE));
        Assertions.assertEquals("refresh", controller.convertToAccessType(PrivilegeType.REFRESH));
        Assertions.assertEquals("drop", controller.convertToAccessType(PrivilegeType.DROP));
        Assertions.assertEquals("alter", controller.convertToAccessType(PrivilegeType.ALTER));
    }

    @Test
    public void testColumnPrivilegeCheckViewWithExternalAccessController() throws Exception {
        // Mock Ranger to allow all requests, and track whether Ranger was actually called
        final List<RangerAccessResourceImpl> capturedResources = new ArrayList<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                capturedResources.add((RangerAccessResourceImpl) request.getResource());
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        // Set ExternalAccessController for default_catalog to trigger the branch
        AccessControlProvider provider = Authorizer.getInstance();
        RangerStarRocksAccessController rangerController = new RangerStarRocksAccessController();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, rangerController);

        try {
            // Use non-ROOT user to avoid bypassing Ranger
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("test_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            // Parse and analyze a SELECT on the view
            StatementBase stmt = SqlParser.parse("select * from db.test_view",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            // ColumnPrivilege.check should succeed (Ranger allows)
            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertDoesNotThrow(() ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));

            // Verify Ranger was actually called for the view.
            // Before the fix, Ranger was never invoked (privilege silently bypassed).
            Assertions.assertFalse(capturedResources.isEmpty(),
                    "Ranger must be called for View privilege check; if empty, privilege was silently bypassed");
        } finally {
            // Restore NativeAccessController
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testColumnPrivilegeCheckViewDeniedWithExternalAccessController() throws Exception {
        // Mock Ranger to deny all requests
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        // Set ExternalAccessController for default_catalog
        AccessControlProvider provider = Authorizer.getInstance();
        RangerStarRocksAccessController rangerController = new RangerStarRocksAccessController();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, rangerController);

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            // Parse and analyze a SELECT on the view
            StatementBase stmt = SqlParser.parse("select * from db.test_view",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            // ColumnPrivilege.check should throw ErrorReportException (Ranger denies, reportAccessDenied wraps it)
            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testColumnPrivilegeCheckMaterializedViewWithExternalAccessController() throws Exception {
        // Mock Ranger to allow all requests, and track whether Ranger was actually called
        final List<RangerAccessResourceImpl> capturedResources = new ArrayList<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                capturedResources.add((RangerAccessResourceImpl) request.getResource());
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        // Set ExternalAccessController for default_catalog
        AccessControlProvider provider = Authorizer.getInstance();
        RangerStarRocksAccessController rangerController = new RangerStarRocksAccessController();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, rangerController);

        try {
            // Use non-ROOT user to avoid bypassing Ranger
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("test_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            // Parse and analyze a SELECT on the materialized view
            StatementBase stmt = SqlParser.parse("select * from db.test_mv",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            // ColumnPrivilege.check should succeed (Ranger allows)
            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertDoesNotThrow(() ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));

            // Verify Ranger was actually called for the materialized view.
            // Before the fix, Ranger was never invoked (privilege silently bypassed).
            Assertions.assertFalse(capturedResources.isEmpty(),
                    "Ranger must be called for MV privilege check; if empty, privilege was silently bypassed");
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testColumnPrivilegeCheckMaterializedViewDeniedWithExternalAccessController() throws Exception {
        // Mock Ranger to deny all requests
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        // Set ExternalAccessController for default_catalog
        AccessControlProvider provider = Authorizer.getInstance();
        RangerStarRocksAccessController rangerController = new RangerStarRocksAccessController();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, rangerController);

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            // Parse and analyze a SELECT on the materialized view
            StatementBase stmt = SqlParser.parse("select * from db.test_mv",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            // ColumnPrivilege.check should throw ErrorReportException (Ranger denies, reportAccessDenied wraps it)
            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testCheckViewPrivilegeSecurityViewWithRanger() {
        final List<RangerAccessResourceImpl> capturedResources = new ArrayList<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                capturedResources.add((RangerAccessResourceImpl) request.getResource());
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("test_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
            View secView = (View) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_security_view");
            Assertions.assertTrue(secView.isSecurity(),
                    "test_security_view must be marked as security view in beforeClass");

            TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db", "test_security_view");
            Assertions.assertDoesNotThrow(() ->
                    ColumnPrivilege.checkViewPrivilege(ctx, tableName, secView));

            // For a security view referencing db.t1, Ranger must be called for both the
            // underlying base table t1 and the view itself.
            Assertions.assertFalse(capturedResources.isEmpty(),
                    "Ranger must be called for security view privilege check");
            boolean checkedUnderlyingTable = capturedResources.stream()
                    .anyMatch(r -> "t1".equals(r.getValue("table")));
            boolean checkedView = capturedResources.stream()
                    .anyMatch(r -> "test_security_view".equals(r.getValue("view")));
            Assertions.assertTrue(checkedUnderlyingTable,
                    "Security view must trigger underlying table 't1' privilege check");
            Assertions.assertTrue(checkedView,
                    "Security view must also trigger privilege check on the view itself");
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testColumnPrivilegeCheckTableColumnWithExternalAccessController() throws Exception {
        // Mock Ranger to allow all requests, and track whether Ranger was actually called
        final List<RangerAccessResourceImpl> capturedResources = new ArrayList<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                capturedResources.add((RangerAccessResourceImpl) request.getResource());
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        // Set ExternalAccessController for default_catalog to trigger the column-level branch
        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            // Use non-ROOT user to avoid bypassing Ranger
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("test_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            // Select specific columns from a regular OLAP table to trigger column-level privilege check.
            StatementBase stmt = SqlParser.parse("select v4, v5 from db.t1",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertDoesNotThrow(() ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));

            // Verify Ranger was actually called for column-level access on t1
            Assertions.assertFalse(capturedResources.isEmpty(),
                    "Ranger must be called for column-level privilege check on regular table");
            boolean checkedColumn = capturedResources.stream()
                    .anyMatch(r -> "t1".equals(r.getValue("table"))
                            && r.getValue("column") != null);
            Assertions.assertTrue(checkedColumn,
                    "Ranger must be invoked with column-level resource on table 't1'");
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testColumnPrivilegeCheckTableColumnDeniedWithExternalAccessController() throws Exception {
        // Mock Ranger to deny all requests so column-level check throws AccessDeniedException
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            StatementBase stmt = SqlParser.parse("select v4, v5 from db.t1",
                    ctx.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(stmt, ctx);

            // Column-level check denied -> reportAccessDenied wraps it into ErrorReportException
            QueryStatement queryStmt = (QueryStatement) stmt;
            Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.check(ctx, queryStmt, Collections.emptyList()));
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    @Test
    public void testCheckViewPrivilegeSecurityViewReferencingMv() {
        // Mock Ranger to allow all requests, and track whether Ranger was actually called
        final List<RangerAccessResourceImpl> capturedResources = new ArrayList<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                capturedResources.add((RangerAccessResourceImpl) request.getResource());
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("test_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
            View secView = (View) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_security_view_mv");
            Assertions.assertTrue(secView.isSecurity(),
                    "test_security_view_mv must be marked as security view in beforeClass");

            TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    "db", "test_security_view_mv");
            Assertions.assertDoesNotThrow(() ->

                    ColumnPrivilege.checkViewPrivilege(ctx, tableName, secView));

            // For a security view referencing a materialized view, Ranger must be invoked
            // for both the underlying MV and the view itself.
            // RangerStarRocksResource uses key "materialized_view" for MVs (not "table").
            Assertions.assertFalse(capturedResources.isEmpty(),
                    "Ranger must be called for security view privilege check");
            boolean checkedUnderlyingMv = capturedResources.stream()
                    .anyMatch(r -> "test_mv".equals(r.getValue("materialized_view")));
            boolean checkedView = capturedResources.stream()
                    .anyMatch(r -> "test_security_view_mv".equals(r.getValue("view")));
            Assertions.assertTrue(checkedUnderlyingMv,
                    "Security view must trigger underlying materialized view 'test_mv' privilege check");
            Assertions.assertTrue(checkedView,
                    "Security view must also trigger privilege check on the view itself");
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    /**
     * Regression test for the should-fix item: when a security view's underlying base
     * table is denied, the error must be reported against the base object (TABLE or,
     * since the fix routes security-view refs through column-level checks, COLUMN of
     * the base table) — never as ObjectType.VIEW with the security view's name.
     */
    @Test
    public void testCheckViewPrivilegeSecurityViewBaseTableDeniedReportsAsTable() {
        // Ranger denies every request -> base table t1 access will be denied
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
            View secView = (View) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_security_view");
            Assertions.assertTrue(secView.isSecurity());

            TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    "db", "test_security_view");

            ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.checkViewPrivilege(ctx, tableName, secView));

            // The base failure must surface against the base object (TABLE or COLUMN of
            // the base table) — never as VIEW <security_view>. Under the column-level
            // unification fix, a security view's refs are checked via
            // ColumnPrivilege.check(view.getQueryStatement()), so the first deny lands
            // on a base column (e.g. "on COLUMN v4").
            String msg = ex.getMessage();
            Assertions.assertTrue(msg.contains("on TABLE") || msg.contains("on COLUMN"),
                    "base failure must be reported against the base object (TABLE or COLUMN), got: " + msg);
            Assertions.assertFalse(msg.contains("on VIEW test_security_view"),
                    "base failure must NOT be misreported as VIEW <security_view>, got: " + msg);
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    /**
     * Regression test for the should-fix item: when a security view's underlying
     * materialized view is denied, the error must be reported as
     * ObjectType.MATERIALIZED_VIEW, not ObjectType.VIEW.
     */
    @Test
    public void testCheckViewPrivilegeSecurityViewBaseMvDeniedReportsAsMaterializedView() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
            View secView = (View) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_security_view_mv");
            Assertions.assertTrue(secView.isSecurity());

            TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    "db", "test_security_view_mv");

            ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.checkViewPrivilege(ctx, tableName, secView));

            // ObjectType.MATERIALIZED_VIEW is rendered as "MATERIALIZED VIEW" (with a
            // space, no underscore) in user-facing error messages; see ObjectType.java.
            String msg = ex.getMessage();
            Assertions.assertTrue(msg.contains("on MATERIALIZED VIEW"),
                    "base MV failure must be reported with ObjectType.MATERIALIZED_VIEW, got: " + msg);
            Assertions.assertTrue(msg.contains("test_mv"),
                    "base MV failure must mention the base MV name 'test_mv', got: " + msg);
            Assertions.assertFalse(msg.contains("on VIEW test_security_view_mv"),
                    "base MV failure must NOT be misreported as VIEW <security_view>, got: " + msg);
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    /**
     * Cover the {@code isConnectorView()} branch of checkViewPrivilege without
     * requiring a real Hive/Iceberg/Paimon catalog. We mock View#isConnectorView to
     * return true and verify that:
     *   1) the call routes through the TABLE path (Authorizer.checkTableAction),
     *      consistent with the Hive Ranger service type which models views as TABLE;
     *   2) on deny, the error is reported as ObjectType.TABLE, not VIEW.
     */
    @Test
    public void testCheckViewPrivilegeConnectorViewGoesThroughTablePath() {
        // Ranger denies every request so we can observe how the failure is reported
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        // Force the existing internal view to behave like a connector (Hive/Iceberg/Paimon) view
        new MockUp<View>() {
            @Mock
            public boolean isConnectorView() {
                return true;
            }

            @Mock
            public boolean isSecurity() {
                // Connector view branch returns early; security must not interfere.
                return false;
            }
        };

        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new RangerStarRocksAccessController());

        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setCurrentUserIdentity(new UserIdentity("denied_user", "%"));
            ctx.setDatabase("db");
            ctx.setThreadLocalInfo();

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
            View view = (View) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "test_view");
            Assertions.assertTrue(view.isConnectorView(),
                    "MockUp must make isConnectorView() return true");

            TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    "db", "test_view");

            ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class, () ->
                    ColumnPrivilege.checkViewPrivilege(ctx, tableName, view));

            // Connector view must surface as TABLE (Hive Ranger service type consistency),
            // not as VIEW.
            String msg = ex.getMessage();
            Assertions.assertTrue(msg.contains("on TABLE"),
                    "connector view failure must be reported with ObjectType.TABLE, got: " + msg);
            Assertions.assertTrue(msg.contains("test_view"),
                    "connector view failure must mention the view name 'test_view', got: " + msg);
            Assertions.assertFalse(msg.contains("on VIEW"),
                    "connector view must NOT be reported as ObjectType.VIEW, got: " + msg);
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
        }
    }

    /**
     * Regression test for the silent-drop minor item: when ScanColumnCollector hits a
     * scan node whose Table is not present in tableObjToTableName (which happens after
     * a view/MV is expanded by the optimizer), it must skip silently instead of
     * inserting a {@code null} key into scanColumns or throwing NPE. Privilege for
     * views/MVs is handled separately by checkViewPrivilege / checkMaterializedViewAction.
     */
    @Test
    public void testScanColumnCollectorSkipsNullTableName() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
        Table t1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");

        // Empty mapping — simulates the case where the scan table was expanded from a
        // view/MV and was therefore never recorded by TableNameCollector.
        Map<Table, TableName> emptyTableObjToTableName = new HashMap<>();
        Map<TableName, Set<String>> scanColumns = new HashMap<>();

        ColumnPrivilege.ScanColumnCollector collector =
                new ColumnPrivilege.ScanColumnCollector(emptyTableObjToTableName, scanColumns);

        OptExpression scanExpr = new OptExpression(new LogicalOlapScanOperator(t1));

        Assertions.assertDoesNotThrow(() -> collector.visitLogicalTableScan(scanExpr, null));

        Assertions.assertTrue(scanColumns.isEmpty(),
                "scanColumns must stay empty when tableObjToTableName has no entry; "
                        + "must NOT silently insert a null key. Got: " + scanColumns);
        Assertions.assertFalse(scanColumns.containsKey(null),
                "scanColumns must NOT contain a null key (silent drop guard). Got: " + scanColumns);
    }
}
