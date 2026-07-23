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
package com.starrocks.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for ColumnPrivilege.check() to ensure proper privilege checking
 * for different object types with both native and external access controllers.
 */
public class ColumnPrivilegeTest {
    static ConnectContext connectContext;
    static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        
        // Create test database and table
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
        starRocksAssert.withTable("CREATE TABLE `base_table` (\n" +
                "  `k1` bigint NULL,\n" +
                "  `v1` bigint NULL,\n" +
                "  `v2` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        
        // Create a materialized view
        String mvSql = "CREATE MATERIALIZED VIEW test_mv " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3 " +
                "REFRESH DEFERRED MANUAL " +
                "AS SELECT k1, sum(v1) as total_v1 FROM base_table GROUP BY k1";
        CreateMaterializedViewStatement createMvStmt = 
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(mvSql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMvStmt);
        
        // Create a view
        starRocksAssert.withView("CREATE VIEW test_view AS SELECT k1, v1 FROM base_table WHERE k1 > 0");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * Test that when using an external access controller, querying a materialized view
     * calls checkMaterializedViewAction() instead of checkColumnAction().
     * 
     * This test would have caught the bug where MVs were incorrectly subjected to 
     * column-level privilege checks with external access controllers.
     */
    @Test
    public void testMaterializedViewWithExternalAccessController() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setDatabase("test_db");
        
        // Track which check method is called
        AtomicBoolean mvCheckCalled = new AtomicBoolean(false);
        AtomicBoolean columnCheckCalled = new AtomicBoolean(false);
        AtomicReference<String> checkedObjectType = new AtomicReference<>();
        
        // Create a mock external access controller that tracks calls
        ExternalAccessController mockController = new ExternalAccessController() {
            @Override
            public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                                    PrivilegeType privilegeType) throws AccessDeniedException {
                mvCheckCalled.set(true);
                checkedObjectType.set("MATERIALIZED_VIEW");
                // Allow access
            }
            
            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName,
                                          String column, PrivilegeType privilegeType) throws AccessDeniedException {
                columnCheckCalled.set(true);
                checkedObjectType.set("COLUMN:" + column);
                // Allow access
            }
            
            @Override
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
                // Allow access
            }
        };
        
        // Set up the external access controller for the default catalog
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController originalController = provider.getAccessControlOrDefault(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        
        try {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, mockController);
            
            // Parse and analyze a query against the MV
            String sql = "SELECT * FROM test_mv";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            
            // Check privileges - this should call checkMaterializedViewAction, not checkColumnAction
            ColumnPrivilege.check(ctx, (QueryStatement) stmt, Collections.emptyList());
            
            // Verify checkMaterializedViewAction was called, NOT checkColumnAction
            Assertions.assertTrue(mvCheckCalled.get(), 
                    "checkMaterializedViewAction should be called for MV queries with external access controller");
            Assertions.assertFalse(columnCheckCalled.get(), 
                    "checkColumnAction should NOT be called for MV queries - should use object-level check instead");
            Assertions.assertEquals("MATERIALIZED_VIEW", checkedObjectType.get(),
                    "Should check MATERIALIZED_VIEW privileges, not column privileges");
            
        } finally {
            // Restore original controller
            if (originalController != null) {
                provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, originalController);
            } else {
                provider.removeAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }
        }
    }

    /**
     * Test that when using an external access controller, querying a view
     * calls checkViewAction() instead of checkColumnAction().
     */
    @Test
    public void testViewWithExternalAccessController() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setDatabase("test_db");
        
        AtomicBoolean viewCheckCalled = new AtomicBoolean(false);
        AtomicBoolean columnCheckCalled = new AtomicBoolean(false);
        
        ExternalAccessController mockController = new ExternalAccessController() {
            @Override
            public void checkViewAction(ConnectContext context, TableName tableName,
                                        PrivilegeType privilegeType) throws AccessDeniedException {
                viewCheckCalled.set(true);
            }
            
            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName,
                                          String column, PrivilegeType privilegeType) throws AccessDeniedException {
                columnCheckCalled.set(true);
            }
            
            @Override
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
            }
        };
        
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController originalController = provider.getAccessControlOrDefault(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        
        try {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, mockController);
            
            String sql = "SELECT * FROM test_view";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            
            ColumnPrivilege.check(ctx, (QueryStatement) stmt, Collections.emptyList());
            
            Assertions.assertTrue(viewCheckCalled.get(), 
                    "checkViewAction should be called for view queries with external access controller");
            Assertions.assertFalse(columnCheckCalled.get(), 
                    "checkColumnAction should NOT be called for view queries");
            
        } finally {
            if (originalController != null) {
                provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, originalController);
            } else {
                provider.removeAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }
        }
    }

    /**
     * Test that when using an external access controller, querying a regular table
     * still calls checkColumnAction() (the correct behavior for tables).
     */
    @Test
    public void testRegularTableWithExternalAccessController() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setDatabase("test_db");
        
        AtomicBoolean columnCheckCalled = new AtomicBoolean(false);
        AtomicBoolean tableCheckCalled = new AtomicBoolean(false);
        
        ExternalAccessController mockController = new ExternalAccessController() {
            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName,
                                          String column, PrivilegeType privilegeType) throws AccessDeniedException {
                columnCheckCalled.set(true);
            }
            
            @Override
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
                tableCheckCalled.set(true);
            }
        };
        
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController originalController = provider.getAccessControlOrDefault(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        
        try {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, mockController);
            
            String sql = "SELECT k1, v1 FROM base_table";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
            
            ColumnPrivilege.check(ctx, (QueryStatement) stmt, Collections.emptyList());
            
            // For regular tables with external access controller, column-level check should be used
            Assertions.assertTrue(columnCheckCalled.get(), 
                    "checkColumnAction should be called for regular table queries with external access controller");
            
        } finally {
            if (originalController != null) {
                provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, originalController);
            } else {
                provider.removeAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }
        }
    }

    /**
     * Test that with native access controller, MV queries use checkMaterializedViewAction.
     */
    @Test  
    public void testMaterializedViewWithNativeAccessController() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setDatabase("test_db");
        
        // With native controller (default), MV queries should work without errors for ROOT user
        String sql = "SELECT * FROM test_mv";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        
        // Should not throw - ROOT has all privileges
        Assertions.assertDoesNotThrow(() -> 
                ColumnPrivilege.check(ctx, (QueryStatement) stmt, Collections.emptyList()));
    }
}
