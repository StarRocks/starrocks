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

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.hive.RangerHiveAccessController;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalAnalyzeAuthorizationTest {
    private static ConnectContext root;
    private static ConnectContext user;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ConnectorPlanTestBase.mockHiveCatalog(root);
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.getCatalogMgr().createCatalog("iceberg", "iceberg0", "", Map.of(
                "type", "iceberg", "iceberg.catalog.type", "hive", "hive.metastore.uris", "thrift://127.0.0.1:9083"));
        ((MockedMetadataMgr) state.getMetadataMgr()).registerMockedMetadata("iceberg0", new MockIcebergMetadata());
        starRocksAssert = new StarRocksAssert(root);
        ddl("CREATE USER external_stats_user");
        ddl("GRANT USAGE ON CATALOG hive0 TO external_stats_user");
        ddl("GRANT USAGE ON CATALOG iceberg0 TO external_stats_user");
        user = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("external_stats_user", "%"));
    }

    private static void ddl(String sql) throws Exception {
        root.setThreadLocalInfo();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, root), root);
    }

    private static void grant(String action, String table) throws Exception {
        changePrivilege("GRANT " + action, table, " TO external_stats_user");
    }

    private static void revoke(String action, String table) throws Exception {
        changePrivilege("REVOKE " + action, table, " FROM external_stats_user");
    }

    private static void changePrivilege(String action, String table, String grantee) throws Exception {
        int separator = table.indexOf('.');
        String previous = root.getCurrentCatalog();
        root.setCurrentCatalog(table.substring(0, separator));
        try {
            ddl(action + " ON TABLE " + table.substring(separator + 1) + grantee);
        } finally {
            root.setCurrentCatalog(previous);
        }
    }

    private static void check(String sql) throws Exception {
        user.setThreadLocalInfo();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, user);
        Authorizer.check(statement, user);
    }

    private static void denied(String sql, String privilege) {
        RuntimeException error = Assertions.assertThrows(RuntimeException.class, () -> check(sql), sql);
        Assertions.assertTrue(error.getMessage().contains(privilege), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("Access denied"), error.getMessage());
    }

    private static void checkGrantLifecycle(String table, String column) throws Exception {
        List<String> statements = List.of("ANALYZE TABLE " + table,
                "CREATE ANALYZE TABLE " + table,
                "DROP STATS " + table,
                "ANALYZE TABLE " + table + " DROP HISTOGRAM ON " + column);
        for (String sql : statements) {
            denied(sql, "SELECT");
        }
        grant("SELECT", table);
        try {
            for (String sql : statements) {
                denied(sql, "INSERT");
            }
            grant("INSERT", table);
            try {
                for (String sql : statements) {
                    check(sql);
                }
                revoke("SELECT", table);
                try {
                    for (String sql : statements) {
                        denied(sql, "SELECT");
                    }
                } finally {
                    grant("SELECT", table);
                }
            } finally {
                revoke("INSERT", table);
            }
        } finally {
            revoke("SELECT", table);
        }
    }

    @Test
    public void testExternalPrivilegesWithoutNativeNamesake() throws Exception {
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("tpch"));
        checkGrantLifecycle("hive0.tpch.customer", "c_custkey");
        checkGrantLifecycle("iceberg0.unpartitioned_db.t0", "id");
    }

    @Test
    public void testNativeNamesakePrivilegesDoNotAuthorizeExternalTable() throws Exception {
        root.setThreadLocalInfo();
        starRocksAssert.withDatabase("tpch");
        starRocksAssert.withTable("CREATE TABLE tpch.customer (id INT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        try {
            grant("SELECT, INSERT", "default_catalog.tpch.customer");
            checkGrantLifecycle("hive0.tpch.customer", "c_custkey");
            revoke("SELECT, INSERT", "default_catalog.tpch.customer");
            // External rights must also work when the native namesake exists but is not authorized.
            checkGrantLifecycle("hive0.tpch.customer", "c_custkey");
        } finally {
            root.setThreadLocalInfo();
            starRocksAssert.dropDatabase("tpch");
        }
    }

    @Test
    public void testKillExternalAnalyzeStatusChecksBothPrivileges() throws Exception {
        String table = "hive0.tpch.customer";
        long id = 99112233L;
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.getAnalyzeMgr().addAnalyzeStatus(new ExternalAnalyzeStatus(id, "hive0", "tpch", "customer",
                "customer", List.of(), StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE,
                Map.of(), LocalDateTime.now()));
        StmtExecutor executor = new StmtExecutor(user, new KillAnalyzeStmt(id));
        Runnable check = () -> executor.checkPrivilegeForKillAnalyzeStmt(user, id);
        try {
            Assertions.assertThrows(RuntimeException.class, check::run);
            grant("SELECT", table);
            try {
                Assertions.assertThrows(RuntimeException.class, check::run);
                grant("INSERT", table);
                try {
                    Assertions.assertDoesNotThrow(check::run);
                } finally {
                    revoke("INSERT", table);
                }
            } finally {
                revoke("SELECT", table);
            }
        } finally {
            state.getAnalyzeMgr().getAnalyzeStatusMap().remove(id);
        }
    }

    @Test
    public void testResolvedTableUsesRegisteredCatalogController() throws Exception {
        String catalog = "MixedCaseIceberg";
        AccessController controller = Mockito.mock(AccessController.class);
        Authorizer.getInstance().setAccessControl(catalog, controller);
        boolean old = GlobalVariable.enableTableNameCaseInsensitive;
        try {
            GlobalVariable.enableTableNameCaseInsensitive = true;
            TableName name = new TableName(catalog, "db", "t");
            Assertions.assertNotEquals(catalog, name.getCatalog());
            Table table = Mockito.mock(Table.class);
            Mockito.when(table.getCatalogName()).thenReturn(catalog);
            Mockito.when(table.isTable()).thenReturn(true);
            Authorizer.checkResolvedTableAction(user, name, table, PrivilegeType.SELECT);
            Authorizer.checkResolvedTableAction(user, name, table, PrivilegeType.INSERT);
            Mockito.verify(controller).checkTableAction(user, name, PrivilegeType.SELECT);
            Mockito.verify(controller).checkTableAction(user, name, PrivilegeType.INSERT);
            Mockito.doThrow(new AccessDeniedException()).when(controller)
                    .checkTableAction(user, name, PrivilegeType.INSERT);
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkResolvedTableAction(user, name, table, PrivilegeType.INSERT));
        } finally {
            GlobalVariable.enableTableNameCaseInsensitive = old;
            Authorizer.getInstance().removeAccessControl(catalog);
        }
    }

    @Test
    public void testAnalyzeUsesRangerPolicyAndBackgroundRootStillWorks() throws Exception {
        Set<String> allowed = new HashSet<>();
        new MockUp<RangerBasePlugin>() {
            @Mock
            public void init() {
            }

            @Mock
            public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = Mockito.mock(RangerAccessResult.class);
                Mockito.when(result.getIsAllowed()).thenReturn(allowed.contains(request.getAccessType()));
                return result;
            }
        };
        AccessController previous = Authorizer.getInstance().catalogToAccessControl.get("hive0");
        Authorizer.getInstance().setAccessControl("hive0", new RangerHiveAccessController("stats-test"));
        try {
            denied("ANALYZE TABLE hive0.tpch.customer", "SELECT");
            allowed.add("select");
            denied("ANALYZE TABLE hive0.tpch.customer", "INSERT");
            allowed.add("update"); // Hive Ranger maps INSERT to update.
            check("ANALYZE TABLE hive0.tpch.customer");
            allowed.clear();
            ConnectContext stats = StatisticUtils.buildConnectContext();
            Assertions.assertDoesNotThrow(() -> Authorizer.checkActionForAnalyzeStatement(stats,
                    new TableName("hive0", "tpch", "customer")));
        } finally {
            if (previous == null) {
                Authorizer.getInstance().removeAccessControl("hive0");
            } else {
                Authorizer.getInstance().setAccessControl("hive0", previous);
            }
        }
    }

    @Test
    public void testAutomaticCollectionContextRetainsAccess() {
        ConnectContext stats = StatisticUtils.buildConnectContext();
        Assertions.assertEquals(UserIdentity.ROOT, stats.getCurrentUserIdentity());
        Assertions.assertDoesNotThrow(() -> Authorizer.checkActionForAnalyzeStatement(stats,
                new TableName("hive0", "tpch", "customer")));
        Assertions.assertDoesNotThrow(() -> Authorizer.checkActionForAnalyzeStatement(stats,
                new TableName("iceberg0", "unpartitioned_db", "t0")));
    }
}
