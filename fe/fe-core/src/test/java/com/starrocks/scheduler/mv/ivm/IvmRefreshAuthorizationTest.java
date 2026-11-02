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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** An incremental refresh reads its base tables as the MV creator, the way pct's INSERT OVERWRITE does. */
public class IvmRefreshAuthorizationTest extends MVIVMIcebergTestBase {
    private static final AtomicInteger NEXT_USER = new AtomicInteger();
    private static final String MV_NAME = "ivm_refresh_auth_mv";

    private String creatorName;
    private ConnectContext creator;
    private MaterializedView mv;

    @BeforeEach
    public void createCreatorAndMv() throws Exception {
        creatorName = "ivm_refresh_creator_" + NEXT_USER.incrementAndGet();
        executeAsRoot("CREATE USER '" + creatorName + "' IDENTIFIED BY ''");
        executeAsRoot("GRANT USAGE ON CATALOG iceberg0 TO '" + creatorName + "'");
        executeInIcebergCatalogAsRoot("GRANT SELECT ON TABLE unpartitioned_db.t0 TO '" + creatorName + "'");
        creator = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(creatorName, "%"));
        connectContext.setThreadLocalInfo();

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`" + MV_NAME + "` " +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('refresh_mode' = 'incremental')\n" +
                "AS SELECT id, count(*) AS row_count FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id");
        mv = getMv(MV_NAME);
        seedTvrBaselineAtVersionZero(mv);
    }

    @AfterEach
    public void dropCreatorAndMv() throws Exception {
        connectContext.setThreadLocalInfo();
        starRocksAssert.dropMaterializedView(MV_NAME);
        executeAsRoot("DROP USER IF EXISTS '" + creatorName + "'");
    }

    @Test
    public void testCreatorWithSelectRefreshesIncrementally() throws Exception {
        String plan = getMVRefreshExecPlan(buildTaskRunAsCreator()).getExplainString(TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "state_union", "TABLE: " + MV_NAME);
    }

    @Test
    public void testCreatorWithoutSelectIsDenied() throws Exception {
        executeInIcebergCatalogAsRoot("REVOKE SELECT ON TABLE unpartitioned_db.t0 FROM '" + creatorName + "'");
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline());

        assertRefreshDenied(buildTaskRunAsCreator(), "SELECT", "TABLE t0");
        Assertions.assertEquals(baseline, tvrBaseline());
    }

    @Test
    public void testRegrantedCreatorRefreshesAgain() throws Exception {
        executeInIcebergCatalogAsRoot("REVOKE SELECT ON TABLE unpartitioned_db.t0 FROM '" + creatorName + "'");
        assertRefreshDenied(buildTaskRunAsCreator(), "SELECT", "TABLE t0");

        executeInIcebergCatalogAsRoot("GRANT SELECT ON TABLE unpartitioned_db.t0 TO '" + creatorName + "'");
        String plan = getMVRefreshExecPlan(buildTaskRunAsCreator()).getExplainString(TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "state_union");
    }

    @Test
    public void testDroppedCreatorIsDenied() throws Exception {
        TaskRun taskRun = buildTaskRunAsCreator();
        executeAsRoot("DROP USER '" + creatorName + "'");

        assertRefreshDenied(taskRun, "SELECT", "TABLE t0");
    }

    @Test
    public void testRootBasedRefreshIgnoresCreatorPrivileges() throws Exception {
        executeInIcebergCatalogAsRoot("REVOKE SELECT ON TABLE unpartitioned_db.t0 FROM '" + creatorName + "'");
        boolean previous = Config.mv_use_creator_based_authorization;
        Config.mv_use_creator_based_authorization = false;
        try {
            String plan = getMVRefreshExecPlan(buildTaskRunAsCreator()).getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "state_union");
        } finally {
            Config.mv_use_creator_based_authorization = previous;
        }
    }

    @Test
    public void testExternalAccessControllerChecksRefreshedColumns() throws Exception {
        String catalog = MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME;
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController previous = provider.catalogToAccessControl.put(catalog, new ExternalAccessController() {
            @Override
            public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType) {
            }

            @Override
            public void checkDbAction(ConnectContext context, String catalogName, String db,
                                      PrivilegeType privilegeType) {
            }

            @Override
            public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
            }

            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName, String column,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
                if (column.equalsIgnoreCase("id")) {
                    throw new AccessDeniedException();
                }
            }
        });
        try {
            assertRefreshDenied(buildTaskRunAsCreator(), "SELECT", "COLUMN id");
        } finally {
            if (previous == null) {
                provider.catalogToAccessControl.remove(catalog);
            } else {
                provider.catalogToAccessControl.put(catalog, previous);
            }
        }
    }

    private TaskRun buildTaskRunAsCreator() {
        try (var ignored = creator.bindScope()) {
            return buildMVTaskRun(mv, "test");
        }
    }

    private Map<BaseTableInfo, TvrVersionRange> tvrBaseline() {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }

    private static void assertRefreshDenied(TaskRun taskRun, String privilege, String object) {
        Exception e = Assertions.assertThrows(Exception.class, () -> getMVRefreshExecPlan(taskRun));
        String trace = ExceptionUtils.getStackTrace(e);
        Assertions.assertTrue(trace.contains("Access denied"), trace);
        Assertions.assertTrue(trace.contains(privilege), trace);
        Assertions.assertTrue(trace.contains(object), trace);
    }

    private static void executeInIcebergCatalogAsRoot(String sql) throws Exception {
        String originalCatalog = connectContext.getCurrentCatalog();
        connectContext.setCurrentCatalog(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        try {
            executeAsRoot(sql);
        } finally {
            connectContext.setCurrentCatalog(originalCatalog);
        }
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statement, connectContext);
            DDLStmtExecutor.execute(statement, connectContext);
        }
    }
}
