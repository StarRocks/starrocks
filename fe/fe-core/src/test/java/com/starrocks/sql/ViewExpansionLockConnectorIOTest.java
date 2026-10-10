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

package com.starrocks.sql;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * The unlocked pre-pass that keeps connector metadata off the lock critical path only sees the relations
 * written in the statement. A view is one opaque name there: the tables it reads only appear once the locked
 * analyzer expands it, so a view defined over an external catalog used to resolve its base tables from the
 * connector with the PlannerMetaLock held. Any statement joining an internal table to such a view hits it,
 * which makes it the more common shape -- not the rarer one -- of the same defect the pre-pass was written
 * for.
 *
 * <p>Samples are OR-accumulated per entry point (one planning run reaches these along several paths, and a
 * plain put would keep only the last, lock-free one) and confined to the test thread, because the asynchronous
 * MV plan cache resolves the same tables on its own executor.
 */
public class ViewExpansionLockConnectorIOTest extends ConnectorPlanTestBase {

    private final Map<String, Boolean> underLock = Maps.newConcurrentMap();
    private volatile Thread testThread;

    private void record(String key) {
        if (Thread.currentThread() != testThread) {
            return;
        }
        underLock.merge(key, LockHoldDepth.isUnderLock(), Boolean::logicalOr);
    }

    private void probeConnectorCalls() {
        testThread = Thread.currentThread();
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(Invocation invocation, ConnectContext context, String catalogName, String dbName,
                                  String tblName) {
                if (!CatalogMgr.isInternalCatalog(catalogName)) {
                    record("getTable:" + catalogName + "." + tblName);
                }
                return invocation.proceed(context, catalogName, dbName, tblName);
            }
        };
    }

    private void assertNothingWentRemoteUnderTheLock(String sql) {
        // Without this the check below passes on an empty map, i.e. whenever the probe never fired.
        Assertions.assertFalse(underLock.isEmpty(),
                "planning never reached the external catalog, the probe proves nothing, for: " + sql);
        underLock.forEach((site, held) -> Assertions.assertFalse(held,
                "an FE metadata lock was held while the view body was resolved, at " + site + ", for: " + sql
                        + "; full samples: " + underLock));
    }

    @AfterEach
    public void dropViews() throws Exception {
        for (String view : List.of("test.v_hive_direct", "test.v_hive_nested", "test.v_hive_inner")) {
            try {
                starRocksAssert.dropView(view);
            } catch (Exception ignored) {
                // created per test; not every test creates every view
            }
        }
    }

    /**
     * The case this test exists for: the view is an internal object, so the statement takes the lock for it
     * and for t0, and the hive table inside the view body used to be resolved with both held.
     */
    @Test
    public void testAViewBodyOverAConnectorIsResolvedBeforeTheLock() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.v_hive_direct AS "
                + "SELECT c_custkey, c_name FROM hive0.tpch.customer");
        probeConnectorCalls();
        String sql = "SELECT t0.v1, v.c_name FROM test.t0 JOIN test.v_hive_direct v ON t0.v1 = v.c_custkey";
        getFragmentPlan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /** A view whose body reads another view that reads the connector: the pre-pass has to recurse. */
    @Test
    public void testANestedViewBodyIsResolvedBeforeTheLock() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.v_hive_inner AS "
                + "SELECT c_custkey, c_name FROM hive0.tpch.customer");
        starRocksAssert.withView("CREATE VIEW test.v_hive_nested AS "
                + "SELECT c_custkey, c_name FROM test.v_hive_inner");
        probeConnectorCalls();
        String sql = "SELECT t0.v1, v.c_name FROM test.t0 JOIN test.v_hive_nested v ON t0.v1 = v.c_custkey";
        getFragmentPlan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /**
     * The same view named twice in one statement. Bodies are handed out once each -- the analyzer rewrites
     * the AST it expands, so two references must not share one -- which means the pre-pass has to have
     * recorded one per reference. If it recorded only one, the second reference falls back to expanding under
     * the lock and this fails.
     */
    @Test
    public void testAViewReferencedTwiceIsResolvedBeforeTheLockBothTimes() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.v_hive_direct AS "
                + "SELECT c_custkey, c_name FROM hive0.tpch.customer");
        probeConnectorCalls();
        String sql = "SELECT a.c_name, b.c_name FROM test.t0 "
                + "JOIN test.v_hive_direct a ON t0.v1 = a.c_custkey "
                + "JOIN test.v_hive_direct b ON t0.v2 = b.c_custkey";
        getFragmentPlan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /**
     * A CTE named like a table inside the view body must not be shadowed by a CTE of the same name in the
     * enclosing statement: the body is its own name scope. Here the outer statement declares {@code customer}
     * as a CTE while the view body reads the real {@code hive0.tpch.customer}; the plan has to keep reading
     * the hive table, and has to reach it before the lock.
     */
    @Test
    public void testAnEnclosingCteDoesNotShadowANameInsideTheViewBody() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.v_hive_direct AS "
                + "SELECT c_custkey, c_name FROM hive0.tpch.customer");
        probeConnectorCalls();
        String sql = "WITH customer AS (SELECT v1 AS c_custkey FROM test.t0) "
                + "SELECT v.c_name FROM customer JOIN test.v_hive_direct v ON customer.c_custkey = v.c_custkey";
        String plan = getFragmentPlan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
        Assertions.assertTrue(plan.contains("HdfsScanNode") || plan.contains("HIVE"),
                "the view body stopped reading the hive table, so the CTE shadowed it: " + plan);
    }

    /**
     * Planning a view must not change because its body was parsed earlier. Same statement, once with the
     * pre-pass on and once with it off.
     */
    @Test
    public void testPreResolvingTheViewBodyPlansTheSameThing() throws Exception {
        starRocksAssert.withView("CREATE VIEW test.v_hive_direct AS "
                + "SELECT c_custkey, c_name FROM hive0.tpch.customer");
        String sql = "SELECT t0.v1, v.c_name FROM test.t0 JOIN test.v_hive_direct v ON t0.v1 = v.c_custkey";
        boolean preparse = com.starrocks.common.Config.enable_experimental_external_table_preparse;
        String withPreparse;
        try {
            com.starrocks.common.Config.enable_experimental_external_table_preparse = true;
            withPreparse = getFragmentPlan(sql);
            com.starrocks.common.Config.enable_experimental_external_table_preparse = false;
            Assertions.assertEquals(withPreparse, getFragmentPlan(sql),
                    "the plan differs depending on whether the view body was pre-resolved");
        } finally {
            com.starrocks.common.Config.enable_experimental_external_table_preparse = preparse;
        }
    }
}
