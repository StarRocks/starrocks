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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Planning a statement that mixes an internal table with an external one asks the external catalog for
 * partition names, statistics and file lists. None of that is protected by the FE metadata lock -- an external
 * table is never in the lock set -- so the lock must not be held while it happens: its hold time would become
 * the external system's latency, and transaction publish takes the same table's WRITE lock with a 1000ms
 * tryLock.
 *
 * <p>The planner already releases the lock before optimizing when every table in the statement can be planned
 * from a private snapshot. What this test pins is the case that does not qualify on its own: an internal table
 * carrying more related MVs than {@code skip_whole_phase_lock_mv_limit}.
 */
public class PlannerLockConnectorIOTest extends ConnectorPlanTestBase {

    private static final String MIXED_QUERY =
            "SELECT t0.v1, l.l_orderkey FROM test.t0 JOIN hive0.partitioned_db.lineitem_par l "
                    + "ON t0.v1 = l.l_orderkey";

    /**
     * Per connector entry point, whether any call to it ran while an FE metadata lock was held.
     *
     * <p>Accumulated with OR rather than overwritten: one planning run calls these several times along
     * different paths, and a plain put would keep only the last sample and go green on a version that does hold
     * the lock for the earlier ones.
     */
    private final Map<String, Boolean> underLock = Maps.newConcurrentMap();

    private void record(String key) {
        underLock.merge(key, LockHoldDepth.isUnderLock(), Boolean::logicalOr);
    }

    /**
     * Mounted on MetadataMgr rather than on the connector: it is the one door all of these go through, which
     * keeps the probe independent of which connector the query happens to use.
     */
    private void probeConnectorCalls() {
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(Invocation invocation, OptimizerContext session, String catalogName,
                                                 Table table, Map<ColumnRefOperator, Column> columns,
                                                 List<PartitionKey> partitionKeys, ScalarOperator predicate,
                                                 long limit, TvrVersionRange versionRange) {
                record("getTableStatistics:" + table.getName());
                return invocation.proceed(session, catalogName, table, columns, partitionKeys, predicate, limit,
                        versionRange);
            }

            @Mock
            public List<String> listPartitionNames(Invocation invocation, String catalogName, String dbName,
                                                  String tableName, ConnectorMetadataRequestContext requestContext) {
                record("listPartitionNames:" + tableName);
                return invocation.proceed(catalogName, dbName, tableName, requestContext);
            }

            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Invocation invocation, Table table, GetRemoteFilesParams params) {
                record("getRemoteFiles:" + table.getName());
                return invocation.proceed(table, params);
            }
        };
    }

    private void assertNothingWentRemoteUnderTheLock() {
        // Without this the check below passes on an empty map, i.e. whenever the probe never fired.
        Assertions.assertFalse(underLock.isEmpty(),
                "planning never reached the external catalog, the probe proves nothing");
        underLock.forEach((site, held) -> Assertions.assertFalse(held,
                "an FE metadata lock was held while planning contacted the external catalog, at " + site
                        + "; full samples: " + underLock));
    }

    /**
     * The case this test exists for. {@code skip_whole_phase_lock_mv_limit} weighs the cost of snapshotting an
     * internal table against the time the lock is held instead, and a table over that limit used to lose the
     * weigh-in unconditionally -- including for a statement whose planning goes to an external catalog, where
     * the other side of the scale is an external round trip rather than CPU.
     */
    @Test
    public void testConnectorMetadataIsNotFetchedUnderTheLockWhenABaseTableIsOverTheMvLimit() throws Exception {
        probeConnectorCalls();
        int limit = Config.skip_whole_phase_lock_mv_limit;
        Config.skip_whole_phase_lock_mv_limit = 0;
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.planner_lock_mv "
                + "DISTRIBUTED BY HASH(v1) BUCKETS 3 REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('replication_num' = '1') AS SELECT v1, v2 FROM test.t0");
        try {
            getFragmentPlan(MIXED_QUERY);
            assertNothingWentRemoteUnderTheLock();
        } finally {
            Config.skip_whole_phase_lock_mv_limit = limit;
            starRocksAssert.dropMaterializedView("test.planner_lock_mv");
        }
    }

    /** The same statement with nothing pushing it over the limit, so the probe itself is known to work. */
    @Test
    public void testConnectorMetadataIsNotFetchedUnderTheLockByDefault() throws Exception {
        probeConnectorCalls();
        getFragmentPlan(MIXED_QUERY);
        assertNothingWentRemoteUnderTheLock();
    }

    /**
     * The same property across the statement shapes a mixed query actually takes in the wild, and across more
     * than one connector. A join is the easy case; a connector table reached through a subquery, a CTE, a set
     * operation or a view is the one where a walk that gives up early, or an optimizer path that resolves the
     * external side at a different moment, would put the round trip back under the lock.
     */
    @Test
    public void testConnectorMetadataStaysOutOfTheLockForEveryMixedShape() throws Exception {
        probeConnectorCalls();
        int limit = Config.skip_whole_phase_lock_mv_limit;
        Config.skip_whole_phase_lock_mv_limit = 0;
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.planner_lock_shapes_mv "
                + "DISTRIBUTED BY HASH(v1) BUCKETS 3 REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('replication_num' = '1') AS SELECT v1, v2 FROM test.t0");
        try {
            for (String sql : List.of(
                    // IN subquery
                    "SELECT * FROM test.t0 WHERE v1 IN "
                            + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)",
                    // CTE
                    "WITH c AS (SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par) "
                            + "SELECT * FROM test.t0 JOIN c ON test.t0.v1 = c.l_orderkey",
                    // set operation
                    "SELECT v1 FROM test.t0 UNION ALL "
                            + "SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par",
                    // aggregation over the join, so statistics matter to the plan that comes out
                    "SELECT t0.v1, count(*) FROM test.t0 JOIN hive0.partitioned_db.lineitem_par l "
                            + "ON t0.v1 = l.l_orderkey GROUP BY t0.v1",
                    // a second connector, and two of them in one statement
                    "SELECT * FROM test.t0 JOIN jdbc0.partitioned_db0.tbl0 ON true",
                    "SELECT * FROM test.t0 JOIN hive0.partitioned_db.lineitem_par ON true "
                            + "JOIN jdbc0.partitioned_db0.tbl0 ON true")) {
                underLock.clear();
                getFragmentPlan(sql);
                Assertions.assertFalse(underLock.isEmpty(),
                        "planning never reached the external catalog for: " + sql);
                underLock.forEach((site, held) -> Assertions.assertFalse(held,
                        "an FE metadata lock was held while planning contacted the external catalog, at " + site
                                + ", for: " + sql));
            }
        } finally {
            Config.skip_whole_phase_lock_mv_limit = limit;
            starRocksAssert.dropMaterializedView("test.planner_lock_shapes_mv");
        }
    }

    /**
     * Going lock-free must not change what comes out. The same mixed statement is planned with the base table
     * over the limit and then under it; the two plans have to match, or this change bought a shorter lock by
     * quietly planning something else.
     */
    @Test
    public void testTheLockFreePathPlansTheSameThing() throws Exception {
        String overTheLimit;
        int limit = Config.skip_whole_phase_lock_mv_limit;
        Config.skip_whole_phase_lock_mv_limit = 0;
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.planner_lock_plan_mv "
                + "DISTRIBUTED BY HASH(v1) BUCKETS 3 REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('replication_num' = '1') AS SELECT v1, v2 FROM test.t0");
        try {
            overTheLimit = getFragmentPlan(MIXED_QUERY);
        } finally {
            Config.skip_whole_phase_lock_mv_limit = limit;
        }
        try {
            Assertions.assertEquals(getFragmentPlan(MIXED_QUERY), overTheLimit,
                    "the plan differs depending on whether the base table was over the MV limit");
        } finally {
            starRocksAssert.dropMaterializedView("test.planner_lock_plan_mv");
        }
    }

    /**
     * {@code cbo_use_lock_db} is deliberately left alone. It is an INVISIBLE session variable whose only
     * purpose is to force the locked planning path -- the escape hatch to reach for when the optimistic path
     * itself is the suspect -- so honouring it is the point, and planning lock-free anyway would quietly take
     * the switch away. Pinned here so the exclusion stays a decision rather than an oversight: a change that
     * makes the switch stop forcing the lock has to come here and say so.
     */
    @Test
    public void testCboUseDbLockStillForcesTheLockedPath() throws Exception {
        probeConnectorCalls();
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            getFragmentPlan(MIXED_QUERY);
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(false);
        }
        Assertions.assertFalse(underLock.isEmpty(), "planning never reached the external catalog");
        Assertions.assertTrue(underLock.values().stream().anyMatch(Boolean::booleanValue),
                "cbo_use_lock_db no longer forces the whole planning phase under the lock: " + underLock);
    }
}
