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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CREATE MATERIALIZED VIEW reloads the new mv as the last step of the creation, and that reload resolves every
 * base table -- a connector round trip for an external one, and a recursive reload for a hierarchical mv. It
 * used to run inside the database write lock that the creation takes, so one slow external catalog held that
 * database's lock for the whole round trip while every query and every transaction publish on it waited.
 *
 * <p>The drop side is here for the same reason rather than in a test of its own: rolling back a creation that
 * has already been journaled <em>is</em> a drop, and it runs precisely when the external catalog is the thing
 * that just failed.
 */
public class MVCreateLockTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    /**
     * Pinned on the property -- no FE metadata lock is held while the new mv resolves its base tables -- rather
     * than on the call's position in the source, so it keeps holding if the code moves.
     *
     * <p>The samples are accumulated with OR, not overwritten: the same base table is resolved several times
     * during one creation, and a plain put would keep only the last one and go green on a version that does
     * hold the lock for the earlier ones.
     */
    @Test
    public void testExternalBaseTableIsResolvedOutsideTheLock() throws Exception {
        Map<String, Boolean> resolvedUnderLock = Maps.newConcurrentMap();
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(Invocation invocation, ConnectContext context,
                                                          BaseTableInfo baseTableInfo) {
                resolvedUnderLock.merge(baseTableInfo.getTableName(), LockHoldDepth.isUnderLock(),
                        Boolean::logicalOr);
                return invocation.proceed(context, baseTableInfo);
            }
        };

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.create_lock_external_mv\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT l_orderkey, l_suppkey FROM hive0.partitioned_db.lineitem_par;");
        try {
            // Without this the assertion below passes on an absent key, i.e. whenever the probe never fired.
            Assertions.assertTrue(resolvedUnderLock.containsKey("lineitem_par"),
                    "the external base table was never resolved, the probe proves nothing: " + resolvedUnderLock);
            Assertions.assertEquals(Boolean.FALSE, resolvedUnderLock.get("lineitem_par"),
                    "an external base table must not be resolved while a metadata lock is held");
        } finally {
            starRocksAssert.dropMaterializedView("test.create_lock_external_mv");
        }
    }

    /**
     * The consequence, measured instead of inferred: with a connector made deliberately slow, no interval
     * during which a database lock was held may overlap an interval spent inside that connector.
     *
     * <p>A unit-test connector answers instantly, so lock-held time in this suite is otherwise always about
     * zero and a critical section wrapped around a remote call looks exactly like one that is not -- which is
     * why "we never measured the hold time" was the honest state of this area. Injecting the latency is what
     * makes the hold time mean something.
     *
     * <p>Stated as an overlap rather than as "held for less than N ms" on purpose: a threshold in
     * milliseconds is a statement about how fast the machine running the test is, and is the usual way a test
     * like this becomes a flake. Overlap is the same property and does not depend on the machine at all.
     */
    @Test
    public void testNoDatabaseLockIsHeldAcrossASlowConnectorCall() throws Exception {
        final long connectorDelayMs = 300;
        List<long[]> connectorCalls = Lists.newArrayList();
        List<long[]> dbLockHolds = Lists.newArrayList();
        Deque<Long> lockStarts = new ArrayDeque<>();
        final long testThreadId = Thread.currentThread().getId();

        new MockUp<Locker>() {
            @Mock
            public void lockDatabase(Invocation invocation, Long dbId, LockType lockType) {
                invocation.proceed(dbId, lockType);
                if (Thread.currentThread().getId() == testThreadId) {
                    lockStarts.push(System.currentTimeMillis());
                }
            }

            @Mock
            public void unLockDatabase(Invocation invocation, Long dbId, LockType lockType) {
                if (Thread.currentThread().getId() == testThreadId && !lockStarts.isEmpty()) {
                    dbLockHolds.add(new long[] {lockStarts.pop(), System.currentTimeMillis()});
                }
                invocation.proceed(dbId, lockType);
            }
        };

        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(Invocation invocation, ConnectContext context,
                                                          BaseTableInfo baseTableInfo) {
                if (!baseTableInfo.isInternalCatalog() && Thread.currentThread().getId() == testThreadId) {
                    long start = System.currentTimeMillis();
                    try {
                        Thread.sleep(connectorDelayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    connectorCalls.add(new long[] {start, System.currentTimeMillis()});
                }
                return invocation.proceed(context, baseTableInfo);
            }
        };

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.create_lock_slow_mv\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT l_orderkey, l_suppkey FROM hive0.partitioned_db.lineitem_par;");
        try {
            // Both guards matter: with no slow call, or with no lock taken at all, the overlap check below
            // is vacuously true.
            Assertions.assertFalse(connectorCalls.isEmpty(), "the connector was never called, nothing was measured");
            Assertions.assertFalse(dbLockHolds.isEmpty(), "no database lock was taken, nothing was measured");

            for (long[] hold : dbLockHolds) {
                for (long[] call : connectorCalls) {
                    Assertions.assertFalse(hold[0] < call[1] && call[0] < hold[1],
                            "a database lock was held from " + hold[0] + " to " + hold[1]
                                    + ", across a connector call from " + call[0] + " to " + call[1]);
                }
            }
        } finally {
            starRocksAssert.dropMaterializedView("test.create_lock_slow_mv");
        }
    }

    /**
     * Dropping an mv runs under the database write lock at every caller, and its cleanup used to resolve every
     * base table -- through the connector for an external one -- only to strip the relationship off the
     * resolved object. For an external base table that object is not where the relationship lives:
     * ConnectorTblMetaInfoMgr holds it and MetadataMgr#getTable re-applies it onto whatever instance the
     * connector cache returns, so the resolve was a remote call bought for nothing.
     *
     * <p>It matters most on the path this change added: rolling back a creation whose reload failed, where
     * the catalog being contacted is by definition the one that just failed.
     */
    @Test
    public void testDropDoesNotResolveExternalBaseTables() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.drop_lock_external_mv\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT l_orderkey, l_suppkey FROM hive0.partitioned_db.lineitem_par;");

        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "drop_lock_external_mv");
        Assertions.assertNotNull(mv);
        // Without this the count below is zero for the wrong reason.
        Assertions.assertTrue(mv.getBaseTableInfos().stream().anyMatch(info -> !info.isInternalCatalog()),
                "the mv has no external base table, the count proves nothing");

        AtomicInteger externalResolves = new AtomicInteger();
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(Invocation invocation, ConnectContext context,
                                                          BaseTableInfo baseTableInfo) {
                if (!baseTableInfo.isInternalCatalog()) {
                    externalResolves.incrementAndGet();
                }
                return invocation.proceed(context, baseTableInfo);
            }
        };

        starRocksAssert.dropMaterializedView("test.drop_lock_external_mv");

        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "drop_lock_external_mv"), "the mv should be gone");
        Assertions.assertEquals(0, externalResolves.get(),
                "dropping an mv must not resolve its external base tables through the connector");
    }
}
