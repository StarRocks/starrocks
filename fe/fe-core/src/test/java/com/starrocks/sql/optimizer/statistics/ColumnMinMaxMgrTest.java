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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Drives ColumnMinMaxMgr's real loader against a catalog table, with the [_META_] scan itself mocked: which
// loads run, what they leave in the cache, and when a cached entry is served or re-collected.
public class ColumnMinMaxMgrTest extends PlanTestBase {
    private static final String TABLE = "minmax_mgr_t";
    // 2 partitions x 3 buckets
    private static final int TABLET_NUM = 6;

    private int savedInterval;
    private int savedMaxTablets;
    // Tablet counts done by the min_max_stats_collect_max_tablets check.
    private AtomicInteger tabletChecks;
    // A fresh manager per test, so no test sees another's cache.
    private ColumnMinMaxMgr mgr;
    private MetaScanMock metaScan;
    private AtomicInteger loads;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `" + TABLE + "` (\n" +
                "  `k1` int NULL,\n" +
                "  `v1` int NULL,\n" +
                "  `s1` varchar(10) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`) (\n" +
                "  PARTITION p1 VALUES LESS THAN (\"10\"),\n" +
                "  PARTITION p2 VALUES LESS THAN (\"20\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @BeforeEach
    public void setUp() {
        savedInterval = Config.min_max_stats_collect_interval_sec;
        savedMaxTablets = Config.min_max_stats_collect_max_tablets;
        Config.min_max_stats_collect_interval_sec = 0;
        Config.min_max_stats_collect_max_tablets = 100000;
        mgr = new ColumnMinMaxMgr();
        metaScan = new MetaScanMock();
        loads = new AtomicInteger();
        tabletChecks = new AtomicInteger();
        new MockUp<ColumnMinMaxMgr.CacheLoader>() {
            @Mock
            public CompletableFuture<?> asyncLoad(Invocation invocation, ColumnIdentifier key, Executor executor) {
                loads.incrementAndGet();
                return invocation.proceed();
            }
        };
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            long countMetaScanTablets(Invocation invocation, OlapTable table) {
                tabletChecks.incrementAndGet();
                return invocation.proceed();
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.min_max_stats_collect_interval_sec = savedInterval;
        Config.min_max_stats_collect_max_tablets = savedMaxTablets;
    }

    private static final class MetaScanMock extends MockUp<SimpleExecutor> {
        private final AtomicInteger calls = new AtomicInteger();
        private volatile String[] answer = {null, null};
        private volatile RuntimeException error;

        @Mock
        public List<TResultBatch> executeDQL(Invocation invocation, String sql, ConnectContext context) {
            if (!sql.contains("[_META_]")) {
                return invocation.proceed();
            }
            calls.incrementAndGet();
            if (error != null) {
                throw error;
            }
            String json = "{\"data\":[" + quote(answer[0]) + "," + quote(answer[1]) + "]}";
            TResultBatch batch = new TResultBatch();
            batch.setRows(Lists.newArrayList(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8))));
            return Lists.newArrayList(batch);
        }

        private static String quote(String value) {
            return value == null ? "null" : "\"" + value + "\"";
        }
    }

    private static OlapTable table() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", TABLE);
    }

    private static ColumnIdentifier column(String name) {
        OlapTable table = table();
        return new ColumnIdentifier(table.getId(), table.getColumn(name).getColumnId());
    }

    // What the optimizer rules pass as the requested version.
    private static StatsVersion currentVersion() {
        return new StatsVersion(-1, StatisticUtils.getTableLastUpdateTimestamp(table()));
    }

    // What a load does to the table-level version: a partition's visible version time moves forward.
    private static void simulateLoad() {
        OlapTable table = table();
        long next = StatisticUtils.getTableLastUpdateTimestamp(table) + 1000;
        PhysicalPartition partition = table.getPhysicalPartitions().iterator().next();
        partition.updateVisibleVersion(partition.getVisibleVersion() + 1, next);
    }

    // Waits for the load getStats() has started: getStatsSync() joins the in-flight future of a present key.
    private Optional<IMinMaxStatsMgr.ColumnMinMax> join(ColumnIdentifier id, StatsVersion version) {
        return mgr.getStatsSync(id, version);
    }

    private long cachedEntries() {
        return mgr.estimateCount().get("ColumnMinMax");
    }

    // A5: a NULL result (empty table, all-NULL column) used to be cached as a version-less empty that never
    // expired. It now expires with the table version, while a column that can never be collected stays cached.
    @Test
    public void testNullResultIsRecollectedAfterLoad() {
        ColumnIdentifier v1 = column("v1");
        ColumnIdentifier s1 = column("s1");
        StatsVersion version = currentVersion();

        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertTrue(join(v1, version).isEmpty());
        assertTrue(mgr.getStats(s1, version).isEmpty());
        assertTrue(join(s1, version).isEmpty());
        assertEquals(1, metaScan.calls.get(), "only v1 is scanned, s1 is not a numeric column");
        assertEquals(2, loads.get());

        // Same version: neither is loaded again.
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertTrue(mgr.getStats(s1, version).isEmpty());
        assertEquals(2, loads.get());

        // Data arrives.
        simulateLoad();
        metaScan.answer = new String[] {"1", "15"};
        StatsVersion loaded = currentVersion();
        assertTrue(mgr.getStats(v1, loaded).isEmpty(), "the NULL entry is stale now, a reload is triggered");
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), join(v1, loaded));
        assertEquals(2, metaScan.calls.get());

        // s1 can never be collected, whatever the version.
        assertTrue(mgr.getStats(s1, loaded).isEmpty());
        assertTrue(join(s1, loaded).isEmpty());
        assertEquals(3, loads.get(), "s1 is not loaded again");
    }

    // A5: a failed load used to be cached as a version-less empty, so the column lost the optimization until
    // DDL or eviction. It is not cached any more, and is retried once the throttle allows.
    @Test
    public void testFailedLoadIsNotCached() {
        Config.min_max_stats_collect_interval_sec = 3600;
        ColumnIdentifier v1 = column("v1");
        StatsVersion version = currentVersion();
        metaScan.error = new RuntimeException("injected scan failure");

        assertTrue(mgr.getStats(v1, version).isEmpty());
        await().atMost(10, TimeUnit.SECONDS).until(() -> cachedEntries() == 0);
        assertEquals(1, metaScan.calls.get());

        // Still within the throttle interval: no retry yet.
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(1, metaScan.calls.get());

        // The throttle allows it: retried, and this time it succeeds.
        Config.min_max_stats_collect_interval_sec = 0;
        metaScan.error = null;
        metaScan.answer = new String[] {"1", "15"};
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), join(v1, version));
        assertEquals(2, metaScan.calls.get());
    }

    // A3: a table over min_max_stats_collect_max_tablets is not scanned. The skip is not cached: like a failed
    // load, the table is checked again on the next collection the throttle allows, so its tablets are counted at
    // most once per throttle interval, not once per query.
    @Test
    public void testTableOverTabletLimitIsNotCollected() {
        Config.min_max_stats_collect_interval_sec = 3600;
        Config.min_max_stats_collect_max_tablets = TABLET_NUM - 1;
        metaScan.answer = new String[] {"1", "15"};
        ColumnIdentifier v1 = column("v1");
        StatsVersion version = currentVersion();

        assertTrue(mgr.getStats(v1, version).isEmpty());
        await().atMost(10, TimeUnit.SECONDS).until(() -> loads.get() == 1 && cachedEntries() == 0);
        assertEquals(1, tabletChecks.get());

        // Within the throttle interval the table is not checked again.
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(1, loads.get());
        assertEquals(1, tabletChecks.get());

        // Once the throttle allows, it is checked again, and is still too big.
        Config.min_max_stats_collect_interval_sec = 0;
        simulateLoad();
        assertTrue(mgr.getStats(v1, currentVersion()).isEmpty());
        await().atMost(10, TimeUnit.SECONDS).until(() -> loads.get() == 2 && cachedEntries() == 0);
        assertEquals(2, tabletChecks.get());
        assertEquals(0, metaScan.calls.get());
    }

    // A3: lowering the limit below the table keeps serving the value already cached for the current version,
    // and only stops the next collection.
    @Test
    public void testCachedValueIsServedUntilTableMovesOn() {
        metaScan.answer = new String[] {"1", "15"};
        ColumnIdentifier v1 = column("v1");
        StatsVersion version = currentVersion();
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), join(v1, version));
        assertEquals(1, metaScan.calls.get());

        Config.min_max_stats_collect_max_tablets = TABLET_NUM - 1;
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), mgr.getStats(v1, version));

        simulateLoad();
        StatsVersion loaded = currentVersion();
        assertTrue(mgr.getStats(v1, loaded).isEmpty());
        assertTrue(join(v1, loaded).isEmpty());
        assertEquals(1, metaScan.calls.get(), "the reload after the table moved on is skipped");
    }

    // A3: raising the limit to admit the table, or disabling it with a value <= 0, takes effect on the next
    // collection the throttle allows, without waiting for the table version to move.
    @Test
    public void testRaisingOrDisablingLimitAdmitsTable() {
        metaScan.answer = new String[] {"1", "15"};
        ColumnIdentifier v1 = column("v1");
        StatsVersion version = currentVersion();

        Config.min_max_stats_collect_max_tablets = TABLET_NUM - 1;
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertTrue(join(v1, version).isEmpty());
        assertEquals(0, metaScan.calls.get());

        // A limit equal to the tablet count admits the table: the count is the base index tablets of all
        // partitions, what the [_META_] scan reads.
        Config.min_max_stats_collect_max_tablets = TABLET_NUM;
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), join(v1, version));
        assertEquals(1, metaScan.calls.get());

        // <= 0 disables the limit.
        mgr = new ColumnMinMaxMgr();
        Config.min_max_stats_collect_max_tablets = TABLET_NUM - 1;
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertTrue(join(v1, version).isEmpty());
        assertEquals(1, metaScan.calls.get());
        Config.min_max_stats_collect_max_tablets = 0;
        assertTrue(mgr.getStats(v1, version).isEmpty());
        assertEquals(Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "15")), join(v1, version));
        assertEquals(2, metaScan.calls.get());
    }
}
