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

package com.starrocks.sql.plan;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

// Uses the real CacheDictManager (the mock one answers true for every table), so the dict lookup has to
// resolve the database of the scanned table on its own.
public class LowCardinalityCrossDbViewTest extends PlanTestBase {
    private static ColumnIdentifier seededColumn;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("lc_view_base_db").useDatabase("lc_view_base_db")
                .withTable("CREATE TABLE lc_view_base_db.t1 (k1 int, c1 varchar(20)) " +
                        "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.withDatabase("lc_view_db").useDatabase("lc_view_db")
                .withView("CREATE VIEW lc_view_db.v1 AS SELECT k1, c1 FROM lc_view_base_db.t1");
        starRocksAssert.useDatabase("test");

        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);

        OlapTable t1 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("lc_view_base_db", "t1");
        seededColumn = new ColumnIdentifier(t1.getId(), t1.getColumn("c1").getColumnId());
        ColumnDict dict = new ColumnDict(
                ImmutableMap.of(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)), 1), Long.MAX_VALUE);
        dictCache().put(seededColumn, CompletableFuture.completedFuture(Optional.of(dict)));
    }

    @AfterAll
    public static void afterClass() {
        dictCache().synchronous().invalidate(seededColumn);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    private static AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>> dictCache() {
        return Deencapsulation.getField(CacheDictManager.getInstance(), "dictStatistics");
    }

    @Test
    public void testViewBaseTableInAnotherDbUsesGlobalDict() throws Exception {
        String hint = "/*+ SET_VAR(always_collect_low_card_dict=true) */";
        // Baseline: the statement names the base table's database itself.
        String plan = getFragmentPlan("SELECT " + hint + " c1, count(*) FROM lc_view_base_db.t1 GROUP BY c1");
        assertContains(plan, "Decode");

        // The statement names only the view's database; the base table's database must still be found.
        plan = getFragmentPlan("SELECT " + hint + " c1, count(*) FROM lc_view_db.v1 GROUP BY c1");
        assertContains(plan, "Decode");
    }
}
