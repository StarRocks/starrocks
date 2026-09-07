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

import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The JSON fixtures below are literals rather than output of the current writer, so these
 * assertions still describe an older release once bare ASYNC stops being accepted as input.
 */
public class MaterializedViewRefreshSchemeUpgradeTest {
    private static final String LEGACY_BARE_ASYNC_JSON =
            "{\"moment\":\"IMMEDIATE\",\"type\":\"ASYNC\",\"asyncRefreshContext\":{"
                    + "\"defineStartTime\":false,\"starTime\":0,\"step\":0},\"lastRefreshTime\":0}";

    private static final String LEGACY_SCHEDULED_ASYNC_JSON =
            "{\"moment\":\"IMMEDIATE\",\"type\":\"ASYNC\",\"asyncRefreshContext\":{"
                    + "\"defineStartTime\":false,\"starTime\":0,\"step\":1,\"timeUnit\":\"HOUR\"},"
                    + "\"lastRefreshTime\":0}";

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(ctx);
        Config.default_mv_refresh_immediate = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.upgrade_base\n"
                        + "(\n"
                        + "    k1 date,\n"
                        + "    k2 int,\n"
                        + "    v1 int\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`k1`, `k2`)\n"
                        + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                        + "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void testLegacyBareAsyncDeserializesAsLoadTriggered() {
        MaterializedView.MvRefreshScheme scheme =
                GsonUtils.GSON.fromJson(LEGACY_BARE_ASYNC_JSON, MaterializedView.MvRefreshScheme.class);

        Assertions.assertEquals(MaterializedViewRefreshType.ASYNC, scheme.getType());
        Assertions.assertNull(scheme.getAsyncRefreshContext().getTimeUnit());
        Assertions.assertEquals(0, scheme.getAsyncRefreshContext().getStep());
    }

    @Test
    public void testLegacyScheduledAsyncDeserializesUnchanged() {
        MaterializedView.MvRefreshScheme scheme =
                GsonUtils.GSON.fromJson(LEGACY_SCHEDULED_ASYNC_JSON, MaterializedView.MvRefreshScheme.class);

        Assertions.assertEquals(MaterializedViewRefreshType.ASYNC, scheme.getType());
        Assertions.assertEquals("HOUR", scheme.getAsyncRefreshContext().getTimeUnit());
        Assertions.assertEquals(1, scheme.getAsyncRefreshContext().getStep());
    }

    @Test
    public void testLegacyBareAsyncMvRendersOnChangeAndStaysReParseable() throws Exception {
        final String mvName = "mv_legacy_bare_async";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName
                + " DISTRIBUTED BY HASH(`k2`) BUCKETS 3"
                + " REFRESH ON_CHANGE"
                + " AS SELECT k1, k2 FROM test.upgrade_base");
        MaterializedView mv = starRocksAssert.getMv("test", mvName);
        mv.setRefreshScheme(GsonUtils.GSON.fromJson(
                LEGACY_BARE_ASYNC_JSON, MaterializedView.MvRefreshScheme.class));

        Assertions.assertTrue(mv.isLoadTriggeredRefresh());
        Assertions.assertEquals("ON_BASE_TABLE_CHANGE", mv.getRefreshTriggerString());

        String ddl = mv.getMaterializedViewDdlStmt(false);
        Assertions.assertTrue(ddl.contains("REFRESH ON_CHANGE"), ddl);
        Assertions.assertFalse(ddl.contains("REFRESH ASYNC"), ddl);
        Assertions.assertDoesNotThrow(() -> SqlParser.parse(ddl, ctx.getSessionVariable()),
                "AlterJobMgr.recreateMVQuery re-parses this DDL when activating the view");

        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testLegacyScheduledAsyncMvStillRendersSchedule() throws Exception {
        final String mvName = "mv_legacy_scheduled_async";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName
                + " DISTRIBUTED BY HASH(`k2`) BUCKETS 3"
                + " REFRESH SCHEDULE EVERY(INTERVAL 1 HOUR)"
                + " AS SELECT k1, k2 FROM test.upgrade_base");
        MaterializedView mv = starRocksAssert.getMv("test", mvName);
        mv.setRefreshScheme(GsonUtils.GSON.fromJson(
                LEGACY_SCHEDULED_ASYNC_JSON, MaterializedView.MvRefreshScheme.class));

        Assertions.assertFalse(mv.isLoadTriggeredRefresh());
        Assertions.assertEquals("SCHEDULED", mv.getRefreshTriggerString());

        String ddl = mv.getMaterializedViewDdlStmt(false);
        Assertions.assertTrue(ddl.contains("REFRESH SCHEDULE"), ddl);
        Assertions.assertFalse(ddl.contains("REFRESH ASYNC"), ddl);
        Assertions.assertDoesNotThrow(() -> SqlParser.parse(ddl, ctx.getSessionVariable()));

        starRocksAssert.dropMaterializedView(mvName);
    }
}
