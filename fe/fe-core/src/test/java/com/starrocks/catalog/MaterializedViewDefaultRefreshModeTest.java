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

import com.google.common.collect.Maps;
import com.starrocks.catalog.mv.MVPlanValidationResult;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MaterializedViewDefaultRefreshModeTest extends MVTestBase {
    private static final String MV_NAME = "mv_default_refresh_mode";
    private static final String MV_DDL = "CREATE MATERIALIZED VIEW " + MV_NAME + " PARTITION BY dt "
            + "DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL "
            + "AS SELECT dt, k, SUM(v) AS s FROM t_default_refresh_mode GROUP BY dt, k";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t_default_refresh_mode (dt DATE, k INT, v INT) DUPLICATE KEY(dt) "
                + "PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-02'), "
                + "PARTITION p2 VALUES LESS THAN ('2024-01-03')) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
    }

    @AfterEach
    public void resetDefaultRefreshMode() {
        Config.default_mv_refresh_mode = "pct";
    }

    @Test
    public void testViewCreatedUnderIncrementalDefaultIsPct() throws Exception {
        Config.default_mv_refresh_mode = "incremental";
        starRocksAssert.withMaterializedView(MV_DDL, () -> {
            MaterializedView mv = getMv(DB_NAME, MV_NAME);
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getRefreshMode());
            assertRewritableAndPartitionRefreshable(mv);
        });
    }

    @Test
    public void testConfigChangedAfterCreateDoesNotRelabelView() throws Exception {
        starRocksAssert.withMaterializedView(MV_DDL, () -> {
            Config.default_mv_refresh_mode = "incremental";
            MaterializedView mv = getMv(DB_NAME, MV_NAME);
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getRefreshMode());
            assertRewritableAndPartitionRefreshable(mv);

            starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + MV_NAME + " SET ('refresh_mode' = 'pct')");
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getRefreshMode());
        });
    }

    @Test
    public void testImageLoadDoesNotRelabelView() throws Exception {
        starRocksAssert.withMaterializedView(MV_DDL, () -> {
            Config.default_mv_refresh_mode = "incremental";
            MaterializedView mv = getMv(DB_NAME, MV_NAME);
            mv.getTableProperty().gsonPostProcess();
            assertEquals(MaterializedView.RefreshMode.PCT, mv.getRefreshMode());
            assertRewritableAndPartitionRefreshable(mv);
        });
    }

    @Test
    public void testExplicitRefreshModeIsKept() {
        Config.default_mv_refresh_mode = "incremental";
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE, "auto");
        TableProperty explicit = new TableProperty(properties).buildMvProperties();
        assertEquals("auto", explicit.getMvRefreshMode());

        TableProperty unspecified = new TableProperty(Maps.newHashMap()).buildMvProperties();
        assertTrue("pct".equalsIgnoreCase(unspecified.getMvRefreshMode()), unspecified.getMvRefreshMode());
    }

    private static void assertRewritableAndPartitionRefreshable(MaterializedView mv) throws Exception {
        MVPlanValidationResult result = MvRewritePreprocessor.isMVValidToRewriteQuery(connectContext, mv, null,
                true, false, connectContext.getSessionVariable().getOptimizerExecuteTimeout());
        assertTrue(result.getStatus().isValid(), result.getReason());
        UtFrameUtils.parseStmtWithNewParser("REFRESH MATERIALIZED VIEW " + MV_NAME
                + " PARTITION START ('2024-01-01') END ('2024-01-02')", connectContext);
    }
}
