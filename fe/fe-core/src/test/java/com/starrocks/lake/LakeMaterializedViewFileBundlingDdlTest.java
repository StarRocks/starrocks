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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SHOW CREATE MATERIALIZED VIEW prints the DDL that rebuilds a shared-data MV, and ALTER ... ACTIVE re-analyzes it.
 * The MV's file_bundling has to survive the rebuild whichever way it is set and whatever the cluster default is.
 */
public class LakeMaterializedViewFileBundlingDdlTest {
    private static final String DB = "db_for_lake_mv_file_bundling_ddl";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private boolean savedEnableFileBundling;

    @BeforeAll
    public static void setUpClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        starRocksAssert.withTable("CREATE TABLE base_table (k1 INT, k2 INT, v BIGINT) DUPLICATE KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES('replication_num' = '1')");
    }

    @BeforeEach
    public void setUp() {
        savedEnableFileBundling = Config.enable_file_bundling;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_file_bundling = savedEnableFileBundling;
    }

    private static MaterializedView getMv(String name) {
        return (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static MaterializedView createMv(String name, String sql) throws Exception {
        starRocksAssert.withMaterializedView(sql);
        return getMv(name);
    }

    private static MaterializedView createMv(String name, boolean fileBundling) throws Exception {
        return createMv(name, "CREATE MATERIALIZED VIEW " + name + " DISTRIBUTED BY HASH(k1) BUCKETS 2 "
                + "REFRESH DEFERRED MANUAL PROPERTIES('" + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING + "' = '"
                + fileBundling + "') AS SELECT k1, SUM(v) AS total FROM base_table GROUP BY k1");
    }

    private static MaterializedView rebuild(MaterializedView mv, String name) throws Exception {
        String ddl = ddlOf(mv);
        return createMv(name, ddl.replace("`" + mv.getName() + "`", "`" + name + "`"));
    }

    private static String ddlOf(MaterializedView mv) {
        List<String> ddl = Lists.newArrayList();
        AstToStringBuilder.getDdlStmt(mv, ddl, null, null, false, true /* hide password */);
        return ddl.get(0);
    }

    private static void assertPrintedOnce(String ddl, boolean fileBundling) {
        String key = "\"" + PropertyAnalyzer.PROPERTIES_FILE_BUNDLING + "\"";
        assertEquals(ddl.indexOf(key), ddl.lastIndexOf(key), ddl);
        assertTrue(ddl.contains(key + " = \"" + fileBundling + "\""), ddl);
    }

    @Test
    public void testShowCreateRebuildsTheSameValue() throws Exception {
        for (boolean fileBundling : new boolean[] {false, true}) {
            // The value a dropped property would fall back to is the opposite of the MV's.
            Config.enable_file_bundling = !fileBundling;
            String name = "mv_ddl_" + fileBundling;
            MaterializedView mv = createMv(name, fileBundling);
            assertEquals(fileBundling, mv.isFileBundling());
            assertPrintedOnce(ddlOf(mv), fileBundling);

            MaterializedView rebuilt = rebuild(mv, name + "_rebuilt");

            assertEquals(fileBundling, rebuilt.isFileBundling());
        }
    }

    @Test
    public void testMvWithoutThePropertyIsRebuiltAsFalse() throws Exception {
        Config.enable_file_bundling = true;
        MaterializedView mv = createMv("mv_no_entry", false);
        // An MV created before file_bundling applied to materialized views has no entry for it, and reads as false.
        mv.getTableProperty().getProperties().remove(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING);
        assertFalse(mv.isFileBundling());
        assertPrintedOnce(ddlOf(mv), false);

        MaterializedView rebuilt = rebuild(mv, "mv_no_entry_rebuilt");

        assertFalse(rebuilt.isFileBundling());
    }

    @Test
    public void testActivateMvWithoutTheProperty() throws Exception {
        MaterializedView mv = createMv("mv_activate", false);
        mv.getTableProperty().getProperties().remove(PropertyAnalyzer.PROPERTIES_FILE_BUNDLING);
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_activate INACTIVE");
        assertFalse(mv.isActive());

        // ACTIVE re-analyzes the DDL, which now carries the property this MV has no entry for.
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv_activate ACTIVE");

        assertTrue(mv.isActive(), mv.getInactiveReason());
        assertFalse(mv.isFileBundling());
    }
}
