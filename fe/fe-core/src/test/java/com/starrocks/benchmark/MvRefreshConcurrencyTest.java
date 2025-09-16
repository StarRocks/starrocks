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

package com.starrocks.benchmark;

import com.google.api.client.util.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.schema.MSchema;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;

import java.util.List;
import java.util.Random;

/**
 * This is a test in which:
 * x dbs which contains one table
 * y mvs which contains x/2 tables and uses `union all` to concatenate them
 *
 * refresh mvs with concurrency to test lock and performance
 */
@Disabled
@BenchmarkMode(org.openjdk.jmh.annotations.Mode.AverageTime)
public class MvRefreshConcurrencyTest extends MVTestBase {

    private static String buildDbName(int idx) {
        return "mock_db_" + idx;
    }

    private static String buildTableName(int idx) {
        return "mock_t_" + idx;
    }

    private static String buildMVName(int idx) {
        return "mock_mv_" + idx;
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        // Env
        Config.mv_plan_cache_max_size = 1024;
        CachingMvPlanContextBuilder.getInstance().rebuildCache();
        starRocksAssert.getCtx().setDumpInfo(null);
    }

    @BeforeEach
    public void before() {
    }

    private static String buildMV(Random rnd, List<MTable> tables, int mvIdx) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW test." + buildMVName(mvIdx) + " REFRESH ASYNC" +
                " AS ");

        List<String> sqls = Lists.newArrayList();
        int tableNum = tables.size();
        for (int i = 0; i < tableNum / 2; i++) {
            int idx = rnd.nextInt(tableNum);
            MTable table = tables.get(idx);
            String sql = String.format("select k1, v1, v2 from %s.%s  ", table.getDbName(), buildTableName(idx));
            sqls.add(sql);
        }
        sb.append(String.join(" union all ", sqls));
        return sb.toString();
    }

    private int refreshFinishedMVCount(List<MaterializedView> mvs) {
        int count = 0;
        for (MaterializedView mv : mvs) {
            if (starRocksAssert.waitRefreshFinished(mv.getId())) {
                count += 1;
            }
        }
        return count;
    }

    private void testRefreshWithConcurrency(int tableNum, int mvNum) {
        Random rnd = new Random();

        // Base tables
        MTable mTable = MSchema.getTable("t1");
        List<MTable> tables = Lists.newArrayList();
        for (int i = 0; i < tableNum; i++) {
            MTable copied = mTable.copyWithName(buildTableName(i));
            copied.withDbName(buildDbName(i));
            tables.add(copied);
        }

        starRocksAssert.withMTables(cluster, tables, () -> {
            // create mvs
            List<MaterializedView> mvs = Lists.newArrayList();
            try {
                for (int i = 0; i < mvNum; i++) {
                    logSysInfo("create mv " + i);
                    String sql = buildMV(rnd, tables, i);
                    logSysInfo(sql);
                    starRocksAssert.withMaterializedView(sql);

                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                    String mvName = buildMVName(i);
                    Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), mvName);
                    Assertions.assertTrue(table != null);
                    mvs.add((MaterializedView) table);
                    starRocksAssert.useDatabase("test");
                    starRocksAssert.refreshMV(connectContext, mvName);
                }
                LOG.info("prepared {} materialized views", mvNum);

                // refresh mv with concurrency
                int finishedCount = 0;
                while (finishedCount != mvNum) {
                    finishedCount = refreshFinishedMVCount(mvs);
                }
                Assertions.assertTrue(finishedCount == mvNum);
            } finally {
                starRocksAssert.useDatabase("test");
                for (MaterializedView mv : mvs) {
                    starRocksAssert.dropMaterializedView(mv.getName());
                }
            }
        });
    }

    @Test
    @Disabled
    public void testWithTables2_c4() {
        testRefreshWithConcurrency(4, 2);
    }

    @Test
    @Disabled
    public void testWithTables10_c4() {
        testRefreshWithConcurrency(10, 4);
    }

    @Test
    @Disabled
    public void testWithTables20_c4() {
        testRefreshWithConcurrency(20, 10);
    }

    @Test
    @Disabled
    public void testWithTables50_c16() {
        Config.task_runs_concurrency = 16;
        testRefreshWithConcurrency(50, 50);
        Config.task_runs_concurrency = 4;
    }

    @Test
    @Disabled
    public void testWithTables50_c50() {
        Config.task_runs_concurrency = 50;
        testRefreshWithConcurrency(50, 50);
        Config.task_runs_concurrency = 4;
    }
}
