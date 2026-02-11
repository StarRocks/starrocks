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
package com.starrocks.statistic.virtual;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class VirtualStatisticPropagationTest extends PlanTestBase {
    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        ConnectContext.get().getSessionVariable().setEnableUnnestVirtualStatistics(true);

        starRocksAssert.withTable("create table arr_table(k INT, arr array<VARCHAR(128)>)" +
                "duplicate key(k) distributed by hash(k) buckets 1 " +
                "properties('replication_num'='1');");

        starRocksAssert.withTable("create table join_partner(k VARCHAR(128))" +
                "duplicate key(k) distributed by hash(k) buckets 1 " +
                "properties('replication_num'='1');");

        final var statisticsStorage = new EmptyStatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                if (table.getName().equalsIgnoreCase("arr_table") && column.equalsIgnoreCase("arr")) {
                    return ColumnStatistic.builder() //
                            .setNullsFraction(0.0) //
                            .build();
                }

                if (table.getName().equalsIgnoreCase("arr_table") &&
                        column.equalsIgnoreCase(VirtualStatistic.UNNEST.getVirtualColumnName("arr"))) {
                    return ColumnStatistic.builder() //
                            .setNullsFraction(0.987) //
                            .build();
                }

                return ColumnStatistic.builder() //
                        .setNullsFraction(0.1) //
                        .build();
            }
        };
        setTableStatistics((OlapTable) connectContext.getGlobalStateMgr().getLocalMetastore()
                .getDb("test").getTable("arr_table"), 1337);
        connectContext.getGlobalStateMgr().setStatisticStorage(statisticsStorage);
    }

    @AfterAll
    public static void afterClass() {
        try {
            ConnectContext.get().getSessionVariable().setEnableUnnestVirtualStatistics(false);
            starRocksAssert.dropTable("arr_table");
            starRocksAssert.dropTable("join_table");
        } catch (Exception e) {
            // ignore exceptions.
        }
        PlanTestBase.afterClass();
    }

    @Test
    void itShouldUseVirtualUnnestStatsToRewriteTheJoin() throws Exception {
        // GIVEN
        String sql = "select unnest from arr_table arr_table JOIN lateral unnest(arr) AS tarr(unnest) " +
                "LEFT JOIN join_partner jp ON tarr.unnest = jp.k;";
        try {
            ConnectContext.get().getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);
            // WHEN
            String plan = getCostExplain(sql);
            // THEN
            assertNotNull(plan);
            assertContains(plan, "rand_col"); // JOIN rewrite should be triggered.
        } finally {
            ConnectContext.get().getSessionVariable().setEnableStatsToOptimizeSkewJoin(false);
        }
    }

    @Test
    void itShouldPropagateUnnestStatsAfterScan() throws Exception {
        // GIVEN
        String sql = "select unnest from arr_table arr_table, unnest(arr) AS tarr(unnest)";
        // WHEN
        String plan = getCostExplain(sql);
        // THEN
        assertNotNull(plan);
        assertContains(plan, "  1:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [VARCHAR(128)]\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * unnest-->[-Infinity, Infinity, 0.987, NaN, NaN] ESTIMATE"); // <-- unnest has correct stats.
    }

    @Test
    void itShouldNotPropagateUnnestStatsAfterScanIfDisabled() throws Exception {
        // GIVEN
        String sql = "select unnest from arr_table arr_table, unnest(arr) AS tarr(unnest)";
        try {
            ConnectContext.get().getSessionVariable().setEnableUnnestVirtualStatistics(false);
            // WHEN
            String plan = getCostExplain(sql);
            // THEN
            assertNotNull(plan);
            assertContains(plan, "  1:TableValueFunction\n" +
                    "  |  tableFunctionName: unnest\n" +
                    "  |  columns: [unnest]\n" +
                    "  |  returnTypes: [VARCHAR(128)]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  column statistics: \n" +
                    "  |  * unnest-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"); // <-- unnest has unknown stats.
        } finally {
            ConnectContext.get().getSessionVariable().setEnableUnnestVirtualStatistics(true);
        }
    }
}

