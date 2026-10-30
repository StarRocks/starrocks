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

import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TPC-DS 1T plans while some tables' column statistics are still UNKNOWN (e.g. not yet collected by
// query-triggered analyze); row counts stay known.
public class TPCDS1TUnknownColumnStatsTest extends TPCDS1TTestBase {
    private static class UnknownColumnStorage extends MockTPCDSStatisticStorage {
        private final Set<String> unknownTables;

        UnknownColumnStorage(Set<String> unknownTables) {
            this.unknownTables = unknownTables;
        }

        @Override
        public ColumnStatistic getColumnStatistic(Table table, String columnName) {
            if (unknownTables.contains(table.getName())) {
                return ColumnStatistic.unknown();
            }
            return super.getColumnStatistic(table, columnName);
        }
    }

    @Test
    public void testQ72CatalogSalesUnknown() throws Exception {
        GlobalStateMgr.getCurrentState().setStatisticStorage(new UnknownColumnStorage(Set.of("catalog_sales")));
        String plan = getFragmentPlan(Q72);
        // catalog_sales is filtered by the dimension tables first, then joins inventory on item and week together,
        // instead of an item-only catalog_sales x inventory join at the bottom.
        assertContains(plan, "join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 36: inv_item_sk = 1: cs_item_sk\n" +
                "  |  equal join conjunct: 121: d_week_seq = 93: d_week_seq");
    }

    // the cardinality of the first hash join in a cost explain
    private static long joinCardinality(String plan) {
        Matcher matcher = Pattern.compile("\\d+:HASH JOIN\n(?:  \\|  .*\n)*?  \\|  cardinality: (\\d+)\n")
                .matcher(plan);
        Assertions.assertTrue(matcher.find(), plan);
        return Long.parseLong(matcher.group(1));
    }

    // item's columns are unknown, so the filter on i_category is a guess; the key borrowed from ss_item_sk must not
    // stack on that guess: the join keeps the unknown-statistics estimate, the larger side's rows
    @Test
    public void testUnknownDimensionWithFilter() throws Exception {
        GlobalStateMgr.getCurrentState().setStatisticStorage(new UnknownColumnStorage(Set.of("item")));
        String plan = getCostExplain("select count(*) from store_sales join item on ss_item_sk = i_item_sk " +
                "where i_category = 'Music'");
        Assertions.assertEquals(2879987999L, joinCardinality(plan), plan);
    }

    // a VALUES key has no column statistics: borrowing i_item_sk's NDV would estimate 3 rows, below the
    // unknown-statistics estimate, which is kept as the floor
    @Test
    public void testValuesAsJoinKey() throws Exception {
        String plan = getCostExplain("select * from item join (values (1), (2), (3)) t(k) on i_item_sk = t.k");
        Assertions.assertEquals(300000L, joinCardinality(plan), plan);
    }
}
